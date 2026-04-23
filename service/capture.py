"""Stream capture: streamlink → ffmpeg rolling buffer per live streamer.

Uses ffmpeg's built-in segment muxer to keep the last N minutes on disk with
no manual cleanup: old segments get overwritten as new ones arrive. Clip
extraction (step 4) reads from these segments directly.

Layout on disk:
    /mnt/clipper-storage/clipper/buffers/<streamer>/
        seg_000.ts
        seg_001.ts
        ...
        seg_019.ts   # wraps after this, back to seg_000.ts
"""
from __future__ import annotations

import asyncio
import logging
import shutil
import signal
from pathlib import Path
from typing import Dict

from .config import Config

log = logging.getLogger(__name__)


class CaptureSession:
    """One live streamer → one streamlink-ffmpeg pipeline."""

    def __init__(self, streamer: str, stream_id: str, cfg: Config):
        self.streamer = streamer
        self.stream_id = stream_id
        self.cfg = cfg
        self.buffer_dir = cfg.data_dir / "buffers" / streamer
        self.log_path = cfg.log_dir / f"capture_{streamer}.log"
        self._proc: asyncio.subprocess.Process | None = None
        self._stderr_task: asyncio.Task | None = None

    @property
    def segment_seconds(self) -> int:
        return 30

    @property
    def segment_count(self) -> int:
        # 10-minute buffer at 30s per segment = 20 segments
        return max(6, (self.cfg.buffer_max_minutes * 60) // self.segment_seconds)

    async def start(self) -> None:
        self.buffer_dir.mkdir(parents=True, exist_ok=True)
        # Wipe any stale segments from a previous run so the rolling counter starts fresh.
        for p in self.buffer_dir.glob("seg_*.ts"):
            try:
                p.unlink()
            except Exception:
                pass

        seg_pattern = str(self.buffer_dir / "seg_%03d.ts")
        # streamlink URL; 720p60 balances quality vs bandwidth; `best` if 720p60 unavailable.
        cmd = (
            "streamlink "
            f"https://www.twitch.tv/{self.streamer} 720p60,720p,best "
            "--twitch-disable-ads "
            "--stream-segment-threads 2 "
            "--hls-live-restart "
            "--stdout "
            "2>/dev/null "
            "| ffmpeg -hide_banner -loglevel warning -fflags nobuffer "
            "-i - -c copy -f segment "
            f"-segment_time {self.segment_seconds} "
            f"-segment_wrap {self.segment_count} "
            "-reset_timestamps 1 "
            f'"{seg_pattern}"'
        )

        log.info("capture[%s]: starting (%d x %ds segments in %s)",
                 self.streamer, self.segment_count, self.segment_seconds, self.buffer_dir)

        # Open capture log for persistent stderr recording.
        log_file = open(self.log_path, "ab", buffering=0)
        self._proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=log_file,
        )
        # Keep reference to the file so it's not GC'd while process runs.
        self._log_file = log_file
        log.info("capture[%s]: pid=%s", self.streamer, self._proc.pid)

    async def stop(self) -> None:
        if not self._proc:
            return
        log.info("capture[%s]: stopping pid=%s", self.streamer, self._proc.pid)
        try:
            self._proc.send_signal(signal.SIGINT)
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                self._proc.kill()
                await self._proc.wait()
        except ProcessLookupError:
            pass
        finally:
            self._proc = None
            try:
                self._log_file.close()
            except Exception:
                pass


class CaptureManager:
    """Registers on Monitor.on_live / on_offline hooks. Owns active sessions."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.sessions: Dict[str, CaptureSession] = {}
        self._require_binaries()

    def _require_binaries(self) -> None:
        for b in ("streamlink", "ffmpeg"):
            if not shutil.which(b):
                raise RuntimeError(f"Required binary missing from PATH: {b}")

    async def on_live(self, login: str, stream_id: str, stream_meta: dict) -> None:
        if login in self.sessions:
            log.warning("capture[%s]: already running, ignoring on_live", login)
            return
        session = CaptureSession(login, stream_id, self.cfg)
        try:
            await session.start()
            self.sessions[login] = session
        except Exception:
            log.exception("capture[%s]: failed to start", login)

    async def on_offline(self, login: str, stream_id: str) -> None:
        session = self.sessions.pop(login, None)
        if session:
            await session.stop()

    async def shutdown(self) -> None:
        for login, session in list(self.sessions.items()):
            try:
                await session.stop()
            except Exception:
                log.exception("capture[%s]: stop failed on shutdown", login)
        self.sessions.clear()
