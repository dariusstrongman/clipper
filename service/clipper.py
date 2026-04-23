"""Clip extraction: on chat spike, cut a +/- N sec window out of the rolling
segment buffer and write it as a single .mp4 under clips/.

Flow:
  spike fires at wall time T
  wait (clip_post_seconds + 10) so the post-spike audio is captured
  list newest segments by mtime
  concat the last 4 (covers ~120 sec)
  seek to position of (T - clip_pre_seconds) and extract
    (clip_pre_seconds + clip_post_seconds) seconds

Output: <data_dir>/clips/<streamer>_<yyyymmdd_hhmmss>.mp4
       with a matching clipper_clips row in Supabase.
"""
from __future__ import annotations

import asyncio
import logging
import shlex
import time
from datetime import datetime, timezone
from pathlib import Path

from .config import Config
from .db import Supabase

log = logging.getLogger(__name__)


async def _run(cmd: str, timeout: int = 60) -> tuple[int, str]:
    """Run a shell command; return (returncode, combined_output)."""
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        return -1, "TIMEOUT"
    return proc.returncode or 0, (out or b"").decode("utf-8", errors="replace")


class ClipExtractor:
    """One per process; handles clip-extract jobs serially per streamer so
    ffmpeg processes don't fight over the segment files."""

    def __init__(self, cfg: Config, db: Supabase):
        self.cfg = cfg
        self.db = db
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock_for(self, streamer: str) -> asyncio.Lock:
        if streamer not in self._locks:
            self._locks[streamer] = asyncio.Lock()
        return self._locks[streamer]

    async def on_spike(self, streamer: str, stream_id: str, spike_row: dict) -> None:
        """Callback for ChatManager. Fire-and-forget a clip extract task."""
        asyncio.create_task(self._extract(streamer, stream_id, spike_row))

    async def _extract(self, streamer: str, stream_id: str, spike_row: dict) -> None:
        async with self._lock_for(streamer):
            try:
                await self._do_extract(streamer, stream_id, spike_row)
            except Exception:
                log.exception("clip[%s]: extraction failed", streamer)

    async def _do_extract(self, streamer: str, stream_id: str, spike_row: dict) -> None:
        spike_id = spike_row.get("id")
        detected_at = spike_row.get("detected_at")
        # detected_at is ISO8601; convert to wall-time unix seconds
        if isinstance(detected_at, str):
            dt = datetime.fromisoformat(detected_at.replace("Z", "+00:00"))
            spike_ts = dt.timestamp()
        else:
            spike_ts = time.time()

        wait = self.cfg.clip_post_seconds + 10
        log.info("clip[%s]: spike at %s, waiting %ds for post-spike capture",
                 streamer, detected_at, wait)
        await asyncio.sleep(wait)

        buffer_dir = self.cfg.data_dir / "buffers" / streamer
        segs = sorted(buffer_dir.glob("seg_*.ts"), key=lambda p: p.stat().st_mtime)
        if len(segs) < 2:
            log.warning("clip[%s]: only %d segments available, skipping", streamer, len(segs))
            return

        # Take last 4 segments for ~120 sec of recent history (or fewer if buffer is small).
        use = segs[-4:] if len(segs) >= 4 else segs
        latest_mtime = use[-1].stat().st_mtime
        oldest_mtime = use[0].stat().st_mtime

        clips_dir = self.cfg.data_dir / "clips"
        clips_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.fromtimestamp(spike_ts, tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        base = f"{streamer}_{stamp}"
        concat_list = clips_dir / f"{base}.concat.txt"
        concat_mp4  = clips_dir / f"{base}.concat.mp4"
        final_mp4   = clips_dir / f"{base}.mp4"

        # Write concat list
        with open(concat_list, "w", encoding="utf-8") as f:
            for s in use:
                f.write(f"file '{s.resolve()}'\n")

        # 1. Concat the recent segments with a quick copy-codec pass.
        cmd1 = (
            f'ffmpeg -y -hide_banner -loglevel warning '
            f'-f concat -safe 0 -i {shlex.quote(str(concat_list))} '
            f'-c copy {shlex.quote(str(concat_mp4))}'
        )
        rc, out = await _run(cmd1, timeout=60)
        if rc != 0:
            log.error("clip[%s]: concat failed rc=%d %s", streamer, rc, out[:300])
            concat_list.unlink(missing_ok=True)
            return

        # 2. Figure out the seek offset inside the concatenated file.
        # The last segment's mtime ≈ current wall time (it's still being written).
        # oldest_mtime ≈ (start of concat).
        # Offset of spike in concat = spike_ts - oldest_mtime.
        # We want to START the clip at (spike_ts - pre_seconds).
        offset_start = (spike_ts - self.cfg.clip_pre_seconds) - oldest_mtime
        # Guard: never negative.
        offset_start = max(0.0, offset_start)
        duration = self.cfg.clip_pre_seconds + self.cfg.clip_post_seconds

        # 3. Trim to the clip window. Re-encode to clean mp4 (faststart, aac audio).
        cmd2 = (
            f'ffmpeg -y -hide_banner -loglevel warning '
            f'-ss {offset_start:.2f} -i {shlex.quote(str(concat_mp4))} '
            f'-t {duration} '
            f'-c:v libx264 -preset veryfast -crf 20 '
            f'-c:a aac -b:a 128k '
            f'-movflags +faststart '
            f'{shlex.quote(str(final_mp4))}'
        )
        rc, out = await _run(cmd2, timeout=120)
        concat_list.unlink(missing_ok=True)
        concat_mp4.unlink(missing_ok=True)
        if rc != 0:
            log.error("clip[%s]: trim failed rc=%d %s", streamer, rc, out[:300])
            return

        size = final_mp4.stat().st_size
        log.info("clip[%s]: wrote %s (%.1f MB, %ss)", streamer, final_mp4.name, size / 1e6, duration)

        # 4. Log to Supabase for the dashboard / manual-upload workflow.
        try:
            await self.db.insert("clipper_clips", {
                "spike_id": spike_id,
                "stream_id": stream_id,
                "streamer": streamer,
                "source_path": str(final_mp4),
                "duration_sec": duration,
                "status": "pending",
            })
        except Exception:
            log.exception("clip[%s]: DB insert failed", streamer)
