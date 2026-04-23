"""Post-processing pipeline for raw clips.

Pipeline (per clip):
  1. Extract 16kHz mono WAV audio for Whisper.
  2. Whisper API -> verbose_json with segment-level timestamps.
  3. Emit SRT from segments.
  4. FFmpeg pass 1: blur-background vertical reformat (1080x1920).
  5. FFmpeg pass 2: burn the SRT captions in with TikTok-style styling.
  6. GPT-4o-mini: hooky title + TikTok hashtags from the transcript.
  7. Thumbnail: snapshot at 40% through the clip.
  8. Update clipper_clips: processed_path, thumbnail_path, transcript,
     title, hashtags, status='ready'.

The vertical reformat uses a blurred background + centered 16:9 foreground.
This is what auto-clippers use so nothing in the frame gets cut off.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import shlex
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Callable

from openai import AsyncOpenAI

from .config import Config
from .db import Supabase

log = logging.getLogger(__name__)


def _format_srt_ts(seconds: float) -> str:
    """Whisper segments -> SRT timestamp (HH:MM:SS,mmm)."""
    seconds = max(0.0, seconds)
    hh = int(seconds // 3600)
    mm = int((seconds % 3600) // 60)
    ss = seconds % 60
    return f"{hh:02d}:{mm:02d}:{ss:06.3f}".replace(".", ",")


def _split_into_caption_lines(segment_text: str, max_chars_per_line: int = 32) -> str:
    """Break a Whisper segment into caption-friendly wrapped lines.
    TikTok captions read best with max 2 lines."""
    text = re.sub(r"\s+", " ", segment_text).strip()
    if not text:
        return ""
    if len(text) <= max_chars_per_line:
        return text
    # Word-wrap
    words = text.split(" ")
    lines: list[str] = []
    cur = ""
    for w in words:
        if len(cur) + 1 + len(w) <= max_chars_per_line:
            cur = (cur + " " + w) if cur else w
        else:
            if cur:
                lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    # TikTok looks best with 2 lines; merge tail lines
    if len(lines) > 2:
        lines = [lines[0], " ".join(lines[1:])]
    return "\\N".join(lines)  # \N is ASS newline; SRT uses plain \n


async def _run_cmd(cmd: str, timeout: int = 180) -> tuple[int, str]:
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


class Processor:
    """Consumes raw clip rows (status=pending) and produces processed vertical
    captioned clips with titles. Runs on a background asyncio task that polls
    the DB for new pending clips every few seconds."""

    def __init__(self, cfg: Config, db: Supabase):
        self.cfg = cfg
        self.db = db
        self._ai = AsyncOpenAI(api_key=cfg.openai_api_key) if cfg.openai_api_key else None
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=10)
            except Exception:
                self._task.cancel()

    async def _run(self) -> None:
        if not self._ai:
            log.warning("processor: OPENAI_API_KEY missing; processor disabled")
            return
        log.info("processor: started")
        while not self._stop.is_set():
            try:
                rows = await self.db.select(
                    "clipper_clips",
                    "status=eq.pending&order=created_at.asc&limit=1",
                )
            except Exception:
                log.exception("processor: poll failed")
                await asyncio.sleep(10)
                continue
            if not rows:
                await asyncio.sleep(5)
                continue
            row = rows[0]
            try:
                await self._process(row)
            except Exception:
                log.exception("processor: job failed for clip %s", row.get("id"))
                try:
                    await self.db.update(
                        "clipper_clips",
                        f"id=eq.{row['id']}",
                        {"status": "failed", "error": "processing exception"},
                    )
                except Exception:
                    pass

    async def _process(self, row: dict) -> None:
        clip_id = row["id"]
        streamer = row["streamer"]
        src = Path(row["source_path"])
        if not src.exists():
            log.error("processor[%s]: source missing %s", streamer, src)
            await self.db.update(
                "clipper_clips", f"id=eq.{clip_id}",
                {"status": "failed", "error": "source file missing"},
            )
            return

        log.info("processor[%s]: processing %s", streamer, src.name)
        await self.db.update(
            "clipper_clips", f"id=eq.{clip_id}", {"status": "processing"},
        )

        work = self.cfg.data_dir / "processed"
        work.mkdir(parents=True, exist_ok=True)
        base = src.stem
        audio  = work / f"{base}.wav"
        srt    = work / f"{base}.srt"
        noaudio_caps = work / f"{base}.vertical.mp4"
        final  = work / f"{base}.final.mp4"
        thumb  = work / f"{base}.jpg"

        try:
            # 1. Extract audio for Whisper
            rc, out = await _run_cmd(
                f'ffmpeg -y -hide_banner -loglevel error '
                f'-i {shlex.quote(str(src))} '
                f'-vn -acodec pcm_s16le -ar 16000 -ac 1 '
                f'{shlex.quote(str(audio))}'
            )
            if rc != 0:
                raise RuntimeError(f"audio extract failed: {out[:200]}")

            # 2. Whisper transcription
            with open(audio, "rb") as f:
                result = await self._ai.audio.transcriptions.create(
                    model="whisper-1",
                    file=f,
                    response_format="verbose_json",
                    timestamp_granularities=["segment"],
                )
            segments = getattr(result, "segments", None) or result.get("segments", []) if isinstance(result, dict) else result.segments
            transcript = (getattr(result, "text", None) or (result.get("text") if isinstance(result, dict) else "")).strip()

            # 3. Build SRT
            srt_lines: list[str] = []
            for i, seg in enumerate(segments, start=1):
                start = seg.start if hasattr(seg, "start") else seg.get("start", 0)
                end   = seg.end   if hasattr(seg, "end")   else seg.get("end",   start + 2)
                text  = seg.text  if hasattr(seg, "text")  else seg.get("text",  "")
                wrapped = _split_into_caption_lines(text).replace("\\N", "\n")
                if not wrapped:
                    continue
                srt_lines.append(str(i))
                srt_lines.append(f"{_format_srt_ts(start)} --> {_format_srt_ts(end)}")
                srt_lines.append(wrapped)
                srt_lines.append("")
            srt.write_text("\n".join(srt_lines), encoding="utf-8")

            # 4. Vertical reformat (1080x1920) with blurred-fill background.
            # Split input into two streams: one blurred & cover-scaled as bg,
            # one letterboxed at 1080 wide centered as fg.
            vf_vertical = (
                "split=2[bg][fg];"
                "[bg]scale=1080:1920:force_original_aspect_ratio=increase,"
                    "crop=1080:1920,gblur=sigma=18[bg];"
                "[fg]scale=1080:-2[fg];"
                "[bg][fg]overlay=(W-w)/2:(H-h)/2"
            )
            rc, out = await _run_cmd(
                f'ffmpeg -y -hide_banner -loglevel error '
                f'-i {shlex.quote(str(src))} '
                f'-filter_complex "{vf_vertical}" '
                f'-c:v libx264 -preset veryfast -crf 20 '
                f'-c:a aac -b:a 128k -movflags +faststart '
                f'{shlex.quote(str(noaudio_caps))}',
                timeout=240,
            )
            if rc != 0:
                raise RuntimeError(f"vertical reformat failed: {out[:300]}")

            # 5. Burn captions over the vertical version.
            # Alignment=2 = bottom-center, MarginV=260 keeps clear of TikTok UI.
            # Colours are ASS format: &HBBGGRR.
            srt_escaped = str(srt).replace('\\', '/').replace(':', '\\:').replace(',', '\\,')
            style = (
                "FontName=Arial Black,FontSize=16,"
                "PrimaryColour=&HFFFFFF,OutlineColour=&H000000,BackColour=&H00000000,"
                "BorderStyle=1,Outline=3,Shadow=0,"
                "Alignment=2,MarginV=260"
            )
            rc, out = await _run_cmd(
                f'ffmpeg -y -hide_banner -loglevel error '
                f'-i {shlex.quote(str(noaudio_caps))} '
                f'-vf "subtitles={srt_escaped}:force_style=\'{style}\'" '
                f'-c:v libx264 -preset veryfast -crf 20 '
                f'-c:a copy -movflags +faststart '
                f'{shlex.quote(str(final))}',
                timeout=240,
            )
            if rc != 0:
                raise RuntimeError(f"caption burn failed: {out[:300]}")

            # 6. Thumbnail at 40% through the clip
            duration = float(row.get("duration_sec") or 30)
            await _run_cmd(
                f'ffmpeg -y -hide_banner -loglevel error '
                f'-ss {duration * 0.4:.2f} -i {shlex.quote(str(final))} '
                f'-frames:v 1 -q:v 3 {shlex.quote(str(thumb))}',
                timeout=30,
            )

            # 7. Title + hashtags from GPT
            title = await self._gpt_title(streamer, transcript or "")
            hashtags = await self._gpt_hashtags(streamer, transcript or "")

            # 8. Clean up intermediates; keep final + thumb + srt
            try: audio.unlink()
            except Exception: pass
            try: noaudio_caps.unlink()
            except Exception: pass

            size_mb = final.stat().st_size / 1e6
            log.info("processor[%s]: %s -> %s (%.1f MB) title=%r",
                     streamer, src.name, final.name, size_mb, title[:60])

            await self.db.update(
                "clipper_clips", f"id=eq.{clip_id}",
                {
                    "processed_path": str(final),
                    "thumbnail_path": str(thumb),
                    "transcript": transcript,
                    "title": title,
                    "hashtags": hashtags,
                    "status": "ready",
                },
            )
        except Exception as e:
            log.exception("processor[%s]: pipeline exception", streamer)
            await self.db.update(
                "clipper_clips", f"id=eq.{clip_id}",
                {"status": "failed", "error": str(e)[:300]},
            )

    async def _gpt_title(self, streamer: str, transcript: str) -> str:
        if not self._ai:
            return ""
        try:
            resp = await self._ai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content":
                        "You write viral TikTok / YouTube Shorts titles for clipped Twitch moments. "
                        "Respond with ONLY the title, max 70 characters. Hooky, punchy, in-the-moment. "
                        "No hashtags. No emoji. No quotes around the title."},
                    {"role": "user", "content":
                        f"Streamer: {streamer}\n\nWhat was said in the clip:\n{transcript[:1500]}"},
                ],
                max_tokens=40,
                temperature=0.8,
            )
            title = (resp.choices[0].message.content or "").strip().strip('"').strip("'")
            return title[:100]
        except Exception:
            log.exception("gpt title failed")
            return ""

    async def _gpt_hashtags(self, streamer: str, transcript: str) -> str:
        if not self._ai:
            return ""
        try:
            resp = await self._ai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "user", "content":
                        f"Generate 6-8 TikTok hashtags for a clip from {streamer}'s Twitch stream. "
                        f"Transcript excerpt: {transcript[:500]}\n"
                        f"Rules: space-separated, start each with #, no explanation, "
                        f"mix clip-relevant and streamer-relevant."},
                ],
                max_tokens=60,
                temperature=0.6,
            )
            tags = (resp.choices[0].message.content or "").strip()
            # Keep only hashtag-looking tokens
            toks = [t for t in tags.split() if t.startswith("#") and 2 <= len(t) <= 30]
            return " ".join(toks[:8])
        except Exception:
            log.exception("gpt hashtags failed")
            return ""
