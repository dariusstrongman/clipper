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
import base64
import json
import logging
import random
import re
import shlex
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Callable

import aiohttp
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


def _split_into_caption_lines(segment_text: str, max_chars_per_line: int = 28) -> str:
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


async def _detect_silences(src: Path, noise_db: int = -28, min_dur: float = 0.3) -> list[tuple[float, float]]:
    """Run ffmpeg silencedetect and return a list of (silence_start, silence_end) tuples
    in seconds. These are natural pause boundaries we can snap clip cuts to so we
    never chop a word in half. noise_db is permissive enough (-28dB) to catch real
    breathing pauses even with background music."""
    cmd = (
        f'ffmpeg -hide_banner -nostats -i {shlex.quote(str(src))} '
        f'-af silencedetect=noise={noise_db}dB:d={min_dur} '
        f'-f null -'
    )
    rc, out = await _run_cmd(cmd, timeout=60)
    silences: list[tuple[float, float]] = []
    cur_start: float | None = None
    for line in out.splitlines():
        m = re.search(r'silence_start:\s*(-?\d+(?:\.\d+)?)', line)
        if m:
            cur_start = max(0.0, float(m.group(1)))
            continue
        m = re.search(r'silence_end:\s*(-?\d+(?:\.\d+)?)', line)
        if m and cur_start is not None:
            end = float(m.group(1))
            if end > cur_start:
                silences.append((cur_start, end))
            cur_start = None
    return silences


def _snap_boundaries(pick_start: float, pick_end: float,
                     silences: list[tuple[float, float]],
                     total_dur: float,
                     tol: float = 3.0) -> tuple[float, float]:
    """Snap a clip range to the nearest silence boundaries so cuts never land
    mid-word. Only snaps if a silence is within `tol` seconds of the pick."""
    new_start, new_end = pick_start, pick_end

    # Snap start: find the silence whose END is closest to (and within tol of) pick_start.
    # We want to START SPEECH just after a silence ends.
    best_dist = tol + 1
    for s_start, s_end in silences:
        # Candidate silence must end near pick_start (either slightly before or overlapping)
        if s_end > pick_start + 0.5:
            continue
        dist = pick_start - s_end
        if 0 <= dist <= tol and dist < best_dist:
            best_dist = dist
            new_start = s_end

    # If no silence found BEFORE pick_start, look for one bracketing it
    # (pick_start falls inside a silence window - snap to the silence's end)
    if new_start == pick_start:
        for s_start, s_end in silences:
            if s_start <= pick_start <= s_end:
                new_start = s_end
                break

    # Snap end: find the silence whose START is closest to (and within tol of) pick_end.
    # We want to END right before the next silence starts.
    best_dist = tol + 1
    for s_start, s_end in silences:
        if s_start < pick_end - 0.5:
            continue
        dist = s_start - pick_end
        if 0 <= dist <= tol and dist < best_dist:
            best_dist = dist
            new_end = s_start

    if new_end == pick_end:
        for s_start, s_end in silences:
            if s_start <= pick_end <= s_end:
                new_end = s_start
                break

    # Clamp to source bounds + enforce min duration
    new_start = max(0.0, new_start)
    new_end = min(total_dur, new_end)
    if new_end - new_start < 6.0:
        # Snap was too aggressive; fall back to original picks
        return pick_start, pick_end
    return new_start, new_end


# Streamer-specific context passed into every AI call. The model thinks more
# like someone who actually follows each streamer when it has this.
STREAMER_PROFILES: dict[str, dict[str, str]] = {
    "ddg": {
        "content": "rapper and music personality. Concerts, tour content, chat interaction during "
                   "shows, relationship drama, diss tracks, hot takes, beef with other artists.",
        "clip_gold": "music performance peaks, crowd reactions, unexpected reveals, moments "
                     "addressing haters or ex-drama, punchlines from freestyles, shade at other rappers.",
        "avoid": "soundcheck, long intros, silent gaps, bland 'hello chat' chatter, menu screens.",
    },
    "marlon": {
        "content": "soccer/football streamer. M3FC team content, tournament play, trash talking "
                   "teammates, reactions to goals and plays, clutch moments in FIFA/competitive matches.",
        "clip_gold": "goals, clutch saves, bad misses, teammate arguments, smooth skill moves, "
                     "funny rage moments, iconic commentary lines during gameplay.",
        "avoid": "uncontested possession, settings menus, queue time, standard passing.",
    },
    "jasontheween": {
        "content": "IRL streamer. Dating show format, confrontations with guests and exes, "
                   "reality-show-style reveals, relationship content.",
        "clip_gold": "awkward confrontations, shock reveals, emotional outbursts, truth bombs, "
                     "someone walking out, unexpected guest entrances, one-line shutdowns.",
        "avoid": "driving/transit segments, polite small talk, explanation intros.",
    },
    "lacy": {
        "content": "IRL and gaming streamer. Reactions, conversations with other streamers, "
                   "relationship content, real-world encounters, lifestyle moments.",
        "clip_gold": "genuine laughter, unexpected reactions, relatable takes, flirtation, "
                     "bold statements, conflict with strangers, viral one-liners.",
        "avoid": "quiet gameplay, technical difficulties, long eating segments.",
    },
    "jaycinco": {
        "content": "Kick streamer focused on fitness/gym content and gaming. Often hosts guests "
                   "like Yourrage. Workout streams, gym fails, gaming reactions, guest banter.",
        "clip_gold": "heavy lifts and PRs, fails, gym confrontations, guest banter, shock reactions, "
                     "insane form or form-breakdown moments.",
        "avoid": "warm-ups, setting up equipment, technical talk.",
    },
    "deshaefrost": {
        "content": "high-energy gaming streamer. Gaming moments, IRL bits, rage moments, "
                   "banter with chat and friends.",
        "clip_gold": "rage outbursts, clutch plays, shock moments, iconic reactions, funny fails, "
                     "line-of-the-year quotes mid-game.",
        "avoid": "menu navigation, loading screens, quiet gameplay, streaming setup chatter.",
    },
    "jynxzi": {
        "content": "high-energy Rainbow Six Siege streamer. Primarily ranked Siege gameplay, "
                   "esports tournament reactions, IRL streams, beef with other streamers, "
                   "girlfriend appearances, gambling/slot streams, viral reaction content.",
        "clip_gold": "ranked clutches, ace rounds, rage outbursts, surprise reveals, beef moments, "
                     "big-money gambling reactions, iconic catchphrases ('SIEGE!'), Bri-related moments, "
                     "screaming reactions to big plays.",
        "avoid": "menu navigation, lobby waits, technical setup, calm tutorial-style talk, slow inventory checks.",
    },
}


def _streamer_context(login: str) -> str:
    """Return a formatted profile block for AI prompts. Empty if streamer unknown."""
    p = STREAMER_PROFILES.get((login or "").lower())
    if not p:
        return ""
    return (
        f"ABOUT {login.upper()} (use this to judge what's actually clip-worthy for THIS streamer):\n"
        f"- Content style: {p['content']}\n"
        f"- Gold-tier clips look like: {p['clip_gold']}\n"
        f"- Avoid clipping: {p['avoid']}\n"
    )


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

            # 2. Whisper transcription with segment-level timestamps
            with open(audio, "rb") as f:
                result = await self._ai.audio.transcriptions.create(
                    model="whisper-1",
                    file=f,
                    response_format="verbose_json",
                    timestamp_granularities=["segment"],
                )
            segments = getattr(result, "segments", None) or result.get("segments", []) if isinstance(result, dict) else result.segments
            transcript = (getattr(result, "text", None) or (result.get("text") if isinstance(result, dict) else "")).strip()

            def _seg(attr, d, default=0):
                return (getattr(d, attr, None) if not isinstance(d, dict) else d.get(attr, default)) or default
            total_dur = max([float(_seg("end", s, 0)) for s in segments] + [0.0])
            if total_dur < 5:
                total_dur = float(row.get("duration_sec") or 30)

            # 3. Detect natural silence boundaries (used as snap targets + AI hints)
            silences = await _detect_silences(src, noise_db=-28, min_dur=0.3)
            log.info("processor[%s]: detected %d silence gaps", streamer, len(silences))

            # 4. Fetch spike context (chat sample + velocity + decay)
            spike_ctx = await self._fetch_spike_context(row.get("spike_id"))
            chat_sample = spike_ctx.get("sample", [])
            chat_velocity = spike_ctx.get("velocity_str", "unknown")
            spike_offset = float(self.cfg.clip_pre_seconds)

            # 5. Vision pass on the full source clip. This is the cure for the
            #    "AI invents drama from gameplay-callout transcripts" problem.
            #    Without vision, the AI is genuinely blind to what chat reacted
            #    to. Skipped if ANTHROPIC_API_KEY is unset.
            visual_context = await self._vision_describe(src, work, base, 0.0, total_dur)
            if visual_context:
                log.info("processor[%s]: vision context (%d chars)", streamer, len(visual_context))

            # 6. ONE unified GPT call: scores, decides post/auto_upload,
            #    picks start/end, generates title + backup titles + description + hashtags.
            decision = await self._gpt_decide(
                streamer=streamer,
                segments=segments,
                total_dur=total_dur,
                spike_offset=spike_offset,
                transcript=transcript,
                chat_sample=chat_sample,
                chat_velocity=chat_velocity,
                silences=silences,
                visual_context=visual_context,
            )
            if not decision:
                # AI failed - bail out, mark failed, don't process media
                try: src.unlink(missing_ok=True)
                except Exception: pass
                try: audio.unlink()
                except Exception: pass
                await self.db.update(
                    "clipper_clips", f"id=eq.{clip_id}",
                    {"status": "failed", "error": "AI decision call failed",
                     "transcript": transcript, "source_path": None},
                )
                return

            post = bool(decision.get("post", False))
            auto_upload = bool(decision.get("auto_upload", False))
            viral_score = float(decision.get("viral_score", 0))
            hook_score = float(decision.get("hook_score", 0))
            context_score = float(decision.get("context_score", 0))
            pacing_score = float(decision.get("pacing_score", 0))
            retention_score = float(decision.get("retention_score", 0))
            category = str(decision.get("category", "unknown"))[:30]
            reason = str(decision.get("reason", ""))[:400]
            reject_reason = decision.get("reject_reason") or None
            if reject_reason: reject_reason = str(reject_reason)[:400]
            title = (decision.get("title") or "").strip()[:140]
            backup_titles = decision.get("backup_titles") or []
            backup_titles = [str(t).strip()[:140] for t in backup_titles if t][:3]
            description = (decision.get("description") or "").strip()[:1000]
            hashtags_list = decision.get("hashtags") or []
            hashtags = " ".join([str(h).strip() for h in hashtags_list if h])[:300]
            # Hook overlay: short teaser burned onto first 2 sec. ASCII-safe sanitize
            # so ffmpeg drawtext doesn't choke on quotes/colons/backslashes.
            hook_overlay_raw = (decision.get("hook_overlay") or "").strip()
            hook_overlay = re.sub(r"[^A-Za-z0-9 ,.!?-]", "", hook_overlay_raw)[:80]

            # A/B variant assignment: 50/50 split.
            #   'A' = treatment (overlay burned in - the new pipeline)
            #   'B' = control   (no overlay - baseline for comparison)
            # Switch off the overlay for variant B so we have a real apples-to-apples
            # test of whether the 3-second retention overlay is actually doing work.
            variant = random.choice(['A', 'B'])
            burn_overlay = (variant == 'A') and bool(hook_overlay)
            log.info("processor[%s]: variant=%s overlay=%s", streamer, variant, burn_overlay)

            log.info(
                "processor[%s]: decision post=%s auto=%s viral=%.1f hook=%.1f ctx=%.1f pace=%.1f ret=%.1f cat=%s",
                streamer, post, auto_upload, viral_score, hook_score, context_score, pacing_score, retention_score, category,
            )
            if reason:
                log.info("processor[%s]: reason=%s", streamer, reason[:200])

            # Per-streamer daily cap: max 4 auto-approvals per streamer per UTC day.
            # Prevents one chatty stream from flooding the dashboard's auto-promote queue.
            # Excess high-quality clips drop to status=ready for manual review.
            # Hard floor: any clip under 12 seconds CANNOT auto-promote, no matter
            # what the AI scored it. 12 sec is the minimum viable Short / TikTok.
            # Below that you have a meme-snippet, not a clip worth posting.
            if auto_upload and pick_dur < 12.0:
                log.info("processor[%s]: auto_upload blocked - clip too short (%.1fs < 12s)",
                         streamer, pick_dur)
                auto_upload = False

            AUTO_DAILY_CAP = 4
            if auto_upload:
                # Use 'Z' suffix - PostgREST query strings interpret '+' as space,
                # which would break the '+00:00' offset.
                day_start = datetime.now(timezone.utc).replace(
                    hour=0, minute=0, second=0, microsecond=0
                ).strftime("%Y-%m-%dT%H:%M:%SZ")
                try:
                    existing = await self.db.select(
                        "clipper_clips",
                        f"streamer=eq.{streamer}&auto_upload=eq.true&created_at=gt.{day_start}&select=id",
                    )
                    if len(existing) >= AUTO_DAILY_CAP:
                        log.info("processor[%s]: auto_upload cap hit (%d/day) - downgrading to ready",
                                 streamer, AUTO_DAILY_CAP)
                        auto_upload = False
                except Exception:
                    log.exception("processor[%s]: cap check failed, allowing auto_upload", streamer)

            # 6. If post=false, save metadata + clean up. Don't process media.
            if not post:
                try: src.unlink(missing_ok=True)
                except Exception: pass
                try: audio.unlink()
                except Exception: pass
                try: srt.unlink(missing_ok=True)
                except Exception: pass
                await self.db.update(
                    "clipper_clips", f"id=eq.{clip_id}",
                    {
                        "status": "rejected",
                        "transcript": transcript,
                        "score": round(viral_score, 1),
                        "hook_score": round(hook_score, 1),
                        "context_score": round(context_score, 1),
                        "pacing_score": round(pacing_score, 1),
                        "retention_score": round(retention_score, 1),
                        "category": category,
                        "score_reason": reason,
                        "reject_reason": reject_reason or reason,
                        "auto_upload": False,
                        "source_path": None,
                    },
                )
                return

            # 7. post=true - process media. Snap AI's start/end to silence boundaries.
            raw_start = float(decision.get("start_second", spike_offset - 18))
            raw_end = float(decision.get("end_second", spike_offset + 20))
            raw_start = max(0.0, min(raw_start, total_dur - 6.0))
            raw_end = max(raw_start + 6.0, min(raw_end, total_dur))
            if raw_end - raw_start > 55.0:
                raw_end = raw_start + 55.0

            pick_start, pick_end = _snap_boundaries(raw_start, raw_end, silences, total_dur, tol=3.0)
            if abs(pick_start - raw_start) > 0.2 or abs(pick_end - raw_end) > 0.2:
                log.info("processor[%s]: snap [%.2f-%.2f] -> [%.2f-%.2f]",
                         streamer, raw_start, raw_end, pick_start, pick_end)
            pick_dur = max(6.0, min(60.0, pick_end - pick_start))
            pick_end = pick_start + pick_dur
            log.info("processor[%s]: final pick [%.1f-%.1fs] from %.1fs source (len=%.1fs)",
                     streamer, pick_start, pick_end, total_dur, pick_dur)

            # 8. Build SRT from segments within the pick window, times shifted so 0.0 = pick_start.
            srt_lines: list[str] = []
            kept = 0
            for seg in segments:
                s = float(_seg("start", seg, 0))
                e = float(_seg("end", seg, s + 2))
                text = _seg("text", seg, "") or ""
                if e < pick_start or s > pick_end:
                    continue
                new_s = max(0.0, s - pick_start)
                new_e = min(pick_dur, e - pick_start)
                if new_e <= new_s:
                    continue
                wrapped = _split_into_caption_lines(text).replace("\\N", "\n")
                if not wrapped:
                    continue
                kept += 1
                srt_lines.append(str(kept))
                srt_lines.append(f"{_format_srt_ts(new_s)} --> {_format_srt_ts(new_e)}")
                srt_lines.append(wrapped)
                srt_lines.append("")
            srt.write_text("\n".join(srt_lines), encoding="utf-8")

            # 9. Vertical reformat + trim in one frame-accurate ffmpeg pass.
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
                f'-ss {pick_start:.3f} -t {pick_dur:.3f} '
                f'-filter_complex "{vf_vertical}" '
                f'-c:v libx264 -preset veryfast -crf 20 '
                f'-c:a aac -b:a 128k -movflags +faststart '
                f'{shlex.quote(str(noaudio_caps))}',
                timeout=240,
            )
            if rc != 0:
                raise RuntimeError(f"vertical reformat failed: {out[:300]}")

            # 10. Burn captions + (if present) hook overlay onto first 2 seconds.
            # Hook overlay is the highest-leverage retention play: 3-second
            # retention is THE algorithmic gate on TikTok / Shorts.
            srt_escaped = str(srt).replace('\\', '/').replace(':', '\\:').replace(',', '\\,')
            style = (
                "FontName=Arial Black,FontSize=20,"
                "PrimaryColour=&HFFFFFF,OutlineColour=&H000000,BackColour=&H00000000,"
                "BorderStyle=1,Outline=3,Shadow=0,"
                "Alignment=2,MarginV=320"
            )
            # Build the video filter chain: subtitles first, then drawtext overlay
            # (only for variant A - variant B is the no-overlay control).
            vf_parts = ["subtitles=" + srt_escaped + ":force_style='" + style + "'"]
            if burn_overlay:
                # White text in a black pill at top of frame, fade in at 0.1s, out at 1.9s.
                # Position: top-center, y=180 (clear of TikTok top chrome).
                # Size 64pt = readable on phone but doesn't dominate the frame.
                hook_safe = hook_overlay.replace("'", "")  # already sanitized but defensive
                drawtext = (
                    "drawtext=text='" + hook_safe + "'"
                    ":fontfile=/usr/share/fonts/truetype/dejavu/DejaVu-Sans-Bold.ttf"
                    ":fontsize=64:fontcolor=white"
                    ":box=1:boxcolor=black@0.78:boxborderw=22"
                    ":x=(w-text_w)/2:y=180"
                    ":enable='between(t,0.1,2.0)'"
                )
                vf_parts.append(drawtext)
            vf_chain = ",".join(vf_parts)

            rc, out = await _run_cmd(
                f'ffmpeg -y -hide_banner -loglevel error '
                f'-i {shlex.quote(str(noaudio_caps))} '
                f'-vf "{vf_chain}" '
                f'-c:v libx264 -preset veryfast -crf 20 '
                f'-c:a copy -movflags +faststart '
                f'{shlex.quote(str(final))}',
                timeout=240,
            )
            if rc != 0:
                # Fallback: try without the hook overlay if drawtext borked
                if burn_overlay:
                    log.warning("processor[%s]: caption+overlay failed, retrying without overlay: %s",
                                streamer, out[:200])
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

            # 11. Thumbnail at 40% through the clip
            await _run_cmd(
                f'ffmpeg -y -hide_banner -loglevel error '
                f'-ss {pick_dur * 0.4:.2f} -i {shlex.quote(str(final))} '
                f'-frames:v 1 -q:v 3 {shlex.quote(str(thumb))}',
                timeout=30,
            )

            # 12. Cleanup intermediates
            try: audio.unlink()
            except Exception: pass
            try: noaudio_caps.unlink()
            except Exception: pass
            try: src.unlink(missing_ok=True)
            except Exception: pass

            size_mb = final.stat().st_size / 1e6
            final_status = "approved" if auto_upload else "ready"
            log.info("processor[%s]: %s -> %s (%.1f MB) status=%s title=%r",
                     streamer, src.name, final.name, size_mb, final_status, title[:60])

            await self.db.update(
                "clipper_clips", f"id=eq.{clip_id}",
                {
                    "processed_path": str(final),
                    "thumbnail_path": str(thumb),
                    "transcript": transcript,
                    "title": title,
                    "backup_titles": backup_titles,
                    "description": description,
                    "hashtags": hashtags,
                    "hook_overlay": hook_overlay or None,
                    "variant": variant,
                    "score": round(viral_score, 1),
                    "hook_score": round(hook_score, 1),
                    "context_score": round(context_score, 1),
                    "pacing_score": round(pacing_score, 1),
                    "retention_score": round(retention_score, 1),
                    "category": category,
                    "score_reason": reason,
                    "auto_upload": auto_upload,
                    "duration_sec": round(pick_dur, 1),
                    "source_path": None,
                    "status": final_status,
                    "approved_at": (datetime.now(timezone.utc).isoformat() if auto_upload else None),
                },
            )
        except Exception as e:
            log.exception("processor[%s]: pipeline exception", streamer)
            await self.db.update(
                "clipper_clips", f"id=eq.{clip_id}",
                {"status": "failed", "error": str(e)[:300]},
            )

    async def _vision_describe(self, src: Path, work: Path, base: str,
                               pick_start: float, pick_dur: float) -> str:
        """Sample 5 frames evenly across the picked clip range, send them to
        Claude Sonnet 4.6 vision, and return a factual description of what's
        on screen. This is the cure for AI hallucination in the title pass:
        the AI now actually SEES the clip instead of inferring vibes from a
        gameplay-callout transcript.

        Cost: ~$0.03 per call (5 small JPEGs in, ~400 tokens out via Claude
        Sonnet 4.6). Skipped entirely if ANTHROPIC_API_KEY is unset.

        Returns empty string on any failure - caller falls back to text-only
        scoring (the old behavior)."""
        if not self.cfg.anthropic_api_key:
            return ""
        if pick_dur < 1.0:
            return ""

        # Sample 5 frames evenly across the picked window
        timestamps = [pick_start + (pick_dur * i / 4.0) for i in range(5)]
        frames: list[Path] = []
        for i, ts in enumerate(timestamps):
            fp = work / f"{base}.frame{i}.jpg"
            cmd = (
                f'ffmpeg -y -hide_banner -loglevel error '
                f'-ss {ts:.2f} -i {shlex.quote(str(src))} '
                f'-frames:v 1 -q:v 4 -vf scale=720:-1 '
                f'{shlex.quote(str(fp))}'
            )
            rc, _ = await _run_cmd(cmd, timeout=15)
            if rc == 0 and fp.exists() and fp.stat().st_size > 0:
                frames.append(fp)

        if len(frames) < 2:
            log.warning("vision: only got %d/%d frames, skipping", len(frames), len(timestamps))
            return ""

        # Build Claude API request
        content: list[dict] = [
            {"type": "text", "text":
                f"You are watching {len(frames)} frames sampled evenly across a "
                f"{pick_dur:.0f}-second Twitch stream clip (frame 1 = start, "
                f"frame {len(frames)} = end). Describe ONLY what is visibly on "
                "screen across the frames. Be FACTUAL and specific.\n\n"
                "Cover:\n"
                "- The streamer's face, expression, body language, gestures (if visible).\n"
                "- What's behind them: game UI, gameplay action, kill feed, score, "
                "any HUD elements, any chat overlay text, any screenshare.\n"
                "- Any visible text overlays, alerts, donation popups, sub notifications, "
                "or text on screen.\n"
                "- Any obvious visual events: a kill, a death, someone walking in, "
                "facecam reaction, jump scare, anything notable.\n\n"
                "Be concrete. Quote any visible text. Name games or apps you recognize. "
                "If frames are mostly identical, say so.\n\n"
                "DO NOT speculate about audio, dialogue, or emotional context. "
                "Only describe what is VISIBLE. Maximum 180 words.",
            }
        ]
        for fp in frames:
            try:
                data_b64 = base64.b64encode(fp.read_bytes()).decode("ascii")
            except Exception:
                continue
            content.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/jpeg",
                    "data": data_b64,
                }
            })

        body = {
            "model": "claude-sonnet-4-6",
            "max_tokens": 400,
            "messages": [{"role": "user", "content": content}],
        }

        result = ""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": self.cfg.anthropic_api_key,
                        "anthropic-version": "2023-06-01",
                        "Content-Type": "application/json",
                    },
                    json=body,
                    timeout=aiohttp.ClientTimeout(total=60),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for block in data.get("content", []):
                            if block.get("type") == "text":
                                result = (block.get("text") or "").strip()
                                break
                    else:
                        log.warning("vision: claude returned %d: %s", resp.status,
                                    (await resp.text())[:200])
        except Exception:
            log.exception("vision: claude call failed")
            result = ""

        # Clean up frame files
        for fp in frames:
            try: fp.unlink(missing_ok=True)
            except Exception: pass

        return result[:1200]  # Cap so we don't bloat the GPT prompt

    async def _fetch_spike_context(self, spike_id: str | None) -> dict:
        """Pull chat sample + velocity from the spike row that triggered this clip.
        Returns {sample: [...], velocity_str: '40 msgs/5s = 8.0/sec', velocity_per_sec: float}.
        """
        if not spike_id:
            return {"sample": [], "velocity_str": "no spike id", "velocity_per_sec": 0.0}
        try:
            rows = await self.db.select(
                "clipper_spikes",
                f"id=eq.{spike_id}&select=sample_messages,messages_in_window,window_seconds",
            )
            if not rows:
                return {"sample": [], "velocity_str": "spike not found", "velocity_per_sec": 0.0}
            r = rows[0]
            try:
                sample = json.loads(r.get("sample_messages") or "[]")
            except Exception:
                sample = []
            msgs = int(r.get("messages_in_window") or 0)
            win = int(r.get("window_seconds") or 5)
            vps = msgs / win if win > 0 else 0.0
            return {
                "sample": sample,
                "velocity_str": f"{msgs} messages in {win}s = {vps:.1f} msgs/sec",
                "velocity_per_sec": vps,
            }
        except Exception:
            log.exception("fetch spike context failed")
            return {"sample": [], "velocity_str": "fetch error", "velocity_per_sec": 0.0}

    async def _gpt_decide(self, streamer: str, segments, total_dur: float,
                          spike_offset: float, transcript: str,
                          chat_sample: list, chat_velocity: str,
                          silences: list[tuple[float, float]],
                          visual_context: str = "") -> dict | None:
        """ONE unified GPT call. Replaces the old classify + pick_range + title +
        hashtags chain. Returns a full decision dict matching the user's JSON
        prompt schema (post, auto_upload, viral_score, hook_score, context_score,
        pacing_score, category, start_second, end_second, title, backup_titles,
        description, hashtags, reason, reject_reason).

        Returns None on AI failure - caller marks the clip status='failed'."""
        if not self._ai:
            return None

        # Compact whisper segments + silence map for the prompt
        seg_data = []
        for seg in segments[:80]:
            s = float(seg.start if hasattr(seg, "start") else seg.get("start", 0))
            e = float(seg.end if hasattr(seg, "end") else seg.get("end", s + 2))
            t = (seg.text if hasattr(seg, "text") else seg.get("text", "")) or ""
            seg_data.append({"start": round(s, 2), "end": round(e, 2), "text": t.strip()})

        silence_data = [{"start": round(s, 2), "end": round(e, 2)} for s, e in silences[:25]]

        profile = _streamer_context(streamer)

        # System prompt = user's JSON contract, formatted as natural-language sections.
        system = """You are an elite short-form viral clip director for YouTube Shorts, TikTok, and Reels.

GOAL
Pick only clips with strong viral potential. Choose the perfect start/end time. Create a high-retention title. Write a clean optimized description.

RULES
- Be ruthless. Reject average clips.
- Optimize for: retention, rewatches, instant hook, curiosity, shareability.
- Platform: YouTube Shorts (also works for TikTok/Reels).
- Audience: people scrolling fast who do not know the full stream context.
- Ideal clip length: 12-35 seconds. Max 55 seconds.
- Minimum auto-upload threshold: viral_score >= 8.3.

SELECTION CRITERIA
- Hook precision: identify the FIRST FRAME that creates curiosity, surprise, or emotion. Trim everything before it. 1 second too early kills the clip - viewer scrolls. 1 second too late confuses them about what they're watching. Be RUTHLESS about cutting dead lead-in.
- Instant hook: the chosen start must become interesting within the first 1-2 seconds.
- No context needed: a random viewer should understand why the moment is funny, shocking, awkward, impressive, or dramatic.
- Clear payoff: the clip must have a clear reaction, reveal, joke, argument, fail, skill moment, or uncomfortable moment.
- Tight pacing: remove setup, dead air, repeated words, boring lead-in, and weak ending.
- Strong retention: the clip must stay engaging the WHOLE length. Energy must not drop halfway. The ending must land cleanly with a payoff - not peter out mid-sentence or end on dead air.
- Use the silence boundaries provided for clean cuts so words are never chopped mid-syllable.
- The chat spike happens at detected_at_second - this usually means the actual on-stream moment is slightly BEFORE that due to Twitch delay.

VIRAL CATEGORIES
funny, awkward, drama, rage, reaction, insane play, exposed, unexpected, chat caught it, streamer gets cooked.

REJECT IF
- needs too much backstory
- first 2 seconds are boring
- mostly silence
- only chat spam with no real moment
- sub bomb or raid hype
- inside joke that outsiders will not understand
- low energy conversation
- unclear audio
- clip is mainly filler
- TRANSCRIPT IS GAMEPLAY CALLOUTS WITH NO REACTION OR PUNCHLINE. Examples to reject:
    "watch the triple wall, flank up there"
    "go to that window you went to last time"
    "first point, mingo's looking"
    "I'm gonna push, you cover the angle"
    "team coordination", strategy talk, "they're at A-site"
  These are ROUTINE GAMEPLAY. Not viral. The chat spike was probably for an
  off-screen visual moment we cannot see (a kill, a flick, a fail) - we cannot
  score what we cannot hear. SCORE 1-3 AND REJECT.

==================== DARIUS'S TASTE PROFILE (overrides viral scoring) ====================

This is the most important rule in the prompt. The user (Darius) reviews
clips manually and rejects ~95% of what the AI generates because the AI
over-weights generic "viral signals" he does not actually want to post.
His pick history shows a sharp split:

KEEP (score these viral 7-9, eligible to post):
- Specific drama with stakes (someone calling someone out, confrontation, beef, exposing a lie WITH the spoken receipts)
- Quotable mid-stream confessions ("she did WHAT in ninth grade", "her mom called about THIS")
- Personality moments with a memorable spoken line you can quote
- Streamer cooking themselves (DDG's police call, Lacy's confession about her past)
- Off-script jokes between two streamers, especially with named streamers (Marlon vs Yourrage, etc.)

REJECT (score these viral 1-5, post=false EVEN IF chat spiked hard):
- "[Streamer] reacts to [thing]" patterns. Reaction-only with no quotable substance.
- "[Streamer]'s reaction to winning X" (pack opens, prize wins, gambling pulls)
- Pure gameplay clutches ("INSANE 1v3", "ace clutch", "kill streak"). Skill plays without spoken substance bore his audience.
- "INSANE play" / "GOES CRAZY" labels with no quotable audio
- Generic "wild moment" / "crazy reaction" with no specific named subject
- Anything where the would-be title is "[Streamer] reacts to [vague thing]"

PATTERN TEST: Before scoring, ask yourself - if the only honest title for this is "X reacts to Y" or "X's INSANE play", it is generic and he will reject it. Score viral <= 5.

STREAMER NOTES (his observed pick rate):
- DDG, Jasontheween: keep when there is real drama or a quotable moment. Otherwise generic.
- Jynxzi: keep ONLY for personality (jokes, beef, off-script moments). REJECT all gameplay clutches.
- Lacy: 94% rejection rate observed. ONLY keep clips with a specific quotable confession or personal-life drama (no generic reactions).
- Marlon: most clips rejected. Keep only confrontations or specific quotable lines.

==================== ANTI-HALLUCINATION RULES (CRITICAL) ====================

This is the rule that separates a real review from making things up:

1. THE TRANSCRIPT IS GROUND TRUTH. The title, hook_overlay, description, and
   reason MUST be derivable from the transcript. If the transcript says
   "go to that window", DO NOT title it "He just said something WILD". You
   would be inventing a moment that doesn't exist in the audio.

2. NEVER invent objects, events, characters, or actions not in the transcript.
   If "goldfish" is not in the transcript, do NOT write "Spilled Goldfish".
   If "Arki" is not in the transcript, do NOT write "reacts to Arki".
   If there is no joke in the audio, do NOT title it as a joke.

3. INPUTS YOU HAVE:
       - the audio transcript (Whisper output)
       - the chat sample at the spike
       - the timing of the spike
       - if visual_context_from_vision_pass is provided in the inputs, that
         is a Claude Sonnet vision description of 5 frames sampled across
         the clip. Treat this as visual ground truth.
   When visual_context IS provided: title and hook_overlay can reference
   visual elements ("his face when he saw", "the chat exploding behind him")
   AS LONG AS those elements are confirmed in the visual_context. Never
   invent visual elements that aren't there.
   When visual_context IS NOT provided: you are blind to the video. If the
   transcript alone doesn't carry a quotable moment, REJECT. The chat
   spike may have been for an off-screen visual you cannot see.

4. EVIDENCE CHECK: Before writing the title, copy-paste the most clip-worthy
   sentence from the transcript into your reasoning. If you cannot find one
   sentence that on its own would make a stranger stop scrolling, REJECT.

5. WHEN CHAT IS HYPED BUT TRANSCRIPT IS BORING: this is the most common
   trap. Chat hype alone is NOT a moment. The transcript must carry it.
   Score the SUBSTANCE in the audio, not the volume in chat.

TITLE RULES (built from 2026 VidIQ + retention data + analysis of top viral Twitch clip channels)

LENGTH
- Optimal: 50-80 characters total (the body text + 2 hashtags at end).
- Hard max: 95 (YouTube Shorts hard cap is 100 chars).
- Body of the title (before hashtags) should still be punchy: 30-50 chars.

HASHTAGS IN TITLE (REQUIRED)
Every title MUST end with EXACTLY 2 hashtags, format: `<title body> #streamer #niche`
- First hashtag: streamer's own tag, lowercase. e.g., #ddg, #marlon, #jasontheween, #lacy, #jaycinco, #deshaefrost, #jynxzi
- Second hashtag: a topical tag tied to the moment. Pick from: #twitch, #shorts, #streamerclips, #funny, #drama, #rage, #fyp, #fail, #clutch, #fight, #reaction. Pick what fits the clip best.
- Two spaces minimum between body text and hashtags is fine. Single space OK too.
- Examples:
  "DDG GOES OFF on his Ex Live #ddg #drama"
  "Jynxzi 1v4 CLUTCH That Broke Him #jynxzi #clutch"
  "Marlon SNAPS After This Loss #marlon #rage"

THE 9 PROVEN FORMULAS (pick the ONE that fits the moment best):

1. REACTION FRAME (most common viral pattern for streamer clips):
   "[Streamer] reacts to [specific thing]"
   "[Streamer] sees [X] for the first time"
   Example: "DDG reacts to his ex showing up"

2. CONFRONTATION / VS:
   "[Streamer A] vs [Streamer B] gets heated"
   "[Streamer] wasn't having it"
   Example: "Marlon and Yourrage almost fought"

3. SPECIFIC QUOTE TEASE (don't reveal the line, frame it):
   "[Streamer] said WHAT about [topic]?!"
   "[Streamer]'s response shocked everyone"
   Example: "Lacy's answer left chat speechless"

4. EMOTIONAL PEAK:
   "[Streamer] lost it after this"
   "[Streamer] crashed out live"
   Example: "Jynxzi snapped after this loss"

5. SHOCK / REVEAL:
   "[Streamer] didn't realize the camera caught this"
   "Nobody expected [Streamer] to do this"

6. CATCHPHRASE + CONTEXT (when streamer has signature phrase):
   "[Catchphrase] moment that broke [streamer]"
   Example: "SIEGE moment that broke Jynxzi"

7. NUMBER + SPECIFIC:
   "5 seconds that changed [streamer]'s stream"
   "[$amount] bet goes wrong"

8. POV / DIRECT ADDRESS:
   "POV: you're [streamer]'s chat right now"
   "When [streamer] realizes [outcome]"

9. HOOK REPETITION (advanced - boosts retention by ~15%):
   If the first 5 seconds of the transcript contains a strong line, lead the title with words that ECHO that line. The viewer's brain confirms "yes this is what I clicked for" within the critical 2-second retention window.
   Example: transcript opens "Bro you're not gonna believe..." → title "Bro you're not gonna believe what just happened"

GENERAL RULES
- Streamer name: include it, capitalized correctly. (DDG, Marlon, Jasontheween, Lacy, Jaycinco, Deshaefrost, Jynxzi.)
- Caps: 1-3 power words ONLY. Never all-caps the whole title.
- Power verbs: LOSES IT, SNAPS, GOES OFF, CRASHES OUT, EXPOSES, SPEECHLESS, BREAKS DOWN, CATCHES, SHUTS DOWN, CAUGHT, RIPS INTO, COOKED.
- '...' allowed for cliffhangers if it fits naturally.
- Emojis: max 1 at end if it adds emotion (😳 💀 😂 🔥). Never lead with emoji. Better to skip.
- Quote/curiosity: tease the moment, don't spoil it.
- HONEST: title's promise must match what the clip actually delivers. 2026 algorithm penalizes high-CTR-low-retention as misleading packaging.

DO NOT
- Start with: "Watch", "This", "When", "You won't believe" (over-used, low CTR in 2026 data).
- Wrap title in quotation marks.
- Lie or over-promise.
- Use MORE than 2 hashtags in the title. Save the rest for the description.
- Place hashtags anywhere except at the very end of the title.

DESCRIPTION RULES (the description field on YouTube/TikTok)

The description has 3 jobs in 2026:
  1. First 3 hashtags appear ABOVE the title on YouTube feeds - free real estate.
  2. First 100 chars are visible above the "...more" cut on YouTube.
  3. TikTok uses the WHOLE caption as a search index. Keyword in first 150 chars.

REQUIRED FORMAT (3 blocks, blank lines between):

  #shorts #twitchclips #[streamer]

  [Curiosity hook line, 80-130 chars, includes streamer name + content type as keyword]

  [Optional 1-sentence context expansion]

  #twitch #streamerclips #[game-or-mood] #[2-3 niche tags]

EXAMPLES OF GOOD HOOK LINES (the middle block):
- "DDG didn't think anyone heard him say this on stream"
- "Jynxzi was 1 round away from a full clutch when this happened"
- "Marlon's teammate threw a 4v1 and he had to say it"
- "Lacy clocked the lie before he could finish the sentence"

HASHTAG STRATEGY (5-9 total, pyramid):
- 2 broad/discovery: #shorts #twitchclips (drop #fyp / #viral - 2026 data shows zero algorithmic boost)
- 2-3 niche: #[streamer] #twitch #streamerclips
- 2-3 ultra-niche: #[game] #[mood like funny/drama/rage] #[catchphrase if known]

DO NOT
- Beg for likes / subscribes (hurts retention signal).
- Spam hashtags (>10 looks like junk).
- Fake claims ("nobody saw this coming" if it was clearly anticipated).
- Long paragraphs - keep total description under 350 chars.
- Hashtags mid-sentence - they go in their own blocks at top and bottom.

OUTPUT FORMAT (return STRICT JSON, no markdown, no commentary):
{
  "post": <boolean>,
  "auto_upload": <boolean>,
  "viral_score": <number 1-10>,
  "hook_score": <number 1-10>,
  "context_score": <number 1-10, where 10 means no context needed>,
  "pacing_score": <number 1-10>,
  "retention_score": <number 1-10, does the clip stay engaging the whole length and end cleanly>,
  "category": "<one viral category>",
  "start_second": <number, seconds into source>,
  "end_second": <number, seconds into source>,
  "clip_length_seconds": <number, end-start>,
  "title": "<string, follows TITLE RULES above>",
  "backup_titles": ["<string>", "<string>", "<string>"],
  "description": "<FULL formatted description ready to paste into YouTube/TikTok, INCLUDES the hashtag blocks at top and bottom per DESCRIPTION RULES format>",
  "hashtags": ["<#tag>", ...],
  "hook_overlay": "<short 3-8 word teaser burned onto the first 2 seconds of the video>",
  "reason": "<short explanation>",
  "reject_reason": "<string or null>"
}

HOOK_OVERLAY RULES (critical for retention):
- 3-8 words, ALL CAPS or Sentence Case (you decide which fits the moment).
- Burned onto the first 2 seconds of the video as a text overlay.
- Job: stop scroll in 0.5 seconds. Promise the payoff. Make the viewer wait.
- ASCII only. NO emojis, NO quotes, NO colons, NO backslashes (ffmpeg escaping).
- Examples (match the energy):
  "Wait until DDG hears this"
  "Marlon snapped after this"
  "She had no idea"
  "He didnt see it coming"
  "Watch his face change"
  "This broke the chat"
  "DDG was NOT ready"
- Different from title: title = the click hook. Overlay = the 0-2 sec scroll-stopper.

NOTE on description vs hashtags fields:
- "description" is the COMPLETE pastable block (hashtag-line + hook + optional expansion + hashtag-line). Ready to copy into the YouTube/TikTok description field as-is.
- "hashtags" is the same hashtags listed flat as an array (each starts with #) for dashboard display + analytics. The hashtags inside description and inside this array should be the SAME 5-9 tags.

RETENTION_SCORE rubric (1-10):
- 1-3: peters out, dead air at end, no payoff, viewer drops off mid-clip.
- 4-5: front-loaded but loses energy halfway, ending is weak.
- 6-7: mostly holds attention, has a payoff, ending is acceptable.
- 8-9: stays engaging throughout, builds to a clean payoff, ends on a strong beat.
- 10: rewatchable, every second earns its place.

DECISION LOGIC
- auto_upload = true ONLY IF viral_score >= 8.3 AND hook_score >= 8 AND context_score >= 7 AND pacing_score >= 7 AND retention_score >= 7.5.
- post = false IF viral_score < 7.5 OR hook_score < 7 OR context_score < 6 OR retention_score < 7.
- If unsure, set post = false.

FINAL INSTRUCTION: Return only valid JSON. No markdown. No commentary. No extra text."""

        # User message = the runtime inputs as a single JSON object
        inputs_obj = {
            "streamer_name": streamer,
            "raw_clip_duration_seconds": round(total_dur, 1),
            "transcript": transcript[:3500],
            "whisper_segments": seg_data,
            "chat_sample": chat_sample[:20] if isinstance(chat_sample, list) else [],
            "chat_velocity": chat_velocity,
            "detected_at_second": round(spike_offset, 1),
            "silence_boundaries_for_clean_cuts": silence_data,
            "optional_context": profile,
        }
        # If we ran the Claude vision pass, include the visual description.
        # The decide prompt treats this as ground truth - title MUST match
        # both audio AND visual evidence.
        if visual_context:
            inputs_obj["visual_context_from_vision_pass"] = visual_context

        try:
            resp = await self._ai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(inputs_obj, ensure_ascii=False)},
                ],
                max_tokens=900,
                temperature=0.4,
                response_format={"type": "json_object"},
            )
            raw = (resp.choices[0].message.content or "").strip()
            data = json.loads(raw)

            # Server-side enforcement of decision_logic in case GPT misjudged its
            # own threshold output. We trust GPT's scores but recompute booleans.
            v = float(data.get("viral_score", 0))
            h = float(data.get("hook_score", 0))
            c = float(data.get("context_score", 0))
            p = float(data.get("pacing_score", 0))
            r = float(data.get("retention_score", 0))
            data["post"] = (v >= 7.5 and h >= 7 and c >= 6 and r >= 7)
            # Auto-upload bar tightened: viral 9+ AND hook 8.5+ AND clip length
            # at least 12 seconds (anything shorter than that is a meme snippet,
            # not a clip worth posting). This catches the AI rounding clips down
            # to 6-11 seconds and giving them inflated scores.
            try:
                end_s = float(data.get("end_second", 0)); start_s = float(data.get("start_second", 0))
                length_ok = (end_s - start_s) >= 12.0
            except Exception:
                length_ok = True  # fallback if start/end weren't returned
            data["auto_upload"] = (
                data["post"]
                and v >= 9.0 and h >= 8.5 and c >= 7 and p >= 7 and r >= 7.5
                and length_ok
            )
            return data
        except Exception:
            log.exception("gpt_decide failed")
            return None
