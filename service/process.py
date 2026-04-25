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

            # 5. ONE unified GPT call: scores, decides post/auto_upload,
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
            description = (decision.get("description") or "").strip()[:600]
            hashtags_list = decision.get("hashtags") or []
            hashtags = " ".join([str(h).strip() for h in hashtags_list if h])[:300]

            log.info(
                "processor[%s]: decision post=%s auto=%s viral=%.1f hook=%.1f ctx=%.1f pace=%.1f ret=%.1f cat=%s",
                streamer, post, auto_upload, viral_score, hook_score, context_score, pacing_score, retention_score, category,
            )
            if reason:
                log.info("processor[%s]: reason=%s", streamer, reason[:200])

            # Per-streamer daily cap: max 4 auto-approvals per streamer per UTC day.
            # Prevents one chatty stream from flooding the dashboard's auto-promote queue.
            # Excess high-quality clips drop to status=ready for manual review.
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

            # 10. Burn captions
            srt_escaped = str(srt).replace('\\', '/').replace(':', '\\:').replace(',', '\\,')
            # FontSize bumped to 20 (from 16) for clearer read on mobile.
            # MarginV raised to 320 (from 260) so captions sit well above the
            # TikTok/Shorts/Reels bottom UI (like/share/username chrome) and
            # also gives the 2-line caption block clear headroom so the top
            # line isn't clipped by anything.
            style = (
                "FontName=Arial Black,FontSize=20,"
                "PrimaryColour=&HFFFFFF,OutlineColour=&H000000,BackColour=&H00000000,"
                "BorderStyle=1,Outline=3,Shadow=0,"
                "Alignment=2,MarginV=320"
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
                          silences: list[tuple[float, float]]) -> dict | None:
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

TITLE RULES
- Style: short, curiosity-driven, emotional, human.
- Length: 35-70 characters preferred.
- Avoid: ALL CAPS unless one word for emphasis, clickbait that lies, too much explanation, generic titles, hashtags in title.
- Formula: moment first then streamer; curiosity first then context; emotion first then explanation.
- Examples of vibe:
  "This got weird FAST on [streamer]'s stream"
  "Chat caught it before [streamer] did"
  "[Streamer] instantly regretted this"
  "He exposed himself live on stream"
  "This made the whole chat lose it"

DESCRIPTION RULES
- Style: clean, short, optimized for Shorts.
- Must include: one curiosity sentence, streamer name naturally if relevant, 3-6 relevant hashtags.
- Avoid: long paragraphs, fake claims, begging for likes, too many hashtags, spammy keywords.
- Hashtag pool: #shorts #twitch #streamer #streamerclips #twitchclips #gaming #viralclips #funnyclips (mix in streamer-specific tags too).

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
  "title": "<string>",
  "backup_titles": ["<string>", "<string>", "<string>"],
  "description": "<string>",
  "hashtags": ["<string>", ...],
  "reason": "<short explanation>",
  "reject_reason": "<string or null>"
}

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
            data["auto_upload"] = data["post"] and (v >= 8.3 and h >= 8 and c >= 7 and p >= 7 and r >= 7.5)
            return data
        except Exception:
            log.exception("gpt_decide failed")
            return None
