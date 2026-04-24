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

            # 2b. AI content classification. Rejects boring clips and spam BEFORE
            # we spend CPU on vertical reformat + captioning.
            chat_sample = await self._fetch_chat_sample(row.get("spike_id"))
            score, category, reason = await self._gpt_classify(streamer, transcript, chat_sample)
            log.info("processor[%s]: %s score=%d category=%s reason=%s",
                     streamer, src.name, score, category, reason[:80])
            if score < self.cfg.clip_min_score:
                # Delete the raw source mp4 - rejected clips are pure DB metadata
                # from here on (transcript + score + reason are enough for audit).
                try: src.unlink(missing_ok=True)
                except Exception: pass
                await self.db.update(
                    "clipper_clips", f"id=eq.{clip_id}",
                    {
                        "status": "rejected",
                        "transcript": transcript,
                        "score": score,
                        "category": category,
                        "score_reason": reason,
                        "source_path": None,
                    },
                )
                try: audio.unlink()
                except Exception: pass
                try: srt.unlink(missing_ok=True)
                except Exception: pass
                return

            # 3. Detect silence boundaries in source. GPT will be told to snap cuts
            #    to these, and we'll force-snap after its pick as a safety net.
            silences = await _detect_silences(src, noise_db=-28, min_dur=0.3)
            log.info("processor[%s]: detected %d silence gaps", streamer, len(silences))

            # 4. Pick the actual viral window using GPT (now silence-aware, streamer-aware).
            def _seg(attr, d, default=0):
                return (getattr(d, attr, None) if not isinstance(d, dict) else d.get(attr, default)) or default
            total_dur = max([float(_seg("end", s, 0)) for s in segments] + [0.0])
            if total_dur < 5:
                total_dur = float(row.get("duration_sec") or 30)
            spike_offset = float(self.cfg.clip_pre_seconds)
            raw_start, raw_end = await self._gpt_pick_range(
                streamer, segments, total_dur, spike_offset, transcript, silences
            )

            # 5. Force-snap to silence boundaries so cuts never land mid-word.
            pick_start, pick_end = _snap_boundaries(raw_start, raw_end, silences, total_dur, tol=3.0)
            if abs(pick_start - raw_start) > 0.2 or abs(pick_end - raw_end) > 0.2:
                log.info("processor[%s]: snap [%.2f-%.2f] -> [%.2f-%.2f]",
                         streamer, raw_start, raw_end, pick_start, pick_end)
            pick_dur = max(6.0, min(60.0, pick_end - pick_start))
            pick_end = pick_start + pick_dur
            log.info("processor[%s]: final pick [%.1f-%.1fs] from %.1fs source (len=%.1fs)",
                     streamer, pick_start, pick_end, total_dur, pick_dur)

            # 4. Build SRT from segments within the pick window, times shifted so 0.0 = pick_start.
            srt_lines: list[str] = []
            kept = 0
            for seg in segments:
                s = float(_seg("start", seg, 0))
                e = float(_seg("end", seg, s + 2))
                text = _seg("text", seg, "") or ""
                # Skip segments entirely outside the window
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

            # 5. Vertical reformat + trim in one ffmpeg pass.
            # -ss AFTER -i = frame-accurate seek (slightly slower than pre-input seek,
            # but avoids keyframe drift so SRT timing stays aligned).
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

            # 7. Thumbnail at 40% through the clip
            await _run_cmd(
                f'ffmpeg -y -hide_banner -loglevel error '
                f'-ss {pick_dur * 0.4:.2f} -i {shlex.quote(str(final))} '
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

            # Drop the raw source - we have the final vertical+captioned mp4 now.
            try: src.unlink(missing_ok=True)
            except Exception: pass

            await self.db.update(
                "clipper_clips", f"id=eq.{clip_id}",
                {
                    "processed_path": str(final),
                    "thumbnail_path": str(thumb),
                    "transcript": transcript,
                    "title": title,
                    "hashtags": hashtags,
                    "score": score,
                    "category": category,
                    "score_reason": reason,
                    "duration_sec": round(pick_dur, 1),
                    "source_path": None,
                    "status": "ready",
                },
            )
        except Exception as e:
            log.exception("processor[%s]: pipeline exception", streamer)
            await self.db.update(
                "clipper_clips", f"id=eq.{clip_id}",
                {"status": "failed", "error": str(e)[:300]},
            )

    async def _fetch_chat_sample(self, spike_id: str | None) -> list[str]:
        """Pull the chat sample recorded at the spike for classifier context."""
        if not spike_id:
            return []
        try:
            rows = await self.db.select(
                "clipper_spikes",
                f"id=eq.{spike_id}&select=sample_messages",
            )
            if not rows:
                return []
            raw = rows[0].get("sample_messages") or "[]"
            return json.loads(raw)
        except Exception:
            log.exception("fetch chat sample failed")
            return []

    async def _gpt_classify(self, streamer: str, transcript: str,
                            chat_sample: list[str]) -> tuple[int, str, str]:
        """Score a clip 1-10 + categorize based on transcript + chat reaction.
        Streamer-aware: knows each streamer's content style so it applies the
        right rubric (a Marlon goal reaction needs different scoring than a
        DDG music moment)."""
        if not self._ai:
            return 5, "unknown", "classifier disabled (no API key)"
        profile = _streamer_context(streamer)
        system = (
            "You evaluate short Twitch clips for viral potential on TikTok / YouTube Shorts.\n"
            "Return STRICT JSON only:\n"
            '{"score": <1-10 int>, "category": "<funny|drama|reaction|skill|announcement|boring|spam>", "reason": "<one sentence>"}\n'
            "\n"
            "SCORING PHILOSOPHY:\n"
            "Imagine a stranger scrolling TikTok. Would they stop? Rewatch? Send it to a friend?\n"
            "Score the MOMENT's substance, not the chat reaction volume.\n"
            "\n"
            "- 1-3: boring. Filler chatter, raid/sub hype with no content, silence, unintelligible audio, "
            "bot spam, menu navigation, starting-soon noise.\n"
            "- 4-5: weak. Small reaction, minor joke, chat excited but transcript carries nothing new.\n"
            "- 6-7: solid. Real punchline, actual drama beat, genuine reaction with context, "
            "a line worth hearing.\n"
            "- 8-9: strong clip. Laugh-out-loud line, shocking statement, clean skill moment, "
            "real-stakes drama beat.\n"
            "- 10: rare legend. The kind of clip that gets reposted across fan pages for weeks.\n"
            "\n"
            "SKEPTICISM RULES:\n"
            "- Chat typing 'KEKW/LOL/LETSGO/W' alone is NOT a moment. The transcript must carry it.\n"
            "- 'Starting soon', sub bombs, raid intros, menu talk = reject (score 1-3).\n"
            "- Whisper hallucinations like 'Thanks for watching', 'Subscribe to my channel', "
            "'Bye bye' repeated in a silent or music clip = reject (score 1-2, category spam).\n"
            "- Genuine varied chat reactions (mix of emotes, different words) = signal of real moment.\n"
            "- All chat saying same emoji repeatedly = possible bot raid or sub bomb, be skeptical.\n"
        )
        user = (
            f"{profile}\n"
            f"Transcript of the clip:\n{transcript.strip() or '(silent/unintelligible)'}\n\n"
            f"Chat reactions during the moment (up to 15 messages):\n"
            + "\n".join(f"- {m}" for m in chat_sample[:15])
        )
        try:
            resp = await self._ai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                max_tokens=120,
                temperature=0.2,
                response_format={"type": "json_object"},
            )
            raw = (resp.choices[0].message.content or "").strip()
            data = json.loads(raw)
            score = int(data.get("score", 5))
            score = max(1, min(10, score))
            category = str(data.get("category", "unknown"))[:30]
            reason = str(data.get("reason", ""))[:280]
            return score, category, reason
        except Exception as e:
            log.exception("gpt classify failed")
            return 5, "unknown", f"classifier error: {type(e).__name__}"

    async def _gpt_pick_range(self, streamer: str, segments, total_dur: float,
                              spike_offset: float, transcript: str,
                              silences: list[tuple[float, float]]) -> tuple[float, float]:
        """Ask GPT where the actual viral moment starts and ends inside the source.
        Returns (start_sec, end_sec) relative to the source file. Caller applies
        silence-snap as a safety net after this returns. Falls back to a window
        around the chat spike if GPT fails."""
        fb_start = max(0.0, spike_offset - 20.0)
        fb_end = min(total_dur, spike_offset + 22.0)
        if not self._ai or not segments:
            return fb_start, fb_end

        # Transcript with segment timestamps
        lines = []
        for seg in segments:
            s = float(seg.start if hasattr(seg, "start") else seg.get("start", 0))
            e = float(seg.end if hasattr(seg, "end") else seg.get("end", s + 2))
            t = (seg.text if hasattr(seg, "text") else seg.get("text", "")) or ""
            t = t.strip()
            if not t:
                continue
            lines.append(f"[{s:.1f}-{e:.1f}] {t}")
        transcript_timed = "\n".join(lines)[:4000]

        # Silence map: top 25 pauses, sorted by start time. GPT snaps to these.
        silence_lines = []
        for s_start, s_end in silences[:25]:
            silence_lines.append(f"  gap {s_start:.2f}s -> {s_end:.2f}s (dur {s_end-s_start:.2f}s)")
        silence_block = "\n".join(silence_lines) if silence_lines else "  (no significant pauses detected - music or continuous speech)"

        profile = _streamer_context(streamer)

        system = (
            "You pick IN/OUT times for the punchiest stand-alone moment inside a Twitch clip "
            "that will be posted to TikTok / YouTube Shorts.\n\n"
            "Return STRICT JSON only: {\"start\": <float>, \"end\": <float>}\n\n"
            "RULES (priority order, rule 1 is non-negotiable):\n"
            "1. NEVER CUT MID-WORD OR MID-SENTENCE. Snap start/end to the timestamps of "
            "Whisper segment boundaries OR to the silence gaps provided below. Prefer silence gaps.\n"
            "2. Include SETUP + PAYOFF. A punchline with no setup is dead. A setup with no payoff "
            "is worse. If the moment needs 40 seconds to land, give it 40 seconds.\n"
            "3. The FIRST 2 SECONDS of the clip determine if someone keeps watching. They must "
            "contain strong content: voice with energy, a hook, or an attention-grabbing statement. "
            "NEVER start with dead air, filler words ('um', 'okay so'), or long pauses. If the "
            "natural moment opens with filler, trim into the real start.\n"
            "4. Length: 8-55 seconds. Prefer 15-45s for moments with setup+payoff. Go 8-15s "
            "only for pure standalone reactions or one-liner punchlines with no setup needed.\n"
            "5. Chat spike is at ~spike_offset. Twitch streams are delayed ~15-20s versus what "
            "chat sees, so the ACTUAL on-stream moment is usually slightly BEFORE the spike. "
            "Don't center blindly on spike_offset.\n"
            "6. If a sentence crosses the in/out boundaries, either include the whole sentence "
            "or start/end before it begins. No half-sentences."
        )
        user = (
            f"{profile}\n"
            f"Source total duration: {total_dur:.1f}s\n"
            f"Chat spike at approximately: {spike_offset:.1f}s into source (actual moment likely slightly earlier)\n\n"
            f"TRANSCRIPT with segment timestamps (snap to these boundaries if unsure):\n{transcript_timed}\n\n"
            f"SILENCE GAPS (natural pause boundaries — IDEAL snap points for cuts):\n{silence_block}\n\n"
            f"Full transcript text for reference:\n{transcript[:1500]}"
        )
        try:
            resp = await self._ai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                max_tokens=60,
                temperature=0.2,
                response_format={"type": "json_object"},
            )
            raw = (resp.choices[0].message.content or "").strip()
            data = json.loads(raw)
            start = float(data.get("start", fb_start))
            end = float(data.get("end", fb_end))
            start = max(0.0, min(start, total_dur - 6.0))
            end = max(start + 6.0, min(end, total_dur))
            if end - start > 55.0:
                end = start + 55.0
            return start, end
        except Exception:
            log.exception("gpt pick_range failed, using fallback window")
            return fb_start, fb_end

    async def _gpt_title(self, streamer: str, transcript: str) -> str:
        """Clickbait-style short-form title using 2026 hook formulas proven
        to drive 70%+ 3-second retention on TikTok / YouTube Shorts."""
        if not self._ai:
            return ""
        profile = _streamer_context(streamer)
        # Canonical-casing map so names come out correct regardless of login case
        name_cap = {
            "ddg": "DDG", "marlon": "Marlon", "jasontheween": "Jasontheween",
            "lacy": "Lacy", "jaycinco": "Jaycinco", "deshaefrost": "Deshaefrost",
            "jynxzi": "Jynxzi",
        }.get((streamer or "").lower(), streamer)
        try:
            resp = await self._ai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content":
                        "You write CLICKBAIT TikTok / YouTube Shorts titles for Twitch clips. "
                        "Your ONLY job is maximum 3-second retention and click-through.\n"
                        "\n"
                        "PROVEN HOOK FORMULAS (pick whichever fits best):\n"
                        "- Pattern interrupt: '[Streamer] SAID WHAT About [topic]?!'\n"
                        "- Curiosity gap: 'Nobody Expected [Streamer] To Do This...'\n"
                        "- Shock reveal: '[Streamer] DIDNT Realize The Camera Caught This'\n"
                        "- Reaction frame: '[Streamer] Reacts To [specific thing] And HE WASNT READY'\n"
                        "- Drama tease: '[Streamer] vs [target] Just Got HEATED'\n"
                        "- Specificity + action: '[Streamer] [specific action] Right In Front Of [audience]'\n"
                        "- Emotional peak: '[Streamer] CRASHED OUT / LOST IT / SNAPPED After This'\n"
                        "- Identity: 'This Is Why [Streamer] Is Trending Right Now'\n"
                        "\n"
                        "RULES:\n"
                        "- 40-85 characters total. Sweet spot is 55-70.\n"
                        "- Streamer name MUST be in the title, capitalized correctly.\n"
                        "- Use ALL CAPS on 1-3 key POWER WORDS only. Never the whole title.\n"
                        "- Power verbs: LOSES IT, SNAPS, GOES OFF, CRASHES OUT, DESTROYS, EXPOSES, "
                        "SPEECHLESS, BREAKS DOWN, CATCHES, HUMBLES, RIPS INTO, SHUTS DOWN, CAUGHT, WENT OFF.\n"
                        "- '...' allowed for cliffhangers.\n"
                        "- Do NOT quote what was said verbatim. Tease, don't spoil.\n"
                        "- Do NOT use emojis, hashtags, or wrap the title in quotes.\n"
                        "- Never start with generic openers: 'Watch', 'This', 'When', 'You Won't Believe'.\n"
                        "- Be SPECIFIC where possible. 'DDG got into it with a fan in the front row' beats "
                        "'DDG got into it with someone'.\n"
                        "\n"
                        "VIBE EXAMPLES (do not copy, match the energy):\n"
                        "  DDG DID NOT HOLD BACK On His Ex Live On Stream...\n"
                        "  Marlon SNAPS After Teammate Throws The Game\n"
                        "  Lacy CAUGHT Him Lying Right On Camera\n"
                        "  Jasontheween SHUT HER DOWN In 4 Words\n"
                        "  Jaycinco And Yourrage GOT INTO IT Mid-Gym Session\n"
                        "  Deshaefrost CRASHED OUT Over This Clutch Fail\n"
                        "\n"
                        "Respond with ONLY the title. Nothing else."},
                    {"role": "user", "content":
                        f"{profile}\n"
                        f"Streamer name to use in title: {name_cap}\n\n"
                        f"What actually happens in the clip (full transcript):\n{transcript[:1500]}"},
                ],
                max_tokens=50,
                temperature=0.9,
            )
            title = (resp.choices[0].message.content or "").strip()
            title = title.strip('"').strip("'").strip()
            title = re.sub(r'^[\W_]+', '', title).strip()
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
