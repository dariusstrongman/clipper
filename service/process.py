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

            # 2b. AI content classification. Rejects boring clips and spam BEFORE
            # we spend CPU on vertical reformat + captioning.
            chat_sample = await self._fetch_chat_sample(row.get("spike_id"))
            score, category, reason = await self._gpt_classify(streamer, transcript, chat_sample)
            log.info("processor[%s]: %s score=%d category=%s reason=%s",
                     streamer, src.name, score, category, reason[:80])
            if score < self.cfg.clip_min_score:
                await self.db.update(
                    "clipper_clips", f"id=eq.{clip_id}",
                    {
                        "status": "rejected",
                        "transcript": transcript,
                        "score": score,
                        "category": category,
                        "score_reason": reason,
                    },
                )
                # Keep the raw source on disk so you can manually review if curious.
                try: audio.unlink()
                except Exception: pass
                try: srt.unlink(missing_ok=True)
                except Exception: pass
                return

            # 3. Pick the actual viral window from the captured source using GPT.
            # This gives every clip its own length - no more everything-is-30-sec.
            # Spike happens at offset ~= CLIP_PRE_SECONDS into src.
            def _seg(attr, d, default=0):
                return (getattr(d, attr, None) if not isinstance(d, dict) else d.get(attr, default)) or default
            total_dur = max([float(_seg("end", s, 0)) for s in segments] + [0.0])
            if total_dur < 5:
                total_dur = float(row.get("duration_sec") or 30)
            spike_offset = float(self.cfg.clip_pre_seconds)
            pick_start, pick_end = await self._gpt_pick_range(
                streamer, segments, total_dur, spike_offset, transcript
            )
            pick_dur = max(6.0, min(60.0, pick_end - pick_start))
            pick_end = pick_start + pick_dur
            log.info("processor[%s]: pick [%.1f-%.1fs] from %.1fs source (len=%.1fs)",
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
        """Score a clip 1-10 + categorize based on transcript + chat reaction."""
        if not self._ai:
            return 5, "unknown", "classifier disabled (no API key)"
        prompt = (
            f"Streamer: {streamer}\n\n"
            f"What was said in the clip (Whisper transcript):\n"
            f"{transcript.strip() or '(silent or unintelligible)'}\n\n"
            f"What chat typed during the moment (top 15 messages):\n"
            + "\n".join(f"- {m}" for m in chat_sample[:15])
        )
        system = (
            "You evaluate short Twitch clips for viral potential on TikTok and "
            "YouTube Shorts. Return STRICT JSON only, no prose:\n"
            '{"score": <1-10 int>, "category": "<funny|drama|reaction|skill|announcement|boring|spam>", "reason": "<one short sentence>"}\n'
            "\n"
            "Scoring guide:\n"
            "- 1-3: boring, bot spam, sub hype with no moment, no clear content, silent awkwardness.\n"
            "- 4-5: low-energy reaction, minor moment, chat hype with no payoff.\n"
            "- 6-7: solid moment - real funny line, genuine reaction, clear drama, mild surprise.\n"
            "- 8-9: strong clip-worthy content - big drama, hilarious line, shocking reveal.\n"
            "- 10: rare legendary moment.\n"
            "\n"
            "Be skeptical. Chat saying 'KEKW' or 'LETSGO' alone isn't a viral moment - the transcript needs real content. "
            "Reject raids, sub bombs, 'starting stream soon', or unintelligible audio."
        )
        try:
            resp = await self._ai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": prompt},
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
            # On classifier failure, be permissive: score 5 means "unknown" and
            # we'll let the threshold config decide whether to process.
            return 5, "unknown", f"classifier error: {type(e).__name__}"

    async def _gpt_pick_range(self, streamer: str, segments, total_dur: float,
                              spike_offset: float, transcript: str) -> tuple[float, float]:
        """Ask GPT where the actual viral moment starts and ends inside the source.
        Returns (start_sec, end_sec) relative to the source file. Falls back to
        a sensible window around the chat spike if GPT fails or returns garbage."""
        # Fallback: window around the chat spike (spike happens ~spike_offset into src)
        fb_start = max(0.0, spike_offset - 18.0)
        fb_end = min(total_dur, spike_offset + 20.0)
        if not self._ai or not segments:
            return fb_start, fb_end

        # Serialize transcript with timestamps so GPT can pick natural boundaries
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

        system = (
            "You pick the in/out points for the punchiest stand-alone moment in a "
            "Twitch clip that will be posted as a TikTok / YouTube Short.\n"
            "\n"
            "Rules:\n"
            "- Return STRICT JSON only: {\"start\": <float>, \"end\": <float>}\n"
            "- Both times are seconds relative to the SOURCE file start.\n"
            "- Length MUST be between 8 and 55 seconds. Prefer 15-35s.\n"
            "- NEVER cut mid-sentence. Start and end at natural pause/segment boundaries "
            "using the timestamps in the transcript. This is critical.\n"
            "- Include the setup if the punchline needs it. Include the reaction if it lands.\n"
            "- Don't cut off the moment early or start it in the middle of a word.\n"
            "- The chat spike happened around the given spike_offset - this usually means "
            "the actual moment is slightly BEFORE that (Twitch delay). Don't center blindly.\n"
        )
        user = (
            f"Streamer: {streamer}\n"
            f"Source total duration: {total_dur:.1f}s\n"
            f"Chat spike at approx: {spike_offset:.1f}s into source\n\n"
            f"Transcript with timestamps:\n{transcript_timed}\n\n"
            f"Full transcript text:\n{transcript[:1500]}"
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
            # Sanity clamp
            start = max(0.0, min(start, total_dur - 6.0))
            end = max(start + 6.0, min(end, total_dur))
            if end - start > 55.0:
                end = start + 55.0
            return start, end
        except Exception:
            log.exception("gpt pick_range failed, using fallback window")
            return fb_start, fb_end

    async def _gpt_title(self, streamer: str, transcript: str) -> str:
        """Clickbait-style short-form title."""
        if not self._ai:
            return ""
        try:
            resp = await self._ai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content":
                        "You write CLICKBAIT TikTok / YouTube Shorts titles for Twitch clips. "
                        "Your only goal is maximum click-through rate.\n"
                        "\n"
                        "Style rules:\n"
                        "- 40-85 characters. Under 85.\n"
                        "- Hook in the first 4 words. Curiosity, shock, drama, or beef.\n"
                        "- Power words allowed: INSANE, WILD, LOSES IT, GOES OFF, DESTROYED, EXPOSED, "
                        "SNAPS, BROKE, RIPS INTO, CRASHED OUT, CAUGHT, SHOCKED, SPEECHLESS.\n"
                        "- Use ALL CAPS on 1-3 key words only (not the whole title).\n"
                        "- Use ... for cliffhanger if it fits naturally.\n"
                        "- Do NOT quote what the streamer said word-for-word. Tease it.\n"
                        "- Do NOT use emojis. Do NOT use hashtags. Do NOT use quotation marks around the title.\n"
                        "- Do NOT start with 'Watch', 'This', or 'When' unless it slaps.\n"
                        "- Include the streamer name somewhere (first/last), capitalized correctly.\n"
                        "\n"
                        "Examples of the vibe:\n"
                        "  DDG DID NOT HOLD BACK ON THIS ONE...\n"
                        "  Marlon SNAPS After Chat Trolls Him On Stream\n"
                        "  Lacy GOES OFF On Viewer Mid-Stream Over This\n"
                        "  Jasontheween Was SPEECHLESS After Seeing This\n"
                        "\n"
                        "Respond with ONLY the title. Nothing else."},
                    {"role": "user", "content":
                        f"Streamer: {streamer}\n\nWhat actually happens in the clip:\n{transcript[:1500]}"},
                ],
                max_tokens=50,
                temperature=0.9,
            )
            title = (resp.choices[0].message.content or "").strip()
            # Strip any wrapping quotes GPT added
            title = title.strip('"').strip("'").strip()
            # Strip any leading emoji/punct noise
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
