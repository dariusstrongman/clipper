"""One-shot script to regenerate title/backup_titles/description/hashtags
on EXISTING clipper_clips rows using the 2026 viral prompt rules. Does
NOT re-run Whisper or media processing - just refreshes the AI metadata
from the transcript we already have on disk.

Usage (on EC2 inside the venv):
    cd ~/clipper
    source venv/bin/activate
    python -m scripts.regenerate_titles                # ready clips only
    python -m scripts.regenerate_titles --approved     # also approved
    python -m scripts.regenerate_titles --uploaded     # also uploaded
    python -m scripts.regenerate_titles --all          # everything that has a transcript
    python -m scripts.regenerate_titles --limit 5      # cap for testing
    python -m scripts.regenerate_titles --dry-run      # print, don't update
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from pathlib import Path

# Make `service.*` imports work when run from the clipper repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from openai import AsyncOpenAI
from service import config
from service.db import Supabase
from service.process import _streamer_context, STREAMER_PROFILES

log = logging.getLogger("regen")

# Focused regeneration prompt - same TITLE/DESCRIPTION rules as the unified
# _gpt_decide prompt, but we already have the scores/category/decision so we
# only ask for the strings.
SYSTEM_PROMPT = """You rewrite the title + description + hashtags for a Twitch clip that has already been scored and approved for posting. Use 2026 viral data: VidIQ findings, top Twitch clip channel patterns, YouTube Shorts + TikTok algorithm changes.

Return STRICT JSON only:
{
  "title": "<string>",
  "backup_titles": ["<string>", "<string>", "<string>"],
  "description": "<full pastable block ready for YouTube/TikTok description field>",
  "hashtags": ["<#tag>", ...]
}

================ TITLE RULES ================

LENGTH: 30-50 chars optimal (YouTube Shorts feed truncates at ~40). Hard max 70.

PICK ONE OF THESE 9 PROVEN FORMULAS:
1. REACTION FRAME: "[Streamer] reacts to [specific thing]"
2. CONFRONTATION/VS: "[A] vs [B] gets heated"
3. SPECIFIC QUOTE TEASE: "[Streamer] said WHAT about [topic]?!"
4. EMOTIONAL PEAK: "[Streamer] crashed out / lost it / snapped after this"
5. SHOCK/REVEAL: "Nobody expected [Streamer] to do this"
6. CATCHPHRASE + CONTEXT: "[Catchphrase] moment that broke [Streamer]"
7. NUMBER + SPECIFIC: "5 seconds that changed [Streamer]'s stream"
8. POV / DIRECT ADDRESS: "POV: you're [Streamer]'s chat right now"
9. HOOK REPETITION: title echoes the first 5 sec of the transcript verbatim or near-verbatim (boosts retention ~15%).

GENERAL TITLE RULES:
- Streamer name: include it, capitalized correctly (DDG, Marlon, Jasontheween, Lacy, Jaycinco, Deshaefrost, Jynxzi).
- Caps: 1-3 power words ONLY. Never the whole title.
- Power verbs: LOSES IT, SNAPS, GOES OFF, CRASHES OUT, EXPOSES, SPEECHLESS, BREAKS DOWN, SHUTS DOWN, CAUGHT, COOKED, RIPS INTO.
- '...' allowed for cliffhangers.
- Emojis: max 1 at end (😳 💀 😂 🔥). Never lead with emoji.
- HONEST: title's promise must match what the clip delivers. 2026 algorithm penalizes high-CTR + low-retention as misleading.

DO NOT START WITH: "Watch", "This", "When", "You won't believe" (over-saturated, low CTR in 2026 data).
DO NOT use hashtags inside the title.
DO NOT wrap title in quotation marks.

============== DESCRIPTION RULES ==============

The description has 3 jobs in 2026:
1. First 3 hashtags appear ABOVE the title on YouTube feeds (free real estate).
2. First 100 chars are visible above the "...more" cut.
3. TikTok 2026 indexes the entire caption + audio transcript + on-screen text - keyword in first 150 chars matters.

REQUIRED FORMAT (3 blocks, blank lines between):
  #shorts #twitchclips #[streamer]

  [Curiosity hook line, 80-130 chars, includes streamer name + content type as keyword]

  [Optional 1-sentence context expansion]

  #twitch #streamerclips #[mood/game] #[2-3 niche tags]

GOOD HOOK LINE EXAMPLES:
- "DDG didn't think anyone heard him say this on stream"
- "Jynxzi was 1 round away from a full clutch when this happened"
- "Marlon's teammate threw a 4v1 and he had to say it"
- "Lacy clocked the lie before he could finish the sentence"

HASHTAG STRATEGY (5-9 total):
- 2 broad/discovery: #shorts #twitchclips
- 2-3 niche: #[streamer] #twitch #streamerclips
- 2-3 ultra-niche: #[game-or-mood] #[catchphrase if known]
DROP #fyp / #viral - 2026 data confirms zero algorithmic boost.

DO NOT:
- Beg for likes / subscribes (hurts retention).
- Spam hashtags (>10 looks like junk).
- Include fake claims.
- Long paragraphs - keep total description under 350 chars.

The "hashtags" array in your output should contain the SAME tags that appear inside the description (the union of the top + bottom blocks)."""


async def regenerate_one(ai: AsyncOpenAI, clip: dict) -> dict | None:
    streamer = clip.get("streamer", "")
    transcript = (clip.get("transcript") or "").strip()
    category = clip.get("category") or ""
    score = clip.get("score")
    if not transcript:
        return None

    profile = _streamer_context(streamer)
    name_cap = {
        "ddg": "DDG", "marlon": "Marlon", "jasontheween": "Jasontheween",
        "lacy": "Lacy", "jaycinco": "Jaycinco", "deshaefrost": "Deshaefrost",
        "jynxzi": "Jynxzi",
    }.get((streamer or "").lower(), streamer)

    user = (
        f"{profile}\n"
        f"Streamer name (use this casing in title): {name_cap}\n"
        f"Category: {category or '(unknown)'}\n"
        f"Viral score (already determined): {score}\n\n"
        f"Clip transcript (what actually happens):\n{transcript[:2000]}"
    )

    try:
        resp = await ai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user},
            ],
            max_tokens=600,
            temperature=0.6,
            response_format={"type": "json_object"},
        )
        raw = (resp.choices[0].message.content or "").strip()
        return json.loads(raw)
    except Exception as e:
        log.error("regenerate failed for %s: %s", clip.get("id"), e)
        return None


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--approved", action="store_true", help="also include status=approved clips")
    p.add_argument("--uploaded", action="store_true", help="also include status=uploaded clips")
    p.add_argument("--all", action="store_true", help="all of ready/approved/uploaded")
    p.add_argument("--limit", type=int, default=200)
    p.add_argument("--dry-run", action="store_true", help="don't write changes")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = config.load()
    if not cfg.openai_api_key:
        print("OPENAI_API_KEY missing in .env")
        return
    ai = AsyncOpenAI(api_key=cfg.openai_api_key)

    statuses = ["ready"]
    if args.approved or args.all:
        statuses.append("approved")
    if args.uploaded or args.all:
        statuses.append("uploaded")
    status_filter = "in.(" + ",".join(statuses) + ")"

    async with Supabase(cfg.supabase_url, cfg.supabase_service_key) as db:
        clips = await db.select(
            "clipper_clips",
            f"status={status_filter}&order=created_at.desc&limit={args.limit}"
            f"&select=id,streamer,transcript,category,score,title,description,status",
        )
        print(f"Found {len(clips)} clips with status in {statuses}")

        # Filter to ones with transcripts
        clips = [c for c in clips if (c.get("transcript") or "").strip()]
        print(f"  {len(clips)} have transcripts")

        if not clips:
            return

        for i, clip in enumerate(clips, start=1):
            old_title = clip.get("title") or "(none)"
            print(f"\n[{i}/{len(clips)}] {clip['streamer']:14s} ({clip['status']}) -- old: {old_title[:60]!r}")

            result = await regenerate_one(ai, clip)
            if not result:
                print("    !! AI failed, skipping")
                continue

            new_title = (result.get("title") or "").strip().strip('"').strip("'")
            new_title = re.sub(r"^[\W_]+", "", new_title)[:140]
            backup_titles = result.get("backup_titles") or []
            backup_titles = [str(t).strip()[:140] for t in backup_titles if t][:3]
            description = (result.get("description") or "").strip()[:1000]
            hashtags_list = result.get("hashtags") or []
            hashtags = " ".join([str(h).strip() for h in hashtags_list if h])[:300]

            print(f"    new: {new_title!r} ({len(new_title)} chars)")

            if args.dry_run:
                print("    [dry-run] would write 4 fields")
                continue

            try:
                await db.update(
                    "clipper_clips",
                    f"id=eq.{clip['id']}",
                    {
                        "title": new_title,
                        "backup_titles": backup_titles,
                        "description": description,
                        "hashtags": hashtags,
                    },
                )
                print("    written")
            except Exception as e:
                print(f"    !! db update failed: {e}")

            # rate limit so we don't hammer OpenAI
            await asyncio.sleep(0.5)

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
