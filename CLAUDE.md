# Clipper

Personal automated Twitch clipper for 6 streamers. Detects viral moments via chat spikes, extracts clips, AI-scores them for quality, picks variable-length cuts that don't chop voices mid-word, produces TikTok-ready vertical mp4s with burned-in captions and clickbait titles. Visible at `https://www.stromation.com/clipper` (admin password: `Kyomi123`). Auto-cleanup keeps disk usage flat.

## Current state (Apr 2026)

### Done

**Capture / detection layer:**
- [x] Monitor — polls Twitch Helix every 30s, logs live/offline transitions to `clipper_streams`. On startup, reuses any open stream row instead of creating duplicates.
- [x] Capture — `streamlink → ffmpeg` rolling 10-min segment buffer per live streamer (`buffers/<streamer>/seg_XXX.ts`, 30-sec segments, wraps after 20)
- [x] Chat spike detection — Twitch IRC over WebSocket (anonymous justinfan auth), triggers on ≥40 msgs in 5 sec with 90 sec cooldown
- [x] Clip extraction — on spike, ffmpeg pulls **70 sec** (35 pre + 35 post) out of buffer to `clips/<streamer>_<utc>.mp4`. Wide window gives the AI breathing room to pick a good in/out range later.
- [x] Streamers (6): `ddg, marlon, jasontheween, lacy, jaycinco, deshaefrost`

**Processing / AI layer:**
- [x] Whisper transcription with segment-level timestamps
- [x] GPT-4o-mini **classifier** — score 1-10 + category (funny/drama/reaction/skill/announcement/boring/spam). Skeptical rubric, rejects Whisper hallucinations and bot raid patterns. **Streamer-aware** via `STREAMER_PROFILES` so a Marlon goal scores by Marlon's rubric, not a generic streamer rubric.
- [x] GPT-4o-mini **pick_range** — picks variable in/out points (8-55s) from the source. Gets:
  - Whisper segment timestamps
  - **Silence map from `ffmpeg silencedetect`** — must snap to natural pauses
  - Streamer profile context
  - Chat-spike offset (with note that Twitch delay means actual moment is slightly earlier)
- [x] **Silence-snap safety net** — `_snap_boundaries` enforces silence-aligned cuts AFTER GPT picks, so even if GPT fails to snap perfectly, the final cut never lands mid-word. ±3 sec tolerance window.
- [x] Vertical reformat + burned-in captions — 1080×1920 with blurred-fill background, Arial Black 16pt, 260px bottom margin (clears TikTok UI)
- [x] **Clickbait title generation** — uses 2026-researched hook formulas (pattern interrupt, curiosity gap, shock reveal, drama tease, emotional peak, identity). Power verbs (LOSES IT, CRASHES OUT, SNAPS, etc.). Streamer name always capitalized correctly.
- [x] Hashtag generation
- [x] Thumbnail at 40% of clip duration
- [x] Real `duration_sec` saved to DB (matches actual final length, not the 30-sec default)

**Storage / ops:**
- [x] Auto-cleanup (`service/cleanup.py`) — runs hourly + once on startup
  - Source mp4 deleted immediately after processing (don't need it once final exists)
  - Rejected clips' files deleted 24h after rejection (DB row stays for audit)
  - Uploaded clips' files deleted 30d after upload (DB row stays for stats)
  - Failed clips' files deleted 48h after failure
  - Orphan files in `/clips` and `/processed` older than 48h with no DB row deleted (crash safety net)
  - Ready + approved clips never auto-deleted
- [x] Systemd unit with PATH including venv so streamlink resolves
- [x] Logs to `/mnt/clipper-storage/clipper/logs/service.log`

**Frontend / review layer (NEW this session):**
- [x] Admin dashboard at `https://www.stromation.com/clipper` (file: `Stromation/clipper.html` in the Stromation repo)
  - Password gate (`Kyomi123`) + `noindex`
  - Live streamers panel with placeholder cards for not-yet-streamed accounts
  - Ready-to-review queue with embedded `<video>` playback
  - Approve / Reject / Mark Uploaded buttons (PATCH Supabase directly)
  - Spike feed + rejected-by-AI section (collapsed by default)
  - 30s auto-refresh
- [x] **`clips.stromation.com`** — nginx on EC2 serving `/mnt/clipper-storage/clipper/` over HTTPS with HTTP basic auth (`darius` user, `htpasswd`-protected). Letsencrypt cert. Locked to `/clips/` and `/processed/` paths only — `/logs/` and other dirs not exposed.

### Not done yet (next-session priorities)

- [ ] **Twitch Clips API polling** — poll `/helix/clips?broadcaster_id=X&started_at=recent` every 2 min. Any clip with `view_count > 20` is a definitively-viral moment (other humans already validated it). Extract from our buffer using the clip's timestamp. Strongest signal possible.
- [ ] **Audio volume spike detection** — second trigger independent of chat. ffmpeg `astats` / `volumedetect` periodically on the rolling buffer. Catches loud reactions/laughter even when chat is slow. Cross-signal with chat for composite virality score.
- [ ] **Multi-signal composite virality score** — weighted combination of chat_velocity (0.3) + chat_decay (0.15) + audio_rms (0.25) + transcript_content (0.2) + visual (0.1). Tiered output: `score>80 auto-promote`, `60-80 review`, `<60 reject`. Replaces the current binary `>= CLIP_MIN_SCORE` cutoff.
- [ ] **Post-spike chat decay analysis** — measure chat rate for 60s after the spike. Sustained = real moment, crashed back = noise/raid. Cheap heuristic, big quality win.
- [ ] **Visual sanity check via Claude Sonnet vision** — sample 4-6 frames per clip, score visual engagement (face visible, expression intensity, on-screen text). Reject visually-dead clips. ~$0.01/clip, only on score ≥ 7.
- [ ] **Post-stream ranking** — collect all triggered clips per stream, rank by composite score, only auto-promote top 3-5. Prevents dashboard clutter on long streams.
- [ ] **YouTube Shorts auto-upload** (Data API v3 + OAuth on a dedicated channel). New `service/upload.py` polls for `status=approved`, uploads, sets `youtube_url`.
- [ ] **TikTok auto-upload** — official API is heavily restricted (requires whitelist), so manual upload from dashboard for now. Cookie-based unofficial APIs exist but break frequently.
- [ ] **Hook caption overlay** — burn 0-2s text overlay on the clip ("Wait for it...", "DDG ended his career here") for that scroll-stop layered hook pattern.
- [ ] **`MONITORED_STREAMERS` mirror sync** — currently the dashboard hardcodes the streamer list at `clipper.html` line 371. When `.env` `STREAMERS=` changes, that array also has to update. Could be auto-synced if the service writes the list to a Supabase row on startup.

## Tech stack

- **Python 3.12** asyncio service
- **Twitch Helix API** for stream status (client_credentials flow, auto-refresh)
- **Twitch IRC** over WebSocket for chat (anonymous `justinfan<random>`)
- **streamlink** + **ffmpeg** for stream capture
- **OpenAI Whisper** (`whisper-1`) for transcription
- **OpenAI GPT-4o-mini** for classification, pick_range, title, hashtags
- **ffmpeg silencedetect** for natural pause detection
- **Supabase** REST (anon key for dashboard, service key for service writes) — same project as Stromation/ResumeGo/TBE
- **systemd** for 24/7 reliability
- **nginx + Letsencrypt** for clip serving over HTTPS basic auth
- **GitHub Pages** for the admin dashboard hosting (under stromation.com)

## Deployment

**Server:** AWS EC2 Ubuntu 24.04, the same one that runs n8n at `n8n.stromation.com`.
**Public IP:** `44.217.216.46`
**SSH:** `ssh -i <key.pem> ubuntu@44.217.216.46`
**Code path:** `/home/ubuntu/clipper` (git repo at https://github.com/dariusstrongman/clipper)
**Data path:** `/mnt/clipper-storage/clipper/` — 100GB EBS gp3 volume (auto-cleanup keeps it lean)
**Logs:** `/mnt/clipper-storage/clipper/logs/service.log`

### Service commands

```bash
sudo systemctl status clipper       # is it running?
sudo systemctl restart clipper      # apply .env changes
sudo systemctl stop clipper
sudo systemctl start clipper
journalctl -u clipper -f            # systemd journal (start/stop only, NOT app logs)
tail -f /mnt/clipper-storage/clipper/logs/service.log   # actual app log
```

### Deploying a code change

```bash
# Locally
cd ~/Desktop/clipper
# edit / test
git add -A && git commit -m "..." && git push

# On the server
cd ~/clipper
git pull
sudo systemctl restart clipper
tail -f /mnt/clipper-storage/clipper/logs/service.log
```

If the change is to `clipper.service` itself:
```bash
sudo cp clipper.service /etc/systemd/system/clipper.service
sudo systemctl daemon-reload
sudo systemctl restart clipper
```

### Updating the dashboard (clipper.html)

`clipper.html` lives in the **Stromation** repo, not this one:

```bash
cd ~/Desktop/Stromation
# edit clipper.html
git add clipper.html && git commit -m "..." && git push
# GitHub Pages auto-deploys in ~60s
```

Hard refresh (`Ctrl+Shift+R`) to bust the browser cache.

## Configuration (`~/clipper/.env` on server)

```
TWITCH_CLIENT_ID=g2equb4hpff0wdwa6su8zejk6c3urc
TWITCH_CLIENT_SECRET=<from dev.twitch.tv "clipper pro" app>
TWITCH_APP_ACCESS_TOKEN=                # leave empty to use secret-based refresh flow
SUPABASE_URL=https://iadzcnzgbtuigyodeqas.supabase.co
SUPABASE_SERVICE_KEY=<sb_secret_... — same key as Stromation/ResumeGo. Find in live .env.>
OPENAI_API_KEY=sk-proj-...              # same as ResumeGo
STREAMERS=ddg,marlon,jasontheween,lacy,jaycinco,deshaefrost
DATA_DIR=/mnt/clipper-storage/clipper
LOG_DIR=/mnt/clipper-storage/clipper/logs
POLL_INTERVAL_SECONDS=30
CHAT_SPIKE_WINDOW_SECONDS=5
CHAT_SPIKE_MIN_MSGS=40
CHAT_SPIKE_COOLDOWN_SECONDS=90
CLIP_PRE_SECONDS=35                     # bumped from 12 to give GPT pick more buffer
CLIP_POST_SECONDS=35                    # bumped from 18 likewise
BUFFER_MAX_MINUTES=10
CLIP_MIN_SCORE=6
```

Never commit `.env`. The Twitch Client Secret is the only project-specific credential; everything else is shared with the Stromation umbrella.

## Architecture

```
Twitch Helix API (monitor poll every 30s)
    + Twitch IRC chat (anonymous WS, listen for PRIVMSG velocity)
    ↓
streamlink https://twitch.tv/<streamer> → ffmpeg
    segment muxer: 30s × 20 = 10 min rolling buffer
    buffers/<streamer>/seg_000.ts .. seg_019.ts  (overwrites in place)
    ↓  (chat spike: ≥40 msgs / 5s, 90s cooldown)
ClipExtractor: wait clip_post+10s, concat last ~4 segments,
    trim to [spike_ts - CLIP_PRE_SECONDS, spike_ts + CLIP_POST_SECONDS] = 70s
    clips/<streamer>_<utc>.mp4  (libx264 veryfast, aac, faststart)
    ↓  (status=pending in clipper_clips)
Processor polls pending rows every 5s:
    1. ffmpeg strip audio → 16kHz mono wav (for Whisper)
    2. OpenAI Whisper → transcript + segment-level timestamps
    3. GPT-4o-mini classify (streamer-aware) → {score, category, reason}
       if score < CLIP_MIN_SCORE: drop source mp4, status=rejected, stop
    4. ffmpeg silencedetect → list of natural pause boundaries
    5. GPT-4o-mini pick_range (silence-aware, streamer-aware) → {start, end}
       — must include setup + payoff
       — first 2s must have content not dead air
       — must snap to silence boundaries listed
    6. _snap_boundaries safety net — force-snap GPT's pick to nearest silence
    7. Build SRT from segments WITHIN [start, end], times shifted to clip-relative
    8. ffmpeg trim (-ss/-t after -i for frame accuracy) + blur-fill vertical 1080×1920
    9. ffmpeg subtitles filter burns SRT in
    10. ffmpeg snapshot @ 40% of duration → thumbnail jpg
    11. GPT-4o-mini → clickbait title (2026 hook formulas)
    12. GPT-4o-mini → hashtags
    13. Drop source mp4 (don't need it; final + thumbnail + srt are all that matter)
    14. Supabase update: processed_path, thumbnail_path, transcript, title, hashtags,
                        score, category, score_reason, duration_sec, status='ready'
    ↓
Dashboard at stromation.com/clipper polls Supabase every 30s
    Renders ready clips with <video src="https://clips.stromation.com/processed/...">
    Basic auth prompt fires on first video load (browser caches per session)
    Approve / Reject / Mark Uploaded → PATCH clipper_clips
    ↓
Manual download mp4 from dashboard → upload to YouTube Shorts / TikTok
    Click "Mark Uploaded" → cleanup task purges files in 30 days
```

Per-streamer asyncio locks on clip extraction so parallel ffmpegs don't fight over the buffer.

## File layout

```
/home/ubuntu/clipper/                  # code (git)
├── venv/
├── service/
│   ├── main.py                        # asyncio entry; wires monitor/capture/chat/clipper/processor/cleanup
│   ├── config.py                      # loads .env -> Config dataclass
│   ├── twitch.py                      # Helix API wrapper (app-token OR static)
│   ├── db.py                          # async Supabase REST client
│   ├── monitor.py                     # polls stream status, reuses open rows on restart
│   ├── capture.py                     # streamlink->ffmpeg rolling buffer per streamer
│   ├── chat.py                        # IRC WS listener + message-velocity spike detector
│   ├── clipper.py                     # on-spike clip extraction from buffer
│   ├── process.py                     # Whisper / classify / pick_range / snap / vertical / captions / title
│   └── cleanup.py                     # hourly disk cleanup (rejected+uploaded+orphans)
├── sql/
│   ├── schema.sql                     # initial tables
│   └── migration_001_scoring.sql      # adds score/category/score_reason columns
├── clipper.service                    # systemd unit (PATH includes venv)
├── install-systemd.sh                 # sudo bash install-systemd.sh to register unit
├── setup.sh                           # venv + pip + data dirs on fresh server
├── .env.example                       # template
├── requirements.txt
└── CLAUDE.md / README.md

/mnt/clipper-storage/clipper/          # data (EBS volume)
├── buffers/<streamer>/seg_XXX.ts      # live rolling buffer (auto-rotates)
├── clips/<streamer>_<utc>.mp4         # raw 16:9 source clips (deleted after processing)
├── processed/                         # final vertical + captioned + thumbnail + srt
│   ├── <base>.final.mp4
│   ├── <base>.jpg
│   └── <base>.srt
└── logs/service.log                   # app log

# Stromation repo (separate)
~/Desktop/Stromation/clipper.html      # admin dashboard at stromation.com/clipper
```

## Streamer profiles (`service/process.py`)

`STREAMER_PROFILES` dict tells the AI what each streamer's content looks like. Used by classify, pick_range, and title prompts so each streamer gets the right rubric:

| Streamer | Content style | What's clip-gold | What to avoid |
|---|---|---|---|
| ddg | rapper / music personality, concerts, drama | music peaks, crowd reactions, ex/beef moments, freestyle punchlines | soundcheck, intros, silent gaps, bland chatter |
| marlon | soccer streamer, M3FC team, tournaments | goals, clutch saves, teammate arguments, skill moves, rage moments | uncontested possession, settings menus, queue time |
| jasontheween | IRL dating-show format | confrontations, shock reveals, walk-outs, one-line shutdowns | driving, polite small talk |
| lacy | IRL + gaming, conversations | genuine laughter, unexpected reactions, flirtation, conflict | quiet gameplay, eating segments |
| jaycinco | Kick fitness/gym + gaming, hosts guests like Yourrage | heavy lifts, fails, gym confrontations, guest banter | warm-ups, equipment setup |
| deshaefrost | high-energy gaming + IRL | rage outbursts, clutch plays, shock moments, funny fails | menus, loading, quiet gameplay |

Adding a new streamer: add to `STREAMERS=` env var **and** add a profile to `STREAMER_PROFILES` dict in `process.py`. Without the profile the AI falls back to a generic rubric — works but less accurate.

## Supabase schema

Three tables in the same project as Stromation/ResumeGo/TBE (`iadzcnzgbtuigyodeqas.supabase.co`):

- `clipper_streams` — one row per live session. `streamer, twitch_user_id, started_at, ended_at, title, game, peak_viewers`
- `clipper_spikes` — one row per detected chat explosion. `stream_id, streamer, detected_at, messages_in_window, window_seconds, sample_messages (JSON)`
- `clipper_clips` — one row per extracted clip. `spike_id, stream_id, streamer, source_path, processed_path, thumbnail_path, duration_sec, transcript, title, hashtags, score, category, score_reason, status, youtube_url, tiktok_uploaded, error, created_at, approved_at, uploaded_at`

**Status flow:** `pending` → `processing` → `ready` | `rejected` | `failed`. Dashboard adds: `approved`, `uploaded`.

RLS is enabled but policies are open (`USING (true)` since the dashboard sits behind a password prompt). To tighten later, scope by anon vs service role.

## Web Dashboard

`https://www.stromation.com/clipper`

- Password: `Kyomi123` (sessionStorage cached)
- Source: `Stromation/clipper.html`
- Stats row: live count, spikes today, ready, approved, uploaded
- Streamers panel: 6 cards, hardcoded list (`MONITORED_STREAMERS` array at line ~371, mirror of `STREAMERS` env)
- Ready clips: cards with embedded `<video>`, AI title, score chip, category chip, transcript with show-more, hashtags, action buttons
- Approved/uploaded: collapsed by default
- Recent spikes: collapsed table with sample chat messages
- Rejected by AI: collapsed grid, useful for tuning `CLIP_MIN_SCORE`
- 30s auto-refresh

**Whenever `STREAMERS` env changes, also update `MONITORED_STREAMERS` array in clipper.html and push to Stromation repo.** Otherwise new streamers won't appear as cards (their streams will still show, but no placeholder before they go live for the first time).

## clips.stromation.com (nginx clip server)

Set up once, no maintenance after.

```nginx
# /etc/nginx/sites-available/clips
server {
    listen 80;
    listen [::]:80;
    server_name clips.stromation.com;
    root /mnt/clipper-storage/clipper;
    autoindex off;

    # Lock to /clips and /processed only - don't expose /logs/, /buffers/
    location ~ ^/(clips|processed)/ {
        auth_basic "Clips";
        auth_basic_user_file /etc/nginx/.clips_htpasswd;
        try_files $uri =404;
    }
    location / { return 404; }
}
# certbot --nginx -d clips.stromation.com adds the listen 443 + ssl_certificate lines
```

DNS: A record `clips.stromation.com` → `44.217.216.46` (already set in Hostinger).

To add a new htpasswd user:
```bash
sudo htpasswd /etc/nginx/.clips_htpasswd <username>
```

## Common tasks

### Watch a clip
1. Open `stromation.com/clipper`
2. Find it in the Ready section
3. First clip click triggers basic auth prompt (`darius` + your htpasswd password). Browser caches per session.
4. Watch, decide. Approve → file kept. Reject → file deleted in 24h. Mark Uploaded after you post it.

### Add a streamer

1. Edit `.env` on server: add to `STREAMERS=...`
2. Add the streamer's profile to `STREAMER_PROFILES` in `service/process.py` (so the AI knows the streamer's style)
3. Update `MONITORED_STREAMERS` array in `Stromation/clipper.html` (line ~371)
4. `git pull && sudo systemctl restart clipper` on server
5. Push the Stromation repo for the dashboard
6. Watch the log for `Monitoring ... newstreamer(<id>)`. If you see `Unknown Twitch logins: ['newstreamer']`, the username is wrong.

### Tune scoring / spike sensitivity

| Symptom | Fix |
|---|---|
| Too much junk passing classifier | Raise `CLIP_MIN_SCORE` to 7 or 8 |
| Missing real moments | Lower `CLIP_MIN_SCORE` to 5, or expand `STREAMER_PROFILES` clip_gold descriptions |
| Too many spikes triggered | Raise `CHAT_SPIKE_MIN_MSGS` to 50 or 60 |
| Clips cut off mid-sentence | Already fixed via silencedetect snap. If still bad, lower silence detector noise threshold to `-32dB` or extend `tol` in `_snap_boundaries` to 4-5s |
| Clips miss the moment (Twitch delay) | Raise `CLIP_PRE_SECONDS` to 40-45 |
| Clips run too long with reaction tail | Lower `CLIP_POST_SECONDS` to 25-30 |
| Titles feel formulaic | Tweak `_gpt_title` prompt — add or remove hook formulas |

After any `.env` change: `sudo systemctl restart clipper`. After any code change: `git pull && sudo systemctl restart clipper`.

### Pause during off-hours

```bash
sudo systemctl stop clipper
# later
sudo systemctl start clipper
```

Streams in progress are picked up on next poll. No duplicate `clipper_streams` rows (Monitor reuses open rows on restart).

### Investigate a bad clip

```sql
SELECT id, streamer, title, score, score_reason, duration_sec, transcript
FROM clipper_clips
WHERE id = '<uuid>';
```

`score_reason` is the AI's explanation. If it's wrong, the prompt needs work.

### Disk health

```bash
df -h /mnt/clipper-storage
du -sh /mnt/clipper-storage/clipper/clips /mnt/clipper-storage/clipper/processed
tail -100 /mnt/clipper-storage/clipper/logs/service.log | grep cleanup
```

Steady-state should be ~50 MB/day with auto-cleanup running. If it's growing fast, check that cleanup ran (search for `cleanup: sweep done freed=`).

## Cost per clip (rough)

- Whisper: ~$0.003 (avg 50 sec audio)
- GPT classifier: ~$0.0005
- GPT pick_range: ~$0.0008
- GPT title + hashtags: ~$0.0015
- **Total: ~$0.006 per clip processed.** Rejections cost ~$0.004 (no pick/title).
- 100 clips/day = ~$0.60. 1000/day = ~$6.
- EBS: $8/mo for 100GB.
- EC2: shared with n8n, no incremental cost.
- nginx + Letsencrypt: free.

## Known issues / gotchas

- **Twitch app access tokens** expire (~60 days). We use `client_credentials` flow so the service refreshes itself. Leave `TWITCH_APP_ACCESS_TOKEN` blank to use the secret-based flow.
- **systemd doesn't inherit the venv** — `clipper.service` manually sets `PATH=/home/ubuntu/clipper/venv/bin:...` to find pip-installed `streamlink`. If you ever rebuild the venv at a different path, update this line.
- **Clip extraction uses segment file mtimes** to locate the spike inside the concatenated buffer. Accurate to ~1 sec. With CLIP_PRE_SECONDS=35, this jitter is <3% of the pre-buffer so it's fine.
- **Multiple service restarts during one live stream** used to create duplicate `clipper_streams` rows; `Monitor._resolve_users` now reuses open rows. Old duplicates can be closed with: `UPDATE clipper_streams SET ended_at=now() WHERE ended_at IS NULL AND id NOT IN (SELECT DISTINCT ON (streamer) id FROM clipper_streams WHERE ended_at IS NULL ORDER BY streamer, started_at DESC);`
- **Processor polls DB every 5 sec** — up to 5-sec delay between clip extraction and Whisper starting. Not a bug, just a scale consideration.
- **Whisper hallucinations** in music-heavy clips ("Thanks for watching", "Subscribe to my channel") — classifier explicitly rejects these.
- **Background music** (especially DDG concerts) makes silencedetect find fewer pauses. The snap then has nothing to snap to and falls back to GPT's pick. Usually still fine because GPT was given Whisper segment boundaries as alternates.
- **Dashboard streamer list is hardcoded** — must be updated whenever `.env` `STREAMERS=` changes (see "Add a streamer" above).

## How to explain this project to another Claude

"Personal Twitch auto-clipper for 6 streamers (DDG, Marlon, Jasontheween, Lacy, Jaycinco, Deshaefrost). Captures rolling 10-min buffers via streamlink+ffmpeg per live streamer, listens to chat via IRC, detects viral moments by message velocity, extracts 70-sec source clips, AI-scores them with streamer-aware rubrics via Whisper + GPT-4o-mini, picks variable-length cuts (8-55s) using GPT with silence-snap safety net so cuts never land mid-word, produces 1080×1920 vertical captioned mp4s with clickbait titles using 2026 hook formulas. Auto-cleanup keeps disk flat. Admin dashboard at stromation.com/clipper with embedded video playback via clips.stromation.com (nginx + basic auth + Letsencrypt). Runs 24/7 on AWS EC2 via systemd. Next: Twitch Clips API polling, audio-energy second trigger, multi-signal composite score, Claude vision sanity check, post-stream top-N ranking, YouTube Shorts auto-upload."

---

Code lives at https://github.com/dariusstrongman/clipper. Dashboard lives in https://github.com/dariusstrongman/Stromation (`clipper.html`).
