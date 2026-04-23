# Clipper

Personal automated Twitch clipper for 4 streamers. Detects viral moments via chat spikes, extracts clips, AI-scores them for quality, produces TikTok-ready vertical mp4s with burned-in captions and GPT-generated titles. Drops them in Supabase + on-disk ready for manual upload.

## Current state (Apr 2026)

**Done (steps 1-5):**
- [x] Monitor — polls Twitch Helix every 30 sec, logs live/offline transitions to `clipper_streams`
- [x] Capture — streamlink → ffmpeg rolling 10-min segment buffer per live streamer (`buffers/<streamer>/seg_XXX.ts`, 30-sec segments, wraps after 20)
- [x] Chat spike detection — Twitch IRC over WebSocket, triggers on ≥40 msgs in 5 sec with 90 sec cooldown
- [x] Clip extraction — on spike, ffmpeg pulls 30 sec (12 pre + 18 post) out of buffer to `clips/<streamer>_<utc>.mp4`
- [x] AI scoring — Whisper transcript + chat sample → GPT-4o-mini rates 1-10 with category (funny/drama/reaction/skill/announcement/boring/spam), rejects < 6
- [x] Vertical reformat + captions — 1080×1920 with blurred-fill background + TikTok-style burned captions
- [x] Title + hashtags — GPT-4o-mini generates hooky title + 6-8 hashtags
- [x] Thumbnail — JPG snapshot at 40% of clip
- [x] Systemd service — runs 24/7, auto-restarts on crash

**Not done:**
- [ ] Web dashboard on stromation.com/admin-clipper for reviewing/approving clips from phone
- [ ] YouTube Shorts auto-upload (official Data API v3 + OAuth)
- [ ] TikTok auto-upload (manual for now — official API is restricted)
- [ ] Twitch Clips API polling — detect viral moments even when chat is quiet
- [ ] Audio volume spike detection — catch loud reactions / screams independent of chat
- [ ] Viewer count delta detection — raid-ins / spike-sharing moments

## Deployment

**Server:** AWS EC2 Ubuntu 24.04, the same one that runs n8n at `n8n.myaibuffet.com`.
**SSH:** `ssh -i <key.pem> ubuntu@<ec2-public-ip>`.
**Code path:** `/home/ubuntu/clipper` (git repo at <https://github.com/dariusstrongman/clipper>)
**Data path:** `/mnt/clipper-storage/clipper/` — 100GB EBS gp3 volume, currently ~70% free.
**Logs:** `/mnt/clipper-storage/clipper/logs/service.log`

### Service commands

```bash
sudo systemctl status clipper       # is it running?
sudo systemctl restart clipper      # apply .env or Dockerfile changes
sudo systemctl stop clipper
sudo systemctl start clipper
journalctl -u clipper -f            # live systemd journal
tail -f /mnt/clipper-storage/clipper/logs/service.log   # live app log
```

### Deploying a code change

```bash
# Locally
cd /c/Users/Darius/Desktop/clipper
# edit / test
git add -A && git commit -m "..." && git push

# On the server
cd ~/clipper
git pull
sudo systemctl restart clipper
```

If the change is to `clipper.service` itself:
```bash
sudo cp clipper.service /etc/systemd/system/clipper.service
sudo systemctl daemon-reload
sudo systemctl restart clipper
```

## Credentials (`~/clipper/.env` on server)

```
TWITCH_CLIENT_ID=g2equb4hpff0wdwa6su8zejk6c3urc
TWITCH_CLIENT_SECRET=<from dev.twitch.tv "clipper pro" app>
SUPABASE_URL=https://iadzcnzgbtuigyodeqas.supabase.co
SUPABASE_SERVICE_KEY=<same sb_secret_... key as Stromation/ResumeGo; find it in the live .env on the server>
# (never paste real keys into CLAUDE.md or any committed file)
OPENAI_API_KEY=sk-proj-...  # same as ResumeGo
STREAMERS=ddg,marlon,jasontheween,lacy
DATA_DIR=/mnt/clipper-storage/clipper
LOG_DIR=/mnt/clipper-storage/clipper/logs
POLL_INTERVAL_SECONDS=30
CHAT_SPIKE_WINDOW_SECONDS=5
CHAT_SPIKE_MIN_MSGS=40
CHAT_SPIKE_COOLDOWN_SECONDS=90
CLIP_PRE_SECONDS=12
CLIP_POST_SECONDS=18
BUFFER_MAX_MINUTES=10
CLIP_MIN_SCORE=6
```

Don't commit .env. The Twitch Client Secret is the only project-specific credential; everything else is shared with Stromation.

## Architecture

```
Twitch Helix API (monitor poll every 30s)
    + Twitch IRC chat (anonymous WS, listen for PRIVMSG velocity)
    ↓
streamlink https://twitch.tv/<streamer> → ffmpeg
    segment muxer: 30s×20 rolling buffer = 10 min
    buffers/<streamer>/seg_000.ts .. seg_019.ts
    ↓  (chat spike: ≥40 msgs in 5s with 90s cooldown)
ClipExtractor: wait clip_post+10s, concat last 4 segments,
    trim to [spike_ts - CLIP_PRE_SECONDS, spike_ts + CLIP_POST_SECONDS]
    clips/<streamer>_<utc>.mp4  (libx264 veryfast, aac, faststart)
    ↓  (status=pending in clipper_clips)
Processor polls pending rows every 5s:
    1. ffmpeg strip audio → 16kHz mono wav
    2. OpenAI Whisper → transcript + segments
    3. GPT-4o-mini classify → {score, category, reason}
         if score < CLIP_MIN_SCORE: status=rejected, stop
    4. Build SRT from Whisper segments (2-line 32-char wrap)
    5. ffmpeg blur-fill vertical reformat → processed/<base>.vertical.mp4
    6. ffmpeg subtitles filter burns SRT in → processed/<base>.final.mp4
    7. ffmpeg snapshot @ 40% → processed/<base>.jpg
    8. GPT-4o-mini → viral title + hashtags
    9. Supabase update status=ready, processed_path, title, hashtags, score
```

Per-streamer asyncio locks on clip extraction so parallel ffmpegs don't fight over the buffer.

## File layout

```
/home/ubuntu/clipper/              # code
├── venv/
├── service/
│   ├── main.py                    # asyncio entry; wires monitor + capture + chat + clipper + processor
│   ├── config.py                  # loads .env -> Config dataclass
│   ├── twitch.py                  # Helix API wrapper (app-token OR static token)
│   ├── db.py                      # async Supabase REST client
│   ├── monitor.py                 # polls stream status, resumes open streams on restart
│   ├── capture.py                 # streamlink->ffmpeg rolling buffer per streamer
│   ├── chat.py                    # IRC WS listener + message-velocity spike detector
│   ├── clipper.py                 # on-spike clip extraction from buffer
│   └── process.py                 # Whisper, AI scoring, vertical, captions, title, thumbnail
├── sql/
│   ├── schema.sql                 # initial tables
│   └── migration_001_scoring.sql  # adds score/category/score_reason columns
├── clipper.service                # systemd unit
├── install-systemd.sh             # sudo bash install-systemd.sh to register unit
├── setup.sh                       # venv + pip + data dirs on fresh server
├── .env.example                   # template
├── requirements.txt
└── README.md / CLAUDE.md

/mnt/clipper-storage/clipper/      # data (EBS volume)
├── buffers/<streamer>/seg_XXX.ts  # live rolling buffer
├── clips/                         # raw 16:9 clips from extractor
├── processed/                     # final vertical + captioned + thumbnails
├── pending/                       # (unused for now; approval dashboard slot)
├── uploaded/                      # (unused for now; post-upload move)
└── logs/service.log               # app log (systemd also writes here)
```

## Supabase schema

Three tables in the same project as Stromation/ResumeGo/TBE (`iadzcnzgbtuigyodeqas.supabase.co`):

- `clipper_streams` — one row per live session. `streamer, twitch_user_id, started_at, ended_at, title, game, peak_viewers`
- `clipper_spikes` — one row per detected chat explosion. `stream_id, streamer, detected_at, messages_in_window, window_seconds, sample_messages (JSON of last 15 msgs)`
- `clipper_clips` — one row per extracted clip. `spike_id, stream_id, streamer, source_path, processed_path, thumbnail_path, transcript, title, hashtags, score, category, score_reason, status, youtube_url, tiktok_uploaded`

**Status values:** `pending` → `processing` → `ready` | `rejected` | `failed`. Later: `approved`, `uploaded`.

## Common tasks

### Grab a ready clip for upload

```bash
# On your laptop
scp -i <key.pem> ubuntu@<ec2-ip>:/mnt/clipper-storage/clipper/processed/<filename>.final.mp4 ~/Desktop/
```

Find the filename from Supabase `clipper_clips.processed_path` where `status=ready`.

### See what's queued

Supabase dashboard → `clipper_clips` → filter `status=ready`. Sort by `score DESC` to see best first.
Title + hashtags are there too; copy-paste into TikTok/Shorts upload form.

### Add a streamer

1. Edit `.env` on server: add to `STREAMERS=ddg,marlon,jasontheween,lacy,NEW`
2. `sudo systemctl restart clipper`
3. Check log for `Monitoring new(<twitch_user_id>)` — if the username resolves, done.

### Tune scoring / spikes

- Too many boring clips making it through → raise `CLIP_MIN_SCORE=7` or `8`
- Too few clips, service is catching everything → raise `CHAT_SPIKE_MIN_MSGS=50` or `60`
- Missing funny moments because chat reacts slowly → raise `CLIP_PRE_SECONDS=20` (buffer can handle up to ~600 sec)

Any tune: edit `.env`, `sudo systemctl restart clipper`.

### Pause during off-hours

```bash
sudo systemctl stop clipper
# later
sudo systemctl start clipper
```

Streamers will get captured from when the service restarts; any stream already live when the service starts is picked up on the next poll (no duplicate `clipper_streams` row — Monitor._resolve_users reuses open rows).

## Next steps to pick up

Ordered by impact:

1. **Twitch Clips API polling** — poll `/helix/clips?broadcaster_id=X&started_at=recent` every 2 min, treat any clip with view_count > 20 as a definitively-viral moment, extract from our buffer using the clip's timestamp. Strongest signal possible since other humans already decided it was clip-worthy. Same pipeline thereafter.
2. **Web dashboard at stromation.com/admin-clipper** — password-protected like the other admin pages. Lists clips with `status=ready`, shows thumbnail + title + score + category, has download button and "mark uploaded" action. Lets you review from phone instead of SCPing.
3. **YouTube Shorts auto-upload** — Google Cloud Console project + OAuth to a dedicated YouTube channel. New `service/upload.py` polls for `status=approved`, uploads via Data API v3, updates `youtube_url` field.
4. **Audio volume spike detection** — ffmpeg `silencedetect` / `volumedetect` filters applied periodically on the rolling buffer. Cross-signal with chat: either signal fires clip extraction.
5. **Multi-streamer scaling** — if this goes well, add more streamers via `STREAMERS=` list. Server can handle 8-10 concurrent before bandwidth / disk gets tight.

## Known issues / gotchas

- Twitch app access tokens expire (~60 days). We use `client_credentials` flow so the service refreshes itself. The `TWITCH_APP_ACCESS_TOKEN` field in `.env` is for legacy static tokens; leave it blank to use the secret-based flow.
- systemd doesn't inherit the venv, so `clipper.service` manually sets `PATH=/home/ubuntu/clipper/venv/bin:...` to find pip-installed `streamlink`.
- Clip extraction math relies on segment file mtimes to locate the spike inside the concatenated buffer. Accurate to ~1 sec, which is fine for 30-sec clips.
- Multiple service restarts during one live stream used to create duplicate `clipper_streams` rows; `Monitor._resolve_users` now looks up open streams on startup and reuses them. Old duplicates from before this fix can be closed with: `UPDATE clipper_streams SET ended_at=now() WHERE ended_at IS NULL AND id NOT IN (SELECT DISTINCT ON (streamer) id FROM clipper_streams WHERE ended_at IS NULL ORDER BY streamer, started_at DESC);`
- Processor polls the DB every 5 sec for pending clips, so there's up to a 5-sec delay between clip extraction and Whisper starting. Not a bug, just a scale consideration.

## Cost per clip (rough)

- Whisper transcription: ~$0.003 (30 sec of audio)
- GPT classifier: ~$0.0005
- GPT title + hashtags: ~$0.001
- **Total: ~$0.005 per clip.** 100 clips/day = ~$0.50. 1000/day = ~$5.
- EBS: $8/mo for 100GB.
- EC2: already paid for running n8n.

## How to explain this project to another Claude

"Personal Twitch auto-clipper for 4 streamers (DDG, Marlon, Jasontheween, Lacy). Monitors Twitch live status, captures rolling 10-min buffers via streamlink+ffmpeg, listens to chat via IRC, detects viral moments by message velocity, extracts 30-sec clips, AI-scores them for quality via Whisper + GPT, skips boring/spam clips, produces 1080x1920 vertical captioned mp4s with GPT-generated titles, stores metadata in Supabase. Runs 24/7 on AWS EC2 via systemd. Next step is Twitch Clips API polling for quiet-chat streams, then a web dashboard for phone-based review, then YouTube Shorts auto-upload."

---

Code lives at <https://github.com/dariusstrongman/clipper>. Talk to @dariusstrongman for access.
