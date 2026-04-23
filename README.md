# Twitch Clipper

Personal automated Twitch clipper for DDG, Marlon, Lacy, and Jasontheween.
Watches their streams, detects viral moments via chat spikes, extracts clips,
reformats for vertical (TikTok / Shorts), captions them with Whisper, and
uploads approved ones to YouTube Shorts.

## Architecture

```
Twitch API (poll: live status)
    + Twitch IRC (chat spike detection)
    ↓
streamlink → FFmpeg (rolling live buffer per streamer)
    ↓  (on chat spike)
FFmpeg (extract +/- 15 sec around spike)
    ↓
FFmpeg (vertical 9:16 crop) + Whisper (captions)
    ↓
GPT-4o-mini (title + hashtags)
    ↓
Supabase (log metadata, pending review)
    ↓  (you approve in dashboard)
YouTube Data API v3 → Shorts
TikTok: download + manual upload from phone for now
```

## Layout

```
/home/ubuntu/clipper/           # code lives here
├── venv/                        # python3.12 venv
├── service/
│   ├── main.py                  # asyncio orchestrator
│   ├── twitch.py                # helix API wrapper + IRC chat client
│   ├── capture.py               # streamlink → rolling buffer
│   ├── spikes.py                # chat message velocity + spike detection
│   ├── clip.py                  # FFmpeg extract + vertical crop + captions
│   ├── db.py                    # Supabase client
│   └── config.py                # loads .env
├── .env                         # NEVER commit
└── requirements.txt

/mnt/clipper-storage/clipper/   # data lives here (separate drive)
├── buffers/                    # rolling mp4 chunks per streamer
├── clips/                       # raw clips extracted on spike
├── processed/                   # vertical + captioned final
├── pending/                     # queued for your review
├── uploaded/                    # after upload (auto-delete after 7d)
└── logs/
```

## Setup (one-time)

1. **Register a Twitch application** at <https://dev.twitch.tv/console/apps>
   - Name: anything (e.g., `clipper-personal`)
   - OAuth Redirect URL: `http://localhost` (unused but required)
   - Category: Application Integration
   - Client Type: Confidential
   - Save, copy the **Client ID**
   - Click *Manage* → *New Secret* → copy the **Client Secret**

2. **Run the Supabase schema** (`sql/schema.sql`) in your Supabase SQL editor.

3. **Copy `.env.example` → `.env`** and fill in:
   - `TWITCH_CLIENT_ID`
   - `TWITCH_CLIENT_SECRET`
   - `SUPABASE_URL` (already in your env)
   - `SUPABASE_SERVICE_KEY` (already in your env)
   - `OPENAI_API_KEY` (already in your env)

4. **On the server:**
   ```bash
   cd /home/ubuntu
   git clone <this-repo> clipper
   cd clipper
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   cp .env.example .env
   nano .env  # paste credentials
   ```

5. **Test the monitor** (step 1 — just checks if streamers are live):
   ```bash
   python -m service.main --monitor-only
   ```
   You should see log lines like `ddg: offline` every 30 seconds.

## Roadmap

- [x] Project scaffold
- [ ] Step 1: Twitch monitor (poll live status every 30s, log to Supabase)
- [ ] Step 2: Stream capture via streamlink (rolling 10-min buffer)
- [ ] Step 3: Chat listener + spike detection
- [ ] Step 4: Clip extraction on spike
- [ ] Step 5: Vertical reformat + Whisper captions
- [ ] Step 6: GPT title/hashtag generation
- [ ] Step 7: Admin dashboard on stromation.com/admin-clipper
- [ ] Step 8: YouTube Shorts upload API
- [ ] Step 9: Systemd service (auto-restart on crash)
