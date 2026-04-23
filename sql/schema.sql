-- Run in Supabase SQL Editor.
-- Personal clipper tables. Service-role only; no RLS needed since the
-- dashboard runs password-protected on admin-clipper.html.

CREATE TABLE IF NOT EXISTS clipper_streams (
    id              uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    streamer        text NOT NULL,
    twitch_user_id  text,
    started_at      timestamptz NOT NULL,
    ended_at        timestamptz,
    title           text,
    game            text,
    peak_viewers    int,
    created_at      timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS clipper_streams_streamer_idx ON clipper_streams (streamer, started_at DESC);

CREATE TABLE IF NOT EXISTS clipper_spikes (
    id              uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    stream_id       uuid REFERENCES clipper_streams(id) ON DELETE CASCADE,
    streamer        text NOT NULL,
    detected_at     timestamptz NOT NULL,
    messages_in_window int,
    window_seconds  int,
    sample_messages text,   -- jsonb would be fine; keeping text for simplicity
    created_at      timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS clipper_spikes_stream_idx ON clipper_spikes (stream_id, detected_at);

CREATE TABLE IF NOT EXISTS clipper_clips (
    id              uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    spike_id        uuid REFERENCES clipper_spikes(id) ON DELETE SET NULL,
    stream_id       uuid REFERENCES clipper_streams(id) ON DELETE SET NULL,
    streamer        text NOT NULL,
    source_path     text,        -- on-disk path at /mnt/clipper-storage/...
    processed_path  text,        -- final vertical + captioned
    thumbnail_path  text,
    duration_sec    numeric,
    transcript      text,
    title           text,
    hashtags        text,
    status          text DEFAULT 'pending',  -- pending, approved, rejected, uploaded, failed
    youtube_url     text,
    tiktok_uploaded boolean DEFAULT false,
    error           text,
    created_at      timestamptz DEFAULT now(),
    approved_at     timestamptz,
    uploaded_at     timestamptz
);
CREATE INDEX IF NOT EXISTS clipper_clips_status_idx ON clipper_clips (status, created_at DESC);

-- Enable RLS and grant service role (matches pattern from resumego/tbe)
ALTER TABLE clipper_streams ENABLE ROW LEVEL SECURITY;
ALTER TABLE clipper_spikes  ENABLE ROW LEVEL SECURITY;
ALTER TABLE clipper_clips   ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "clipper_streams_service" ON clipper_streams;
DROP POLICY IF EXISTS "clipper_spikes_service"  ON clipper_spikes;
DROP POLICY IF EXISTS "clipper_clips_service"   ON clipper_clips;

CREATE POLICY "clipper_streams_service" ON clipper_streams FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "clipper_spikes_service"  ON clipper_spikes  FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "clipper_clips_service"   ON clipper_clips   FOR ALL USING (true) WITH CHECK (true);
