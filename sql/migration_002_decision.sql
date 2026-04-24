-- Adds AI decision metadata columns produced by the unified _gpt_decide() call.
-- Run in Supabase SQL Editor on the same project.

ALTER TABLE clipper_clips
    ADD COLUMN IF NOT EXISTS hook_score    numeric,
    ADD COLUMN IF NOT EXISTS context_score numeric,
    ADD COLUMN IF NOT EXISTS pacing_score  numeric,
    ADD COLUMN IF NOT EXISTS description   text,
    ADD COLUMN IF NOT EXISTS backup_titles jsonb,
    ADD COLUMN IF NOT EXISTS auto_upload   boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS reject_reason text;

-- Optional helpful index for the dashboard's "approved/uploaded" feed
CREATE INDEX IF NOT EXISTS clipper_clips_auto_upload_idx
    ON clipper_clips (auto_upload, created_at DESC) WHERE auto_upload = true;
