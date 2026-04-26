-- Blind A/B/C/D trial. Each clip gets a random bucket label that's the
-- ONLY thing the user sees during triage - the AI scores, category, and
-- reasoning are hidden so the human's decision isn't biased by AI confidence.
-- After a few days the data is decoded server-side to reveal what kinds of
-- clips the user actually approves vs rejects, irrespective of AI bias.
-- Run once in Supabase SQL editor.

ALTER TABLE clipper_clips
    ADD COLUMN IF NOT EXISTS trial_bucket text;

CREATE INDEX IF NOT EXISTS clipper_clips_trial_bucket_idx
    ON clipper_clips (trial_bucket, status, created_at DESC) WHERE trial_bucket IS NOT NULL;

-- Backfill existing ready clips so they show up in the trial queue tonight
UPDATE clipper_clips
SET trial_bucket = (ARRAY['A','B','C','D'])[1 + floor(random() * 4)::int]
WHERE status IN ('ready','approved','uploaded') AND trial_bucket IS NULL;
