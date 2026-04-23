-- Add clip scoring + category columns to support AI content classification.
-- Run once in Supabase SQL editor.

ALTER TABLE clipper_clips ADD COLUMN IF NOT EXISTS score         integer;
ALTER TABLE clipper_clips ADD COLUMN IF NOT EXISTS category      text;
ALTER TABLE clipper_clips ADD COLUMN IF NOT EXISTS score_reason  text;

-- New status value: 'rejected' for clips that scored below threshold.
-- Existing statuses: pending, processing, ready, approved, uploaded, failed.
