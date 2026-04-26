-- Adds A/B variant tracking. Each clip is randomly assigned to:
--   'A' = treatment (with first-2-sec hook text overlay)
--   'B' = control  (no overlay, baseline)
-- After enough clips of each, compare view counts to validate that the
-- hook overlay is actually moving the 3-second-retention needle in
-- production - not just in the OpusClip dataset.
-- Run once in Supabase SQL editor.

ALTER TABLE clipper_clips
    ADD COLUMN IF NOT EXISTS variant text;

CREATE INDEX IF NOT EXISTS clipper_clips_variant_idx
    ON clipper_clips (variant, created_at DESC) WHERE variant IS NOT NULL;
