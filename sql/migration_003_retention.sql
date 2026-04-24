-- Adds retention_score - measures whether the clip stays engaging the WHOLE length,
-- has a clean ending, and doesn't drop energy halfway. Different from hook_score
-- which only judges the first 2 seconds.

ALTER TABLE clipper_clips
    ADD COLUMN IF NOT EXISTS retention_score numeric;
