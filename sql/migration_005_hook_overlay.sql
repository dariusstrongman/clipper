-- Adds the hook_overlay column. Short text (3-8 words) burned onto the
-- first 2 seconds of the final mp4 to grab the viewer in the critical
-- 3-second retention window. Examples: "Wait until you hear this",
-- "DDG had no clue", "This broke the chat".
-- Run once in Supabase SQL editor.

ALTER TABLE clipper_clips
    ADD COLUMN IF NOT EXISTS hook_overlay text;
