-- Original `score` column was created as INTEGER but the unified _gpt_decide
-- rounds viral_score to 1 decimal (e.g. 7.8). PostgreSQL strict type checking
-- rejects 5.0 -> integer column with code 22P02 "invalid input syntax for
-- type integer". Result: every PATCH after the new pipeline rolled out
-- failed with HTTP 400 and clips piled up in status=failed.
--
-- Fix: relax to numeric. Other *_score columns are already numeric.
-- Run once in Supabase SQL editor.

ALTER TABLE clipper_clips ALTER COLUMN score TYPE numeric;
