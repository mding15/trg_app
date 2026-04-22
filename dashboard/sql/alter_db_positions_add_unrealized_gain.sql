-- Migration: add unrealized_gain to db_positions
-- Run once against the target database.

ALTER TABLE db_positions
    ADD COLUMN IF NOT EXISTS unrealized_gain FLOAT;
