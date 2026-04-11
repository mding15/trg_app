-- Migration: add broker to proc_positions, proc_positions_hist, and position_var
-- Run once against the target database.

ALTER TABLE proc_positions
    ADD COLUMN IF NOT EXISTS broker TEXT NULL;

ALTER TABLE proc_positions_hist
    ADD COLUMN IF NOT EXISTS broker TEXT NULL;

ALTER TABLE position_var
    ADD COLUMN IF NOT EXISTS broker TEXT NULL;
