-- Migration: add total_cost to proc_positions and proc_positions_hist
-- Run once against the target database.

ALTER TABLE proc_positions
    ADD COLUMN IF NOT EXISTS total_cost NUMERIC;

ALTER TABLE proc_positions_hist
    ADD COLUMN IF NOT EXISTS total_cost NUMERIC;
