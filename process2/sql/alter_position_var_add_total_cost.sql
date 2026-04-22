-- Migration: add total_cost to position_var
-- Run once against the target database.

ALTER TABLE position_var
    ADD COLUMN IF NOT EXISTS total_cost NUMERIC;
