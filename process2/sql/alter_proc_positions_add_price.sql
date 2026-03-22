-- Migration: add last_price and last_price_date to proc_positions and proc_positions_hist
-- Run once against the target database.

ALTER TABLE proc_positions
    ADD COLUMN IF NOT EXISTS last_price      NUMERIC,
    ADD COLUMN IF NOT EXISTS last_price_date DATE;

ALTER TABLE proc_positions_hist
    ADD COLUMN IF NOT EXISTS last_price      NUMERIC,
    ADD COLUMN IF NOT EXISTS last_price_date DATE;
