-- Migration: add total_cost to port_positions and port_position_var
-- Run once against the target database.

ALTER TABLE port_positions
    ADD COLUMN IF NOT EXISTS total_cost NUMERIC;

ALTER TABLE port_position_var
    ADD COLUMN IF NOT EXISTS total_cost NUMERIC;
