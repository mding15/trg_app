-- Migration: add broker_name to port_position_var
-- broker_account already exists; this adds the companion broker_name column.
ALTER TABLE port_position_var
    ADD COLUMN IF NOT EXISTS broker_name TEXT NULL;
