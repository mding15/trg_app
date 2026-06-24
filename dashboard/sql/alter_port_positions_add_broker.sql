-- Migration: add broker_name and broker_account to port_positions
ALTER TABLE port_positions
    ADD COLUMN IF NOT EXISTS broker_name    TEXT NULL,
    ADD COLUMN IF NOT EXISTS broker_account TEXT NULL;
