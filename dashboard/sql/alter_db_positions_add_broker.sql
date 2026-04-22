-- Migration: add broker and broker_account to db_positions
-- Run once against the target database.

ALTER TABLE db_positions
    ADD COLUMN IF NOT EXISTS broker         TEXT,
    ADD COLUMN IF NOT EXISTS broker_account TEXT;
