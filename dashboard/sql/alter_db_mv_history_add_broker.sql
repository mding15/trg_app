-- Migration: add broker and broker_account to db_mv_history, update UNIQUE constraint
-- Run once against the target database.

ALTER TABLE db_mv_history
    ADD COLUMN IF NOT EXISTS broker         TEXT,
    ADD COLUMN IF NOT EXISTS broker_account TEXT;

-- Drop the old 3-column unique constraint and replace with 5-column one.
-- The old constraint name may vary — check with \d db_mv_history if this fails.
ALTER TABLE db_mv_history
    DROP CONSTRAINT IF EXISTS db_mv_history_account_id_as_of_date_security_id_key;

ALTER TABLE db_mv_history
    ADD CONSTRAINT db_mv_history_unique
    UNIQUE (account_id, as_of_date, security_id, broker, broker_account);
