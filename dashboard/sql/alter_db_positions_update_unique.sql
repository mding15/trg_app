-- Migration: update UNIQUE constraint on db_positions to include broker and broker_account
-- broker and broker_account columns were added in alter_db_positions_add_broker.sql
-- Run once against the target database.

-- Drop the old 3-column unique constraint and replace with 5-column one.
-- The old constraint name may vary — check with \d db_positions if this fails.
ALTER TABLE db_positions
    DROP CONSTRAINT IF EXISTS db_positions_account_id_as_of_date_security_id_key;

ALTER TABLE db_positions
    ADD CONSTRAINT db_positions_unique
    UNIQUE (account_id, as_of_date, security_id, broker, broker_account);
