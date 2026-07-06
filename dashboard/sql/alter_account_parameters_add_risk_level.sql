-- Migration: add risk_level to account_parameters and its history table
ALTER TABLE account_parameters
    ADD COLUMN IF NOT EXISTS risk_level VARCHAR(20) DEFAULT 'custom';

ALTER TABLE account_parameters_history
    ADD COLUMN IF NOT EXISTS risk_level VARCHAR(20);
