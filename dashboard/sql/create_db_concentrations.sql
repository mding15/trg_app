-- Migration: create db_concentrations table
-- Run once against the target database.

CREATE TABLE IF NOT EXISTS db_concentrations (
    id              SERIAL PRIMARY KEY,
    account_id      INT NOT NULL,
    as_of_date      DATE NOT NULL,
    category        TEXT NOT NULL,
    category_name   TEXT NULL,
    max_weight      FLOAT NULL,
    limit_value     FLOAT NULL,
    ratio           FLOAT NULL,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, as_of_date, category)
);
