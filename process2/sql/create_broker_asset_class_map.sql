-- Migration: create broker_asset_class_map table
-- Maps broker-specific security codes to standardised asset_class values.

CREATE TABLE broker_asset_class_map (
    broker        TEXT NOT NULL,
    security_code TEXT NOT NULL,
    asset_class   TEXT,
    PRIMARY KEY (broker, security_code)
);
