-- Table: treasury_yield
-- Daily US Treasury par yield curve rates fetched from treasury.gov.
-- One row per trading day. Values are in percentage points (e.g. 4.32 = 4.32%).
-- Populated by detl/tr_extract.py (run weekdays).

CREATE TABLE treasury_yield (
    date         DATE        PRIMARY KEY,

    bc_1month    NUMERIC,
    bc_2month    NUMERIC,
    bc_3month    NUMERIC,
    bc_6month    NUMERIC,
    bc_1year     NUMERIC,
    bc_2year     NUMERIC,
    bc_3year     NUMERIC,
    bc_5year     NUMERIC,
    bc_7year     NUMERIC,
    bc_10year    NUMERIC,
    bc_20year    NUMERIC,
    bc_30year    NUMERIC,

    insert_time  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
