-- Table: position_var
-- Stores VaR results joined with position data, one row per position per account per date.

CREATE TABLE position_var (
    -- key
    as_of_date               DATE        NOT NULL,
    account_id               INTEGER     NOT NULL,
    pos_id                   TEXT        NOT NULL,

    -- identifiers
    security_id              TEXT,
    security_name            TEXT,
    isin                     TEXT,
    cusip                    TEXT,
    ticker                   TEXT,
    broker_account           TEXT,

    -- position
    quantity                 NUMERIC,
    market_value             NUMERIC,
    currency                 TEXT,
    last_price               NUMERIC,
    last_price_date          DATE,

    -- security attributes
    asset_class              TEXT,
    asset_type               TEXT,
    class                    TEXT,
    sc1                      TEXT,
    sc2                      TEXT,
    country                  TEXT,
    region                   TEXT,
    sector                   TEXT,
    industry                 TEXT,
    expected_return          NUMERIC,
    coupon_rate              NUMERIC,
    option_type              TEXT,
    option_strike            NUMERIC,
    payment_frequency        TEXT,
    maturity_date            DATE,
    underlying_security_id   TEXT,
    underlying_id            TEXT,
    underlying_price         NUMERIC,
    is_option                BOOLEAN,

    -- exclusion
    excluded                 BOOLEAN,
    exclude_reason           TEXT,

    -- sensitivities
    risk_free_rate           NUMERIC,
    tenor                    NUMERIC,
    delta                    NUMERIC,
    gamma                    NUMERIC,
    vega                     NUMERIC,
    iv                       NUMERIC,
    ir_tenor                 NUMERIC,
    yield                    NUMERIC,
    duration                 NUMERIC,
    convexity                NUMERIC,
    ir_pv01                  NUMERIC,
    sp_pv01                  NUMERIC,
    spread_duration          NUMERIC,
    spread_convexity         NUMERIC,

    -- VaR results
    delta_var                NUMERIC,
    ir_var                   NUMERIC,
    spread_var               NUMERIC,
    gamma_var                NUMERIC,
    std                      NUMERIC,
    marginal_std             NUMERIC,
    var                      NUMERIC,
    tvar                     NUMERIC,
    marginal_var             NUMERIC,
    marginal_tvar            NUMERIC,

    -- metadata
    insert_time              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (as_of_date, account_id, pos_id)
);
