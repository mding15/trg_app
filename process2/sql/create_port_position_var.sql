-- Table: port_position_var
-- Stores VaR results for uploaded portfolios, one row per position per portfolio.
-- Mirrors position_var but keyed by port_id instead of (as_of_date, account_id).

CREATE TABLE port_position_var (
    -- key
    port_id                  INTEGER     NOT NULL REFERENCES portfolio_info(port_id),
    pos_id                   TEXT        NOT NULL,

    -- date from the uploaded portfolio file
    as_of_date               DATE,

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
    total_cost               NUMERIC,
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
    mg_std                   NUMERIC,
    var_95                   NUMERIC,
    var_99                   NUMERIC,
    es_95                    NUMERIC,
    es_99                    NUMERIC,
    mg_var_95                NUMERIC,
    mg_var_99                NUMERIC,
    mg_es_95                 NUMERIC,
    mg_es_99                 NUMERIC,
    -- beta
    beta                     NUMERIC,

    -- metadata
    insert_time              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (port_id, pos_id)
);
