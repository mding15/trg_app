"""
create_tables.py — Create the dashboard DB tables.

Tables:
    account              — master account registry (supports parent hierarchy via parent_account_id)
    db_mv_history        — daily per-symbol market value snapshots
    db_portfolio_summary — pre-computed portfolio summary per (account_id, as_of_date)
    db_positions         — pre-computed positions per (account_id, as_of_date, ticker)
    mssb_secty_map       — MSSB security identifier mapping
    proc_positions       — processed positions per (account_id, as_of_date, position_id)
    mssb_posit           — raw MSSB position feed
    broker_account       — broker account registry (account_id lookup for raw feeds)
    proc_positions_hist  — historical archive of displaced proc_positions rows
    position_var         — per-position risk/VaR metrics per (as_of_date, account_id, pos_id)
"""
from __future__ import annotations

import sys
import os

# Allow running from this directory or from trg_app root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from database2 import pg_connection


def create_tables() -> None:
    """Create all application tables if they do not already exist."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS account (
                    account_id       SERIAL PRIMARY KEY,
                    account_name     VARCHAR(120) NOT NULL,
                    short_name       VARCHAR(20) NULL,
                    owner_id         INT NOT NULL,
                    client_id        INT NOT NULL,
                    parent_account_id INT DEFAULT NULL REFERENCES account(account_id),
                    create_time      TIMESTAMP DEFAULT NOW(),
                    next_run_time    TIMESTAMP NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS db_mv_history (
                    id           SERIAL PRIMARY KEY,
                    account_id   INT NOT NULL,
                    as_of_date   DATE NOT NULL,
                    security_id  TEXT NOT NULL,
                    market_value FLOAT,
                    UNIQUE (account_id, as_of_date, security_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS db_portfolio_summary (
                    id              SERIAL PRIMARY KEY,
                    account_id      INT NOT NULL,
                    as_of_date      DATE NOT NULL,
                    aum             FLOAT,
                    num_positions   INT,
                    day_pnl         FLOAT,
                    day_return      FLOAT,
                    mtd_return      FLOAT,
                    ytd_return      FLOAT,
                    one_year_return FLOAT,
                    unrealized_gain  FLOAT,
                    var_1d_95        FLOAT,
                    var_1d_99        FLOAT,
                    var_10d_99       FLOAT,
                    es_1d_95         FLOAT,
                    es_99            FLOAT,
                    volatility       FLOAT,
                    sharpe           FLOAT,
                    beta             FLOAT,
                    max_drawdown     FLOAT,
                    top_five_conc    FLOAT,
                    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
                    UNIQUE (account_id, as_of_date)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS db_positions (
                    id              SERIAL PRIMARY KEY,
                    account_id      INT NOT NULL,
                    as_of_date      DATE NOT NULL,
                    security_id     TEXT NOT NULL,
                    ticker          TEXT NULL,
                    name            TEXT NULL,
                    asset_class     TEXT NULL,
                    currency        TEXT NULL,
                    market_value    FLOAT,
                    weight          FLOAT,
                    day_pnl         FLOAT,
                    day_return      FLOAT,
                    mtd_return      FLOAT,
                    ytd_return      FLOAT,
                    one_year_return FLOAT,
                    var_contrib     FLOAT,
                    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
                    UNIQUE (account_id, as_of_date, security_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mssb_secty_map (
                 	id           SERIAL PRIMARY KEY,
                    trg_sec_id   INT NOT NULL,
                    sec_cusip    VARCHAR(20) NOT NULL,
                    sec_isin     VARCHAR(20) NOT NULL,
                    sec_sedol    VARCHAR(20) NOT NULL,
                    sec_symbol   VARCHAR(20) NOT NULL,
                    description  text null,
                    updated_at   TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS proc_positions (
                    id              SERIAL PRIMARY KEY,
                    as_of_date      date NOT NULL,
                    account_id      int NOT NULL,
                    position_id     text NOT NULL,
                    security_id     text NOT NULL,
                    security_name   text NOT NULL,
                    isin            text NOT NULL,
                    cusip           text NOT NULL,
                    ticker          text NOT NULL,
                    quantity        float NULL,
                    market_value    float NULL,
                    asset_class     text NULL,
                    currency        text NOT NULL,
                    broker_account  text NOT NULL,
                    insert_time     TIMESTAMP NOT NULL DEFAULT NOW(),
                    last_price      numeric NULL,
                    last_price_date date NULL,
                    feed_source     text NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mssb_posit (
                    feed_date                     date NULL,
                    routing_code                  text NULL,
                    account                       text NULL,
                    cusip                         text NULL,
                    security_description          text NULL,
                    quantity                      numeric NULL,
                    market_base                   numeric NULL,
                    market_local                  numeric NULL,
                    coupon_rate                   numeric NULL,
                    issue_date                    date NULL,
                    maturity_date                 date NULL,
                    original_face                 numeric NULL,
                    factor                        numeric NULL,
                    currency                      text NULL,
                    symbol                        text NULL,
                    total_cost                    numeric NULL,
                    exchange                      text NULL,
                    account_type                  text NULL,
                    security_code                 text NULL,
                    security_no                   text NULL,
                    sedol                         text NULL,
                    isin                          text NULL,
                    settlement_quantity           numeric NULL,
                    market_base_sd                numeric NULL,
                    product_id                    text NULL,
                    restricted_sec_flag           text NULL,
                    restricted_qnty               numeric NULL,
                    long_short_indicator          text NULL,
                    blank_1                       text NULL,
                    quantity_1                    numeric NULL,
                    symbol_cusip                  text NULL,
                    position_as_of_date           date NULL,
                    alternate_security_indicator  text NULL,
                    wash_sales_indicator          text NULL,
                    partial_call_quantity         numeric NULL,
                    blank_2                       text NULL,
                    accrued_interest              numeric NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS proc_positions_hist (
                    id              SERIAL PRIMARY KEY,
                    as_of_date      date NOT NULL,
                    account_id      int NOT NULL,
                    position_id     text NOT NULL,
                    security_id     text NOT NULL,
                    security_name   text NOT NULL,
                    isin            text NOT NULL,
                    cusip           text NOT NULL,
                    ticker          text NOT NULL,
                    quantity        float NULL,
                    market_value    float NULL,
                    asset_class     text NULL,
                    currency        text NOT NULL,
                    broker_account  text NOT NULL,
                    insert_time     TIMESTAMP NOT NULL DEFAULT NOW(),
                    archived_at     TIMESTAMP NOT NULL DEFAULT NOW(),
                    last_price      numeric NULL,
                    last_price_date date NULL,
                    feed_source     text NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS broker_account (
                    id SERIAL PRIMARY KEY,
                    account_id INT NOT NULL,
                    broker_account text NOT NULL,
                    broker text NOT NULL,
                    routing_code text NOT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS account_access (
                    id         SERIAL PRIMARY KEY,
                    account_id INT NOT NULL,
                    user_id    INT NOT NULL,
                    is_default BOOLEAN NOT NULL DEFAULT FALSE,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS db_asset_allocation (
                    id             SERIAL PRIMARY KEY,
                    account_id     INT          NOT NULL,
                    as_of_date     DATE         NOT NULL,
                    asset_class    VARCHAR(64)  NOT NULL,
                    market_value   FLOAT,
                    weight         FLOAT,
                    bmk_weight     FLOAT,
                    period_return  FLOAT,
                    var_contrib    FLOAT,
                    updated_at     TIMESTAMP    NOT NULL DEFAULT NOW(),
                    UNIQUE (account_id, as_of_date, asset_class)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scenario_definition (
                    scenario_id  SERIAL       PRIMARY KEY,
                    name         VARCHAR(128) NOT NULL,
                    period       VARCHAR(64),
                    severity     VARCHAR(16)  NOT NULL,
                    is_active    BOOLEAN      NOT NULL DEFAULT TRUE
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS account_scenario (
                    account_id   INT NOT NULL,
                    scenario_id  INT NOT NULL REFERENCES scenario_definition (scenario_id),
                    PRIMARY KEY (account_id, scenario_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS db_stress_results (
                    id           SERIAL PRIMARY KEY,
                    account_id   INT   NOT NULL,
                    as_of_date   DATE  NOT NULL,
                    scenario_id  INT   NOT NULL REFERENCES scenario_definition (scenario_id),
                    pnl_usd      FLOAT,
                    pnl_pct      FLOAT,
                    updated_at   TIMESTAMP NOT NULL DEFAULT NOW(),
                    UNIQUE (account_id, as_of_date, scenario_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS db_risk_alerts (
                    id          SERIAL PRIMARY KEY,
                    account_id  INT         NOT NULL,
                    as_of_date  DATE        NOT NULL,
                    seq         SMALLINT    NOT NULL,
                    msg         TEXT        NOT NULL,
                    level       VARCHAR(16) NOT NULL,
                    updated_at  TIMESTAMP   NOT NULL DEFAULT NOW(),
                    UNIQUE (account_id, as_of_date, seq)
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS position_var (
                    as_of_date              date NOT NULL,
                    account_id              int NOT NULL,
                    pos_id                  text NOT NULL,
                    security_id             text NULL,
                    security_name           text NULL,
                    isin                    text NULL,
                    cusip                   text NULL,
                    ticker                  text NULL,
                    broker_account          text NULL,
                    quantity                numeric NULL,
                    market_value            numeric NULL,
                    currency                text NULL,
                    last_price              numeric NULL,
                    last_price_date         date NULL,
                    asset_class             text NULL,
                    asset_type              text NULL,
                    class                   text NULL,
                    sc1                     text NULL,
                    sc2                     text NULL,
                    country                 text NULL,
                    region                  text NULL,
                    sector                  text NULL,
                    industry                text NULL,
                    expected_return         numeric NULL,
                    coupon_rate             numeric NULL,
                    option_type             text NULL,
                    option_strike           numeric NULL,
                    payment_frequency       text NULL,
                    maturity_date           date NULL,
                    underlying_security_id  text NULL,
                    underlying_id           text NULL,
                    underlying_price        numeric NULL,
                    is_option               boolean NULL,
                    excluded                boolean NULL,
                    exclude_reason          text NULL,
                    risk_free_rate          numeric NULL,
                    tenor                   numeric NULL,
                    delta                   numeric NULL,
                    gamma                   numeric NULL,
                    vega                    numeric NULL,
                    iv                      numeric NULL,
                    ir_tenor                numeric NULL,
                    yield                   numeric NULL,
                    duration                numeric NULL,
                    convexity               numeric NULL,
                    ir_pv01                 numeric NULL,
                    sp_pv01                 numeric NULL,
                    spread_duration         numeric NULL,
                    spread_convexity        numeric NULL,
                    delta_var               numeric NULL,
                    ir_var                  numeric NULL,
                    spread_var              numeric NULL,
                    gamma_var               numeric NULL,
                    std                     numeric NULL,
                    marginal_std            numeric NULL,
                    var                     numeric NULL,
                    tvar                    numeric NULL,
                    marginal_var            numeric NULL,
                    marginal_tvar           numeric NULL,
                    insert_time             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (as_of_date, account_id, pos_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS proc_asof_date (
                    as_of_date  DATE      NOT NULL,
                    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS demo_request (
                    id          SERIAL       PRIMARY KEY,
                    first_name  VARCHAR(100) NOT NULL,
                    last_name   VARCHAR(100) NOT NULL,
                    email       VARCHAR(120) NOT NULL,
                    company     VARCHAR(100) NOT NULL,
                    aum         VARCHAR(50)  NULL,
                    interest    VARCHAR(100) NULL,
                    message     TEXT         NULL,
                    status      VARCHAR(20)  NOT NULL DEFAULT 'new',
                    create_date TIMESTAMP    NOT NULL DEFAULT NOW()
                )
            """)

        conn.commit()


def migrate_tables() -> None:
    """
    Add new columns to existing tables. Safe to run multiple times — uses
    ADD COLUMN IF NOT EXISTS (PostgreSQL >= 9.6).
    """
    add_cols = [
        "unrealized_gain   FLOAT",
        "var_1d_95         FLOAT",
        "var_1d_99         FLOAT",
        "var_10d_99        FLOAT",
        "es_1d_95          FLOAT",
        "es_99             FLOAT",
        "volatility        FLOAT",
        "sharpe            FLOAT",
        "beta              FLOAT",
        "max_drawdown      FLOAT",
        "top_five_conc     FLOAT",
    ]
    drop_cols = [
        "var_1d_95_pct",
        "var_1d_99_pct",
        "var_10d_99_pct",
        "es_99_pct",
    ]
    with pg_connection() as conn:
        with conn.cursor() as cur:
            for col in add_cols:
                cur.execute(f"ALTER TABLE db_portfolio_summary ADD COLUMN IF NOT EXISTS {col};")
            for col in drop_cols:
                cur.execute(f"ALTER TABLE db_portfolio_summary DROP COLUMN IF EXISTS {col};")
            # account hierarchy
            cur.execute("""
                ALTER TABLE account
                    ADD COLUMN IF NOT EXISTS parent_account_id INT DEFAULT NULL
                        REFERENCES account(account_id);
            """)
            # per-user default account
            cur.execute("""
                ALTER TABLE account_access
                    ADD COLUMN IF NOT EXISTS is_default BOOLEAN NOT NULL DEFAULT FALSE;
            """)
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_account_access_default
                    ON account_access (user_id)
                    WHERE is_default = TRUE;
            """)
        conn.commit()


if __name__ == "__main__":
    create_tables()
    migrate_tables()
    print("Tables created/migrated successfully.")
