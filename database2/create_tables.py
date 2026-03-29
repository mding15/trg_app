"""
create_tables.py — Create the dashboard DB tables.

Tables:
    db_mv_history        — daily per-symbol market value snapshots
    db_portfolio_summary — pre-computed portfolio summary per (account_id, as_of_date)
    db_positions         — pre-computed positions per (account_id, as_of_date, ticker)
    mssb_secty_map       — MSSB security identifier mapping
    proc_positions       — processed positions per (account_id, as_of_date, position_id)
    mssb_posit           — raw MSSB position feed
    broker_account       — broker account registry (account_id lookup for raw feeds)
    proc_positions_hist  — historical archive of displaced proc_positions rows
"""
from __future__ import annotations

import sys
import os

# Allow running from this directory or from trg_app root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from database2 import pg_connection


def create_tables() -> None:
    """Create the four dashboard tables if they do not already exist."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
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
                    asset_class     text NOT NULL,
                    currency        text NOT NULL,
                    broker_account  text NOT NULL,
                    feed_source     text NOT NULL,
                    insert_time     TIMESTAMP NOT NULL DEFAULT NOW()
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
                    asset_class     text NOT NULL,
                    currency        text NOT NULL,
                    broker_account  text NOT NULL,
                    feed_source     text NOT NULL,
                    insert_time     TIMESTAMP NOT NULL DEFAULT NOW(),
                    archived_at     TIMESTAMP NOT NULL DEFAULT NOW()
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
                    id SERIAL PRIMARY KEY,
                    account_id INT NOT NULL,
                    user_id INT NOT NULL,
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
                CREATE TABLE IF NOT EXISTS proc_asof_date (
                    as_of_date  DATE      NOT NULL,
                    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
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
        conn.commit()


if __name__ == "__main__":
    create_tables()
    migrate_tables()
    print("Tables created/migrated successfully.")
