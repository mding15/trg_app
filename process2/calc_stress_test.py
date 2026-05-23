"""
calc_stress_test.py — Stress test P&L for all accounts, stored in st_account_summary.

Reads pre-processed positions from position_var and pre-calculated security-level
stress returns from st_security_pnl (populated by calc_stress_test_pnl.py).

Formula: account_st_pnl = sum(st_security_pnl.pnl * position_var.market_value)
         grouped by (account_id, scenario_id)
         — inner join, so only positions with a matching row in st_security_pnl contribute.

Usage:
    python calc_stress_test.py                          # all accounts, latest date
    python calc_stress_test.py --date 2025-09-30        # all accounts, specific date
    python calc_stress_test.py --account-id 5           # one account, latest date
    python calc_stress_test.py --date 2025-09-30 --account-id 5

Prerequisite: run calc_stress_test_pnl.py first to populate st_security_pnl.

CREATE TABLE (run once):
    CREATE TABLE st_account_summary (
        id          INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        account_id  INTEGER NOT NULL,
        as_of_date  DATE    NOT NULL,
        scenario_id INTEGER NOT NULL,
        st_pnl      FLOAT   NULL,
        CONSTRAINT uq_st_account_summary UNIQUE (account_id, as_of_date, scenario_id),
        FOREIGN KEY (scenario_id) REFERENCES st_scenarios (scenario_id)
    );
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from psycopg2.extras import execute_batch

from database2 import pg_connection, get_proc_asof_date


# ── logging ────────────────────────────────────────────────────────────────────

def _setup_logger(as_of_date, account_id: int | None) -> logging.Logger:
    log_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'log')
    os.makedirs(log_dir, exist_ok=True)
    account_suffix = f'_account{account_id}' if account_id is not None else ''
    log_file = os.path.join(
        log_dir,
        f'calc_stress_test_{as_of_date}{account_suffix}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    )
    logger = logging.getLogger(f'calc_stress_test_{as_of_date}{account_suffix}')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger


# ── data loading ───────────────────────────────────────────────────────────────

def _load_positions(conn, as_of_date, account_id: int | None) -> pd.DataFrame:
    """
    Return DataFrame (account_id, security_id, market_value) from position_var.
    Filters to a single account_id when provided.
    """
    if account_id is not None:
        sql = """
            SELECT account_id, security_id, market_value
            FROM position_var
            WHERE as_of_date = %s AND account_id = %s
        """
        params = (as_of_date, account_id)
    else:
        sql = """
            SELECT account_id, security_id, market_value
            FROM position_var
            WHERE as_of_date = %s
        """
        params = (as_of_date,)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return pd.DataFrame(cur.fetchall(), columns=['account_id', 'security_id', 'market_value'])


def _load_security_pnl(conn) -> pd.DataFrame:
    """Return DataFrame (security_id, scenario_id, pnl) from st_security_pnl."""
    with conn.cursor() as cur:
        cur.execute('SELECT security_id, scenario_id, pnl FROM st_security_pnl')
        return pd.DataFrame(cur.fetchall(), columns=['security_id', 'scenario_id', 'pnl'])


# ── calculation ────────────────────────────────────────────────────────────────

def _calc_account_pnl(positions_df: pd.DataFrame, security_pnl_df: pd.DataFrame) -> pd.DataFrame:
    """
    Join positions to security stress returns and aggregate to account level.

    Returns DataFrame (account_id, scenario_id, st_pnl).
    Positions with no matching security_id in st_security_pnl are excluded (inner join).
    """
    positions_df = positions_df.copy()
    positions_df['market_value'] = pd.to_numeric(positions_df['market_value'], errors='coerce').fillna(0.0)

    merged = positions_df.merge(security_pnl_df, on='security_id', how='inner')
    merged['position_pnl'] = merged['pnl'] * merged['market_value']

    result = (
        merged
        .groupby(['account_id', 'scenario_id'], as_index=False)['position_pnl']
        .sum()
        .rename(columns={'position_pnl': 'st_pnl'})
    )
    return result


# ── database write ─────────────────────────────────────────────────────────────

def _delete_results(conn, account_ids: list[int], as_of_date) -> int:
    with conn.cursor() as cur:
        cur.execute(
            'DELETE FROM st_account_summary WHERE as_of_date = %s AND account_id = ANY(%s)',
            (as_of_date, account_ids),
        )
        return cur.rowcount


def _insert_results(conn, df: pd.DataFrame, as_of_date) -> int:
    if df.empty:
        return 0
    df = df.copy()
    df['as_of_date'] = as_of_date
    rows = df[['account_id', 'as_of_date', 'scenario_id', 'st_pnl']].to_dict(orient='records')
    sql = """
        INSERT INTO st_account_summary (account_id, as_of_date, scenario_id, st_pnl)
        VALUES (%(account_id)s, %(as_of_date)s, %(scenario_id)s, %(st_pnl)s)
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, rows)
    return len(rows)


# ── main ───────────────────────────────────────────────────────────────────────

def calculate_stress_test(as_of_date=None, account_id: int | None = None):
    """
    Calculate stress test P&L for all (or one) account(s) and write to st_account_summary.

    as_of_date=None  — uses the latest as_of_date in position_var.
    account_id=None  — processes all accounts found in position_var for that date.
    """
    if as_of_date is None:
        as_of_date = get_proc_asof_date()

    logger = _setup_logger(as_of_date, account_id)
    logger.info(f'=== Start stress test: as_of_date={as_of_date} account_id={account_id} ===')

    with pg_connection() as conn:
        positions_df = _load_positions(conn, as_of_date, account_id)
        if positions_df.empty:
            logger.warning(f'No positions found in position_var for as_of_date={as_of_date}'
                           + (f' account_id={account_id}' if account_id else '') + ' — nothing to do.')
            return

        security_pnl_df = _load_security_pnl(conn)
        if security_pnl_df.empty:
            logger.error('st_security_pnl is empty — run calc_stress_test_pnl.py first.')
            return

        account_ids = positions_df['account_id'].unique().tolist()
        logger.info(
            f'Positions loaded: {len(positions_df)} rows across {len(account_ids)} account(s). '
            f'Security PnL loaded: {len(security_pnl_df)} rows.'
        )

        result_df = _calc_account_pnl(positions_df, security_pnl_df)
        logger.info(
            f'Calculated: {len(result_df)} account×scenario rows '
            f'({result_df["scenario_id"].nunique()} scenario(s), '
            f'{result_df["account_id"].nunique()} account(s))'
        )

        deleted = _delete_results(conn, account_ids, as_of_date)
        logger.info(f'Deleted {deleted} existing rows from st_account_summary')

        inserted = _insert_results(conn, result_df, as_of_date)
        conn.commit()
        logger.info(f'Inserted {inserted} rows into st_account_summary')

    logger.info('=== Done ===')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Calculate stress test P&L for all accounts and store in st_account_summary.'
    )
    parser.add_argument('--date', metavar='YYYY-MM-DD',
                        help='as_of_date to process (default: latest in position_var)')
    parser.add_argument('--account-id', metavar='ACCOUNT_ID', type=int,
                        help='Process a single account_id; default: all accounts')
    args = parser.parse_args()

    calculate_stress_test(args.date, args.account_id)
