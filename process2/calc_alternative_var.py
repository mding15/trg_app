"""
calc_alternative_var.py — Calculate VaR metrics for all alternative positions.

Reads positions from position_var (asset_class = 'Alternative'), runs calc_var()
per account using distributions from the PNL category, and writes results to
alternative_var (delete + re-insert per account × as_of_date).

Usage:
    python calc_alternative_var.py                    # latest date, all accounts
    python calc_alternative_var.py --date 2025-12-31
    python calc_alternative_var.py --account-id 1003
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values

from database2 import pg_connection
from process2.var_engine import calc_var


# ── logging ───────────────────────────────────────────────────────────────────

def _setup_logger(as_of_date, account_id=None) -> logging.Logger:
    log_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'log')
    os.makedirs(log_dir, exist_ok=True)
    account_suffix = f'_account{account_id}' if account_id is not None else ''
    log_file = os.path.join(
        log_dir,
        f'calc_alternative_var_{as_of_date}{account_suffix}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    )
    logger = logging.getLogger(f'calc_alternative_var_{as_of_date}{account_suffix}')
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


# ── DB read ───────────────────────────────────────────────────────────────────

def fetch_alt_positions(as_of_date, account_id: int | None = None) -> pd.DataFrame:
    """
    Return alternative positions from position_var for the given as_of_date.
    Columns returned: account_id, as_of_date, pos_id, security_id, market_value.
    """
    sql = """
        SELECT account_id, as_of_date, pos_id, security_id, market_value
        FROM position_var
        WHERE asset_class = 'Alternative'
          AND as_of_date = %s
    """
    params: list = [as_of_date]
    if account_id is not None:
        sql += ' AND account_id = %s'
        params.append(account_id)

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def _get_latest_as_of_date() -> str:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(as_of_date) FROM position_var WHERE asset_class = 'Alternative'"
            )
            result = cur.fetchone()[0]
    if result is None:
        raise ValueError("No alternative positions found in position_var.")
    return result.isoformat()


# ── DB write ──────────────────────────────────────────────────────────────────

_TABLE_COLS = [
    'account_id', 'as_of_date', 'pos_id', 'security_id', 'market_value',
    'std', 'mg_std',
    'var_95', 'var_99',
    'es_95',  'es_99',
    'mg_var_95', 'mg_var_99',
    'mg_es_95',  'mg_es_99',
    'unadj_std', 'unadj_mg_std',
    'unadj_var_95', 'unadj_var_99',
    'unadj_es_95',  'unadj_es_99',
    'unadj_mg_var_95', 'unadj_mg_var_99',
    'unadj_mg_es_95',  'unadj_mg_es_99',
]


def _delete_existing(account_ids: list[int], as_of_date, cur) -> None:
    cur.execute(
        'DELETE FROM alternative_var WHERE account_id = ANY(%s) AND as_of_date = %s',
        (account_ids, as_of_date),
    )


def _insert_rows(df: pd.DataFrame, cur) -> int:
    if df.empty:
        return 0
    df = df.replace({np.nan: None, pd.NaT: None})
    cols    = [c for c in _TABLE_COLS if c in df.columns]
    col_sql = ', '.join(f'"{c}"' for c in cols)
    sql     = f'INSERT INTO alternative_var ({col_sql}) VALUES %s'
    records = [tuple(row) for row in df[cols].itertuples(index=False, name=None)]
    execute_values(cur, sql, records)
    return len(records)


# ── VaR calculation ───────────────────────────────────────────────────────────

def _calc_account(acc_pos: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Run calc_var() twice for one account's alternative positions:
      - adjusted metrics   (category='PNL') stored as-is
      - unadjusted metrics (category='ALT') stored with 'unadj_' prefix
    Returns a DataFrame with account_id, as_of_date, pos_id, security_id,
    market_value + 20 metric columns.
    """
    engine_input = acc_pos[['pos_id', 'security_id', 'market_value']].rename(columns={
        'security_id':  'SecurityID',
        'market_value': 'MarketValue',
    })

    adj_metrics   = calc_var(engine_input, category='PNL')
    unadj_metrics = calc_var(engine_input, category='ALT').add_prefix('unadj_')

    result = (
        acc_pos[['account_id', 'as_of_date', 'pos_id', 'security_id', 'market_value']]
        .set_index('pos_id')
        .join(adj_metrics)
        .join(unadj_metrics)
        .reset_index()
    )

    n_ok  = result['std'].notna().sum()
    n_nan = result['std'].isna().sum()
    logger.info(
        f"  account_id={acc_pos['account_id'].iloc[0]}: "
        f"{n_ok} positions with adj metrics, {n_nan} without PnL data"
    )
    return result


# ── main ──────────────────────────────────────────────────────────────────────

def run(as_of_date=None, account_id: int | None = None) -> None:
    if as_of_date is None:
        as_of_date = _get_latest_as_of_date()

    logger = _setup_logger(as_of_date, account_id)
    logger.info(
        f"=== calc_alternative_var: as_of_date={as_of_date}"
        + (f"  account_id={account_id}" if account_id is not None else "  (all accounts)")
        + " ==="
    )

    positions = fetch_alt_positions(as_of_date, account_id)
    if positions.empty:
        logger.warning(f"No alternative positions found for as_of_date={as_of_date}. Exiting.")
        return

    all_account_ids = positions['account_id'].unique().tolist()
    logger.info(f"Loaded {len(positions)} positions across {len(all_account_ids)} account(s).")

    result_frames: list[pd.DataFrame] = []

    for acct_id in all_account_ids:
        acc_pos = positions[positions['account_id'] == acct_id]
        try:
            result_frames.append(_calc_account(acc_pos, logger))
        except Exception as e:
            logger.error(f"account_id={acct_id}: FAILED — {e}")
            continue

    if not result_frames:
        logger.warning("No results produced — nothing to insert.")
        return

    all_results    = pd.concat(result_frames, ignore_index=True)
    as_of_date_val = pd.to_datetime(as_of_date).date()
    inserted_ids   = all_results['account_id'].unique().tolist()

    with pg_connection() as conn:
        with conn.cursor() as cur:
            _delete_existing(inserted_ids, as_of_date_val, cur)
            n = _insert_rows(all_results, cur)
        conn.commit()

    logger.info(f"Inserted {n} rows into alternative_var for account_ids={sorted(inserted_ids)}.")
    logger.info("=== Done ===")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Calculate VaR metrics for alternative positions.'
    )
    parser.add_argument('--date', metavar='YYYY-MM-DD',
                        help='as_of_date to process (default: latest in position_var)')
    parser.add_argument('--account-id', metavar='ACCOUNT_ID', type=int,
                        help='Process a single account (default: all accounts)')
    args = parser.parse_args()
    run(args.date, args.account_id)
