"""
calculate_var.py — Calculate VaR for all accounts and store results in position_var.

Main function: calculate_var(feed_source=None, as_of_date=None, account_id=None)

Steps:
    1. Preprocess positions from proc_positions (column mapping, security info, prices).
    2. For each account: run VaR, re-attach excluded positions (VaR columns = NULL).
    3. Insert results into position_var per account (delete + re-insert).
    Failed accounts are logged and skipped.

Run populate_parent_positions.py before this script to ensure parent account rows
are present in proc_positions.

Usage:
    python calculate_var.py                                          # all feeds, latest date
    python calculate_var.py --feed-source mssb                      # mssb only, latest date
    python calculate_var.py --date 2025-09-30                        # all feeds, specific date
    python calculate_var.py --feed-source mssb --date 2025-09-30    # mssb, specific date
    python calculate_var.py --feed-source mssb --account-id 5       # one account
    python calculate_var.py --feed-source mssb --date 2025-09-30 --account-id 5
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from database2 import get_proc_asof_date, pg_connection
from process2 import var_engine
from process2.db_position_var import insert_results
from process2.preprocess_var import preprocess_var


# ── Beta helpers (used by dashboard upload pipeline) ─────────────────────────

def fetch_betas_bulk(beta_keys: list[str], sec_ids: list[str]) -> dict[str, dict[str, float]]:
    """Fetch betas from sec_beta for the given keys and security IDs.

    Returns {beta_key: {security_id: beta_value}}.
    Securities with no row in sec_beta are absent from the inner dict.
    """
    if not beta_keys or not sec_ids:
        return {k: {} for k in beta_keys}

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT beta_key, security_id, beta FROM sec_beta'
                ' WHERE beta_key = ANY(%s) AND security_id = ANY(%s)',
                (beta_keys, sec_ids),
            )
            rows = cur.fetchall()

    result: dict[str, dict[str, float]] = {k: {} for k in beta_keys}
    for beta_key, security_id, beta in rows:
        if beta is not None:
            result[beta_key][security_id] = float(beta)
    return result


def add_beta_to_result(result: pd.DataFrame, betas: dict[str, float],
                       logger: logging.Logger) -> pd.DataFrame:
    """Map {security_id: beta} onto result['SecurityID'] and store in result['beta'].

    Positions with no matching beta get NaN.
    """
    result = result.copy()
    result['beta'] = result['SecurityID'].map(betas)
    matched = result['beta'].notna().sum()
    logger.info(f'Beta: matched {matched}/{len(result)} positions')
    return result


# ── logging setup ─────────────────────────────────────────────────────────────

def _setup_logger(feed_source: str | None, as_of_date, account_id=None) -> logging.Logger:
    log_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'log')
    os.makedirs(log_dir, exist_ok=True)
    fs_part        = f'{feed_source}_' if feed_source is not None else ''
    account_suffix = f'_account{account_id}' if account_id is not None else ''
    log_file = os.path.join(
        log_dir,
        f'calculate_var_{fs_part}{as_of_date}{account_suffix}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    )
    logger = logging.getLogger(f'calculate_var_{fs_part}{as_of_date}{account_suffix}')
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


# ── VaR runner helper ─────────────────────────────────────────────────────────

def _run_var(
    label: str,
    positions: pd.DataFrame,
    as_of_date,
    logger: logging.Logger,
) -> int:
    """
    Run VaR engine on all positions, insert into position_var, and return rows inserted.
    Positions without PnL data in the HDF store receive NaN metrics.
    Raises on engine or DB failure — caller decides whether to skip or abort.
    """
    sens        = var_engine.calc_sensitivity_metrics(positions, as_of_date)
    var_metrics = var_engine.calc_var(positions, sensitivity_metrics=sens)
    result = positions.set_index('pos_id').join(var_metrics, how='left').reset_index()
    n = insert_results(result, as_of_date)
    logger.info(f"{label}: {len(positions)} positions, {n} rows inserted")
    return n


# ── main ───────────────────────────────────────────────────────────────────────

def calculate_var(feed_source: str | None = None, as_of_date=None, account_id: int | None = None):
    """
    Run VaR for the given feed_source and as_of_date.

    Parent account rows must already be present in proc_positions (written by
    populate_parent_positions.py) — this function treats all accounts identically.

    feed_source=None  — process all feed sources combined.
    account_id=None   — process all accounts.
    account_id=<id>   — process that single account only.

    If as_of_date is not provided, the latest as_of_date across proc_positions is used.
    """
    if as_of_date is None:
        as_of_date = get_proc_asof_date()

    logger = _setup_logger(feed_source, as_of_date, account_id)

    # ── Single account mode ───────────────────────────────────────────────────
    if account_id is not None:
        logger.info(
            f"=== Start VaR: feed_source={feed_source!r} as_of_date={as_of_date} "
            f"account_id={account_id} ==="
        )
        try:
            all_positions = preprocess_var(as_of_date, feed_source, account_ids=[account_id])
        except Exception as e:
            logger.error(f"preprocess_var failed: {e}")
            raise

        try:
            n = _run_var(f"account_id={account_id}", all_positions, as_of_date, logger)
        except Exception as e:
            logger.error(f"account_id={account_id}: FAILED — {e}")
            raise

        logger.info(f"Total: {n} rows inserted")
        logger.info("=== Done ===")
        return

    # ── Full run (no account_id specified) ────────────────────────────────────
    logger.info(f"=== Start VaR: feed_source={feed_source!r} as_of_date={as_of_date} (all accounts) ===")

    try:
        all_positions = preprocess_var(as_of_date, feed_source)
    except Exception as e:
        logger.error(f"preprocess_var failed: {e}")
        raise

    all_account_ids = all_positions['account_id'].unique()

    total_inserted = 0
    for acct_id in all_account_ids:
        acc_pos = all_positions[all_positions['account_id'] == acct_id]
        try:
            total_inserted += _run_var(f"account_id={acct_id}", acc_pos, as_of_date, logger)
        except Exception as e:
            logger.error(f"account_id={acct_id}: FAILED — {e}")
            continue

    logger.info(f"Total: {total_inserted} rows inserted")
    logger.info("=== Done ===")


def test():
    from pathlib import Path

    output_dir = Path(__file__).resolve().parent / 'test_output'
    output_dir.mkdir(exist_ok=True)

    as_of_date = get_proc_asof_date()
    print(f'as_of_date: {as_of_date}')

    positions = pd.DataFrame({
        'pos_id':      [1, 2, 3],
        'SecurityID':  ['T10001757','T10001739','T10001618'],
        'MarketValue': [100_000.0, 100_000.0, 100_000.0],
    })
    print(f'Test positions:\n{positions}\n')

    sens = var_engine.calc_sensitivity_metrics(positions, as_of_date)
    var_metrics = var_engine.calc_var(positions, sensitivity_metrics=sens)

    sens_path = output_dir / 'sensitivities.csv'
    var_path  = output_dir / 'var_metrics.csv'
    sens.to_csv(sens_path)
    var_metrics.to_csv(var_path)
    print(f'Saved: {sens_path}')
    print(f'Saved: {var_path}')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Calculate VaR and store results in position_var.')
    parser.add_argument('--test', action='store_true',
                        help='Run test() with synthetic positions and save results to test_output/')
    parser.add_argument('--feed-source', default=None, metavar='FEED_SOURCE',
                        help='Feed source to process (e.g. mssb); default: all feed sources')
    parser.add_argument('--date', metavar='YYYY-MM-DD',
                        help='as_of_date to process (default: latest in proc_positions)')
    parser.add_argument('--account-id', metavar='ACCOUNT_ID', type=int,
                        help='Process a single account_id; default: all accounts')
    args = parser.parse_args()

    if args.test:
        test()
    else:
        calculate_var(args.feed_source, args.date, args.account_id)
