"""
dashboard_process.py — Daily computation process for the dashboard.

Steps:
    1. Get as_of_date from proc_asof_date table (or use provided value)
    2. Get all account_ids for that date
    3. For each account_id:
       a. Read positions from position_var, aggregate by security_id
       b. Write per-security market values to db_mv_history
       c. Compute portfolio summary (returns from db_mv_history)
       d. Write to db_portfolio_summary
       e. Compute positions (returns from db_mv_history)
       f. Write to db_positions

Usage:
    python dashboard_process.py                                    # as_of_date from proc_asof_date table
    python dashboard_process.py --date 2026-03-29                  # specific date
    python dashboard_process.py --date 2026-03-29 --account-id 5  # single account, specific date
    python dashboard_process.py --account-id 5                    # single account, latest date
    python dashboard_process.py --register                        # register/update job in scheduler
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from database2 import pg_connection, get_proc_asof_date
from dashboard.positions_calc import (
    get_account_ids_on_date,
    get_positions_on_date,
    compute_portfolio_summary,
    compute_positions,
)
from dashboard.positions_db import (
    delete_mv_history,
    delete_portfolio_summary,
    delete_positions,
    write_mv_history,
    write_portfolio_summary,
    write_positions,
)


# ── logging setup ──────────────────────────────────────────────────────────────

def _setup_logger(as_of_date, account_id=None) -> logging.Logger:
    log_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'log')
    os.makedirs(log_dir, exist_ok=True)
    account_suffix = f'_account{account_id}' if account_id is not None else ''
    log_file = os.path.join(
        log_dir,
        f'dashboard_process_{as_of_date}{account_suffix}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    )
    logger = logging.getLogger(f'dashboard_process_{as_of_date}{account_suffix}')
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


# ── helpers ────────────────────────────────────────────────────────────────────

def _build_mv_rows(df: pd.DataFrame) -> list[dict]:
    """Aggregate market_value by security_id and return as list of {security_id, market_value}."""
    df = df.copy()
    df['market_value'] = pd.to_numeric(df['market_value'], errors='coerce').fillna(0.0)
    agg = df.groupby('security_id', as_index=False)['market_value'].sum()
    return [
        {"security_id": row["security_id"], "market_value": float(row["market_value"])}
        for _, row in agg.iterrows()
        if row["security_id"]
    ]


# ── main ───────────────────────────────────────────────────────────────────────

def run(as_of_date=None, account_id=None) -> None:
    if as_of_date is None:
        as_of_date = get_proc_asof_date()

    logger = _setup_logger(as_of_date, account_id)
    logger.info(f"=== Dashboard process started for as_of_date={as_of_date} ===")

    # 1. Determine account_ids to process
    if account_id is not None:
        account_ids = [account_id]
        logger.info(f"Single-account mode: account_id={account_id}")
    else:
        with pg_connection() as conn:
            account_ids = get_account_ids_on_date(conn, as_of_date)

        if not account_ids:
            logger.warning("No accounts found for as_of_date. Aborting.")
            return

        logger.info(f"Processing {len(account_ids)} account(s): {account_ids}")

    # 2. Process each account
    for account_id in account_ids:
        logger.info(f"--- account_id={account_id} ---")

        with pg_connection() as conn:
            df = get_positions_on_date(conn, as_of_date, account_id)

        if df.empty:
            logger.warning(f"No positions found for account_id={account_id}. Skipping.")
            continue

        # a. Write market values to db_mv_history
        deleted = delete_mv_history(account_id, as_of_date)
        logger.info(f"Deleted {deleted} existing rows from db_mv_history for {as_of_date}.")
        mv_rows = _build_mv_rows(df)
        write_mv_history(account_id, as_of_date, mv_rows)
        logger.info(f"Wrote {len(mv_rows)} rows to db_mv_history.")

        # b. Compute and write portfolio summary
        delete_portfolio_summary(account_id, as_of_date)
        summary = compute_portfolio_summary(account_id, as_of_date=as_of_date)
        write_portfolio_summary(account_id, summary)
        logger.info(f"Portfolio summary written  aum={summary.get('aum')}  asOfDate={summary.get('asOfDate')}")

        # c. Compute and write positions
        delete_positions(account_id, as_of_date)
        positions = compute_positions(account_id, as_of_date=as_of_date)
        write_positions(account_id, as_of_date, positions)
        logger.info(f"Wrote {len(positions)} positions to db_positions.")

    logger.info("=== Dashboard process completed. ===")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Run dashboard computation process')
    mutex = parser.add_mutually_exclusive_group()
    mutex.add_argument('--register', action='store_true',
                       help='Register/update this job in the scheduler')
    mutex.add_argument('--account-id', metavar='ACCOUNT_ID', type=int, default=None,
                       help='Process a single account_id only (default: all accounts)')
    parser.add_argument('--date', metavar='YYYY-MM-DD', default=None,
                        help='as_of_date to process (default: read from proc_asof_date table)')
    args = parser.parse_args()

    if args.register:
        from process_scheduler.register import register_by_id
        register_by_id('dashboard_process')
    else:
        run(as_of_date=args.date, account_id=args.account_id)
