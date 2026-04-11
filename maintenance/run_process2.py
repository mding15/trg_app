"""
run_process2.py — Run the full process2 pipeline for a given account_id and date(s).

Runs the following steps in order for each date:
    1. process_mssb_positions  — parse raw MSSB feed into proc_positions
    2. calculate_var           — compute VaR from proc_positions into position_var
    3. dashboard_process.run   — compute dashboard summaries from position_var

Stops immediately if any step fails.

Usage:
    python run_process2.py --account-id 5                      # default date from proc_asof_date
    python run_process2.py --account-id 5 --date 2025-09-30    # specific date
    python run_process2.py --account-id 5 --date all           # all dates in mssb_posit
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database2 import pg_connection, get_proc_asof_date
from process2.process_mssb_positions import process_mssb_positions
from process2.calculate_var import calculate_var
from dashboard import dashboard_process


# ── logging setup ──────────────────────────────────────────────────────────────

def _setup_logger(account_id: int) -> logging.Logger:
    log_dir = Path(__file__).resolve().parent.parent.parent / 'log'
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f'run_process2_account{account_id}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

    logger = logging.getLogger(f'run_process2_account{account_id}')
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


# ── account helpers ───────────────────────────────────────────────────────────

def _is_parent_account(account_id: int) -> bool:
    """Return True if account_id has any child accounts."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM account WHERE parent_account_id = %s LIMIT 1',
                (account_id,),
            )
            return cur.fetchone() is not None


# ── date helpers ───────────────────────────────────────────────────────────────

def _get_all_dates() -> list[str]:
    """Return all distinct feed_dates from mssb_posit, sorted ascending."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT DISTINCT feed_date FROM mssb_posit ORDER BY feed_date')
            return [row[0].isoformat() for row in cur.fetchall()]


# ── pipeline ───────────────────────────────────────────────────────────────────

def _run_date(date: str, account_id: int, is_parent: bool, logger: logging.Logger) -> None:
    """Run all 3 pipeline steps for a single date. Raises on any failure."""
    if is_parent:
        logger.info(f"--- Step 1: skipped (parent account — leaf positions already in proc_positions/proc_positions_hist) ---")
    else:
        logger.info(f"--- Step 1: process_mssb_positions  date={date} ---")
        process_mssb_positions(date, account_id)
        logger.info(f"--- Step 1 complete ---")

    logger.info(f"--- Step 2: calculate_var  date={date} ---")
    calculate_var(feed_source=None, as_of_date=date, account_id=account_id)
    logger.info(f"--- Step 2 complete ---")

    logger.info(f"--- Step 3: dashboard_process.run  date={date} ---")
    dashboard_process.run(as_of_date=date, account_id=account_id)
    logger.info(f"--- Step 3 complete ---")


def run_process2(account_id: int, date: str | None = None) -> None:
    """
    Run the full process2 pipeline for account_id.

    date=None      — use as_of_date from proc_asof_date table
    date='all'     — process all dates in mssb_posit
    date='YYYY-MM-DD' — process that specific date
    """
    logger = _setup_logger(account_id)

    is_parent = _is_parent_account(account_id)

    if date == 'all':
        dates = _get_all_dates()
        if not dates:
            logger.warning("No feed_dates found in mssb_posit. Nothing to process.")
            return
        logger.info(
            f"=== run_process2 started: account_id={account_id}  is_parent={is_parent}  "
            f"dates=all ({len(dates)} date(s): {dates[0]} → {dates[-1]}) ==="
        )
    else:
        as_of = date or get_proc_asof_date()
        dates = [as_of]
        logger.info(f"=== run_process2 started: account_id={account_id}  is_parent={is_parent}  date={as_of} ===")

    for d in dates:
        logger.info(f"====== Processing date={d} ======")
        _run_date(d, account_id, is_parent, logger)
        logger.info(f"====== date={d} complete ======")

    logger.info(
        f"=== run_process2 finished: account_id={account_id}  "
        f"{len(dates)} date(s) processed ==="
    )


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Run the full process2 pipeline (mssb_positions → VaR → dashboard) for one account.'
    )
    parser.add_argument('--account-id', metavar='ACCOUNT_ID', type=int, required=True,
                        help='account_id to process')
    parser.add_argument('--date', metavar='YYYY-MM-DD|all', default=None,
                        help='Date to process: YYYY-MM-DD, "all" (all dates in mssb_posit), '
                             'or omit to use proc_asof_date table')
    args = parser.parse_args()

    run_process2(account_id=args.account_id, date=args.date)
