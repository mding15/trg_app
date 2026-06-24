"""
backfill_process.py — Backfill pipeline for a fixed set of accounts over a date range.

For each date in [start_date, end_date] inclusive:
  Step 1: process_tracked_positions() for each leaf account  (1014, 1015, 1016)
  Step 2: populate_parent_positions.run()  for parent accounts (1013, 1017)
  Step 3: calculate_var()                  for all 5 accounts
  Step 4: dashboard_process.run()          for all 5 accounts

If step 2 fails for a date, steps 3 and 4 are skipped for that date (parent
rows would be missing or stale).  All other per-account failures are caught,
logged, and processing continues.  A failure summary is printed at the end.

Usage:
    python maintenance/backfill_process.py --start-date 2026-01-01 --end-date 2026-06-15
    python maintenance/backfill_process.py --start-date 2026-06-15 --end-date 2026-06-15
"""
from __future__ import annotations

import argparse
import logging
import sys
import traceback
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from process2.tracked_proc_positions import process_tracked_positions
from process2.populate_parent_positions import run as populate_parent_run
from process2.calculate_var import calculate_var
from dashboard.dashboard_process import run as dashboard_run

LEAF_ACCOUNTS   = [1014, 1015, 1016]
PARENT_ACCOUNTS = [1013, 1017]
ALL_ACCOUNTS    = LEAF_ACCOUNTS + PARENT_ACCOUNTS


# ── Logging ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger('backfill_process')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s', '%H:%M:%S')
    )
    logger.addHandler(handler)
    return logger


# ── Date range ────────────────────────────────────────────────────────────────

def _date_range(start: date, end: date) -> list[date]:
    days = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # 0=Mon … 4=Fri
            days.append(current)
        current += timedelta(days=1)
    return days


# ── Per-date processing ───────────────────────────────────────────────────────

def _run_date(
    as_of_date: date,
    log: logging.Logger,
    failures: list[tuple],
) -> None:
    date_str = as_of_date.isoformat()
    log.info('=' * 70)
    log.info(f'Processing date: {date_str}')
    log.info('=' * 70)

    # ── Step 1: tracked positions for each leaf account ───────────────────────
    log.info(f'Step 1 — process_tracked_positions  accounts={LEAF_ACCOUNTS}')
    for acct_id in LEAF_ACCOUNTS:
        try:
            process_tracked_positions(date_str, account_id=acct_id)
        except Exception:
            msg = traceback.format_exc()
            log.error(f'  Step 1 FAILED  account_id={acct_id}\n{msg}')
            failures.append((date_str, 1, acct_id, msg.splitlines()[-1]))

    # ── Step 2: populate parent positions ────────────────────────────────────
    log.info(f'Step 2 — populate_parent_positions  parents={PARENT_ACCOUNTS}')
    try:
        populate_parent_run(date_str, parent_account_ids=PARENT_ACCOUNTS)
    except Exception:
        msg = traceback.format_exc()
        log.error(f'  Step 2 FAILED — skipping steps 3 and 4 for {date_str}\n{msg}')
        failures.append((date_str, 2, None, msg.splitlines()[-1]))
        return

    # ── Step 3: calculate_var for all accounts ────────────────────────────────
    log.info(f'Step 3 — calculate_var  accounts={ALL_ACCOUNTS}')
    for acct_id in ALL_ACCOUNTS:
        try:
            calculate_var(feed_source=None, as_of_date=date_str, account_id=acct_id)
        except Exception:
            msg = traceback.format_exc()
            log.error(f'  Step 3 FAILED  account_id={acct_id}\n{msg}')
            failures.append((date_str, 3, acct_id, msg.splitlines()[-1]))

    # ── Step 4: dashboard process for all accounts ────────────────────────────
    log.info(f'Step 4 — dashboard_process  accounts={ALL_ACCOUNTS}')
    for acct_id in ALL_ACCOUNTS:
        try:
            dashboard_run(date_str, account_id=acct_id)
        except Exception:
            msg = traceback.format_exc()
            log.error(f'  Step 4 FAILED  account_id={acct_id}\n{msg}')
            failures.append((date_str, 4, acct_id, msg.splitlines()[-1]))


# ── Entry point ───────────────────────────────────────────────────────────────

def run(start_date: date, end_date: date) -> None:
    log = _setup_logger()
    dates = _date_range(start_date, end_date)

    log.info(f'Backfill: {start_date} → {end_date}  ({len(dates)} date(s))')
    log.info(f'Leaf accounts   : {LEAF_ACCOUNTS}')
    log.info(f'Parent accounts : {PARENT_ACCOUNTS}')

    failures: list[tuple] = []

    for d in dates:
        _run_date(d, log, failures)

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info('=' * 70)
    if failures:
        log.warning(f'Completed with {len(failures)} failure(s):')
        for date_str, step, acct_id, err in failures:
            acct_part = f'  account_id={acct_id}' if acct_id is not None else ''
            log.warning(f'  {date_str}  step={step}{acct_part}  {err}')
    else:
        log.info(f'Completed successfully — {len(dates)} date(s) processed.')
    log.info('=' * 70)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Backfill pipeline for a fixed set of accounts over a date range.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'Examples:\n'
            '  python maintenance/backfill_process.py --start-date 2026-01-01 --end-date 2026-06-15\n'
            '  python maintenance/backfill_process.py --start-date 2026-06-15 --end-date 2026-06-15\n'
        ),
    )
    parser.add_argument('--start-date', required=True, metavar='YYYY-MM-DD',
                        help='First date to process (inclusive)')
    parser.add_argument('--end-date',   required=True, metavar='YYYY-MM-DD',
                        help='Last date to process (inclusive)')
    args = parser.parse_args()

    try:
        start_date = date.fromisoformat(args.start_date)
        end_date   = date.fromisoformat(args.end_date)
    except ValueError as e:
        parser.error(f'Invalid date — {e}')

    if start_date > end_date:
        parser.error(f'--start-date {start_date} is after --end-date {end_date}.')

    run(start_date, end_date)


if __name__ == '__main__':
    main()
