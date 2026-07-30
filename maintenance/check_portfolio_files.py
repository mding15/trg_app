"""
check_portfolio_files.py — Audit portfolio_info: verify that the physical
file backing every row actually exists on disk.

For each row, resolves the path the same way the app does
(CLIENT_DIR/<client_id>/<filename>, via get_portfolio_file_path) and checks
whether it exists. Writes a full report to a CSV file and prints a summary.
Read-only — never touches the database or any files.

Usage:
    python maintenance/check_portfolio_files.py
    python maintenance/check_portfolio_files.py --missing-only
    python maintenance/check_portfolio_files.py --port-type tracked
    python maintenance/check_portfolio_files.py --port-type legacy
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from database2 import pg_connection
from dashboard.upload_portfolio import get_portfolio_file_path
from _paths import CSV_DIR


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("check_portfolio_files")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


def _fetch_rows(port_type: str | None) -> list[dict]:
    """Fetch all portfolio_info rows, optionally filtered by port_type.
    port_type='legacy' means port_type IS NULL (rows predating the column)."""
    sql = """
        SELECT port_id, port_name, filename, client_id, account_id, port_type, status
        FROM portfolio_info
    """
    params: tuple = ()
    if port_type == 'legacy':
        sql += " WHERE port_type IS NULL"
    elif port_type is not None:
        sql += " WHERE port_type = %s"
        params = (port_type,)
    sql += " ORDER BY port_id"

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [
        {
            'port_id':    r[0],
            'port_name':  r[1],
            'filename':   r[2],
            'client_id':  r[3],
            'account_id': r[4],
            'port_type':  r[5] or 'legacy',
            'status':     r[6],
        }
        for r in rows
    ]


def _check_row(row: dict) -> dict:
    """Add file_path and exists (or a reason it can't be checked) to a row."""
    if not row['filename'] or row['client_id'] is None:
        return {**row, 'file_path': None, 'exists': None, 'note': 'missing filename or client_id'}
    file_path = get_portfolio_file_path(row['client_id'], row['filename'])
    return {**row, 'file_path': str(file_path), 'exists': file_path.exists(), 'note': ''}


def run(port_type: str | None, missing_only: bool) -> None:
    log = _setup_logger()

    log.info(f"Fetching portfolio_info rows{f' (port_type={port_type})' if port_type else ''} …")
    rows = _fetch_rows(port_type)
    log.info(f"  {len(rows)} row(s)")

    checked = [_check_row(r) for r in rows]

    n_exists    = sum(1 for r in checked if r['exists'] is True)
    n_missing   = sum(1 for r in checked if r['exists'] is False)
    n_unchecked = sum(1 for r in checked if r['exists'] is None)

    log.info("─" * 60)
    log.info(f"Total rows       : {len(checked)}")
    log.info(f"File exists      : {n_exists}")
    log.info(f"File MISSING     : {n_missing}")
    log.info(f"Could not check  : {n_unchecked}  (missing filename or client_id on the row)")
    log.info("─" * 60)

    if n_missing:
        log.warning("Rows with a missing file:")
        for r in checked:
            if r['exists'] is False:
                log.warning(
                    f"    port_id={r['port_id']}  port_type={r['port_type']}  "
                    f"account_id={r['account_id']}  client_id={r['client_id']}  "
                    f"filename={r['filename']}  path={r['file_path']}"
                )
    if n_unchecked:
        log.warning("Rows that could not be checked:")
        for r in checked:
            if r['exists'] is None:
                log.warning(
                    f"    port_id={r['port_id']}  port_type={r['port_type']}  "
                    f"filename={r['filename']}  client_id={r['client_id']}"
                )

    out_rows = [r for r in checked if r['exists'] is not True] if missing_only else checked
    if not out_rows:
        log.info("Nothing to write (no missing/unchecked rows and --missing-only was set).")
        return

    CSV_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "_missing" if missing_only else ""
    out_path = CSV_DIR / f"portfolio_file_check{suffix}_{timestamp}.csv"

    df = pd.DataFrame(out_rows)
    df.to_csv(out_path, index=False)
    log.info(f"Report written to {out_path}  ({len(out_rows)} row(s))")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit portfolio_info: verify the physical file for every row exists on disk.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python maintenance/check_portfolio_files.py\n"
            "  python maintenance/check_portfolio_files.py --missing-only\n"
            "  python maintenance/check_portfolio_files.py --port-type tracked\n"
            "  python maintenance/check_portfolio_files.py --port-type legacy\n"
        ),
    )
    parser.add_argument("--port-type", choices=["tracked", "adhoc", "legacy"], default=None,
                        help="Only check rows of this port_type; default: all rows")
    parser.add_argument("--missing-only", action="store_true",
                        help="Only include missing/unchecked rows in the CSV report (console summary always covers all rows)")
    args = parser.parse_args()

    run(args.port_type, args.missing_only)


if __name__ == "__main__":
    main()
