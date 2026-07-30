"""
dump_position_var.py — Dump all rows of the position_var table for a given
as_of_date to a CSV file. Defaults to the latest as_of_date in the table.

The latest as_of_date is determined from the position_var table itself
(MAX(as_of_date)), across all accounts.

Usage:
    python dump_position_var.py
    python dump_position_var.py --date 2026-03-02
    python dump_position_var.py --dry-run

Options:
    --date      Position date to dump (YYYY-MM-DD); default: latest as_of_date in position_var
    --dry-run   Show the resolved as_of_date and row count without writing the file
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection
from _paths import CSV_DIR

TABLE = "position_var"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("dump_position_var")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


def _get_latest_as_of_date() -> date | None:
    """Return MAX(as_of_date) from position_var, or None if the table is empty."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(as_of_date) FROM {TABLE}")
            row = cur.fetchone()
    return row[0] if row else None


def _fetch_rows(as_of_date: date) -> pd.DataFrame:
    """Fetch all columns from position_var for *as_of_date*."""
    sql = f"SELECT * FROM {TABLE} WHERE as_of_date = %s"
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (as_of_date,))
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


# ── Core logic ────────────────────────────────────────────────────────────────

def run(dry_run: bool, as_of_date: date | None = None) -> None:
    log = _setup_logger()

    if as_of_date is not None:
        log.info(f"Using supplied as_of_date: {as_of_date}")
    else:
        log.info(f"Determining latest as_of_date for '{TABLE}' …")
        as_of_date = _get_latest_as_of_date()
        if as_of_date is None:
            log.error(f"Table '{TABLE}' is empty — nothing to dump.")
            sys.exit(1)
        log.info(f"  Latest as_of_date: {as_of_date}")

    if dry_run:
        with pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE as_of_date = %s", (as_of_date,))
                total = cur.fetchone()[0]
        log.info("─" * 60)
        log.info("DRY RUN — no file will be written")
        log.info(f"  Source table  : {TABLE}")
        log.info(f"  as_of_date    : {as_of_date}")
        log.info(f"  Row count     : {total}")
        log.info("─" * 60)
        return

    log.info(f"Fetching rows for as_of_date={as_of_date} …")
    df = _fetch_rows(as_of_date)
    log.info(f"  Fetched {len(df)} rows")

    if df.empty:
        log.warning("No rows returned for the latest as_of_date — CSV will not be written.")
        return

    CSV_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CSV_DIR / f"{TABLE}_{as_of_date.strftime('%Y%m%d')}.csv"

    df.to_csv(out_path, index=False)

    log.info("─" * 60)
    log.info(f"Done.  {len(df)} rows written to {out_path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dump all rows of position_var for a given as_of_date to a CSV file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python dump_position_var.py\n"
            "  python dump_position_var.py --date 2026-03-02\n"
            "  python dump_position_var.py --dry-run\n"
        ),
    )
    parser.add_argument("--date", dest="as_of_date", type=_parse_date, default=None,
                        metavar="YYYY-MM-DD",
                        help="Position date to dump; default: latest as_of_date in position_var")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show the resolved as_of_date and row count without writing the file")
    args = parser.parse_args()

    run(args.dry_run, args.as_of_date)


if __name__ == "__main__":
    main()
