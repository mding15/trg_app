"""
dump_current_price.py — Dump the current_price table for a given as-of date.

Fetches all rows from current_price where "Date" matches the given date and
writes them to a timestamped CSV file in maintenance/CSV/.

Usage:
    python dump_current_price.py                        # defaults to today
    python dump_current_price.py --date 2026-05-16
    python dump_current_price.py --date 2026-05-16 --dry-run

Options:
    --date      As-of date to filter on  YYYY-MM-DD  (default: today)
    --dry-run   Show row count without writing the file
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection, get_proc_asof_date

CSV_DIR = Path(__file__).resolve().parent / "CSV"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("dump_current_price")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


def _fetch_rows(as_of_date: date) -> pd.DataFrame:
    sql = 'SELECT * FROM current_price WHERE "Date" = %s ORDER BY "Date", "Ticker"'
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (as_of_date,))
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def _count_rows(as_of_date: date) -> int:
    sql = 'SELECT COUNT(*) FROM current_price WHERE "Date" = %s'
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (as_of_date,))
            return cur.fetchone()[0]


# ── Core logic ────────────────────────────────────────────────────────────────

def run(as_of_date: date, dry_run: bool) -> None:
    log = _setup_logger()
    log.info(f"current_price  date={as_of_date}")

    if dry_run:
        count = _count_rows(as_of_date)
        log.info("─" * 60)
        log.info("DRY RUN — no file will be written")
        log.info(f"  Date      : {as_of_date}")
        log.info(f"  Row count : {count}")
        log.info("─" * 60)
        return

    df = _fetch_rows(as_of_date)
    log.info(f"  Fetched {len(df)} rows")

    if df.empty:
        log.warning(f"No rows found for date {as_of_date} — CSV will not be written.")
        return

    CSV_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = CSV_DIR / f"current_price_{timestamp}.csv"
    df.to_csv(out_path, index=False)

    log.info("─" * 60)
    log.info(f"Done.  {len(df)} rows written to {out_path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dump current_price rows for a given as-of date to CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python dump_current_price.py\n"
            "  python dump_current_price.py --date 2026-05-16\n"
            "  python dump_current_price.py --date 2026-05-16 --dry-run\n"
        ),
    )
    parser.add_argument(
        "--date", default=None, metavar="YYYY-MM-DD",
        help="As-of date to filter on (default: today)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show row count without writing the file",
    )
    args = parser.parse_args()

    if args.date:
        try:
            as_of_date = date.fromisoformat(args.date)
        except ValueError:
            parser.error(f"Invalid date '{args.date}' — expected YYYY-MM-DD.")
    else:
        as_of_date = date.fromisoformat(get_proc_asof_date())

    run(as_of_date, args.dry_run)


if __name__ == "__main__":
    main()
