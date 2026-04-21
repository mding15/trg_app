"""
dump_feed.py — Dump MSSB feed tables for a given feed_date to Excel.

Sheets written (in order):
    mssb_posit   — broker positions  (filtered by feed_date)
    mssb_secty   — securities        (filtered by feed_date)
    mssb_taxlot  — tax lots          (filtered by feed_date)
    mssb_trans   — transactions      (filtered by feed_date)

Each sheet is capped at 1000 rows.

Output:
    maintenance/Excel/dump_feed_{feed_date}.xlsx

Usage:
    python maintenance/dump_feed.py --date 2026-03-02
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection

EXCEL_DIR = Path(__file__).resolve().parent / "Excel"
ROW_LIMIT = 1000

TABLES = ["mssb_posit", "mssb_secty", "mssb_taxlot", "mssb_trans", "mssb_price"]


# ── Logging ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("dump_feed")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


# ── Fetch helper ──────────────────────────────────────────────────────────────

def _fetch(sql: str, params: tuple) -> pd.DataFrame:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


# ── Core ──────────────────────────────────────────────────────────────────────

def run(feed_date: date) -> None:
    log = _setup_logger()
    log.info(f"dump_feed  feed_date={feed_date}")

    EXCEL_DIR.mkdir(parents=True, exist_ok=True)

    sheets: dict[str, pd.DataFrame] = {}

    for table in TABLES:
        log.info(f"Fetching {table} …")
        sheets[table] = _fetch(
            f"SELECT * FROM {table} WHERE feed_date = %s LIMIT {ROW_LIMIT}",
            (feed_date,),
        )

    # ── Write Excel ───────────────────────────────────────────────────────────

    date_str = feed_date.strftime("%Y%m%d")
    out_path = EXCEL_DIR / f"dump_feed_{date_str}.xlsx"

    log.info(f"Writing {out_path} …")
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            for col in df.select_dtypes(include=["datetimetz"]).columns:
                df[col] = df[col].dt.tz_localize(None)
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            log.info(
                f"  Sheet '{sheet_name}': {len(df)} row(s) x {len(df.columns)} col(s)"
                + (" [capped at 1000]" if len(df) == ROW_LIMIT else "")
            )

    log.info("─" * 60)
    log.info(f"Done.  Written to {out_path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    def _parse_date(s: str) -> date:
        return datetime.strptime(s, "%Y-%m-%d").date()

    parser = argparse.ArgumentParser(
        description="Dump MSSB feed tables for a given feed_date to Excel.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python maintenance/dump_feed.py --date 2026-03-02\n"
        ),
    )
    parser.add_argument("--date", dest="feed_date", type=_parse_date, required=True,
                        metavar="YYYY-MM-DD", help="Feed date to dump")
    args = parser.parse_args()

    run(feed_date=args.feed_date)
