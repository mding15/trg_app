"""
update_current_price.py — Copy yh_stock_price rows into current_price for a list of securities.

Reads securities from CSV/ticker.csv (or via --security-id / --ticker / --file),
resolves YH tickers via mkt_data_source, queries yh_stock_price for the date range,
deletes existing current_price rows in that range, then bulk-inserts fresh data.

Input (mutually exclusive; default: CSV/ticker.csv with a 'security_id' column):
    --security-id   colon-separated SecurityIDs  (e.g. T10000108:T10001583)
    --ticker        colon-separated YH tickers   (e.g. SPY:AAPL:QQQ)
    --file          CSV with a 'security_id' column

Date range:
    --start-date    YYYY-MM-DD  (default: one year ago from today)
    --end-date      YYYY-MM-DD  (default: today)

Options:
    --dry-run       Preview row counts without touching the database

Usage:
    python maintenance/update_current_price.py
    python maintenance/update_current_price.py --start-date 2025-01-01
    python maintenance/update_current_price.py --start-date 2025-01-01 --end-date 2025-12-31
    python maintenance/update_current_price.py --security-id T10000108:T10001583
    python maintenance/update_current_price.py --ticker SPY:AAPL:QQQ
    python maintenance/update_current_price.py --file CSV/my_list.csv
    python maintenance/update_current_price.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection

CSV_DIR = Path(__file__).resolve().parent / "CSV"
DEFAULT_CSV = CSV_DIR / "ticker.csv"

# yh_stock_price column → current_price column
_COLUMN_MAP = {
    "ticker": "Ticker",
    "date":   "Date",
    "open":   "Open",
    "high":   "High",
    "low":    "Low",
    "close":  "Close",
    "volume": "Volume",
}


# ── Logging ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("update_current_price")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


# ── Input loading ─────────────────────────────────────────────────────────────

def _ids_from_file(file_path: str) -> list[str]:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    df = pd.read_csv(path)
    if "security_id" not in df.columns:
        raise ValueError(
            f"CSV must have a 'security_id' column (found: {list(df.columns)})"
        )
    return [s.strip() for s in df["security_id"].dropna() if str(s).strip()]


# ── Ticker / security ID resolution ──────────────────────────────────────────

def _resolve_security_ids(security_ids: list[str], log: logging.Logger) -> pd.DataFrame:
    """Return DataFrame(SecurityID, SourceID) for the given SecurityIDs from mkt_data_source."""
    sql = """
        SELECT "SecurityID", "SourceID"
        FROM mkt_data_source
        WHERE "Source" = 'YH' AND "SecurityID" = ANY(%s)
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (security_ids,))
            rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["SecurityID", "SourceID"])
    missing = set(security_ids) - set(df["SecurityID"].tolist())
    if missing:
        log.warning(f"No YH entry in mkt_data_source for SecurityID(s): {sorted(missing)}")
    return df


def _resolve_tickers(tickers: list[str], log: logging.Logger) -> pd.DataFrame:
    """Return DataFrame(SecurityID, SourceID) for the given YH tickers from mkt_data_source."""
    sql = """
        SELECT "SecurityID", "SourceID"
        FROM mkt_data_source
        WHERE "Source" = 'YH' AND "SourceID" = ANY(%s)
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (tickers,))
            rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["SecurityID", "SourceID"])
    missing = set(tickers) - set(df["SourceID"].tolist())
    if missing:
        log.warning(f"No YH entry in mkt_data_source for ticker(s): {sorted(missing)}")
    return df


# ── Database helpers ──────────────────────────────────────────────────────────

def _get_table_columns(table: str) -> list[str]:
    sql = """
        SELECT column_name FROM information_schema.columns
        WHERE table_name = %s ORDER BY ordinal_position
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (table,))
            return [row[0] for row in cur.fetchall()]


def _fetch_stock_prices(tickers: list[str], start_date: date, end_date: date) -> pd.DataFrame:
    sql = """
        SELECT ticker, date, open, high, low, close, volume
        FROM yh_stock_price
        WHERE ticker = ANY(%s) AND date >= %s AND date <= %s
        ORDER BY ticker, date
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (tickers, start_date, end_date))
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def _count_current_price(security_ids: list[str], start_date: date, end_date: date) -> int:
    sql = """
        SELECT COUNT(*) FROM current_price
        WHERE "SecurityID" = ANY(%s) AND "Date" >= %s AND "Date" <= %s
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (security_ids, start_date, end_date))
            return cur.fetchone()[0]


def _delete_current_price(
    security_ids: list[str], start_date: date, end_date: date, log: logging.Logger
) -> None:
    sql = """
        DELETE FROM current_price
        WHERE "SecurityID" = ANY(%s) AND "Date" >= %s AND "Date" <= %s
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (security_ids, start_date, end_date))
            count = cur.rowcount
        conn.commit()
    log.info(f"  Deleted {count} row(s) from current_price")


def _bulk_insert(df: pd.DataFrame, log: logging.Logger) -> None:
    db_cols = _get_table_columns("current_price")
    cols = [c for c in df.columns if c in db_cols]
    subset = df[cols].copy()
    output = StringIO()
    subset.replace({np.nan: r"\N"}, inplace=True)
    subset.to_csv(output, sep="\t", header=False, index=False)
    output.seek(0)
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.copy_from(output, "current_price", columns=cols, null="\\N")
        conn.commit()
    log.info(f"  Inserted {len(subset)} row(s) into current_price")


# ── Core logic ────────────────────────────────────────────────────────────────

def run(
    security_ids: list[str] | None,
    tickers: list[str] | None,
    start_date: date,
    end_date: date,
    dry_run: bool,
    log: logging.Logger,
) -> None:

    # Resolve to SecurityID ↔ SourceID mapping
    if security_ids:
        mapping = _resolve_security_ids(security_ids, log)
    else:
        mapping = _resolve_tickers(tickers, log)

    if mapping.empty:
        log.error("No securities resolved from mkt_data_source — nothing to do.")
        sys.exit(1)

    log.info(f"  Resolved {len(mapping)} security mapping(s)")
    log.info(f"  Date range : {start_date} → {end_date}")

    yh_tickers = mapping["SourceID"].tolist()
    sec_ids    = mapping["SecurityID"].tolist()

    # Fetch source data
    prices = _fetch_stock_prices(yh_tickers, start_date, end_date)
    log.info(f"  Found {len(prices)} row(s) in yh_stock_price")

    if prices.empty:
        log.warning("No data in yh_stock_price for the given tickers/date range.")
        return

    # Rename columns and add SecurityID + PriceTime
    prices = prices.rename(columns=_COLUMN_MAP)
    id_map = mapping.set_index("SourceID")["SecurityID"].to_dict()
    prices["SecurityID"] = prices["Ticker"].map(id_map)
    prices["PriceTime"]  = prices["Date"]

    if dry_run:
        existing = _count_current_price(sec_ids, start_date, end_date)
        log.info("─" * 60)
        log.info("DRY RUN — no data will be written to the database")
        log.info(f"  Date range     : {start_date} → {end_date}")
        log.info(f"  Securities     : {len(mapping)}")
        log.info(f"  YH rows found  : {len(prices)}")
        log.info(f"  Would delete   : {existing} row(s) from current_price")
        log.info(f"  Would insert   : {len(prices)} row(s) into current_price")
        log.info("─" * 60)
        return

    _delete_current_price(sec_ids, start_date, end_date, log)
    _bulk_insert(prices, log)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Copy yh_stock_price data into current_price for a list of securities.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python maintenance/update_current_price.py\n"
            "  python maintenance/update_current_price.py --start-date 2025-01-01\n"
            "  python maintenance/update_current_price.py --start-date 2025-01-01 --end-date 2025-12-31\n"
            "  python maintenance/update_current_price.py --security-id T10000108:T10001583\n"
            "  python maintenance/update_current_price.py --ticker SPY:AAPL:QQQ\n"
            "  python maintenance/update_current_price.py --file CSV/my_list.csv\n"
            "  python maintenance/update_current_price.py --dry-run\n"
        ),
    )

    grp = parser.add_mutually_exclusive_group()
    grp.add_argument(
        "--security-id", "-s", metavar="SEC1:SEC2:...",
        help="Colon-separated SecurityIDs (resolved via mkt_data_source)",
    )
    grp.add_argument(
        "--ticker", "-t", metavar="TICK1:TICK2:...",
        help="Colon-separated YH tickers (resolved via mkt_data_source)",
    )
    grp.add_argument(
        "--file", "-f", metavar="CSV_FILE",
        help="CSV with a 'security_id' column (default: CSV/ticker.csv)",
    )

    parser.add_argument(
        "--start-date", metavar="YYYY-MM-DD", default=None,
        help="Earliest date to copy (default: one year ago from today)",
    )
    parser.add_argument(
        "--end-date", metavar="YYYY-MM-DD", default=None,
        help="Latest date to copy (default: today)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview row counts without touching the database",
    )

    args = parser.parse_args()
    log = _setup_logger()

    # ── Parse dates ───────────────────────────────────────────────────────────
    today = date.today()

    if args.start_date:
        try:
            start_date = date.fromisoformat(args.start_date)
        except ValueError:
            parser.error(f"Invalid --start-date '{args.start_date}' — expected YYYY-MM-DD.")
    else:
        start_date = today - timedelta(days=365)

    if args.end_date:
        try:
            end_date = date.fromisoformat(args.end_date)
        except ValueError:
            parser.error(f"Invalid --end-date '{args.end_date}' — expected YYYY-MM-DD.")
    else:
        end_date = today

    if start_date > end_date:
        parser.error(f"--start-date {start_date} is after --end-date {end_date}.")

    # ── Parse input ───────────────────────────────────────────────────────────
    security_ids: list[str] | None = None
    tickers:      list[str] | None = None

    if args.security_id:
        security_ids = [s.strip() for s in args.security_id.split(":") if s.strip()]
        log.info(f"Input: {len(security_ids)} SecurityID(s) from --security-id")
    elif args.ticker:
        tickers = [t.strip().upper() for t in args.ticker.split(":") if t.strip()]
        log.info(f"Input: {len(tickers)} ticker(s) from --ticker")
    else:
        file_path = args.file or str(DEFAULT_CSV)
        try:
            security_ids = _ids_from_file(file_path)
            log.info(f"Input: {len(security_ids)} SecurityID(s) from {file_path}")
        except (FileNotFoundError, ValueError) as e:
            parser.error(str(e))

    if not security_ids and not tickers:
        parser.error("No securities found in input.")

    log.info(f"start={start_date}  end={end_date}  dry_run={args.dry_run}")

    run(security_ids, tickers, start_date, end_date, args.dry_run, log)

    log.info("─" * 60)
    log.info("Done.")


if __name__ == "__main__":
    main()
