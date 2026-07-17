"""
yh_fetch.py — Fetch Yahoo Finance price data for a list of tickers and write to CSV.

Subcommands:
    hist    — call api_hist_price(): writes hist_price and hist_dividend CSVs
    eod     — call api_eod_price():  writes eod_price CSV (uses today's date)
    profile — call api_stock_profiles(): writes profile CSV
    update  — call extract_yh_price(): fetches historical prices and writes to DB + HDF

hist, eod, and update accept mutually exclusive input (default: CSV/ticker.csv):
    --ticker      colon-separated tickers
    --security-id colon-separated SecurityIDs (resolved to tickers via mkt_data_source)
    --file        CSV with a 'ticker' and/or 'security_id' column
    (no args)     defaults to CSV/ticker.csv

profile requires tickers as a positional arg or --file (CSV with 'ticker' column).

Output files (data/maintenance/CSV/):
    hist_price_YYYYMMDD_HHMMSS.csv
    hist_dividend_YYYYMMDD_HHMMSS.csv
    eod_price_YYYYMMDD_HHMMSS.csv
    profile_YYYYMMDD_HHMMSS.csv

Usage:
    python maintenance/yh_fetch.py hist                                   # uses CSV/ticker.csv
    python maintenance/yh_fetch.py hist    --ticker SPY:AAPL:QQQ
    python maintenance/yh_fetch.py hist    --security-id T10000108:T10001583
    python maintenance/yh_fetch.py hist    --file CSV/ticker.csv
    python maintenance/yh_fetch.py eod                                    # uses CSV/ticker.csv
    python maintenance/yh_fetch.py eod     --ticker SPY:AAPL:QQQ
    python maintenance/yh_fetch.py eod     --security-id T10000108:T10001583
    python maintenance/yh_fetch.py eod     --file CSV/ticker.csv
    python maintenance/yh_fetch.py profile SPY:AAPL:QQQ
    python maintenance/yh_fetch.py profile --file CSV/ticker.csv
    python maintenance/yh_fetch.py update                                 # uses CSV/ticker.csv
    python maintenance/yh_fetch.py update  --ticker SPY:AAPL:QQQ
    python maintenance/yh_fetch.py update  --security-id T10000108:T10001583
    python maintenance/yh_fetch.py update  --file CSV/ticker.csv
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from detl.yh_extract import api_hist_price, api_eod_price, api_stock_profiles
from mkt_data.mkt_data_extract import extract_yh_price, get_yh_source_id
from _paths import CSV_DIR


# ── Logging ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("yh_fetch")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


# ── Ticker loading ────────────────────────────────────────────────────────────

def _tickers_from_file(file_path: str) -> list[str]:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Ticker file not found: {path}")
    df = pd.read_csv(path)
    if "ticker" not in df.columns:
        raise ValueError(f"CSV file must have a 'ticker' column (found: {list(df.columns)})")
    return [t.strip().upper() for t in df["ticker"].dropna() if str(t).strip()]


def _ids_from_file(file_path: str) -> tuple[list[str], list[str]]:
    """Read tickers and/or security_ids from a CSV file for the update subcommand."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    df = pd.read_csv(path)
    tickers     = [t.strip().upper() for t in df["ticker"].dropna()      if str(t).strip()] if "ticker"      in df.columns else []
    security_ids = [s.strip()         for s in df["security_id"].dropna() if str(s).strip()] if "security_id" in df.columns else []
    if not tickers and not security_ids:
        raise ValueError(
            f"CSV must have a 'ticker' or 'security_id' column (found: {list(df.columns)})"
        )
    return tickers, security_ids


# ── Ticker resolution ─────────────────────────────────────────────────────────

def _resolve_tickers(security_ids: list[str], log: logging.Logger) -> list[str]:
    """Look up YH tickers (SourceID) for a list of SecurityIDs via mkt_data_source."""
    df = get_yh_source_id(security_ids=security_ids)
    if df.empty:
        log.warning(f"No YH tickers found in mkt_data_source for security_ids={security_ids}")
        return []
    tickers = df["SourceID"].tolist()
    log.info(f"Resolved security_ids={security_ids} → tickers={tickers}")
    return tickers


def _parse_tickers_input(args, log: logging.Logger) -> list[str]:
    """Parse --ticker / --security-id / --file / default and return a flat ticker list."""
    if args.ticker:
        return [t.strip().upper() for t in args.ticker.split(":") if t.strip()]
    if args.security_id:
        security_ids = [s.strip() for s in args.security_id.split(":") if s.strip()]
        return _resolve_tickers(security_ids, log)
    file_path = args.file or str(CSV_DIR / "ticker.csv")
    file_tickers, file_security_ids = _ids_from_file(file_path)
    log.info(f"Loaded from {file_path}: {len(file_tickers)} ticker(s), {len(file_security_ids)} security_id(s)")
    resolved = _resolve_tickers(file_security_ids, log) if file_security_ids else []
    return file_tickers + resolved


# ── Write helper ──────────────────────────────────────────────────────────────

def _write_csv(df, name: str, log: logging.Logger) -> None:
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = CSV_DIR / f"{name}_{timestamp}.csv"
    df.to_csv(out_path, index=False)
    log.info(f"  Written {len(df)} row(s) to {out_path}")


# ── Subcommand handlers ───────────────────────────────────────────────────────

def run_hist(tickers: list[str], log: logging.Logger) -> None:
    log.info(f"api_hist_price  tickers={tickers}")
    price_df, div_df = api_hist_price(tickers)

    if price_df.empty:
        log.warning("api_hist_price returned no price data.")
    else:
        _write_csv(price_df, "hist_price", log)

    if div_df.empty:
        log.warning("api_hist_price returned no dividend data.")
    else:
        _write_csv(div_df, "hist_dividend", log)


def run_profile(tickers: list[str], log: logging.Logger) -> None:
    log.info(f"api_stock_profiles  tickers={tickers}")
    profile_df = api_stock_profiles(tickers)

    if profile_df.empty:
        log.warning("api_stock_profiles returned no data.")
    else:
        _write_csv(profile_df, "profile", log)


def run_eod(tickers: list[str], log: logging.Logger) -> None:
    today = date.today()
    log.info(f"api_eod_price  tickers={tickers}  date={today}")
    prices_df = api_eod_price(tickers, today)

    if prices_df.empty:
        log.warning("api_eod_price returned no data.")
    else:
        _write_csv(prices_df, "eod_price", log)


def run_update(tickers: list[str] | None, security_ids: list[str] | None, log: logging.Logger) -> None:
    if tickers and security_ids:
        log.info(f"extract_yh_price  tickers={tickers}  security_ids={security_ids}")
    elif tickers:
        log.info(f"extract_yh_price  tickers={tickers}")
    elif security_ids:
        log.info(f"extract_yh_price  security_ids={security_ids}")
    else:
        log.info("extract_yh_price  (all YH securities in mkt_data_source)")
    extract_yh_price(
        security_ids=security_ids or None,
        tickers=tickers or None,
    )
    log.info("extract_yh_price completed — DB and HDF updated")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch Yahoo Finance price data and write to CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python maintenance/yh_fetch.py hist    SPY:AAPL:QQQ\n"
            "  python maintenance/yh_fetch.py hist    --file CSV/ticker.csv\n"
            "  python maintenance/yh_fetch.py eod     SPY:AAPL:QQQ\n"
            "  python maintenance/yh_fetch.py eod     --file CSV/ticker.csv\n"
            "  python maintenance/yh_fetch.py profile SPY:AAPL:QQQ\n"
            "  python maintenance/yh_fetch.py profile --file CSV/ticker.csv\n"
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # hist and eod: --ticker / --security-id / --file / default CSV/ticker.csv
    for cmd, help_text in [
        ("hist", "Fetch historical prices and dividends via api_hist_price()"),
        ("eod",  "Fetch end-of-day prices via api_eod_price()"),
    ]:
        sub = subparsers.add_parser(cmd, help=help_text)
        grp = sub.add_mutually_exclusive_group()
        grp.add_argument("--ticker",      "-t", metavar="TICK1:TICK2:...", help="Colon-separated tickers")
        grp.add_argument("--security-id", "-s", metavar="SEC1:SEC2:...",  help="Colon-separated SecurityIDs")
        grp.add_argument("--file",        "-f", metavar="CSV_FILE",       help="CSV with 'ticker' and/or 'security_id' column (default: CSV/ticker.csv)")

    # profile: positional tickers or --file (unchanged)
    sub = subparsers.add_parser("profile", help="Fetch stock profiles via api_stock_profiles()")
    src = sub.add_mutually_exclusive_group(required=True)
    src.add_argument("tickers", metavar="TICKER1:TICKER2:...", nargs="?", help="Colon-separated tickers")
    src.add_argument("--file", "-f", metavar="CSV_FILE", help="CSV file with a 'ticker' column")

    # update: same options as hist/eod but passes both tickers and security_ids to extract_yh_price
    sub = subparsers.add_parser("update", help="Fetch YH historical prices and write to DB + HDF via extract_yh_price()")
    grp = sub.add_mutually_exclusive_group()
    grp.add_argument("--ticker",      "-t", metavar="TICK1:TICK2:...", help="Colon-separated tickers")
    grp.add_argument("--security-id", "-s", metavar="SEC1:SEC2:...",  help="Colon-separated SecurityIDs")
    grp.add_argument("--file",        "-f", metavar="CSV_FILE",       help="CSV with 'ticker' and/or 'security_id' column (default: CSV/ticker.csv)")

    args = parser.parse_args()
    log = _setup_logger()

    if args.command in ("hist", "eod"):
        try:
            tickers = _parse_tickers_input(args, log)
        except (FileNotFoundError, ValueError) as e:
            parser.error(str(e))
        if not tickers:
            parser.error("No tickers resolved.")
        log.info(f"Command: {args.command}  |  Tickers: {tickers}")
        if args.command == "hist":
            run_hist(tickers, log)
        else:
            run_eod(tickers, log)

    elif args.command == "update":
        try:
            if args.ticker:
                tickers      = [t.strip().upper() for t in args.ticker.split(":") if t.strip()]
                security_ids = []
            elif args.security_id:
                tickers      = []
                security_ids = [s.strip() for s in args.security_id.split(":") if s.strip()]
            else:
                file_path = args.file or str(CSV_DIR / "ticker.csv")
                tickers, security_ids = _ids_from_file(file_path)
                log.info(f"Loaded from {file_path}: {len(tickers)} ticker(s), {len(security_ids)} security_id(s)")
        except (FileNotFoundError, ValueError) as e:
            parser.error(str(e))
        log.info("Command: update")
        run_update(tickers or None, security_ids or None, log)

    else:  # profile
        try:
            if args.file:
                tickers = _tickers_from_file(args.file)
                log.info(f"Loaded {len(tickers)} tickers from {args.file}")
            else:
                tickers = [t.strip().upper() for t in args.tickers.split(":") if t.strip()]
        except (FileNotFoundError, ValueError) as e:
            parser.error(str(e))
        if not tickers:
            parser.error("No tickers provided.")
        log.info(f"Command: profile  |  Tickers: {tickers}")
        run_profile(tickers, log)

    log.info("─" * 60)
    log.info("Done.")
