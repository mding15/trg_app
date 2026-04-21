"""
yh_fetch.py — Fetch Yahoo Finance price data for a list of tickers and write to CSV.

Subcommands:
    hist   — call api_hist_price(): writes hist_price and hist_dividend CSVs
    eod    — call api_eod_price():  writes eod_price CSV (uses today's date)

Tickers can be supplied as a colon-separated string or via a CSV file (--file).
The CSV file must have a 'ticker' column (see CSV/ticker.csv for an example).

Output files (maintenance/CSV/):
    hist_price_YYYYMMDD_HHMMSS.csv
    hist_dividend_YYYYMMDD_HHMMSS.csv
    eod_price_YYYYMMDD_HHMMSS.csv

Usage:
    python maintenance/yh_fetch.py hist SPY:AAPL:QQQ
    python maintenance/yh_fetch.py hist --file CSV/ticker.csv
    python maintenance/yh_fetch.py eod  SPY:AAPL:QQQ
    python maintenance/yh_fetch.py eod  --file CSV/ticker.csv
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from detl.yh_extract import api_hist_price, api_eod_price

CSV_DIR = Path(__file__).resolve().parent / "CSV"


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


def run_eod(tickers: list[str], log: logging.Logger) -> None:
    today = date.today()
    log.info(f"api_eod_price  tickers={tickers}  date={today}")
    prices_df = api_eod_price(tickers, today)

    if prices_df.empty:
        log.warning("api_eod_price returned no data.")
    else:
        _write_csv(prices_df, "eod_price", log)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch Yahoo Finance price data and write to CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python maintenance/yh_fetch.py hist SPY:AAPL:QQQ\n"
            "  python maintenance/yh_fetch.py hist --file CSV/ticker.csv\n"
            "  python maintenance/yh_fetch.py eod  SPY:AAPL:QQQ\n"
            "  python maintenance/yh_fetch.py eod  --file CSV/ticker.csv\n"
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    for cmd, help_text in [
        ("hist", "Fetch historical prices and dividends via api_hist_price()"),
        ("eod",  "Fetch end-of-day prices via api_eod_price()"),
    ]:
        sub = subparsers.add_parser(cmd, help=help_text)
        source = sub.add_mutually_exclusive_group(required=True)
        source.add_argument(
            "tickers",
            metavar="TICKER1:TICKER2:...",
            nargs="?",
            help="Colon-separated list of tickers",
        )
        source.add_argument(
            "--file", "-f",
            metavar="CSV_FILE",
            help="CSV file with a 'ticker' column (e.g. CSV/ticker.csv)",
        )

    args = parser.parse_args()
    log = _setup_logger()

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

    log.info(f"Command: {args.command}  |  Tickers: {tickers}")

    if args.command == "hist":
        run_hist(tickers, log)
    else:
        run_eod(tickers, log)

    log.info("─" * 60)
    log.info("Done.")
