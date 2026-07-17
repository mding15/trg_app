"""
fetch_sp500_history.py — Download S&P 500 index (^GSPC) daily price history from
Yahoo Finance and save it to a CSV file.

Yahoo Finance's ^GSPC series goes back to late 1927. Requires the yfinance package:
    pip install yfinance

Usage:
    python fetch_sp500_history.py
    python fetch_sp500_history.py --start 1950-01-01
    python fetch_sp500_history.py --ticker ^DJI --file dow_history.csv

Output: data/maintenance/CSV/<ticker>_history_<YYYYMMDD_HHMMSS>.csv
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime

import pandas as pd

try:
    import yfinance as yf
except ImportError:
    print("yfinance is not installed. Install it with:  pip install yfinance", file=sys.stderr)
    sys.exit(1)

from _paths import CSV_DIR

DEFAULT_TICKER = '^GSPC'
# Yahoo's ^GSPC history starts 1927-12-30; requesting an earlier start just
# clips to whatever's actually available, so this works for any index/ticker.
DEFAULT_START = '1927-01-01'


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger('fetch_sp500_history')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s', '%H:%M:%S')
    )
    logger.addHandler(handler)
    return logger


def fetch(ticker: str, start: str, end: str | None, log: logging.Logger) -> pd.DataFrame:
    log.info(f"Downloading '{ticker}' from {start} to {end or 'today'} …")
    df = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False)
    if df.empty:
        raise RuntimeError(f"No data returned for '{ticker}' — check the ticker symbol.")

    # Recent yfinance versions return a MultiIndex column header (Price, Ticker)
    # even for a single ticker — flatten it to plain column names (Open/High/Low/...).
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df.index.name = 'Date'
    log.info(f"  {len(df)} row(s), {df.index.min().date()} to {df.index.max().date()}")
    return df


def run(ticker: str, start: str, end: str | None, out_file: str | None) -> None:
    log = _setup_logger()
    df = fetch(ticker, start, end, log)

    CSV_DIR.mkdir(parents=True, exist_ok=True)
    if out_file:
        out_path = CSV_DIR / out_file
    else:
        timestamp   = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_ticker = ticker.replace('^', '').lower()
        out_path    = CSV_DIR / f'{safe_ticker}_history_{timestamp}.csv'

    df.to_csv(out_path)
    log.info(f"Saved: {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Download S&P 500 (or any Yahoo Finance ticker) daily price history to CSV.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'Examples:\n'
            '  python fetch_sp500_history.py\n'
            '  python fetch_sp500_history.py --start 1950-01-01\n'
            '  python fetch_sp500_history.py --ticker ^DJI --file dow_history.csv\n'
        ),
    )
    parser.add_argument('--ticker', default=DEFAULT_TICKER, metavar='TICKER',
                        help=f'Yahoo Finance ticker symbol (default: {DEFAULT_TICKER})')
    parser.add_argument('--start', default=DEFAULT_START, metavar='YYYY-MM-DD',
                        help=f"Start date (default: {DEFAULT_START}, earlier than Yahoo's "
                             f'earliest data so it naturally clips to what exists)')
    parser.add_argument('--end', default=None, metavar='YYYY-MM-DD',
                        help='End date (default: today)')
    parser.add_argument('--file', default=None, metavar='FILENAME',
                        help='Output CSV filename inside data/maintenance/CSV/ '
                             '(default: <ticker>_history_<timestamp>.csv)')
    args = parser.parse_args()
    run(args.ticker, args.start, args.end, args.file)


if __name__ == '__main__':
    main()
