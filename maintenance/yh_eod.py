"""
yh_eod.py — Run end-of-day price extraction for Yahoo Finance securities.

Calls detl.yh_extract.extract_eod(), which:
  1. Fetches EOD quotes from Yahoo Finance for the given tickers (or all YH securities).
  2. Saves raw data to YH_DIR/EOD/<year>/price.<YYYYMMDD>.csv.
  3. Deletes existing current_price rows for the date (idempotent re-run).
  4. Bulk-inserts fresh prices into current_price.

Usage:
    python maintenance/yh_eod.py                          # all YH securities, date from proc_asof_date
    python maintenance/yh_eod.py --date 2026-06-27        # specific date, all YH securities
    python maintenance/yh_eod.py --ticker SPY:AAPL:QQQ    # specific tickers, date from proc_asof_date
    python maintenance/yh_eod.py --ticker SPY:VIX --date 2026-06-27
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from detl.yh_extract import extract_eod

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run Yahoo Finance EOD price extraction.')
    parser.add_argument(
        '--ticker', '-t', metavar='TICK1:TICK2:...',
        help='Colon-separated tickers (default: all YH securities in current_security)',
    )
    parser.add_argument(
        '--date', '-d', metavar='YYYY-MM-DD',
        help='As-of date (default: read from proc_asof_date table)',
    )
    args = parser.parse_args()

    tickers = [t.strip().upper() for t in args.ticker.split(':') if t.strip()] if args.ticker else None
    asof    = datetime.strptime(args.date, '%Y-%m-%d').date() if args.date else None

    extract_eod(tickers=tickers, asof_date=asof)
