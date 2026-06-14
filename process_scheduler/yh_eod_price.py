"""
process_scheduler/yh_eod_price.py

Scheduler entry point for end-of-day price extraction.
Calls yh_extract.extract_eod(), which does the following:

  1. Get as_of_date from proc_asof_date table.
  2. Get tickers from current_security table (DataSource = 'YH'), and build a
     Ticker → SecurityID lookup map.
  3. Call Yahoo Finance GET_QUOTES API to fetch real-time quotes for all tickers.
     Extracts: open, high, low, close, volume, price time.
     Saves raw data to YH_DIR/EOD/<year>/price.<YYYYMMDD>.csv.
  4. Map SecurityID onto the price DataFrame using the ticker → ID map.
  5. Delete any existing rows in current_price for as_of_date (idempotent re-run).
  6. Bulk-insert the fresh prices into current_price.

Output table: current_price (SecurityID, Ticker, Date, Open, High, Low, Close,
              Volume, PriceTime)

Usage:
  python yh_eod_price.py                    # run normally (date from proc_asof_date)
  python yh_eod_price.py 2026-06-09         # override as-of date
  python yh_eod_price.py --register         # register/update job in scheduler
"""

import argparse
import logging
import os
import sys
from datetime import datetime

# Configure the root logger before any trg_app imports.
# database.db_utils → api → api/logging_config.py calls logging.basicConfig()
# which is a no-op if handlers already exist — so we must register our handler first.
_log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'log'))
os.makedirs(_log_dir, exist_ok=True)
_log_file = os.path.join(_log_dir, f'yh_eod_price.{datetime.now().strftime("%Y%m%d.%H%M%S")}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(_log_file)],
)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from detl.yh_extract import extract_eod


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('asof_date', nargs='?', default=None,
                        help='Override as-of date (YYYY-MM-DD)')
    parser.add_argument('--register', action='store_true',
                        help='Register/update job in scheduler')
    args = parser.parse_args()

    if args.register:
        from register import register_by_id
        register_by_id('yh_eod_price')
    else:
        asof = datetime.strptime(args.asof_date, '%Y-%m-%d').date() if args.asof_date else None
        extract_eod(asof_date=asof)