# -*- coding: utf-8 -*-
"""
yh_update.py — Fix bad Yahoo Finance price data for specific securities.

For each given security_id:
    1) update_yh_price() — delete + refetch its full yh_stock_price history
       from Yahoo Finance, then fully overwrite its HDF series.
    2) copy_stock_price_to_current_price() — delete all its current_price
       rows and reinsert the most recent N_DAYS trading days from the
       freshly refetched history.

Usage:
    python maintenance/yh_update.py T10000880,T10001583
"""
import argparse

from detl import yh_extract
from mkt_data import mkt_data_extract

N_DAYS = 250


def main():
    parser = argparse.ArgumentParser(description="Refetch YH price data for specific securities.")
    parser.add_argument("security_ids", help="comma-separated security_ids, e.g. T10000880,T10001583")
    args = parser.parse_args()

    security_ids = [s.strip() for s in args.security_ids.split(",") if s.strip()]

    df = mkt_data_extract.get_yh_source_id(security_ids=security_ids)
    resolved = df['SecurityID'].to_list()
    missing = set(security_ids) - set(resolved)
    if missing:
        print(f"WARNING: not found as YH securities, skipping: {', '.join(sorted(missing))}")
    if df.empty:
        print("No YH securities resolved -- nothing to do.")
        return

    tickers = df['SourceID'].to_list()

    # 1) delete + refetch full price history, rewrite hdf
    mkt_data_extract.update_yh_price(security_ids=resolved)

    # 2) delete + reinsert the last N_DAYS trading days into current_price
    yh_extract.copy_stock_price_to_current_price(tickers, n_days=N_DAYS)


if __name__ == '__main__':
    main()
