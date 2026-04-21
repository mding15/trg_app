# -*- coding: utf-8 -*-
"""
util_hist_prices.py — Fetch historical prices and calculate statistics for all securities.

Statistics per security
-----------------------
    min, max, mean, stdev   — on the price series
    vol                     — annualised daily return volatility (stdev × √252)
    start_date, end_date    — first and last date with a non-null price
    obs_count               — number of non-null price observations

Output columns also include: SecurityID, SecurityName, Currency, AssetClass, AssetType.

Usage
-----
    # default: securities from test_data/test_securities.csv, exports both stats and prices
    python models/util_hist_prices.py

    # all securities from DB
    python models/util_hist_prices.py --all

    # custom securities file (CSV with SecurityID column in models/test_data/)
    python models/util_hist_prices.py --sec_file my_securities.csv

    # filter by date range
    python models/util_hist_prices.py --from_date 2020-01-01 --to_date 2024-12-31

    # export stats only (skip price CSV)
    python models/util_hist_prices.py --no-prices

    # export prices only (skip stats CSV)
    python models/util_hist_prices.py --no-stats

Output
------
    models/test_output/hist_price_stat.csv   — per-security statistics
    models/test_output/hist_prices.csv       — wide price matrix (dates × securities)
"""

import sys
import argparse
import datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database2 import pg_connection
from mkt_data import mkt_timeseries

ANNUALIZE       = 252
TEST_DATA_DIR   = Path(__file__).resolve().parent / 'test_data'
TEST_OUTPUT_DIR = Path(__file__).resolve().parent / 'test_output'


# ---------------------------------------------------------------------------
# Data retrieval
# ---------------------------------------------------------------------------

DEFAULT_SEC_FILE = 'test_securities.csv'


def get_securities(sec_file=None, use_all=False):
    """
    Return DataFrame with SecurityID, SecurityName, Currency, AssetClass, AssetType.

    Priority:
        1. use_all=True  → all securities from security_info (no filter)
        2. sec_file      → filtered to IDs in test_data/<sec_file>
        3. default       → filtered to IDs in test_data/test_securities.csv
    """
    query = """
        SELECT "SecurityID", "SecurityName", "Currency", "AssetClass", "AssetType"
        FROM security_info
        ORDER BY "SecurityID"
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]

    df = pd.DataFrame(rows, columns=cols)

    if use_all:
        print(f"{len(df)} securities from security_info")
        return df

    csv_name = sec_file or DEFAULT_SEC_FILE
    csv_path = TEST_DATA_DIR / csv_name
    if not csv_path.exists():
        raise FileNotFoundError(f"Securities file not found: {csv_path}")
    ids = pd.read_csv(csv_path)['SecurityID'].dropna().tolist()
    df = df[df['SecurityID'].isin(ids)].reset_index(drop=True)
    print(f"{len(df)} securities from {csv_name}")
    return df


def get_prices(sec_ids, from_date=None, to_date=None):
    """Fetch price time series for given security IDs."""
    return mkt_timeseries.get(sec_ids, from_date=from_date, to_date=to_date)


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def calc_stats(prices):
    """
    Calculate per-security statistics from a price DataFrame.

    Returns
    -------
    DataFrame indexed by SecurityID with columns:
        min, max, mean, stdev, vol, obs_count, start_date, end_date
    """
    stat = pd.DataFrame({
        'min':        prices.min(),
        'max':        prices.max(),
        'mean':       prices.mean(),
        'stdev':      prices.std(ddof=1),
        'obs_count':  prices.count(),
        'start_date': prices.apply(lambda s: s.first_valid_index()),
        'end_date':   prices.apply(lambda s: s.last_valid_index()),
    })

    returns = prices.pct_change()
    stat['vol'] = returns.std(ddof=1) * np.sqrt(ANNUALIZE)

    for col in ['min', 'max', 'mean', 'stdev', 'vol']:
        stat[col] = stat[col].round(6)

    stat = stat[stat['obs_count'] > 0]
    stat.index.name = 'SecurityID'
    return stat


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(from_date=None, to_date=None, sec_file=None, use_all=False, export_stats=True, export_prices=True):
    """
    Fetch prices and optionally save the price matrix and/or per-security statistics.

    Parameters
    ----------
    from_date     : datetime.datetime or None
    to_date       : datetime.datetime or None
    sec_file      : str or None — CSV filename in models/test_data/ (SecurityID column)
    use_all       : bool — if True, use all securities from DB (ignores sec_file)
    export_stats  : bool — write per-security statistics CSV
    export_prices : bool — write wide price matrix CSV
    """
    TEST_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    date_range = (
        f"{from_date.strftime('%Y-%m-%d') if from_date else 'all'}"
        f" to {to_date.strftime('%Y-%m-%d') if to_date else 'today'}"
    )
    print(f"util_hist_prices | date range: {date_range}")

    # 1. securities
    securities = get_securities(sec_file=sec_file, use_all=use_all)
    sec_ids = securities['SecurityID'].tolist()

    # 2. prices
    print("Fetching price data...")
    prices = get_prices(sec_ids, from_date=from_date, to_date=to_date)
    print(f"Price matrix: {prices.shape[0]} dates x {prices.shape[1]} securities")

    # 3. export wide price matrix (dates × SecurityIDs)
    if export_prices:
        out_prices = TEST_OUTPUT_DIR / 'hist_prices.csv'
        prices.to_csv(out_prices, index=True)
        print(f"Saved: {out_prices}  ({prices.shape[0]} dates x {prices.shape[1]} securities)")

    # 4. statistics
    if export_stats:
        print("Calculating statistics...")
        stat = calc_stats(prices)

        sec_info = securities.set_index('SecurityID')
        result = sec_info.join(stat, how='left')
        result = result.reset_index()[[
            'SecurityID', 'SecurityName', 'Currency', 'AssetClass', 'AssetType',
            'obs_count', 'start_date', 'end_date',
            'min', 'max', 'mean', 'stdev', 'vol',
        ]]

        out_stats = TEST_OUTPUT_DIR / 'hist_price_stat.csv'
        result.to_csv(out_stats, index=False)
        print(f"Saved: {out_stats}  ({len(result)} securities)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch historical prices and calculate statistics')
    parser.add_argument('--from_date', default=None, metavar='YYYY-MM-DD',
                        help='Start date for price history (default: all available)')
    parser.add_argument('--to_date',   default=None, metavar='YYYY-MM-DD',
                        help='End date for price history (default: today)')
    parser.add_argument('--sec_file',  default=None, metavar='FILENAME',
                        help='CSV filename in models/test_data/ to limit securities (default: all)')
    parser.add_argument('--all',       dest='use_all',  action='store_true',
                        help='Use all securities from DB (default: test_data/test_securities.csv)')
    parser.add_argument('--no-stats',  dest='no_stats',  action='store_true',
                        help='Skip writing the statistics CSV')
    parser.add_argument('--no-prices', dest='no_prices', action='store_true',
                        help='Skip writing the price matrix CSV')
    args = parser.parse_args()

    def _parse(d):
        return datetime.datetime.strptime(d, '%Y-%m-%d') if d else None

    run(
        from_date     = _parse(args.from_date),
        to_date       = _parse(args.to_date),
        sec_file      = args.sec_file,
        use_all       = args.use_all,
        export_stats  = not args.no_stats,
        export_prices = not args.no_prices,
    )
