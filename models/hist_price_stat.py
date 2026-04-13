# -*- coding: utf-8 -*-
"""
hist_price_stat.py — Calculate historical price statistics for all securities.

Statistics per security
-----------------------
    min, max, mean, stdev   — on the price series
    vol                     — annualised daily return volatility (stdev × √252)
    start_date, end_date    — first and last date with a non-null price
    obs_count               — number of non-null price observations

Output columns also include: SecurityID, SecurityName, Currency, AssetClass, AssetType.

Usage
-----
    # all securities, all history
    python models/hist_price_stat.py

    # filter by date range
    python models/hist_price_stat.py --from_date 2020-01-01 --to_date 2024-12-31

    # subset of securities (CSV with SecurityID column in models/test_data/)
    python models/hist_price_stat.py --sec_file my_securities.csv

Output
------
    models/test_output/hist_price_stat.csv   (overwritten each run)
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

ANNUALIZE    = 252
TEST_DATA_DIR   = Path(__file__).resolve().parent / 'test_data'
TEST_OUTPUT_DIR = Path(__file__).resolve().parent / 'test_output'


# ---------------------------------------------------------------------------
# Data retrieval
# ---------------------------------------------------------------------------

def get_securities(sec_file=None):
    """
    Return DataFrame with SecurityID, SecurityName, Currency, AssetClass, AssetType.
    If sec_file is given, filtered to those SecurityIDs; otherwise all in security_info.
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

    if sec_file:
        csv_path = TEST_DATA_DIR / sec_file
        if not csv_path.exists():
            raise FileNotFoundError(f"Securities file not found: {csv_path}")
        ids = pd.read_csv(csv_path)['SecurityID'].dropna().tolist()
        df = df[df['SecurityID'].isin(ids)].reset_index(drop=True)
        print(f"{len(df)} securities from {sec_file}")
    else:
        print(f"{len(df)} securities from security_info")

    return df


def get_prices(sec_ids, from_date=None, to_date=None):
    """Fetch price time series for given security IDs."""
    prices = mkt_timeseries.get(sec_ids, from_date=from_date, to_date=to_date)
    return prices


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
    # price stats (ignoring NaN)
    stat = pd.DataFrame({
        'min':       prices.min(),
        'max':       prices.max(),
        'mean':      prices.mean(),
        'stdev':     prices.std(ddof=1),
        'obs_count': prices.count(),
        'start_date': prices.apply(lambda s: s.first_valid_index()),
        'end_date':   prices.apply(lambda s: s.last_valid_index()),
    })

    # annualised vol from daily returns
    returns = prices.pct_change()
    stat['vol'] = returns.std(ddof=1) * np.sqrt(ANNUALIZE)

    # round price stats for readability
    for col in ['min', 'max', 'mean', 'stdev', 'vol']:
        stat[col] = stat[col].round(6)

    # keep only securities that had any data
    stat = stat[stat['obs_count'] > 0]

    stat.index.name = 'SecurityID'
    return stat


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(from_date=None, to_date=None, sec_file=None):
    """
    Calculate and save historical price statistics for all (or a subset of) securities.

    Parameters
    ----------
    from_date : datetime.datetime or None
    to_date   : datetime.datetime or None
    sec_file  : str or None — CSV filename in models/test_data/ (SecurityID column)
    """
    TEST_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    date_range = (
        f"{from_date.strftime('%Y-%m-%d') if from_date else 'all'}"
        f" to {to_date.strftime('%Y-%m-%d') if to_date else 'today'}"
    )
    print(f"hist_price_stat | date range: {date_range}")

    # 1. securities
    securities = get_securities(sec_file)
    sec_ids = securities['SecurityID'].tolist()

    # 2. prices
    print("Fetching price data...")
    prices = get_prices(sec_ids, from_date=from_date, to_date=to_date)
    print(f"Price matrix: {prices.shape[0]} dates x {prices.shape[1]} securities")

    # 3. statistics
    print("Calculating statistics...")
    stat = calc_stats(prices)

    # 4. join security info
    sec_info = securities.set_index('SecurityID')
    result = sec_info.join(stat, how='left')

    # reorder columns
    result = result.reset_index()[[
        'SecurityID', 'SecurityName', 'Currency', 'AssetClass', 'AssetType',
        'obs_count', 'start_date', 'end_date',
        'min', 'max', 'mean', 'stdev', 'vol',
    ]]

    # 5. save
    out = TEST_OUTPUT_DIR / 'hist_price_stat.csv'
    result.to_csv(out, index=False)
    print(f"Saved: {out}  ({len(result)} securities)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Calculate historical price statistics')
    parser.add_argument(
        '--from_date',
        default=None,
        metavar='YYYY-MM-DD',
        help='Start date for price history (default: all available)',
    )
    parser.add_argument(
        '--to_date',
        default=None,
        metavar='YYYY-MM-DD',
        help='End date for price history (default: today)',
    )
    parser.add_argument(
        '--sec_file',
        default=None,
        metavar='FILENAME',
        help='CSV filename inside models/test_data/ to limit securities (default: all in security_info)',
    )
    args = parser.parse_args()

    def _parse(d):
        return datetime.datetime.strptime(d, '%Y-%m-%d') if d else None

    run(
        from_date = _parse(args.from_date),
        to_date   = _parse(args.to_date),
        sec_file  = args.sec_file,
    )
