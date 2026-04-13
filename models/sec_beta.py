# -*- coding: utf-8 -*-
"""
sec_beta.py — Calculate single-factor beta for all securities in security_info.

Beta is computed via OLS regression of a security's simple daily returns against
a benchmark's simple daily returns over a configurable lookback window.

DB tables
---------
beta_definition   — defines each beta_key (benchmark, lookback, min_obs, etc.)
sec_beta          — stores results; upserted on each run, keyed on (security_id, beta_key)

First-time setup
----------------
1. Create tables:
       python database2/create_tables.py

2. Seed beta_definition (once per beta_key):
       python models/sec_beta.py --seed
   or from Python:
       from models.sec_beta import seed_beta_definition
       seed_beta_definition()

Usage
-----
    # full run — calculates all securities and writes to DB
    python models/sec_beta.py
    python models/sec_beta.py --beta_key SP500_1Y

    # test mode — small securities list, writes CSVs to test_output/, no DB writes
    python models/sec_beta.py --test
    python models/sec_beta.py --test --test_file my_secs.csv --beta_key SP500_1Y

Test inputs / outputs
---------------------
    Input  : models/test_data/<test_file>     (one column: SecurityID)
    Output : models/test_output/raw_prices.csv          — raw prices before any processing
             models/test_output/regression_input.csv    — simple returns after ffill + trim
             models/test_output/regression_results.csv  — beta, r², vol, obs_count per security

Log
---
    Appended to: <parent of trg_app>/log/sec_beta.log
"""

import sys
import os
import argparse
import datetime
import logging
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database2 import pg_connection
from mkt_data import mkt_timeseries

DEFAULT_BETA_KEY = 'SP500_1Y'
ANNUALIZE        = 252          # trading days per year, for vol annualisation

LOG_DIR = Path(__file__).resolve().parent.parent.parent / 'log'


# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------

log = logging.getLogger('sec_beta')


def _setup_logger():
    """Attach console + file handlers if not already set up."""
    if log.handlers:
        return

    log.setLevel(logging.DEBUG)
    fmt = logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s', '%Y-%m-%d %H:%M:%S')

    # console
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    log.addHandler(ch)

    # file — append mode so all runs accumulate
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOG_DIR / 'sec_beta.log', mode='a', encoding='utf-8')
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    log.addHandler(fh)


# ---------------------------------------------------------------------------
# DB reads
# ---------------------------------------------------------------------------

def get_beta_definition(beta_key):
    """Return beta_definition row for beta_key as a dict."""
    query = "SELECT * FROM beta_definition WHERE beta_key = %s"
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (beta_key,))
            row = cur.fetchone()
            if row is None:
                raise ValueError(
                    f"beta_key '{beta_key}' not found in beta_definition. "
                    "Run seed_beta_definition() first."
                )
            cols = [desc[0] for desc in cur.description]
    return dict(zip(cols, row))


def get_all_security_ids():
    """Return list of all SecurityIDs from security_info."""
    query = 'SELECT "SecurityID" FROM security_info'
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
    return [row[0] for row in rows]


# ---------------------------------------------------------------------------
# Price data
# ---------------------------------------------------------------------------

def fetch_raw_prices(sec_ids, benchmark_id, lookback_days, as_of_date=None):
    """
    Fetch raw prices from mkt_timeseries and remove bad values.

    Parameters
    ----------
    as_of_date : datetime.datetime or None
        End date for the price window. Defaults to today if None.

    Returns
    -------
    DataFrame — date-indexed, one column per security_id, no cleaning applied yet.
    """
    all_ids = list(set(sec_ids) | {benchmark_id})

    # use datetime.datetime so comparison against datetime64[ns] index works
    to_date   = as_of_date if as_of_date is not None else datetime.datetime.today()
    from_date = to_date - datetime.timedelta(days=int(lookback_days * 1.6))

    prices = mkt_timeseries.get(all_ids, from_date=from_date, to_date=to_date)

    return prices


def process_prices(prices, benchmark_id, lookback_days):
    """
    Clean, forward fill, and trim to the last `lookback_days` benchmark observations.

    Returns
    -------
    DataFrame — cleaned and trimmed price series.
    """
    # replace prices below 0.001 with NaN before forward fill
    prices = prices.copy()
    prices[prices < 0.001] = None

    prices = prices.ffill()

    if benchmark_id in prices.columns:
        bmk_dates = prices[benchmark_id].dropna().index
        if len(bmk_dates) >= lookback_days:
            cutoff = bmk_dates[-lookback_days]
            prices = prices[prices.index >= cutoff]

    return prices


def calc_returns(prices):
    """Compute simple daily returns from a price DataFrame."""
    return prices.pct_change().dropna(how='all')


# ---------------------------------------------------------------------------
# Beta calculation
# ---------------------------------------------------------------------------

def calc_security_beta(sec_returns, bmk_returns, min_obs):
    """
    OLS regression: sec_returns = alpha + beta * bmk_returns + epsilon.

    Parameters
    ----------
    sec_returns  : pd.Series — simple daily returns of the security
    bmk_returns  : pd.Series — simple daily returns of the benchmark
    min_obs      : int       — minimum observations required

    Returns
    -------
    (beta, r_squared, ann_vol, obs_count, start_date, end_date)
    or None if there are fewer than min_obs aligned observations.
    """
    df = pd.concat([sec_returns, bmk_returns], axis=1, sort=False).dropna()
    df = df[np.isfinite(df).all(axis=1)]
    obs_count = len(df)

    if obs_count < min_obs:
        return None

    y = df.iloc[:, 0].values
    x = df.iloc[:, 1].values

    # OLS with intercept: beta = cov(y,x) / var(x)
    x_mean, y_mean = x.mean(), y.mean()
    x_dm = x - x_mean

    var_x = np.dot(x_dm, x_dm)
    if var_x == 0:
        return None

    beta  = float(np.dot(x_dm, y - y_mean) / var_x)
    alpha = y_mean - beta * x_mean

    # R-squared
    y_hat  = alpha + beta * x
    ss_res = np.sum((y - y_hat) ** 2)
    ss_tot = np.sum((y - y_mean) ** 2)
    r_squared = float(1.0 - ss_res / ss_tot) if ss_tot > 0 else 0.0

    # annualised volatility of security (simple returns)
    ann_vol = float(np.std(y, ddof=1) * np.sqrt(ANNUALIZE))

    # date range of data actually used
    idx = df.index
    start_date = idx[0].date()  if hasattr(idx[0],  'date') else idx[0]
    end_date   = idx[-1].date() if hasattr(idx[-1], 'date') else idx[-1]

    return beta, r_squared, ann_vol, obs_count, start_date, end_date


# ---------------------------------------------------------------------------
# DB write
# ---------------------------------------------------------------------------

def upsert_results(results):
    """
    Upsert a list of result dicts into sec_beta.
    PK is (security_id, beta_key) — existing rows are overwritten.
    """
    if not results:
        return

    query = """
        INSERT INTO sec_beta (
            security_id, beta_key, benchmark_id,
            beta, r_squared, vol, obs_count,
            start_date, end_date, calc_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (security_id, beta_key) DO UPDATE SET
            benchmark_id = EXCLUDED.benchmark_id,
            beta         = EXCLUDED.beta,
            r_squared    = EXCLUDED.r_squared,
            vol          = EXCLUDED.vol,
            obs_count    = EXCLUDED.obs_count,
            start_date   = EXCLUDED.start_date,
            end_date     = EXCLUDED.end_date,
            calc_date    = EXCLUDED.calc_date
    """

    rows = [(
        r['security_id'], r['beta_key'],    r['benchmark_id'],
        r['beta'],        r['r_squared'],   r['vol'],
        r['obs_count'],   r['start_date'],  r['end_date'],
        r['calc_date'],
    ) for r in results]

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(query, rows)
        conn.commit()

    log.info(f"Upserted {len(rows)} rows into sec_beta.")


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def run(beta_key=DEFAULT_BETA_KEY, as_of_date=None, sec_file=None):
    """
    Calculate and upsert betas for all securities in security_info.

    Parameters
    ----------
    beta_key   : str
        Key referencing a row in beta_definition (e.g. 'SP500_1Y').
    as_of_date : datetime.datetime or None
        End date for the lookback window. Defaults to today if None.
    sec_file   : str or None
        CSV filename inside models/test_data/ with a SecurityID column.
        If None, all securities from security_info are used.
    """
    _setup_logger()
    as_of_date = as_of_date if as_of_date is not None else datetime.datetime.today()
    log.info(f"sec_beta run | beta_key={beta_key}  as_of_date={as_of_date.strftime('%Y-%m-%d')}")

    # 1. load beta definition
    defn          = get_beta_definition(beta_key)
    benchmark_id  = defn['benchmark_id']
    lookback_days = int(defn['lookback_days'])
    min_obs       = int(defn.get('min_obs') or 100)
    log.info(f"benchmark_id={benchmark_id}  lookback_days={lookback_days}  min_obs={min_obs}")

    # 2. securities — from file if provided, otherwise all in security_info
    if sec_file:
        csv_path = TEST_DATA_DIR / sec_file
        if not csv_path.exists():
            raise FileNotFoundError(f"Securities file not found: {csv_path}")
        sec_ids = pd.read_csv(csv_path)['SecurityID'].dropna().tolist()
        log.info(f"{len(sec_ids)} securities from {sec_file}")
    else:
        sec_ids = get_all_security_ids()
        log.info(f"{len(sec_ids)} securities from security_info")

    # 3. fetch, process, and compute returns
    log.info("Fetching price data...")
    raw_prices = fetch_raw_prices(sec_ids, benchmark_id, lookback_days, as_of_date)
    log.info(f"Raw price matrix: {raw_prices.shape[0]} dates x {raw_prices.shape[1]} securities")

    prices  = process_prices(raw_prices, benchmark_id, lookback_days)
    returns = calc_returns(prices)

    if benchmark_id not in returns.columns:
        raise ValueError(f"No price data found for benchmark {benchmark_id}")

    # benchmark simple returns (computed once)
    bmk_returns = returns[benchmark_id].dropna()
    calc_date   = datetime.date.today()

    results = []
    skipped = []   # list of (security_id, reason)
    errors  = []   # list of (security_id, error_message)

    # 4. loop over securities
    log.info("Calculating betas...")
    for i, sec_id in enumerate(sec_ids):

        if sec_id not in returns.columns:
            skipped.append((sec_id, 'no price data'))
            continue

        try:
            sec_returns = returns[sec_id].dropna()
            result = calc_security_beta(sec_returns, bmk_returns, min_obs)

            if result is None:
                skipped.append((sec_id, f'obs < {min_obs}'))
                continue

            beta, r_squared, vol, obs_count, start_date, end_date = result

            results.append({
                'security_id':  sec_id,
                'beta_key':     beta_key,
                'benchmark_id': benchmark_id,
                'beta':         beta,
                'r_squared':    r_squared,
                'vol':          vol,
                'obs_count':    obs_count,
                'start_date':   start_date,
                'end_date':     end_date,
                'calc_date':    calc_date,
            })

        except Exception as e:
            errors.append((sec_id, str(e)))
            log.warning(f"Error calculating beta for {sec_id}: {e}")

        if (i + 1) % 500 == 0:
            log.info(f"Processed {i + 1}/{len(sec_ids)}")

    # 5. upsert
    upsert_results(results)

    # 6. save skipped to CSV (append so all runs accumulate)
    if skipped:
        skipped_df = pd.DataFrame(skipped, columns=['security_id', 'reason'])
        skipped_df.insert(0, 'calc_date', calc_date)
        skipped_df.insert(1, 'beta_key',  beta_key)
        skipped_path = LOG_DIR / 'sec_beta_skipped.csv'
        skipped_df.to_csv(skipped_path, mode='a',
                          header=not skipped_path.exists(), index=False)
        log.info(f"Skipped securities saved to {skipped_path}")

    # 7. summary
    log.info(f"Summary — Calculated: {len(results)}  Skipped: {len(skipped)}  Errors: {len(errors)}")

    if skipped:
        log.warning(f"Skipped {len(skipped)} securities (first 10):")
        for sec_id, reason in skipped[:10]:
            log.warning(f"  {sec_id}: {reason}")

    if errors:
        log.warning(f"Errors on {len(errors)} securities (first 10):")
        for sec_id, err in errors[:10]:
            log.warning(f"  {sec_id}: {err}")

    return results, skipped, errors


# ---------------------------------------------------------------------------
# Seed helper
# ---------------------------------------------------------------------------

def seed_beta_definition(
    beta_key       = 'SP500_1Y',
    benchmark_id   = 'T10000108',
    benchmark_name = 'S&P 500',
    lookback_days  = 252,
    return_type    = 'SIMPLE',
    min_obs        = 100,
    description    = 'S&P 500 beta, 1-year lookback (252 trading days), simple returns',
):
    """
    Insert a row into beta_definition. Safe to call multiple times —
    skips silently if beta_key already exists.
    """
    _setup_logger()
    query = """
        INSERT INTO beta_definition (
            beta_key, benchmark_id, benchmark_name,
            lookback_days, return_type, min_obs, description
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (beta_key) DO NOTHING
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (
                beta_key, benchmark_id, benchmark_name,
                lookback_days, return_type, min_obs, description,
            ))
        conn.commit()
    log.info(f"beta_definition seeded: {beta_key}")


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

TEST_DATA_DIR   = Path(__file__).resolve().parent / 'test_data'
TEST_OUTPUT_DIR = Path(__file__).resolve().parent / 'test_output'


def test(test_file='test_securities.csv', beta_key=DEFAULT_BETA_KEY, as_of_date=None):
    """
    Run beta calculation for a small set of securities and write
    intermediate data to CSV — no DB writes.

    Inputs:
        models/test_data/<test_file>   — one column: SecurityID

    Outputs (all in models/test_output/):
        raw_prices.csv        — prices from mkt_timeseries.get() before any processing
        regression_input.csv  — aligned simple returns used in OLS (after ffill + trim)
        regression_results.csv — one row per security: beta, r², vol, obs_count, dates
    """
    _setup_logger()
    TEST_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    as_of_date = as_of_date if as_of_date is not None else datetime.datetime.today()

    # -- load securities from CSV ------------------------------------------
    csv_path = TEST_DATA_DIR / test_file
    if not csv_path.exists():
        raise FileNotFoundError(f"Test data file not found: {csv_path}")

    sec_df  = pd.read_csv(csv_path)
    sec_ids = sec_df['SecurityID'].dropna().tolist()
    log.info(f"sec_beta test | {len(sec_ids)} securities from {csv_path.name}")

    # -- load beta definition from DB -------------------------------------
    defn          = get_beta_definition(beta_key)
    benchmark_id  = defn['benchmark_id']
    lookback_days = int(defn['lookback_days'])
    min_obs       = int(defn.get('min_obs') or 100)
    log.info(f"beta_key={beta_key}  benchmark_id={benchmark_id}  lookback_days={lookback_days}  as_of_date={as_of_date.strftime('%Y-%m-%d')}")

    # -- fetch raw prices -------------------------------------------------
    raw_prices = fetch_raw_prices(sec_ids, benchmark_id, lookback_days, as_of_date)
    out = TEST_OUTPUT_DIR / 'raw_prices.csv'
    raw_prices.to_csv(out)
    log.info(f"Saved {out.name}  ({raw_prices.shape[0]} dates x {raw_prices.shape[1]} securities)")

    # -- process prices (ffill + trim) ------------------------------------
    prices = process_prices(raw_prices, benchmark_id, lookback_days)
    out = TEST_OUTPUT_DIR / 'processed_prices.csv'
    prices.to_csv(out)
    log.info(f"Saved {out.name}  ({prices.shape[0]} dates x {prices.shape[1]} securities)")

    # -- calculate returns ------------------------------------------------
    returns = calc_returns(prices)
    out = TEST_OUTPUT_DIR / 'regression_input.csv'
    returns.to_csv(out)
    log.info(f"Saved {out.name}  ({returns.shape[0]} dates x {returns.shape[1]} securities)")

    # -- run regression for each security ---------------------------------
    if benchmark_id not in returns.columns:
        raise ValueError(f"No return data for benchmark {benchmark_id}")

    bmk_returns = returns[benchmark_id].dropna()
    rows = []

    for sec_id in sec_ids:
        if sec_id not in returns.columns:
            log.warning(f"{sec_id}: no price data — skipped")
            rows.append({'SecurityID': sec_id, 'note': 'no price data'})
            continue

        result = calc_security_beta(returns[sec_id].dropna(), bmk_returns, min_obs)
        if result is None:
            log.warning(f"{sec_id}: obs < {min_obs} — skipped")
            rows.append({'SecurityID': sec_id, 'note': f'obs < {min_obs}'})
            continue

        beta, r_squared, vol, obs_count, start_date, end_date = result
        rows.append({
            'SecurityID': sec_id,
            'beta':        round(beta,      6),
            'r_squared':   round(r_squared, 6),
            'vol':         round(vol,       6),
            'obs_count':   obs_count,
            'start_date':  start_date,
            'end_date':    end_date,
            'note':        '',
        })

    results_df = pd.DataFrame(rows)
    out = TEST_OUTPUT_DIR / 'regression_results.csv'
    results_df.to_csv(out, index=False)
    log.info(f"Saved {out.name}")
    log.info(f"\n{results_df.to_string(index=False)}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Calculate security betas')
    parser.add_argument(
        '--beta_key',
        default=DEFAULT_BETA_KEY,
        help=f'Key from beta_definition table (default: {DEFAULT_BETA_KEY})',
    )
    parser.add_argument(
        '--as_of_date',
        default=None,
        metavar='YYYY-MM-DD',
        help='End date for the lookback window (default: today)',
    )
    parser.add_argument(
        '--sec_file',
        default=None,
        metavar='FILENAME',
        help='CSV filename inside models/test_data/ to limit securities (default: all in security_info)',
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Run in test mode: reads securities from test_data/test_securities.csv, writes CSVs to test_output/, no DB writes',
    )
    parser.add_argument(
        '--test_file',
        default='test_securities.csv',
        metavar='FILENAME',
        help='CSV filename inside models/test_data/ (default: test_securities.csv)',
    )
    parser.add_argument(
        '--seed',
        action='store_true',
        help='Insert the default SP500_1Y row into beta_definition (safe to re-run)',
    )
    args = parser.parse_args()

    as_of_date = (
        datetime.datetime.strptime(args.as_of_date, '%Y-%m-%d')
        if args.as_of_date else None
    )

    if args.seed:
        seed_beta_definition()
    elif args.test:
        test(test_file=args.test_file, beta_key=args.beta_key, as_of_date=as_of_date)
    else:
        run(args.beta_key, as_of_date=as_of_date, sec_file=args.sec_file)
