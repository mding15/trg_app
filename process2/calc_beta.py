"""
calc_beta.py — Calculate single-factor beta for all securities in current_security.

Beta is computed via OLS regression of a security's daily P&L returns (from
security_pnl.h5, category 'PNL') against a benchmark's P&L returns over a
configurable lookback window defined in beta_definition.

Unlike models/sec_beta.py, this script:
  - Reads returns directly from security_pnl.h5 (no price fetch, no pct_change)
  - Sources the security universe from current_security (not security_info)
  - Requires no Flask app context

DB tables
---------
beta_definition   — defines each beta_key (benchmark, lookback, min_obs, etc.)
sec_beta          — stores results; upserted on each run, keyed on (security_id, beta_key)

Usage
-----
    python process2/calc_beta.py
    python process2/calc_beta.py --beta_key SP500_1Y
    python process2/calc_beta.py --date 2026-05-19
    python process2/calc_beta.py --test
    python process2/calc_beta.py --test --test_file my_secs.csv --beta_key SP500_1Y
    python process2/calc_beta.py --seed

Test inputs / outputs
---------------------
    Input  : process2/test_data/<test_file>           (one column: SecurityID)
    Output : process2/test_output/returns.csv         — P&L returns used in regression
             process2/test_output/regression_results.csv — beta, r², vol, obs_count per security
"""
from __future__ import annotations

import argparse
import datetime
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from trg_config import config
from database2 import pg_connection
from utils import hdf_utils

HDF_FILE        = config['VaR_DIR'] / 'security_pnl.h5'
HDF_CATEGORY    = 'PNL'
DEFAULT_BETA_KEY = 'SP500_1Y'
ANNUALIZE        = 252

LOG_DIR          = Path(__file__).resolve().parent.parent.parent / 'log'
TEST_DATA_DIR    = config['TEST_DIR'] / 'src' / 'process2'
TEST_OUTPUT_DIR  = config['TEST_DIR'] / 'src' / 'process2'


# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------

log = logging.getLogger('calc_beta')


def _setup_logger() -> None:
    if log.handlers:
        return
    log.setLevel(logging.DEBUG)
    fmt = logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s', '%Y-%m-%d %H:%M:%S')

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    log.addHandler(ch)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOG_DIR / 'calc_beta.log', mode='a', encoding='utf-8')
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    log.addHandler(fh)


# ---------------------------------------------------------------------------
# DB reads
# ---------------------------------------------------------------------------

def get_beta_definition(beta_key: str) -> dict:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM beta_definition WHERE beta_key = %s', (beta_key,))
            row = cur.fetchone()
            if row is None:
                raise ValueError(
                    f"beta_key '{beta_key}' not found in beta_definition. "
                    "Run with --seed first."
                )
            cols = [desc[0] for desc in cur.description]
    return dict(zip(cols, row))


def get_all_security_ids() -> list[str]:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT "SecurityID" FROM current_security')
            rows = cur.fetchall()
    return [row[0] for row in rows]


# ---------------------------------------------------------------------------
# Returns from HDF
# ---------------------------------------------------------------------------

def fetch_returns(sec_ids: list[str], benchmark_id: str, lookback_days: int) -> pd.DataFrame:
    """
    Read P&L return series from security_pnl.h5 (category 'PNL').

    Returns a DataFrame indexed by scenario date with one column per security.
    Trimmed to the last `lookback_days` rows where the benchmark has data.
    """
    all_ids = list(set(sec_ids) | {benchmark_id})
    returns = hdf_utils.read(all_ids, HDF_CATEGORY, HDF_FILE)

    if returns.empty:
        return returns

    # Trim to last lookback_days benchmark observations
    if benchmark_id in returns.columns:
        bmk_dates = returns[benchmark_id].dropna().index
        if len(bmk_dates) >= lookback_days:
            cutoff  = bmk_dates[-lookback_days]
            returns = returns[returns.index >= cutoff]

    return returns


# ---------------------------------------------------------------------------
# Beta calculation (OLS)
# ---------------------------------------------------------------------------

def calc_security_beta(sec_returns, bmk_returns, min_obs):
    """
    OLS regression: sec_returns = alpha + beta * bmk_returns + epsilon.

    Returns (beta, r_squared, ann_vol, obs_count, start_date, end_date)
    or None if there are fewer than min_obs aligned observations.
    """
    df = pd.concat([sec_returns, bmk_returns], axis=1, sort=False).dropna()
    df = df[np.isfinite(df).all(axis=1)]
    obs_count = len(df)

    if obs_count < min_obs:
        return None

    y = df.iloc[:, 0].values
    x = df.iloc[:, 1].values

    x_mean, y_mean = x.mean(), y.mean()
    x_dm  = x - x_mean
    var_x = np.dot(x_dm, x_dm)
    if var_x == 0:
        return None

    beta  = float(np.dot(x_dm, y - y_mean) / var_x)
    alpha = y_mean - beta * x_mean

    y_hat     = alpha + beta * x
    ss_res    = np.sum((y - y_hat) ** 2)
    ss_tot    = np.sum((y - y_mean) ** 2)
    r_squared = float(1.0 - ss_res / ss_tot) if ss_tot > 0 else 0.0

    ann_vol = float(np.std(y, ddof=1) * np.sqrt(ANNUALIZE))

    start_date = None
    end_date   = None

    return beta, r_squared, ann_vol, obs_count, start_date, end_date


# ---------------------------------------------------------------------------
# DB write
# ---------------------------------------------------------------------------

def upsert_results(results: list[dict]) -> None:
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
    log.info(f'Upserted {len(rows)} rows into sec_beta.')


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def run(beta_key: str = DEFAULT_BETA_KEY, as_of_date=None, sec_file: str | None = None):
    _setup_logger()
    as_of_date = as_of_date or datetime.datetime.today()
    log.info(f'calc_beta run | beta_key={beta_key}  as_of_date={as_of_date.strftime("%Y-%m-%d")}')

    # 1. Beta definition
    defn          = get_beta_definition(beta_key)
    benchmark_id  = defn['benchmark_id']
    lookback_days = int(defn['lookback_days'])
    min_obs       = int(defn.get('min_obs') or 100)
    log.info(f'benchmark_id={benchmark_id}  lookback_days={lookback_days}  min_obs={min_obs}')

    # 2. Securities
    if sec_file:
        csv_path = TEST_DATA_DIR / sec_file
        if not csv_path.exists():
            raise FileNotFoundError(f'Securities file not found: {csv_path}')
        sec_ids = pd.read_csv(csv_path)['SecurityID'].dropna().tolist()
        log.info(f'{len(sec_ids)} securities from {sec_file}')
    else:
        sec_ids = get_all_security_ids()
        log.info(f'{len(sec_ids)} securities from current_security')

    # 3. Fetch returns from HDF
    log.info(f'Reading returns from {HDF_FILE}  category={HDF_CATEGORY}')
    returns = fetch_returns(sec_ids, benchmark_id, lookback_days)
    log.info(f'Returns matrix: {returns.shape[0]} scenarios x {returns.shape[1]} securities')

    if benchmark_id not in returns.columns:
        raise ValueError(f'No return data found for benchmark {benchmark_id}')

    bmk_returns = returns[benchmark_id].dropna()
    calc_date   = datetime.date.today()

    results = []
    skipped = []
    errors  = []

    # 4. Regression loop
    log.info('Calculating betas...')
    for i, sec_id in enumerate(sec_ids):
        if sec_id not in returns.columns:
            skipped.append((sec_id, 'no data in HDF'))
            continue

        try:
            result = calc_security_beta(returns[sec_id].dropna(), bmk_returns, min_obs)
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
            log.warning(f'Error for {sec_id}: {e}')

        if (i + 1) % 500 == 0:
            log.info(f'Processed {i + 1}/{len(sec_ids)}')

    # 5. Upsert
    upsert_results(results)

    # 6. Save skipped log
    if skipped:
        skipped_df = pd.DataFrame(skipped, columns=['security_id', 'reason'])
        skipped_df.insert(0, 'calc_date', calc_date)
        skipped_df.insert(1, 'beta_key',  beta_key)
        skipped_path = LOG_DIR / 'calc_beta_skipped.csv'
        skipped_df.to_csv(skipped_path, mode='a',
                          header=not skipped_path.exists(), index=False)
        log.info(f'Skipped log: {skipped_path}')

    # 7. Summary
    log.info(f'Summary — Calculated: {len(results)}  Skipped: {len(skipped)}  Errors: {len(errors)}')
    if skipped:
        log.warning(f'Skipped {len(skipped)} securities (first 10):')
        for sec_id, reason in skipped[:10]:
            log.warning(f'  {sec_id}: {reason}')
    if errors:
        log.warning(f'Errors on {len(errors)} securities (first 10):')
        for sec_id, err in errors[:10]:
            log.warning(f'  {sec_id}: {err}')

    return results, skipped, errors


# ---------------------------------------------------------------------------
# Test mode
# ---------------------------------------------------------------------------

def test(test_file: str = 'test_securities.csv', beta_key: str = DEFAULT_BETA_KEY):
    """
    Run beta calculation for a small set of securities.
    Writes intermediate CSVs to process2/test_output/ — no DB writes.

    Outputs:
        test_output/returns.csv            — trimmed P&L returns used in regression
        test_output/regression_results.csv — beta, r², vol, obs_count, dates per security
    """
    _setup_logger()
    TEST_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = TEST_DATA_DIR / test_file
    if not csv_path.exists():
        raise FileNotFoundError(f'Test data file not found: {csv_path}')

    sec_ids = pd.read_csv(csv_path)['SecurityID'].dropna().tolist()
    log.info(f'calc_beta test | {len(sec_ids)} securities from {csv_path.name}')

    defn          = get_beta_definition(beta_key)
    benchmark_id  = defn['benchmark_id']
    lookback_days = int(defn['lookback_days'])
    min_obs       = int(defn.get('min_obs') or 100)
    log.info(f'beta_key={beta_key}  benchmark_id={benchmark_id}  lookback_days={lookback_days}')

    returns = fetch_returns(sec_ids, benchmark_id, lookback_days)
    out = TEST_OUTPUT_DIR / 'returns.csv'
    returns.to_csv(out)
    log.info(f'Saved {out.name}  ({returns.shape[0]} scenarios x {returns.shape[1]} securities)')

    if benchmark_id not in returns.columns:
        raise ValueError(f'No return data for benchmark {benchmark_id}')

    bmk_returns = returns[benchmark_id].dropna()
    rows = []

    for sec_id in sec_ids:
        if sec_id not in returns.columns:
            log.warning(f'{sec_id}: no data in HDF — skipped')
            rows.append({'SecurityID': sec_id, 'note': 'no data in HDF'})
            continue

        result = calc_security_beta(returns[sec_id].dropna(), bmk_returns, min_obs)
        if result is None:
            log.warning(f'{sec_id}: obs < {min_obs} — skipped')
            rows.append({'SecurityID': sec_id, 'note': f'obs < {min_obs}'})
            continue

        beta, r_squared, vol, obs_count, start_date, end_date = result
        rows.append({
            'SecurityID':  sec_id,
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
    log.info(f'Saved {out.name}')
    log.info(f'\n{results_df.to_string(index=False)}')


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
    description    = 'S&P 500 beta, 1-year lookback (252 trading days), P&L returns',
):
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
    log.info(f'beta_definition seeded: {beta_key}')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Calculate security betas from security_pnl.h5')
    parser.add_argument(
        '--beta_key', default=DEFAULT_BETA_KEY,
        help=f'Key from beta_definition table (default: {DEFAULT_BETA_KEY})',
    )
    parser.add_argument(
        '--date', default=None, metavar='YYYY-MM-DD',
        help='As-of date for the lookback window (default: today)',
    )
    parser.add_argument(
        '--sec_file', default=None, metavar='FILENAME',
        help='CSV filename inside process2/test_data/ to limit securities (default: all in current_security)',
    )
    parser.add_argument(
        '--test', action='store_true',
        help='Test mode: reads securities from test_data/<test_file>, writes CSVs to test_output/, no DB writes',
    )
    parser.add_argument(
        '--test_file', default='test_securities.csv', metavar='FILENAME',
        help='CSV filename inside process2/test_data/ for test mode (default: test_securities.csv)',
    )
    parser.add_argument(
        '--seed', action='store_true',
        help='Insert the default SP500_1Y row into beta_definition (safe to re-run)',
    )
    args = parser.parse_args()

    as_of_date = (
        datetime.datetime.strptime(args.date, '%Y-%m-%d') if args.date else None
    )

    if args.seed:
        seed_beta_definition()
    elif args.test:
        test(test_file=args.test_file, beta_key=args.beta_key)
    else:
        run(beta_key=args.beta_key, as_of_date=as_of_date, sec_file=args.sec_file)
