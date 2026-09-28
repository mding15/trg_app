"""
calc_options.py — Price stock/VIX options listed in an Excel file.

Reads an options list CSV from data/maintenance/CSV (default: the newest
*_options_*.csv, as written by maintenance/calc_portfolio_var.py) and prices each row via Black-Scholes, reusing the same
approach as check_port_positions.py / process2/calc_options_pnl.py:
engine/eq_option_var.py::calc_price(), tenor from maturity, risk-free rate
from models/ust_curve.py::get_rate(), underlying price from current_price.
Volatility is the row's `iv` column; blank or missing iv defaults by
underlying (_DEFAULT_IV): SPY/QQQ 0.25, VIX 1.2, everything else 0.35. VIX options are priced the same way
with VIX spot as the underlying (matching process2/calc_vix_pnl.py).

Also computes Greeks (delta, gamma, vega, theta) via calc_greeks()/calc_theta().
Prices are per share, with no x100 contract multiplier (codebase convention).

Input columns:
    pos_id, security_id, option_type (Call/Put), option_class, maturity, strike,
    underlying, underlying_sec_id, iv (optional)

Rows that can't be priced (matured, missing inputs, no underlying price, no
rate) keep blank results and a reason in the `status` column.

Read-only: never writes to the database. Results are written to
data/maintenance/CSV/<input_stem>_priced_<timestamp>.csv.

Usage:
    python maintenance/calc_options.py                          # newest *_options_*.csv, date from proc_asof_date
    python maintenance/calc_options.py --date 2026-09-24
    python maintenance/calc_options.py --file my_options.csv
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import get_proc_asof_date
from engine import eq_option_var as opt
from models.ust_curve import get_rate
from mkt_data.price_timeseries import get_current_price
from _paths import CSV_DIR

DEFAULT_PATTERN = '*_options_*.csv'

_DEFAULT_IV = {'SPY': 0.25, 'QQQ': 0.25, 'VIX': 1.2}
_DEFAULT_IV_OTHER = 0.35

_REQUIRED_COLS = ['option_type', 'maturity', 'strike', 'underlying_sec_id', 'iv']
_RESULT_COLS = ['underlying_price', 'underlying_price_date', 'tenor', 'rate',
                'price', 'delta', 'gamma', 'vega', 'theta', 'status']


# ── Helpers ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger('calc_options')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s', '%H:%M:%S')
    )
    logger.addHandler(handler)
    return logger


def _latest_options_csv() -> Path:
    # Skip this script's own output (<input_stem>_priced_<timestamp>.csv also matches the pattern)
    files = sorted((f for f in CSV_DIR.glob(DEFAULT_PATTERN) if '_priced_' not in f.name),
                   key=lambda f: f.stat().st_mtime)
    if not files:
        raise FileNotFoundError(f'No {DEFAULT_PATTERN} in {CSV_DIR} — run maintenance/calc_portfolio_var.py first')
    return files[-1]


def _fill_default_iv(options: pd.DataFrame) -> pd.DataFrame:
    """Fill blank/missing iv from _DEFAULT_IV by underlying."""
    default = options['underlying'].map(lambda u: _DEFAULT_IV.get(str(u).upper(), _DEFAULT_IV_OTHER))
    if 'iv' in options.columns:
        options['iv'] = pd.to_numeric(options['iv'], errors='coerce').fillna(default)
    else:
        options['iv'] = default
    return options


def _price_row(row: pd.Series, as_of_date: date, und_prices: dict) -> dict:
    """Return result columns for one option row."""
    result = dict.fromkeys(_RESULT_COLS)

    if any(pd.isna(row[c]) for c in _REQUIRED_COLS):
        result['status'] = 'missing inputs: ' + ', '.join(c for c in _REQUIRED_COLS if pd.isna(row[c]))
        return result
    if row['option_type'] not in ('Call', 'Put'):
        result['status'] = f"unsupported option_type {row['option_type']!r}"
        return result

    T = (pd.Timestamp(row['maturity']) - pd.Timestamp(as_of_date)).days / 365
    result['tenor'] = T
    if T <= 0:
        result['status'] = f'matured as of {as_of_date}'
        return result

    und = und_prices.get(row['underlying_sec_id'])
    if und is None:
        result['status'] = f"no current_price for {row['underlying_sec_id']} on/before {as_of_date}"
        return result
    S, S_date = float(und[0]), und[1]
    result['underlying_price'] = S
    result['underlying_price_date'] = S_date

    try:
        r = get_rate(T, as_of_date)
    except ValueError as e:
        result['status'] = str(e)
        return result
    result['rate'] = r

    K, sigma = float(row['strike']), float(row['iv'])
    result['price'] = float(opt.calc_price(row['option_type'], S, K, T, r, sigma))
    result['delta'], result['gamma'], result['vega'] = opt.calc_greeks(row['option_type'], S, K, T, r, sigma)
    result['theta'] = opt.calc_theta(row['option_type'], S, K, T, r, sigma)

    if pd.isna(result['price']):
        result['status'] = 'price is NaN'
    else:
        result['status'] = 'ok'
    return result


# ── Core logic ────────────────────────────────────────────────────────────────

def calc_options(options: pd.DataFrame, as_of_date: date) -> pd.DataFrame:
    und_ids = options['underlying_sec_id'].dropna().unique().tolist()
    und_prices = get_current_price(und_ids, as_of_date)

    results = pd.DataFrame(
        [_price_row(r, as_of_date, und_prices) for _, r in options.iterrows()],
        index=options.index,
    )
    return pd.concat([options, results], axis=1)


def run(file_name: str | None, as_of_date: date | None) -> None:
    log = _setup_logger()

    if file_name is None:
        in_path = _latest_options_csv()
    else:
        in_path = Path(file_name)
        if not in_path.is_absolute():
            in_path = CSV_DIR / in_path
    options = pd.read_csv(in_path, dtype={'pos_id': str, 'security_id': str, 'underlying_sec_id': str})
    log.info(f'Read {len(options)} option(s) from {in_path}')
    if options.empty:
        return
    options = _fill_default_iv(options)

    if as_of_date is None:
        as_of_date = date.fromisoformat(get_proc_asof_date())
    log.info(f'as_of_date={as_of_date}')

    priced = calc_options(options, as_of_date)

    log.info('─' * 60)
    show_cols = ['pos_id', 'security_id', 'option_type', 'underlying', 'strike', 'maturity',
                 'iv', 'underlying_price', 'tenor', 'rate', 'price', 'delta', 'status']
    show_cols = [c for c in show_cols if c in priced.columns]
    with pd.option_context('display.width', 250, 'display.max_columns', None,
                           'display.float_format', '{:.4f}'.format):
        log.info(f'Results:\n{priced[show_cols].to_string(index=False)}')

    failed = priced[priced['status'] != 'ok']
    if not failed.empty:
        log.warning(f'{len(failed)} option(s) not priced — see status column.')

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CSV_DIR / f'{in_path.stem}_priced_{timestamp}.csv'
    priced.to_csv(out_path, index=False)
    log.info('─' * 60)
    log.info(f'Results written to {out_path}')


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Price options listed in a CSV file via Black-Scholes.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='Example:\n  python maintenance/calc_options.py --date 2026-09-24\n',
    )
    parser.add_argument('--file', default=None,
                        help=f'input CSV file, relative to data/maintenance/CSV (default: newest {DEFAULT_PATTERN})')
    parser.add_argument('--date', type=date.fromisoformat, default=None, metavar='YYYY-MM-DD',
                        help='as_of_date (default: proc_asof_date table)')
    args = parser.parse_args()

    run(file_name=args.file, as_of_date=args.date)


if __name__ == '__main__':
    main()
