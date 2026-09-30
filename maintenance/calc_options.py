"""
calc_options.py — Price stock/VIX options listed in an Excel file.

Reads an options list CSV from data/maintenance/CSV (default: the newest
*_options_*.csv, as written by maintenance/calc_portfolio_var.py) and prices each row via Black-Scholes, reusing the same
approach as check_port_positions.py / process2/calc_options_pnl.py:
engine/eq_option_var.py::calc_price(), tenor from maturity, risk-free rate
from models/ust_curve.py::get_rate(), underlying price from current_price.
Volatility (model_iv; source in iv_source), first available of:
    1. the row's `iv` column, if the input has one and it is filled ('input')
    2. option_iv for the underlying, latest as_of_date on/before as_of_date,
       interpolated in moneyness (K/S) and tenor — same grid lookup as
       process2/calc_option_price.py ('option_iv <date>')
    3. _DEFAULT_IV ('default')
VIX options are priced the same way with VIX spot as the underlying (matching
process2/calc_vix_pnl.py).

Also computes Greeks (delta, gamma, vega, theta) via calc_greeks()/calc_theta().
Prices are per share, with no x100 contract multiplier (codebase convention).

Input columns:
    pos_id, security_id, option_type (Call/Put), option_class, maturity, strike,
    underlying, underlying_sec_id, iv (optional)

Rows that can't be priced (matured, missing inputs, no underlying price, no
rate) keep blank results and a reason in the `status` column.

P&L distributions: for each priced option security with no PNL/{SecurityID} in
security_pnl.h5 (all of them with --overwrite), builds a scenario P&L
distribution the same way as process2/calc_options_pnl.py (Black-Scholes
re-pricing across underlying price and VIX vol scenarios, percentage returns).
IV is implied from the positions' price, sum(|market_value|) / sum(|quantity|) / 100
(quantity in contracts); if that is unavailable or has no IV solution, the row's
model_iv and model price are used instead (pnl_iv_source 'implied', or the
row's iv_source).
VIX options are skipped (their P&L comes from process2/calc_vix_pnl.py).

Never writes to the database. Results are written to
data/maintenance/CSV/<input_stem>_priced_<timestamp>.csv (per position, with
market_price, pnl_iv, pnl_iv_source, pnl_status) and, if any were generated,
<input_stem>_pnl_<timestamp>.csv (scenarios × security_id). Distributions are
written to the local security_pnl.h5 only with --save.

Usage:
    python maintenance/calc_options.py                          # newest *_options_*.csv, date from proc_asof_date, dry run
    python maintenance/calc_options.py --date 2026-09-24
    python maintenance/calc_options.py --file my_options.csv
    python maintenance/calc_options.py --save                   # write missing P&L to security_pnl.h5
    python maintenance/calc_options.py --save --overwrite       # regenerate existing ones too
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection, get_proc_asof_date
from engine import eq_option_var as opt
from models.ust_curve import get_rate
from mkt_data.price_timeseries import get_current_price
from process2.calc_option_price import _get_iv_grids, interp_iv
from process2.calc_options_pnl import PNL_FILE, load_option_scenarios, _reprice_option
from utils import hdf_utils
from _paths import CSV_DIR

DEFAULT_PATTERN = '*_options_*.csv'

_DEFAULT_IV = 0.35   # when the row has no iv and the underlying has no option_iv

_REQUIRED_COLS = ['option_type', 'maturity', 'strike', 'underlying_sec_id']
_RESULT_COLS = ['underlying_price', 'underlying_price_date', 'tenor', 'rate', 'model_iv', 'iv_source',
                'price', 'delta', 'gamma', 'vega', 'theta', 'status']
_PNL_COLS = ['market_price', 'pnl_iv', 'pnl_iv_source', 'pnl_status']
_CONTRACT_MULTIPLIER = 100   # input quantity is in contracts; prices are per share


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
    # Skip this script's own output (<input_stem>_priced_/_pnl_<timestamp>.csv also match the pattern)
    files = sorted((f for f in CSV_DIR.glob(DEFAULT_PATTERN)
                    if '_priced_' not in f.name and '_pnl_' not in f.name),
                   key=lambda f: f.stat().st_mtime)
    if not files:
        raise FileNotFoundError(f'No {DEFAULT_PATTERN} in {CSV_DIR} — run maintenance/calc_portfolio_var.py first')
    return files[-1]


def _resolve_iv(row: pd.Series, T: float, moneyness: float, iv_grids: dict) -> tuple[float, str]:
    """(iv, iv_source): the row's iv if given, else option_iv for the underlying, else _DEFAULT_IV."""
    iv = pd.to_numeric(row.get('iv'), errors='coerce')
    if pd.notna(iv):
        return float(iv), 'input'
    grid = iv_grids.get(row['underlying_sec_id'])
    if grid is not None:
        iv_date, g = grid
        return interp_iv(g, T, moneyness), f'option_iv {iv_date}'
    return _DEFAULT_IV, 'default'


def _price_row(row: pd.Series, as_of_date: date, und_prices: dict, iv_grids: dict) -> dict:
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

    K = float(row['strike'])
    sigma, result['iv_source'] = _resolve_iv(row, T, K / S, iv_grids)
    result['model_iv'] = sigma
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
    with pg_connection() as conn:
        with conn.cursor() as cur:
            iv_grids = _get_iv_grids(cur, und_ids, as_of_date)

    results = pd.DataFrame(
        [_price_row(r, as_of_date, und_prices, iv_grids) for _, r in options.iterrows()],
        index=options.index,
    )
    return pd.concat([options, results], axis=1)


def _existing_pnl_ids() -> set[str]:
    """SecurityIDs that already have a PNL/{SecurityID} distribution in security_pnl.h5."""
    if not PNL_FILE.exists():
        return set()
    with pd.HDFStore(PNL_FILE, mode='r') as store:
        return {k.split('/')[-1] for k in store.keys() if k.startswith('/PNL/')}


def _market_price(group: pd.DataFrame) -> float:
    """Per-share price implied by the positions: sum(|market_value|) / sum(|quantity|) / 100.
    Absolute values so short positions (negative market_value and/or quantity) give a
    positive price, and longs and shorts of the same security don't net out."""
    if not {'quantity', 'market_value'} <= set(group.columns):
        return np.nan
    qty = pd.to_numeric(group['quantity'], errors='coerce').abs().sum(min_count=1)
    mv  = pd.to_numeric(group['market_value'], errors='coerce').abs().sum(min_count=1)
    if pd.isna(qty) or pd.isna(mv) or qty == 0:
        return np.nan
    price = mv / qty / _CONTRACT_MULTIPLIER
    return price if price > 0 else np.nan


def calc_options_pnl_dist(priced: pd.DataFrame, overwrite: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Scenario P&L distributions for priced options that have none in security_pnl.h5
    (or all of them if overwrite), re-priced the same way as process2/calc_options_pnl.py.

    IV is implied from the positions' market price; if that price is missing or has no
    IV solution, falls back to the row's iv (given or default) and its model price.
    VIX options are skipped (their P&L comes from process2/calc_vix_pnl.py).

    Returns (pnl, status):
        pnl    — scenarios × security_id, percentage returns (as stored in the HDF)
        status — one row per security_id with _PNL_COLS
    """
    existing = _existing_pnl_ids()
    todo, status = [], []

    for sid, group in priced.dropna(subset=['security_id']).groupby('security_id', sort=False):
        row = group.iloc[0]
        st = {'security_id': sid, 'market_price': _market_price(group),
              'pnl_iv': np.nan, 'pnl_iv_source': None, 'pnl_status': None}
        status.append(st)

        if str(row.get('option_class')).upper() == 'VIX':
            st['pnl_status'] = 'skipped (VIX)'
        elif row['status'] != 'ok':
            st['pnl_status'] = f"not priced: {row['status']}"
        elif sid in existing and not overwrite:
            st['pnl_status'] = 'exists (skipped)'
        else:
            todo.append((st, row))

    if not todo:
        return pd.DataFrame(), pd.DataFrame(status, columns=['security_id'] + _PNL_COLS)

    und_dists, vix_dist = load_option_scenarios(list({r['underlying_sec_id'] for _, r in todo}))

    pnl = {}
    for st, row in todo:
        S, K, T, r = row['underlying_price'], float(row['strike']), row['tenor'], row['rate']
        iv = opt.calc_iv(row['option_type'], st['market_price'], S, K, T, r) \
            if pd.notna(st['market_price']) else np.nan
        if pd.notna(iv) and iv > 0:
            price0, st['pnl_iv'], st['pnl_iv_source'] = st['market_price'], iv, 'implied'
        else:
            price0, st['pnl_iv'], st['pnl_iv_source'] = row['price'], row['model_iv'], row['iv_source']

        if row['underlying_sec_id'] not in und_dists.columns:
            st['pnl_status'] = f"no price distribution for underlying {row['underlying_sec_id']}"
            continue
        und_dist = und_dists[row['underlying_sec_id']]
        vol_dist = vix_dist if vix_dist is not None else pd.Series(np.zeros(len(und_dist)), index=und_dist.index)

        reprice_row = pd.Series({'underlying_price': S, 'strike': K, 'tenor': T, 'risk_free_rate': r,
                                 'iv': st['pnl_iv'], 'option_type': row['option_type'], 'price': price0})
        series = _reprice_option(reprice_row, und_dist, vol_dist)
        if series is None:
            st['pnl_status'] = 'cannot reprice'
            continue
        pnl[st['security_id']] = series
        st['pnl_status'] = 'generated'

    return pd.DataFrame(pnl), pd.DataFrame(status, columns=['security_id'] + _PNL_COLS)


def run(file_name: str | None, as_of_date: date | None,
        save: bool = False, overwrite: bool = False) -> None:
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

    if as_of_date is None:
        as_of_date = date.fromisoformat(get_proc_asof_date())
    log.info(f'as_of_date={as_of_date}')

    priced = calc_options(options, as_of_date)

    log.info('─' * 60)
    show_cols = ['pos_id', 'security_id', 'option_type', 'underlying', 'strike', 'maturity',
                 'model_iv', 'iv_source', 'underlying_price', 'tenor', 'rate', 'price', 'delta', 'status']
    show_cols = [c for c in show_cols if c in priced.columns]
    with pd.option_context('display.width', 250, 'display.max_columns', None,
                           'display.float_format', '{:.4f}'.format):
        log.info(f'Results:\n{priced[show_cols].to_string(index=False)}')

    failed = priced[priced['status'] != 'ok']
    if not failed.empty:
        log.warning(f'{len(failed)} option(s) not priced — see status column.')

    # ── P&L distributions for options missing from security_pnl.h5 ──────────
    pnl, pnl_status = calc_options_pnl_dist(priced, overwrite)
    priced = priced.merge(pnl_status, on='security_id', how='left')
    priced['pnl_status'] = priced['pnl_status'].fillna('missing security_id')

    log.info('─' * 60)
    mv = pd.to_numeric(priced.get('market_value'), errors='coerce').groupby(priced['security_id']).sum() \
        if 'market_value' in priced.columns else pd.Series(dtype=float)
    summary = pnl_status.set_index('security_id')
    for sid, st in summary.iterrows():
        line = f"  {sid:<12} {st['pnl_status']}"
        if sid in pnl.columns:
            p = pnl[sid]
            line += (f"  iv={st['pnl_iv']:.4f} ({st['pnl_iv_source']})  "
                     f"min={p.min():+.2%}  p1={p.quantile(0.01):+.2%}  p5={p.quantile(0.05):+.2%}  "
                     f"max={p.max():+.2%}  VaR95={-p.quantile(0.05) * mv.get(sid, np.nan):,.0f}")
        log.info(line)
    log.info(f'P&L: {pnl.shape[1]} generated, {len(pnl_status) - pnl.shape[1]} not generated '
             f'(of {len(pnl_status)} securities)')

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CSV_DIR / f'{in_path.stem}_priced_{timestamp}.csv'
    priced.to_csv(out_path, index=False)
    log.info('─' * 60)
    log.info(f'Results written to {out_path}')

    if pnl.empty:
        return
    pnl_path = CSV_DIR / f'{in_path.stem}_pnl_{timestamp}.csv'
    pnl.to_csv(pnl_path, index_label='scenario')
    log.info(f'P&L distributions written to {pnl_path}')
    if save:
        hdf_utils.save(pnl, 'PNL', PNL_FILE)
        log.info(f'Saved {pnl.shape[1]} P&L distribution(s) to {PNL_FILE}: {", ".join(pnl.columns)}')
    else:
        log.info(f'Dry run — not saved to {PNL_FILE.name}; rerun with --save to write them.')


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Price options listed in a CSV file via Black-Scholes.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='Examples:\n  python maintenance/calc_options.py --date 2026-09-24\n'
               '  python maintenance/calc_options.py --save\n',
    )
    parser.add_argument('--file', default=None,
                        help=f'input CSV file, relative to data/maintenance/CSV (default: newest {DEFAULT_PATTERN})')
    parser.add_argument('--date', type=date.fromisoformat, default=None, metavar='YYYY-MM-DD',
                        help='as_of_date (default: proc_asof_date table)')
    parser.add_argument('--save', action='store_true',
                        help='write generated P&L distributions to security_pnl.h5 (default: dry run)')
    parser.add_argument('--overwrite', action='store_true',
                        help='also regenerate options that already have a P&L distribution')
    args = parser.parse_args()

    run(file_name=args.file, as_of_date=args.date, save=args.save, overwrite=args.overwrite)


if __name__ == '__main__':
    main()
