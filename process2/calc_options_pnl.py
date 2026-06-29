"""
calc_options_pnl.py — Daily P&L for options securities.

P&L is computed by re-pricing each option across historical return scenarios
for the underlying equity and implied volatility (VIX):
    1. Fetch option positions and contract details from proc_positions + option_info.
    2. Fetch current underlying prices from mkt_timeseries.
    3. Compute implied volatility and Greeks (Delta, Gamma, Vega, Theta) per option.
    4. Re-price each option across underlying return and vol scenarios from the VaR HDF.
    5. Save one Series per security under 'PNL/{SecurityID}' in security_pnl.h5.
    6. Save sensitivities (IV, Delta, Gamma, Vega) to security_sensitivity.
       Note: Theta is computed but not yet persisted — add a 'theta' column to
       security_sensitivity and update _SENS_COL_MAP in db_pnl_stat.py to enable it.
    7. Save P&L distribution stats to security_pnl_stat with pnl_type='OPTION'.

Usage:
    python calc_options_pnl.py                     # date from proc_asof_date
    python calc_options_pnl.py --date 2026-06-24   # specific date
    python calc_options_pnl.py test                # run test()
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import numpy as np
import pandas as pd

from trg_config import config
from database2 import pg_connection, get_proc_asof_date
from utils import hdf_utils, var_utils, stat_utils
from mkt_data.price_timeseries import get_current_price
from engine import eq_option_var as opt
from models.ust_curve import get_rate as _ust_get_rate
from process2.db_pnl_stat import save_pnl_stat, save_security_sensitivity


def _get_security_id_by_ticker(ticker: str) -> str | None:
    """Return SecurityID from security_xref for a given Ticker, or None if not found."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT "SecurityID" FROM security_xref'
                ' WHERE "REF_ID" = %s AND "REF_TYPE" = \'Ticker\'',
                (ticker,),
            )
            row = cur.fetchone()
    return row[0] if row else None


def _get_option_securities(as_of_date) -> pd.DataFrame:
    """Return option positions with contract details and market prices for as_of_date.

    Columns: security_id, price, option_type, strike, maturity, underlying_sec_id
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pp.security_id,
                       SUM(pp.market_value) / NULLIF(SUM(pp.quantity), 0) AS price,
                       oi.option_type,
                       oi.strike,
                       oi.maturity,
                       oi.underlying_sec_id
                FROM proc_positions pp
                JOIN security_info si
                  ON pp.security_id = si."SecurityID"
                 AND si."AssetType" = 'Option'
                JOIN option_info oi
                  ON oi.security_id = pp.security_id
                 AND oi.option_class = 'Equity'
                WHERE pp.as_of_date = %s
                GROUP BY pp.security_id, oi.option_type, oi.strike, oi.maturity, oi.underlying_sec_id
                """,
                (as_of_date,),
            )
            rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=['security_id', 'price', 'option_type', 'strike', 'maturity', 'underlying_sec_id'])
    df['price']  = df['price'].astype(float)
    df['strike'] = df['strike'].astype(float)
    return df



def _reprice_option(row: pd.Series, und_dist: pd.Series, vol_dist: pd.Series) -> pd.Series:
    """Re-price one option across all scenarios. Returns P&L Series indexed by scenario."""
    S       = row['underlying_price']
    K       = float(row['strike'])
    T       = row['tenor']
    r       = row['risk_free_rate']
    sigma   = row['iv']
    op_type = row['option_type']
    price0  = float(row['price'])

    if pd.isna(sigma) or sigma <= 0 or T <= 0 or pd.isna(S):
        return pd.Series(np.zeros(len(und_dist)), index=und_dist.index)

    # underlying return scenarios → new spot price
    S_scen = S * (1.0 + und_dist.values)
    # vol change scenarios (returns) → new implied vol, clipped away from zero
    sigma_scen = np.clip(sigma + vol_dist.values * 0.01, 1e-6, None)

    prices = np.vectorize(opt.calc_price)(op_type, S_scen, K, T, r, sigma_scen)
    return pd.Series((prices - price0) / price0, index=und_dist.index)


def calc_options_pnl(as_of_date: date = None) -> pd.DataFrame:
    """Return P&L DataFrame (scenarios × securities) and save to HDF."""
    if as_of_date is None:
        as_of_date = date.fromisoformat(get_proc_asof_date())

    # Step 1: Option positions from proc_positions + option_info
    securities = _get_option_securities(as_of_date)
    print(f'Option securities found: {len(securities)}')

    if securities.empty:
        print('No option securities found — output not written.')
        return pd.DataFrame()

    null_prices = securities['price'].isna().sum()
    if null_prices:
        print(f'Warning: {null_prices} securities excluded (price could not be computed).')
    securities = securities.dropna(subset=['price'])

    # Step 2: Underlying prices, tenor, and risk-free rate
    und_ids = securities['underlying_sec_id'].dropna().unique().tolist()
    und_prices = {k: v[0] for k, v in get_current_price(und_ids, as_of_date).items()}
    securities['underlying_price'] = securities['underlying_sec_id'].map(und_prices)
    securities['tenor'] = securities['maturity'].apply(
        lambda m: max((pd.Timestamp(m) - pd.Timestamp(as_of_date)).days / 365, 0)
        if pd.notna(m) else np.nan
    )
    securities['risk_free_rate'] = securities['tenor'].apply(
        lambda t: _ust_get_rate(t, as_of_date)
    )

    # Step 3: IV and Greeks per option
    def _iv(x):
        if pd.isna(x.underlying_price) or not x.tenor > 0:
            return np.nan
        return opt.calc_iv(x.option_type, x.price, x.underlying_price, x.strike, x.tenor, x.risk_free_rate)

    def _greeks(x):
        if pd.isna(x.iv):
            return np.nan, np.nan, np.nan, np.nan
        d, g, v = opt.calc_greeks(x.option_type, x.underlying_price, x.strike, x.tenor, x.risk_free_rate, x.iv)
        t = opt.calc_theta(x.option_type, x.underlying_price, x.strike, x.tenor, x.risk_free_rate, x.iv)
        return d, g, v, t

    securities['iv'] = securities.apply(_iv, axis=1)
    greeks = securities.apply(_greeks, axis=1)
    securities[['delta', 'gamma', 'vega', 'theta']] = [[*g] for g in greeks]

    # Step 4: Scenario distributions — per-underlying price returns + VIX vol returns
    und_dists = var_utils.get_dist(und_ids, 'DELTA')

    vix_sec_id = _get_security_id_by_ticker('VIX')
    vix_dist = None
    if vix_sec_id:
        vix_dist_df = var_utils.get_dist([vix_sec_id], 'VEGA')
        vix_dist = vix_dist_df[vix_sec_id] if vix_sec_id in vix_dist_df.columns else None

    # Step 5: Scenario P&L per security via BS re-pricing
    pnl_dict = {}
    for _, row in securities.iterrows():
        und_id = row['underlying_sec_id']
        if und_id not in und_dists.columns:
            print(f'Warning: no price distribution for underlying {und_id} — skipping {row.security_id}')
            continue
        und_dist = und_dists[und_id]
        vol_dist = vix_dist if vix_dist is not None else pd.Series(np.zeros(len(und_dist)), index=und_dist.index)
        pnl_dict[row['security_id']] = _reprice_option(row, und_dist, vol_dist)

    if not pnl_dict:
        print('No P&L computed — output not written.')
        return pd.DataFrame()

    pnl = pd.DataFrame(pnl_dict)
    print(f'P&L distributions computed: {pnl.shape[1]} securities, {pnl.shape[0]} scenarios')

    # Step 6: Save P&L to HDF under PNL/{SecurityID}
    output_file = config['VaR_DIR'] / 'security_pnl.h5'
    hdf_utils.save(pnl, 'PNL', output_file)
    print(f'Saved: {output_file}')

    # Step 7: Sensitivities — skewness/kurtosis from P&L distribution merged in
    pnl_stats = pd.DataFrame({
        'SecurityID': pnl.columns,
        'Skewness':   pnl.skew().values,
        'Kurtosis':   pnl.kurt().values,
    })
    sens = (
        securities[securities['security_id'].isin(pnl.columns)]
        [['security_id', 'tenor', 'iv', 'delta', 'gamma', 'vega']]
        .rename(columns={
            'security_id': 'SecurityID',
            'tenor':       'Tenor',
            'iv':          'IV',
            'delta':       'Delta',
            'gamma':       'Gamma',
            'vega':        'Vega',
        })
    )
    sens = sens.merge(pnl_stats, on='SecurityID', how='left')
    n = save_security_sensitivity(sens, as_of_date)
    print(f'Sensitivities written to DB: {n} rows')

    # Step 8: P&L distribution statistics
    stats = stat_utils.dist_stat(pnl)
    n = save_pnl_stat(stats, as_of_date, 'OPTION')
    print(f'Stats written to DB: {n} rows (pnl_type=OPTION)')

    return pnl


def debug(as_of_date: date = None, max_securities: int = 2) -> None:
    """Run calc_options_pnl() step-by-step, capturing all intermediate results to Excel.

    Output: config['TEST_DIR']/src/process2/debug_options_pnl_{date}.xlsx
    Sheets:
        1_positions      — raw option positions + contract details (Step 1)
        2_prices_tenor   — positions enriched with underlying price and tenor (Step 2)
        3_iv_greeks      — positions enriched with IV, delta, gamma, vega, theta (Step 3)
        4_und_dists      — underlying return distributions used for scenarios (Step 4)
        4_vix_dist       — VIX return distribution (Step 4, if available)
        5_pnl            — P&L matrix: scenarios × securities (Step 5)
        6_sensitivities  — per-security IV, Greeks, skewness, kurtosis (Step 6-7)
        7_pnl_stats      — P&L distribution statistics (Step 8)
    """
    if as_of_date is None:
        as_of_date = date.fromisoformat(get_proc_asof_date())

    # Step 1: Option positions from proc_positions + option_info
    securities = _get_option_securities(as_of_date)
    print(f'Step 1: {len(securities)} option securities found')
    if max_securities and len(securities) > max_securities:
        securities = securities.head(max_securities)
        print(f'Step 1: limited to {max_securities} securities for debug')
    step1 = securities.copy()

    if securities.empty:
        print('No option securities found — nothing to debug.')
        return

    null_prices = securities['price'].isna().sum()
    if null_prices:
        print(f'Step 1 warning: {null_prices} securities excluded (no price).')
    securities = securities.dropna(subset=['price'])

    # Step 2: Underlying prices, tenor, and risk-free rate
    und_ids = securities['underlying_sec_id'].dropna().unique().tolist()
    und_prices = {k: v[0] for k, v in get_current_price(und_ids, as_of_date).items()}
    securities['underlying_price'] = securities['underlying_sec_id'].map(und_prices)
    securities['tenor'] = securities['maturity'].apply(
        lambda m: max((pd.Timestamp(m) - pd.Timestamp(as_of_date)).days / 365, 0)
        if pd.notna(m) else np.nan
    )
    securities['risk_free_rate'] = securities['tenor'].apply(
        lambda t: _ust_get_rate(t, as_of_date)
    )
    print(f'Step 2: {len(und_ids)} underlyings, underlying prices + tenors + risk-free rates computed')
    step2 = securities.copy()

    # Step 3: IV and Greeks per option
    def _iv(x):
        if pd.isna(x.underlying_price) or not x.tenor > 0:
            return np.nan
        return opt.calc_iv(x.option_type, x.price, x.underlying_price, x.strike, x.tenor, x.risk_free_rate)

    def _greeks(x):
        if pd.isna(x.iv):
            return np.nan, np.nan, np.nan, np.nan
        d, g, v = opt.calc_greeks(x.option_type, x.underlying_price, x.strike, x.tenor, x.risk_free_rate, x.iv)
        t = opt.calc_theta(x.option_type, x.underlying_price, x.strike, x.tenor, x.risk_free_rate, x.iv)
        return d, g, v, t

    securities['iv'] = securities.apply(_iv, axis=1)
    greeks = securities.apply(_greeks, axis=1)
    securities[['delta', 'gamma', 'vega', 'theta']] = [[*g] for g in greeks]
    print(f'Step 3: IV and Greeks computed ({securities["iv"].notna().sum()} valid)')
    step3 = securities.copy()

    # Step 4: Scenario distributions
    und_dists = var_utils.get_dist(und_ids, 'DELTA')
    vix_sec_id = _get_security_id_by_ticker('VIX')
    vix_dist = None
    if vix_sec_id:
        vix_dist_df = var_utils.get_dist([vix_sec_id], 'VEGA')
        vix_dist = vix_dist_df[vix_sec_id] if vix_sec_id in vix_dist_df.columns else None
    print(f'Step 4: {und_dists.shape[1]} underlying distributions, {und_dists.shape[0]} scenarios; VIX dist: {vix_dist is not None}')

    # Step 5: Scenario P&L per security via BS re-pricing
    pnl_dict = {}
    for _, row in securities.iterrows():
        und_id = row['underlying_sec_id']
        if und_id not in und_dists.columns:
            print(f'Step 5 warning: no distribution for underlying {und_id} — skipping {row.security_id}')
            continue
        und_dist = und_dists[und_id]
        vol_dist = vix_dist if vix_dist is not None else pd.Series(np.zeros(len(und_dist)), index=und_dist.index)
        pnl_dict[row['security_id']] = _reprice_option(row, und_dist, vol_dist)

    if not pnl_dict:
        print('Step 5: No P&L computed — nothing to write.')
        return

    pnl = pd.DataFrame(pnl_dict)
    print(f'Step 5: P&L computed — {pnl.shape[1]} securities × {pnl.shape[0]} scenarios')

    # Step 6-7: Sensitivities with P&L skewness/kurtosis
    pnl_stats = pd.DataFrame({
        'SecurityID': pnl.columns,
        'Skewness':   pnl.skew().values,
        'Kurtosis':   pnl.kurt().values,
    })
    sens = (
        securities[securities['security_id'].isin(pnl.columns)]
        [['security_id', 'tenor', 'iv', 'delta', 'gamma', 'vega']]
        .rename(columns={
            'security_id': 'SecurityID',
            'tenor':       'Tenor',
            'iv':          'IV',
            'delta':       'Delta',
            'gamma':       'Gamma',
            'vega':        'Vega',
        })
    )
    sens = sens.merge(pnl_stats, on='SecurityID', how='left')
    print(f'Step 6-7: {len(sens)} sensitivity rows built')

    # Step 8: P&L distribution statistics
    stats = stat_utils.dist_stat(pnl)
    print(f'Step 8: {stats.shape} stats computed')

    # Write Excel
    out_dir = config['TEST_DIR'] / 'src' / 'process2'
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f'debug_options_pnl_{as_of_date}.xlsx'

    with pd.ExcelWriter(out_file, engine='openpyxl') as writer:
        step1.to_excel(writer, sheet_name='1_positions', index=False)
        step2.to_excel(writer, sheet_name='2_prices_tenor', index=False)
        step3.to_excel(writer, sheet_name='3_iv_greeks', index=False)
        und_dists.to_excel(writer, sheet_name='4_und_dists')
        if vix_dist is not None:
            vix_dist.to_frame('vix_return').to_excel(writer, sheet_name='4_vix_dist')
        pnl.to_excel(writer, sheet_name='5_pnl')
        sens.to_excel(writer, sheet_name='6_sensitivities', index=False)
        stats.to_excel(writer, sheet_name='7_pnl_stats')

    print(f'Debug Excel saved: {out_file}')


def test():
    as_of_date = date.fromisoformat(get_proc_asof_date())

    # Step 1: positions and contract details
    securities = _get_option_securities(as_of_date)
    print(f'Step 1: {len(securities)} option securities\n{securities}\n')

    if securities.empty:
        return

    # Step 2: underlying prices, tenors, and risk-free rates
    und_ids = securities['underlying_sec_id'].dropna().unique().tolist()
    und_prices = {k: v[0] for k, v in get_current_price(und_ids, as_of_date).items()}
    securities['underlying_price'] = securities['underlying_sec_id'].map(und_prices)
    securities['tenor'] = securities['maturity'].apply(
        lambda m: max((pd.Timestamp(m) - pd.Timestamp(as_of_date)).days / 365, 0)
        if pd.notna(m) else np.nan
    )
    securities['risk_free_rate'] = securities['tenor'].apply(
        lambda t: _ust_get_rate(t, as_of_date)
    )

    # Step 3: IV and Greeks
    securities['iv'] = securities.apply(
        lambda x: opt.calc_iv(x.option_type, x.price, x.underlying_price, x.strike, x.tenor, x.risk_free_rate)
        if pd.notna(x.underlying_price) and x.tenor > 0 else np.nan,
        axis=1,
    )
    greeks = securities.apply(
        lambda x: (
            (*opt.calc_greeks(x.option_type, x.underlying_price, x.strike, x.tenor, x.risk_free_rate, x.iv),
             opt.calc_theta(x.option_type, x.underlying_price, x.strike, x.tenor, x.risk_free_rate, x.iv))
            if pd.notna(x.iv) else (np.nan, np.nan, np.nan, np.nan)
        ),
        axis=1,
    )
    securities[['delta', 'gamma', 'vega', 'theta']] = [[*g] for g in greeks]

    print(f'Step 2: IV and Greeks\n{securities[["security_id", "tenor", "iv", "delta", "gamma", "vega", "theta"]]}\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Calculate options P&L')
    parser.add_argument(
        'mode', nargs='?', default='run',
        choices=['run', 'test', 'debug'],
        help='run (default): calc_options_pnl();  test: test();  debug: debug()',
    )
    parser.add_argument(
        '--date', metavar='YYYY-MM-DD', default=None,
        help='As-of date (default: read from proc_asof_date table)',
    )
    parser.add_argument(
        '--max-securities', type=int, default=2, metavar='N',
        help='Max securities to process in debug mode (default: 2)',
    )
    args = parser.parse_args()
    as_of = date.fromisoformat(args.date) if args.date else None

    if args.mode == 'test':
        test()
    elif args.mode == 'debug':
        debug(as_of, max_securities=args.max_securities)
    else:
        calc_options_pnl(as_of)
