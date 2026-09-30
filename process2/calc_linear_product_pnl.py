"""
calc_linear_product_pnl.py — Daily P&L for all linear product securities assuming $1 market value.

Linear products:
    - Equity, Alternative, Commodity, REIT, Cash (all asset types)
    - Bond Fund / Bond ETF

Steps:
    1. Query security_info for all linear product securities.
    2. Apply logic: RF_ID = SecurityID, Sensitivity = 1.
    3. Read price distributions from VaR HDF (DELTA → PRICE category) for
       non-Cash securities.
    4. P&L = Sensitivity × $1 × distribution = distribution (same index as HDF).
       Cash securities (AssetClass 'Cash') get an all-zero P&L distribution
       instead — no price risk; FX risk is handled separately. The zero series
       uses the VaR file's scenario count (metadata 'length').
    5. Save one Series per security under 'PNL/{SecurityID}' in security_pnl.h5.
    6. Compute P&L distribution statistics via dist_stat() and save to log/ as a timestamped CSV.

Usage:
    python calc_linear_product_pnl.py
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from trg_config import config
from database2 import pg_connection, get_proc_asof_date
from utils import hdf_utils, var_utils, stat_utils
from process2.db_pnl_stat import save_pnl_stat, save_security_sensitivity

_LINEAR_ASSET_CLASSES = ['Equity', 'Alternative', 'Commodity', 'REIT', 'Cash']
_BOND_LINEAR_TYPES    = ['Fund', 'ETF']


def _get_linear_securities() -> pd.DataFrame:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT cs."SecurityID", si."AssetClass", si."AssetType"
                FROM current_security cs
                JOIN security_info si ON si."SecurityID" = cs."SecurityID"
                WHERE si."AssetClass" = ANY(%s)
                   OR (si."AssetClass" = %s AND si."AssetType" = ANY(%s))
                """,
                (_LINEAR_ASSET_CLASSES, 'Bond', _BOND_LINEAR_TYPES),
            )
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=['SecurityID', 'AssetClass', 'AssetType'])


def is_linear(asset_class, asset_type) -> bool:
    """Same rule as _get_linear_securities(): a linear asset class, or a Bond Fund/ETF."""
    return asset_class in _LINEAR_ASSET_CLASSES or (asset_class == 'Bond' and asset_type in _BOND_LINEAR_TYPES)


PNL_TYPE = 'LINEAR'
PNL_FILE = config['VaR_DIR'] / 'security_pnl.h5'


def build_linear_pnl(securities: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]]:
    """Steps 2-4 and the sensitivity/stat frames for the given linear securities. No writes.

    securities: columns SecurityID, AssetClass.
    Returns (pnl, sens, stats, no_dist):
        pnl     — scenarios × SecurityID ($1 market value)
        sens    — SecurityID, Delta, Skewness, Kurtosis (input to save_security_sensitivity)
        stats   — dist_stat(pnl), index SecurityID (input to save_pnl_stat)
        no_dist — non-Cash SecurityIDs with no price distribution (left out of pnl)
    """
    # Step 2: gen_delta_riskfactors logic — RF_ID = SecurityID, Sensitivity = 1
    rf_ids = securities['SecurityID'].tolist()

    # Step 3: Price distributions from VaR HDF (DELTA maps to PRICE internally), non-Cash only
    is_cash  = securities['AssetClass'] == 'Cash'
    cash_ids = securities.loc[is_cash, 'SecurityID'].tolist()
    rf_ids   = [sid for sid in rf_ids if sid not in set(cash_ids)]
    dist = var_utils.get_dist(rf_ids, 'DELTA')
    print(f'Distributions loaded: {dist.shape[1]} securities, {dist.shape[0]} scenarios')
    no_dist = [sid for sid in rf_ids if sid not in dist.columns]

    # Step 4: P&L = Sensitivity(1) × MarketValue($1) × distribution = distribution;
    #         Cash: all-zero P&L on the VaR file's scenario index
    n_scenarios = int(var_utils.get_metadata()['length'].iloc[0])
    cash_pnl = pd.DataFrame(0.0, index=pd.RangeIndex(n_scenarios), columns=cash_ids)
    pnl = pd.concat([dist, cash_pnl], axis=1)
    print(f'Cash securities with zero P&L: {len(cash_ids)}')

    # Security-level sensitivities (delta=1; skewness/kurtosis from P&L distribution)
    sens = pd.DataFrame({
        'SecurityID': pnl.columns,
        'Delta':      1.0,
        'Skewness':   pnl.skew(),
        'Kurtosis':   pnl.kurt(),
    })
    stats = stat_utils.dist_stat(pnl)
    return pnl, sens, stats, no_dist


def calc_linear_product_pnl(as_of_date=None) -> pd.DataFrame:
    """Return P&L DataFrame (rows = scenarios, columns = SecurityIDs) and save to HDF."""
    if as_of_date is None:
        as_of_date = get_proc_asof_date()

    # Step 1: Linear product securities from current_security
    securities = _get_linear_securities()
    print(f'Linear product securities found: {len(securities)}')

    # Steps 2-4
    pnl, sens, stats, _ = build_linear_pnl(securities)

    if pnl.empty:
        print('No distributions found — output not written.')
        return pnl

    # Save security-level sensitivities
    n = save_security_sensitivity(sens, as_of_date)
    print(f'Sensitivities written to DB: {n} rows')

    # Step 5: Save one Series per security under 'PNL/{SecurityID}', same as VaR.h5 layout
    hdf_utils.save(pnl, 'PNL', PNL_FILE)
    print(f'Saved: {PNL_FILE}')

    # Step 6: Save P&L distribution statistics
    n = save_pnl_stat(stats, as_of_date, PNL_TYPE)
    print(f'Stats written to DB: {n} rows (pnl_type={PNL_TYPE})')

    return pnl


def test():
    pnl = calc_linear_product_pnl()
    if not pnl.empty:
        print(pnl.iloc[:5, :5])


if __name__ == '__main__':
    # test()
    calc_linear_product_pnl()
