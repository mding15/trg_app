"""
calc_linear_product_pnl.py — Daily P&L for all linear product securities assuming $1 market value.

Linear products:
    - Equity, Alternative, Commodity, REIT, Cash (all asset types)
    - Bond Fund / Bond ETF

Steps:
    1. Query security_info for all linear product securities.
    2. Apply logic: RF_ID = SecurityID, Sensitivity = 1.
    3. Read price distributions from VaR HDF (DELTA → PRICE category).
    4. P&L = Sensitivity × $1 × distribution = distribution (same index as HDF).
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


def calc_linear_product_pnl(as_of_date=None) -> pd.DataFrame:
    """Return P&L DataFrame (rows = scenarios, columns = SecurityIDs) and save to HDF."""
    if as_of_date is None:
        as_of_date = get_proc_asof_date()

    # Step 1: Linear product securities from current_security
    securities = _get_linear_securities()
    print(f'Linear product securities found: {len(securities)}')

    # Step 2: gen_delta_riskfactors logic — RF_ID = SecurityID, Sensitivity = 1
    rf_ids = securities['SecurityID'].tolist()

    # Step 3: Price distributions from VaR HDF (DELTA maps to PRICE internally)
    dist = var_utils.get_dist(rf_ids, 'DELTA')
    print(f'Distributions loaded: {dist.shape[1]} securities, {dist.shape[0]} scenarios')

    if dist.empty:
        print('No distributions found — output not written.')
        return dist

    # Step 4: P&L = Sensitivity(1) × MarketValue($1) × distribution = distribution
    pnl = dist.copy()

    # Save security-level sensitivities (delta=1; skewness/kurtosis from P&L distribution)
    sens = pd.DataFrame({
        'SecurityID': pnl.columns,
        'Delta':      1.0,
        'Skewness':   pnl.skew(),
        'Kurtosis':   pnl.kurt(),
    })
    n = save_security_sensitivity(sens, as_of_date)
    print(f'Sensitivities written to DB: {n} rows')

    # Step 5: Save one Series per security under 'PNL/{SecurityID}', same as VaR.h5 layout
    output_file = config['VaR_DIR'] / 'security_pnl.h5'
    hdf_utils.save(pnl, 'PNL', output_file)
    print(f'Saved: {output_file}')

    # Step 6: Compute and save P&L distribution statistics
    stats = stat_utils.dist_stat(pnl)
    n = save_pnl_stat(stats, as_of_date, 'LINEAR')
    print(f'Stats written to DB: {n} rows (pnl_type=LINEAR)')

    return pnl


def test():
    pnl = calc_linear_product_pnl()
    if not pnl.empty:
        print(pnl.iloc[:5, :5])


if __name__ == '__main__':
    # test()
    calc_linear_product_pnl()
