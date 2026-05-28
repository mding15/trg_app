"""
calc_unadj_alt_pnl.py — Daily P&L for all alternative securities assuming $1 market value.

Steps:
    1. Query current_security for all Alternative securities.
    2. Apply logic: RF_ID = SecurityID, Sensitivity = 1.
    3. Read distributions from VaR HDF (ALT category).
    4. P&L = Sensitivity × $1 × distribution = distribution (same index as HDF).
    5. Save one Series per security under 'ALT/{SecurityID}' in security_pnl.h5.
    6. Compute P&L distribution statistics via dist_stat() and save to log/ as a timestamped CSV.

Usage:
    python calc_unadj_alt_pnl.py
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from trg_config import config
from database2 import pg_connection
from utils import hdf_utils, var_utils, stat_utils


def _get_alternative_securities() -> pd.DataFrame:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT "SecurityID", "AssetClass", "AssetType" FROM current_security '
                'WHERE "AssetClass" = %s',
                ('Alternative',),
            )
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=['SecurityID', 'AssetClass', 'AssetType'])


def calc_unadj_alt_pnl() -> pd.DataFrame:
    """Return P&L DataFrame (rows = scenarios, columns = SecurityIDs) and save to HDF."""
    # Step 1: Alternative securities from current_security
    securities = _get_alternative_securities()
    print(f'Alternative securities found: {len(securities)}')

    # Step 2: RF_ID = SecurityID, Sensitivity = 1
    rf_ids = securities['SecurityID'].tolist()

    # Step 3: Distributions from VaR HDF (ALT category)
    dist = var_utils.get_dist(rf_ids, 'ALT')
    print(f'Distributions loaded: {dist.shape[1]} securities, {dist.shape[0]} scenarios')

    if dist.empty:
        print('No distributions found — output not written.')
        return dist

    # Step 4: P&L = Sensitivity(1) × MarketValue($1) × distribution = distribution
    pnl = dist.copy()

    # Step 5: Save one Series per security under 'ALT/{SecurityID}' in security_pnl.h5
    output_file = config['VaR_DIR'] / 'security_pnl.h5'
    hdf_utils.save(pnl, 'ALT', output_file)
    print(f'Saved: {output_file}')

    # Step 6: Compute and save P&L distribution statistics
    stats = stat_utils.dist_stat(pnl)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    stat_file = config['LOG_DIR'] / f'unadj_alt_pnl_stat_{ts}.csv'
    stats.to_csv(stat_file)
    print(f'Stats saved: {stat_file}')

    return pnl


def test():
    pnl = calc_unadj_alt_pnl()
    if not pnl.empty:
        print(pnl.iloc[:5, :5])


if __name__ == '__main__':
    # test()
    calc_unadj_alt_pnl()
