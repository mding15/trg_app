"""
calc_structured_note_pnl.py — Daily P&L for Structured Note securities assuming $1 market value.

P&L is computed via the structured-note replication model:
    1. Fetch live market prices and security IDs from proc_positions for as_of_date.
    2. Call generate_dist() to calibrate implied vol and return the full P&L distribution.
    3. Save one Series per security under 'PNL/{SecurityID}' in security_pnl.h5.
    4. Aggregate per-leg sensitivities (ΣDelta, ΣGamma, IV) from sn_replica_calc.
    5. Save sensitivities to security_sensitivity and P&L stats via save_pnl_stat().

Usage:
    python calc_structured_note_pnl.py                     # date from proc_asof_date
    python calc_structured_note_pnl.py --date 2026-06-24   # specific date
    python calc_structured_note_pnl.py test                # run test()
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from trg_config import config
from database2 import pg_connection, get_proc_asof_date
from models.structured_note import calculate, calculate_debug
from utils import hdf_utils, stat_utils
from process2.db_pnl_stat import save_pnl_stat, save_security_sensitivity


def _get_sn_securities(as_of_date) -> pd.DataFrame:
    """Return structured note security IDs and market prices for as_of_date.

    Columns: security_id, price
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pp.security_id,
                       SUM(market_value) / NULLIF(SUM(quantity), 0) AS price
                FROM proc_positions pp
                JOIN security_info si
                  ON pp.security_id = si."SecurityID"
                 AND si."AssetType" = 'Structured Note'
                WHERE pp.as_of_date = %s
                GROUP BY pp.security_id
                """,
                (as_of_date,),
            )
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=['security_id', 'price'])



def calc_structured_note_pnl(as_of_date: date = None) -> pd.DataFrame:
    """Return P&L DataFrame (scenarios × securities) and save to HDF.

    Args:
        as_of_date: valuation date; defaults to proc_asof_date.
    """
    if as_of_date is None:
        as_of_date = date.fromisoformat(get_proc_asof_date())

    # Step 1: Structured note securities and prices from proc_positions
    securities = _get_sn_securities(as_of_date)
    print(f'Structured note securities found: {len(securities)}')

    if securities.empty:
        print('No structured note securities found — output not written.')
        return pd.DataFrame()

    null_prices = securities['price'].isna().sum()
    if null_prices:
        print(f'Warning: {null_prices} securities excluded (price could not be computed).')
    securities = securities.dropna(subset=['price'])

    security_ids = securities['security_id'].tolist()
    sn_prices    = dict(zip(securities['security_id'], securities['price'].astype(float)))

    # Step 2: P&L distributions and sensitivities via replication model
    pnl, sens = calculate(security_ids, as_of_date, sn_prices)
    print(f'P&L distributions computed: {pnl.shape[1]} securities, {pnl.shape[0]} scenarios')

    if pnl.empty:
        print('No P&L computed — output not written.')
        return pnl

    # Step 3: Save to HDF under PNL/{SecurityID}
    output_file = config['VaR_DIR'] / 'security_pnl.h5'
    hdf_utils.save(pnl, 'PNL', output_file)
    print(f'Saved: {output_file}')

    # Step 4: Merge skewness/kurtosis from P&L into sensitivities returned by calculate()
    pnl_stats = pd.DataFrame({
        'security_id': pnl.columns,
        'skewness':    pnl.skew().values,
        'kurtosis':    pnl.kurt().values,
    })
    sens = sens.merge(pnl_stats, on='security_id', how='left')
    n = save_security_sensitivity(sens, as_of_date)
    print(f'Sensitivities written to DB: {n} rows')

    # Step 5: P&L distribution statistics
    stats = stat_utils.dist_stat(pnl)
    n = save_pnl_stat(stats, as_of_date, 'STRUCTURED_NOTE')
    print(f'Stats written to DB: {n} rows (pnl_type=STRUCTURED_NOTE)')

    return pnl


def test():
    as_of_date = date.fromisoformat(get_proc_asof_date())

    # Step 1: securities and prices
    securities = _get_sn_securities(as_of_date)
    print(f'Step 1: {len(securities)} structured note securities\n{securities}')

    if securities.empty:
        return

    security_ids = securities['security_id'].tolist()
    sn_prices    = dict(zip(securities['security_id'], securities['price'].astype(float)))

    # Step 2–6: run debug version — writes all intermediate results to Excel
    calculate_debug(security_ids, as_of_date, sn_prices)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Calculate structured note P&L')
    parser.add_argument(
        'mode', nargs='?', default='run',
        choices=['run', 'test'],
        help='run (default): calc_structured_note_pnl();  test: test()',
    )
    parser.add_argument(
        '--date', metavar='YYYY-MM-DD', default=None,
        help='As-of date (default: read from proc_asof_date table)',
    )
    args = parser.parse_args()

    as_of = date.fromisoformat(args.date) if args.date else None

    if args.mode == 'test':
        test()
    else:
        calc_structured_note_pnl(as_of)
