"""
process_uploaded_portfolio.py — Process an uploaded portfolio file through the VaR pipeline.

Pipeline:
    1.  Read the input Excel file (params, positions).
    2a. Map template column names → VaR engine convention.
    2b. Resolve SecurityID via TRG_ID → ISIN → CUSIP → BB_GLOBAL → Ticker.
    2c. Enrich positions with security attributes via update_security_info().
    2d. Fill/update prices via update_position_price(); split active vs excluded.
    3.  Run VaR engine on active positions.
    4.  Build results DataFrame.
    5.  Re-attach excluded positions with NULL VaR columns.
    6.  Add beta column.
    7.  Insert into port_position_var.

Public API:
    process_portfolio(file_path, port_id)
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from database2 import pg_connection
from dashboard.db_port_input import insert_port_parameters, insert_port_positions
from dashboard.db_port_position_var import insert_port_position_var

logger = logging.getLogger(__name__)

# Maps template (Excel) column names → VaR engine column names.
# Columns already matching the engine convention are omitted:
# SecurityName, ISIN, Ticker, Quantity, Currency.
_TEMPLATE_TO_ENGINE = {
    'ID':           'pos_id',
    'Cusip':        'CUSIP',
    'Market Value': 'MarketValue',
    'Asset Class':  'AssetClass',
    'TotalCost':    'total_cost',
}


def process_portfolio(file_path: Path, port_id: int) -> None:
    """
    Process an uploaded portfolio file and insert VaR results into port_position_var.

    file_path: path to the input Excel file.
    port_id:   portfolio ID used as the foreign key in port_position_var.
    """
    # Deferred to avoid circular imports: update_position_price → mkt_timeseries
    # → db_utils → api → routes → upload_portfolio → this module
    from preprocess import read_portfolio
    from process2 import var_engine
    from process2.calculate_var import add_beta_to_result, fetch_betas_bulk
    from process2.security_lookup import lookup_security_ids
    from process2.update_security_info import update_security_info
    from process2.update_position_price import update_position_price

    # ── 1. Read input file ────────────────────────────────────────────────────
    if not file_path.exists():
        raise Exception(f'file not found: {file_path}')

    params, positions, _ = read_portfolio.read_input_file(file_path)

    # ── 2a. Map template columns → engine convention ──────────────────────────
    positions = positions.rename(columns=_TEMPLATE_TO_ENGINE)
    for col in ('SecurityID', 'LastPrice', 'LastPriceDate'):
        if col not in positions.columns:
            positions[col] = None

    asof_date = params.get('AsofDate')

    # ── 2b. Resolve SecurityID (TRG_ID → ISIN → CUSIP → BB_GLOBAL → Ticker) ──
    positions = lookup_security_ids(positions)
    resolved = positions['SecurityID'].notna().sum()
    logger.info(f'Security lookup: {resolved}/{len(positions)} positions resolved')

    # ── 2c. Enrich with security attributes and exclusion flags ───────────────
    positions = update_security_info(positions, asof_date=asof_date)

    # ── 2d. Fill/update prices; split active vs excluded ─────────────────────
    positions = update_position_price(positions, asof_date)
    active   = positions[positions['excluded'] != True].reset_index(drop=True)
    excluded = positions[positions['excluded'] == True].reset_index(drop=True)
    logger.info(f'Split: {len(active)} active, {len(excluded)} excluded')

    insert_port_parameters(params, port_id)
    n_pos = insert_port_positions(positions, port_id)
    logger.info(f'inserted params and {n_pos} rows into port_parameters/port_positions for port_id={port_id}')

    # ── Update portfolio_info with computed market_value and as_of_date ──────
    mv = float(positions['MarketValue'].sum()) if 'MarketValue' in positions.columns else None
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'UPDATE portfolio_info SET market_value = %s, as_of_date = %s WHERE port_id = %s',
                (mv, asof_date, port_id),
            )
        conn.commit()
    logger.info(f'updated portfolio_info: market_value={mv}, as_of_date={asof_date} for port_id={port_id}')

    # ── 3. VaR engine ─────────────────────────────────────────────────────────
    var_metrics = var_engine.calc_var(active)

    # ── 4. Merge metrics back onto active positions ───────────────────────────
    result = active.set_index('pos_id').join(var_metrics, how='left').reset_index()

    # ── 5. Re-attach excluded positions with NULL VaR columns ─────────────────
    if not excluded.empty:
        excl = excluded.copy()
        for col in var_metrics.columns:
            excl[col] = None
        result = pd.concat([result, excl], ignore_index=True)

    # ── 6. Add beta ───────────────────────────────────────────────────────────
    sec_ids    = result['SecurityID'].dropna().unique().tolist()
    beta_key   = 'SP500_1Y'
    betas_bulk = fetch_betas_bulk([beta_key], sec_ids)
    betas      = betas_bulk.get(beta_key, {})
    result     = add_beta_to_result(result, betas, logger)

    # ── 7. Insert into port_position_var ─────────────────────────────────────
    n = insert_port_position_var(result, port_id, asof_date)
    logger.info(f'inserted {n} rows into port_position_var for port_id={port_id}')


def test(port_id: int) -> None:
    # Deferred to avoid circular import: upload_portfolio imports this module
    from api import app
    from dashboard.upload_portfolio import get_portfolio_file_path, _update_portfolio_status

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT port_name, filename, client_id FROM portfolio_info WHERE port_id = %s',
                (port_id,),
            )
            row = cur.fetchone()
    if not row:
        raise Exception(f'portfolio not found: port_id={port_id}')
    port_name, filename, client_id = row
    print(f'found portfolio: name={port_name}, filename={filename}, client_id={client_id}')

    file_path = get_portfolio_file_path(client_id, filename)
    print(f'processing portfolio file: {file_path}')

    try:
        with app.app_context():
            process_portfolio(file_path, port_id)
            _update_portfolio_status(port_id, 'Success')
    except Exception as e:
        print(f'Error processing portfolio: {e}')
        _update_portfolio_status(port_id, 'Error', str(e))


if __name__ == '__main__':
    import argparse
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')
    parser = argparse.ArgumentParser(description='Process an uploaded portfolio for a given port_id.')
    parser.add_argument('--port-id', type=int, required=True, metavar='PORT_ID',
                        help='port_id from portfolio_info to process')
    args = parser.parse_args()
    test(args.port_id)
