"""
dashboard/db_port_input.py — Database operations for port_parameters and port_positions.

Functions:
    insert_port_parameters(params, port_id)   — write raw params to port_parameters
    insert_port_positions(positions, port_id) — write raw positions to port_positions
"""
from __future__ import annotations

import logging
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import numpy as np
import pandas as pd
from psycopg2.extras import execute_batch

from database2 import pg_connection

logger = logging.getLogger(__name__)


# port_parameters columns (excluding port_id), matching params dict keys exactly.
_PARAMS_COLS = [
    'PortfolioName', 'AsofDate', 'ReportDate',
    'RiskHorizon', 'TailMeasure', 'ReturnFrequency',
    'Benchmark', 'ExpectedReturn', 'BaseCurrency',
]

# port_positions columns populated from the enriched positions DataFrame (post step 2d).
# Enriched columns (Class, SC1, Region, asset_class, etc.) are excluded — they
# live in port_position_var, not port_positions.
_POSITIONS_TABLE_COLS = [
    'port_id',
    'pos_id', 'SecurityID', 'SecurityName', 'ISIN', 'CUSIP', 'Ticker',
    'Quantity', 'MarketValue', 'userCurrency', 'userAssetClass', 'ExpectedReturn',
    'OptionType', 'PaymentFrequency', 'MaturityDate', 'OptionStrike',
    'UnderlyingSecurityID', 'CouponRate',
]

# DataFrame column name → DB column name where they differ.
_COL_TO_DB = {'pos_id': 'ID'}

_NUMERIC_COLS = {'Quantity', 'MarketValue', 'ExpectedReturn', 'OptionStrike', 'CouponRate'}
_DATE_COLS    = {'MaturityDate'}
_INT_COLS     = {'PaymentFrequency'}


def insert_port_parameters(params: dict, port_id: int) -> None:
    """
    Delete and re-insert one row into port_parameters for the given port_id.
    params keys must match the table's quoted column names (PortfolioName, AsofDate, …).
    """
    row = {'port_id': port_id}
    for col in _PARAMS_COLS:
        row[col] = params.get(col)

    for date_col in ('AsofDate', 'ReportDate'):
        v = row.get(date_col)
        if v is not None:
            try:
                row[date_col] = pd.to_datetime(v).date()
            except Exception:
                row[date_col] = None

    col_names    = ['port_id'] + _PARAMS_COLS
    col_sql      = ', '.join('port_id' if c == 'port_id' else f'"{c}"' for c in col_names)
    placeholders = ', '.join(f'%({c})s' for c in col_names)
    sql = f'INSERT INTO port_parameters ({col_sql}) VALUES ({placeholders})'

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('DELETE FROM port_parameters WHERE port_id = %s', (port_id,))
            cur.execute(sql, row)
        conn.commit()


def insert_port_positions(positions: pd.DataFrame, port_id: int) -> int:
    """
    Delete and re-insert rows into port_positions for the given port_id.
    Expects the enriched positions DataFrame (post step 2d).
    Returns the number of rows inserted.
    """
    df = positions.copy()
    df['port_id'] = port_id

    for col in _NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    for col in _DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').apply(
                lambda x: x.date() if pd.notna(x) else None
            )

    for col in _INT_COLS:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: int(float(x)) if pd.notna(x) else None
            )

    df = df.replace({np.nan: None, pd.NaT: None})

    cols = [c for c in _POSITIONS_TABLE_COLS if c in df.columns]
    df   = df[cols]

    col_sql      = ', '.join('port_id' if c == 'port_id' else f'"{_COL_TO_DB.get(c, c)}"' for c in cols)
    placeholders = ', '.join(f'%({c})s' for c in cols)
    sql = f'INSERT INTO port_positions ({col_sql}) VALUES ({placeholders})'

    rows = df.to_dict(orient='records')

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('DELETE FROM port_positions WHERE port_id = %s', (port_id,))
            execute_batch(cur, sql, rows)
        conn.commit()

    return len(rows)


def test(port_id: int) -> None:
    from preprocess import read_portfolio
    from dashboard.upload_portfolio import get_portfolio_file_path

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
    print(f'portfolio : {port_name}  (port_id={port_id}, client_id={client_id})')
    print(f'filename  : {filename}')

    file_path = get_portfolio_file_path(client_id, filename)
    print(f'file path : {file_path}')
    if not file_path.exists():
        raise Exception(f'file not found: {file_path}')

    params, positions, _ = read_portfolio.read_input_file(file_path)
    print(f'params    : {params}')
    print(f'positions : {len(positions)} rows, columns: {list(positions.columns)}')

    insert_port_parameters(params, port_id)
    print('insert_port_parameters: OK')

    n = insert_port_positions(positions, port_id)
    print(f'insert_port_positions : {n} rows inserted')


if __name__ == '__main__':
    import argparse
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')
    parser = argparse.ArgumentParser(
        description='Test insert_port_parameters and insert_port_positions for a given port_id.'
    )
    parser.add_argument('--port-id', type=int, required=True, metavar='PORT_ID',
                        help='port_id from portfolio_info to process')
    args = parser.parse_args()
    test(args.port_id)
