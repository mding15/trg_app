"""
db_position_var.py — Database operations for the position_var table.

Functions:
    fetch_proc_positions(as_of_date)  — read from proc_positions
    fetch_latest_as_of_date()         — get latest as_of_date from proc_positions
    insert_results(results, as_of_date) — write to position_var
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import numpy as np
import pandas as pd
from psycopg2.extras import execute_batch

from database2 import pg_connection


# ── read ───────────────────────────────────────────────────────────────────────

def fetch_proc_positions(as_of_date, feed_source: str) -> pd.DataFrame:
    """Fetch proc_positions rows for the given as_of_date and feed_source."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT * FROM proc_positions WHERE as_of_date = %s AND feed_source = %s',
                (as_of_date, feed_source),
            )
            cols = [desc[0] for desc in cur.description]
            df = pd.DataFrame(cur.fetchall(), columns=cols)
            return df.drop(columns=[c for c in ('id', 'insert_time') if c in df.columns])


def fetch_latest_as_of_date(feed_source: str) -> str:
    """Return the latest as_of_date in proc_positions for the given feed_source as an ISO string."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT MAX(as_of_date) FROM proc_positions WHERE feed_source = %s',
                (feed_source,),
            )
            result = cur.fetchone()[0]
    if result is None:
        raise ValueError(f'No rows found in proc_positions for feed_source={feed_source!r}.')
    return result.isoformat()


# ── write ──────────────────────────────────────────────────────────────────────

def insert_results(results: pd.DataFrame, as_of_date) -> int:
    """
    Insert the Results DataFrame into position_var.
    Deletes existing rows for (as_of_date, account_id) before inserting to ensure
    a clean replace on re-runs. Returns the number of rows inserted.
    """
    # rename engine column names to snake_case DB column names
    _ENGINE_TO_DB = {
        'SecurityID':           'security_id',
        'SecurityName':         'security_name',
        'ISIN':                 'isin',
        'CUSIP':                'cusip',
        'Ticker':               'ticker',
        'Quantity':             'quantity',
        'MarketValue':          'market_value',
        'Currency':             'currency',
        'LastPrice':            'last_price',
        'LastPriceDate':        'last_price_date',
        'AssetClass':           'asset_class',
        'AssetType':            'asset_type',
        'Class':                'class',
        'SC1':                  'sc1',
        'SC2':                  'sc2',
        'Country':              'country',
        'Region':               'region',
        'Sector':               'sector',
        'Industry':             'industry',
        'ExpectedReturn':       'expected_return',
        'CouponRate':           'coupon_rate',
        'OptionType':           'option_type',
        'OptionStrike':         'option_strike',
        'PaymentFrequency':     'payment_frequency',
        'MaturityDate':         'maturity_date',
        'UnderlyingSecurityID': 'underlying_security_id',
        'UnderlyingID':         'underlying_id',
        'UnderlyingPrice':      'underlying_price',
        'RiskFreeRate':         'risk_free_rate',
        'Tenor':                'tenor',
        'DELTA':                'delta',
        'GAMMA':                'gamma',
        'VEGA':                 'vega',
        'IV':                   'iv',
        'IR_Tenor':             'ir_tenor',
        'Yield':                'yield',
        'Duration':             'duration',
        'Convexity':            'convexity',
        'IR_PV01':              'ir_pv01',
        'SP_PV01':              'sp_pv01',
        'SpreadDuration':       'spread_duration',
        'SpreadConvexity':      'spread_convexity',
        'DELTA VaR':            'delta_var',
        'IR VaR':               'ir_var',
        'SPREAD VaR':           'spread_var',
        'GAMMA VaR':            'gamma_var',
        'STD':                  'std',
        'Marginal_STD':         'marginal_std',
        'VaR':                  'var',
        'tVaR':                 'tvar',
        'Marginal_VaR':         'marginal_var',
        'Marginal_tVaR':        'marginal_tvar',
    }

    _NUMERIC_COLS = {
        'quantity', 'market_value', 'last_price',
        'expected_return', 'coupon_rate', 'option_strike', 'underlying_price',
        'risk_free_rate', 'tenor', 'delta', 'gamma', 'vega', 'iv',
        'ir_tenor', 'yield', 'duration', 'convexity',
        'ir_pv01', 'sp_pv01', 'spread_duration', 'spread_convexity',
        'delta_var', 'ir_var', 'spread_var', 'gamma_var',
        'std', 'marginal_std', 'var', 'tvar', 'marginal_var', 'marginal_tvar',
    }

    _DATE_COLS = {'last_price_date', 'maturity_date'}
    _BOOL_COLS = {'is_option', 'excluded'}

    _TABLE_COLS = [
        'as_of_date', 'account_id', 'pos_id',
        'security_id', 'security_name', 'isin', 'cusip', 'ticker', 'broker_account',
        'quantity', 'market_value', 'currency', 'last_price', 'last_price_date',
        'asset_class', 'asset_type', 'class', 'sc1', 'sc2',
        'country', 'region', 'sector', 'industry',
        'expected_return', 'coupon_rate', 'option_type', 'option_strike',
        'payment_frequency', 'maturity_date', 'underlying_security_id', 'underlying_id',
        'underlying_price', 'is_option', 'excluded', 'exclude_reason',
        'risk_free_rate', 'tenor', 'delta', 'gamma', 'vega', 'iv',
        'ir_tenor', 'yield', 'duration', 'convexity',
        'ir_pv01', 'sp_pv01', 'spread_duration', 'spread_convexity',
        'delta_var', 'ir_var', 'spread_var', 'gamma_var',
        'std', 'marginal_std', 'var', 'tvar', 'marginal_var', 'marginal_tvar',
    ]

    df = results.copy()
    df['as_of_date'] = pd.to_datetime(as_of_date).date()
    df = df.rename(columns=_ENGINE_TO_DB)

    for col in _NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    for col in _DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

    for col in _BOOL_COLS:
        if col in df.columns:
            df[col] = df[col].map(lambda x: bool(x) if x is not None else None)

    df = df.replace({np.nan: None, pd.NaT: None})

    cols = [c for c in _TABLE_COLS if c in df.columns]
    df = df[cols]

    col_sql      = ', '.join(f'"{c}"' for c in cols)
    placeholders = ', '.join(f'%({c})s' for c in cols)
    update_sql   = ', '.join(
        f'"{c}" = EXCLUDED."{c}"'
        for c in cols if c not in ('as_of_date', 'account_id', 'pos_id')
    )
    sql = f"""
        INSERT INTO position_var ({col_sql})
        VALUES ({placeholders})
        ON CONFLICT (as_of_date, account_id, pos_id) DO UPDATE SET {update_sql}
    """

    rows        = df.to_dict(orient='records')
    as_of       = pd.to_datetime(as_of_date).date()
    account_ids = df['account_id'].dropna().unique().tolist()

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'DELETE FROM position_var WHERE as_of_date = %s AND account_id = ANY(%s)',
                (as_of, account_ids),
            )
            execute_batch(cur, sql, rows)
        conn.commit()

    return len(rows)
