"""
preprocess_var.py — Build enriched positions DataFrame ready for VaR calculation.

Steps:
    1. Fetch all proc_positions rows for the given as_of_date.
    2. Map column names to VaR engine conventions.
    3. Enrich with security attributes via update_security_info().
    4. Fill missing/stale prices via update_position_price().
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from database2 import pg_connection
from process2.db_position_var import fetch_proc_positions
from process2.update_security_info import update_security_info
from process2.update_position_price import update_position_price


# ── column mapping ─────────────────────────────────────────────────────────────

_RENAME = {
    'security_id':     'SecurityID',
    'security_name':   'SecurityName',
    'quantity':        'Quantity',
    'market_value':    'MarketValue',
    'isin':            'ISIN',
    'cusip':           'CUSIP',
    'ticker':          'Ticker',
    'currency':        'Currency',
    'last_price':      'LastPrice',
    'last_price_date': 'LastPriceDate',
    'position_id':     'pos_id',
}

_DROP = ['as_of_date', 'asset_class', 'feed_source']


def _map_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename proc_positions columns to VaR engine conventions and drop unused columns."""
    df = df.drop(columns=[c for c in _DROP if c in df.columns])
    df = df.rename(columns=_RENAME)
    return df


# ── beta fetch ─────────────────────────────────────────────────────────────────

def _fetch_betas(as_of_date, account_ids: list[int]) -> pd.DataFrame:
    """Return (account_id, security_id, beta) rows from sec_beta via account_parameters."""
    if not account_ids:
        return pd.DataFrame(columns=['account_id', 'security_id', 'beta'])
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (pp.account_id, pp.security_id)
                       pp.account_id, pp.security_id, sb.beta
                FROM proc_positions pp
                LEFT JOIN account_parameters ap ON pp.account_id = ap.account_id
                LEFT JOIN sec_beta sb
                       ON pp.security_id = sb.security_id
                      AND sb.beta_key    = ap.beta_key
                WHERE pp.as_of_date = %s
                  AND pp.account_id  = ANY(%s)
                ORDER BY pp.account_id, pp.security_id, ap.updated_at DESC
                """,
                (as_of_date, account_ids),
            )
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=['account_id', 'security_id', 'beta'])


# ── main ───────────────────────────────────────────────────────────────────────

def preprocess_var(
    as_of_date,
    feed_source: str | None = None,
    account_ids: list[int] | None = None,
) -> pd.DataFrame:
    """
    Build enriched positions DataFrame for VaR calculation from proc_positions.

    as_of_date:   the as_of_date value in proc_positions to process.
    feed_source:  only rows with this feed_source are fetched.
    account_ids:  if provided, only rows for those account_ids are fetched.
    """
    positions = fetch_proc_positions(as_of_date, feed_source, account_ids)
    if positions.empty:
        acct_msg = f', account_ids={account_ids}' if account_ids is not None else ''
        raise ValueError(
            f'No proc_positions rows found for as_of_date={as_of_date}, '
            f'feed_source={feed_source!r}{acct_msg}'
        )

    positions = _map_columns(positions)
    positions = update_security_info(positions, asof_date=as_of_date)
    positions = update_position_price(positions, as_of_date)
    positions = positions.reset_index(drop=True)

    acct_ids = [int(a) for a in positions['account_id'].dropna().unique()]
    betas_df = _fetch_betas(as_of_date, acct_ids)
    positions = positions.merge(
        betas_df,
        left_on=['account_id', 'SecurityID'],
        right_on=['account_id', 'security_id'],
        how='left',
    ).drop(columns='security_id')

    return positions
