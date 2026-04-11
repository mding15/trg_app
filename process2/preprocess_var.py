"""
preprocess_var.py — Build (params, positions) ready for VaR calculation.

Steps:
    1. Fetch all proc_positions rows for the given as_of_date.
    2. Map column names to VaR engine conventions.
    3. Enrich with security attributes via update_security_info().
    4. Fill missing/stale prices via update_position_price().
    5. Return (params dict, enriched positions DataFrame).
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

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


# ── params ─────────────────────────────────────────────────────────────────────

def build_params(as_of_date) -> dict:
    """Return hardcoded VaR params for MSSB accounts."""
    as_of_date = pd.to_datetime(as_of_date)
    return {
        'AsofDate':        as_of_date,
        'ReportDate':      as_of_date,
        'RiskHorizon':     '1 Day',
        'TailMeasure':     '95% TailVaR',
        'ReturnFrequency': 'Daily',
        'Benchmark':       'BM_60_40',
        'ExpectedReturn':  'Upload',
        'BaseCurrency':    'USD',
        # ── HARDCODED: PortfolioName should eventually be derived from feed_source ──
        'PortfolioName':   'MSSB',
    }


# ── main ───────────────────────────────────────────────────────────────────────

def preprocess_var(
    as_of_date,
    feed_source: str | None = None,
    account_ids: list[int] | None = None,
) -> tuple[dict, pd.DataFrame]:
    """
    Build (params, positions) for VaR calculation from proc_positions.

    as_of_date:   the as_of_date value in proc_positions to process.
    feed_source:  only rows with this feed_source are fetched.
    account_ids:  if provided, only rows for those account_ids are fetched.
    Returns (params dict, enriched positions DataFrame).
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

    params = build_params(as_of_date)
    return params, positions
