"""
security_lookup.py — Resolve SecurityID from security_xref by ISIN, CUSIP, or Ticker.

Public API:
    lookup_security_ids(positions) -> positions with SecurityID filled in
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from database2 import pg_connection


def lookup_security_ids(positions: pd.DataFrame) -> pd.DataFrame:
    """
    Resolve SecurityID from security_xref using ISIN → CUSIP → Ticker priority.
    All three ref types are fetched in batch (three queries total).
    Positions that cannot be resolved are left with SecurityID = None.

    Expects columns: ISIN, CUSIP, Ticker (all optional — missing columns are skipped).
    Returns a copy of positions with SecurityID set.
    """
    positions = positions.copy()

    isins   = [v for v in positions['ISIN'].dropna().unique()   if v] if 'ISIN'   in positions.columns else []
    cusips  = [v for v in positions['CUSIP'].dropna().unique()  if v] if 'CUSIP'  in positions.columns else []
    tickers = [v for v in positions['Ticker'].dropna().unique() if v] if 'Ticker' in positions.columns else []

    cache: dict[tuple, str] = {}

    with pg_connection() as conn:
        with conn.cursor() as cur:
            if isins:
                cur.execute(
                    'SELECT "REF_ID", "SecurityID" FROM security_xref'
                    ' WHERE "REF_TYPE" = %s AND "REF_ID" = ANY(%s)',
                    ('ISIN', isins),
                )
                for ref_id, sec_id in cur.fetchall():
                    cache[('ISIN', ref_id)] = sec_id

            if cusips:
                cur.execute(
                    'SELECT "REF_ID", "SecurityID" FROM security_xref'
                    ' WHERE "REF_TYPE" = %s AND "REF_ID" = ANY(%s)',
                    ('CUSIP', cusips),
                )
                for ref_id, sec_id in cur.fetchall():
                    cache.setdefault(('CUSIP', ref_id), sec_id)

            if tickers:
                cur.execute(
                    'SELECT "REF_ID", "SecurityID" FROM security_xref'
                    ' WHERE "REF_TYPE" = %s AND "REF_ID" = ANY(%s)',
                    ('Ticker', tickers),
                )
                for ref_id, sec_id in cur.fetchall():
                    cache.setdefault(('Ticker', ref_id), sec_id)

    def _resolve(row) -> str | None:
        isin   = row.get('ISIN')
        cusip  = row.get('CUSIP')
        ticker = row.get('Ticker')
        if isin   and ('ISIN',   isin)   in cache: return cache[('ISIN',   isin)]
        if cusip  and ('CUSIP',  cusip)  in cache: return cache[('CUSIP',  cusip)]
        if ticker and ('Ticker', ticker) in cache: return cache[('Ticker', ticker)]
        return None

    positions['SecurityID'] = positions.apply(_resolve, axis=1)
    return positions
