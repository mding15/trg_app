"""
security_lookup.py — Resolve SecurityID from security_xref by ISIN, CUSIP, BB_GLOBAL, or Ticker.

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
    Resolve SecurityID from security_xref using ISIN → CUSIP → BB_GLOBAL → Ticker priority.
    All four ref types are fetched in batch (four queries total).
    Positions that cannot be resolved are left with SecurityID = None.

    Expects columns: ISIN, CUSIP, BB_GLOBAL, Ticker (all optional — missing columns are skipped).
    Returns a copy of positions with SecurityID set.
    """
    positions = positions.copy()

    isins     = [v for v in positions['ISIN'].dropna().unique()      if v] if 'ISIN'      in positions.columns else []
    cusips    = [v for v in positions['CUSIP'].dropna().unique()     if v] if 'CUSIP'     in positions.columns else []
    bb_globals = [v for v in positions['BB_GLOBAL'].dropna().unique() if v] if 'BB_GLOBAL' in positions.columns else []
    tickers   = [v for v in positions['Ticker'].dropna().unique()    if v] if 'Ticker'    in positions.columns else []

    cache: dict[tuple, str] = {}

    with pg_connection() as conn:
        with conn.cursor() as cur:
            for ref_type, vals in (
                ('ISIN',      isins),
                ('CUSIP',     cusips),
                ('BB_GLOBAL', bb_globals),
                ('Ticker',    tickers),
            ):
                if vals:
                    cur.execute(
                        'SELECT "REF_ID", "SecurityID" FROM security_xref'
                        ' WHERE "REF_TYPE" = %s AND "REF_ID" = ANY(%s)',
                        (ref_type, vals),
                    )
                    for ref_id, sec_id in cur.fetchall():
                        cache.setdefault((ref_type, ref_id), sec_id)

    def _resolve(row) -> str | None:
        isin      = row.get('ISIN')
        cusip     = row.get('CUSIP')
        bb_global = row.get('BB_GLOBAL')
        ticker    = row.get('Ticker')
        if isin      and ('ISIN',      isin)      in cache: return cache[('ISIN',      isin)]
        if cusip     and ('CUSIP',     cusip)     in cache: return cache[('CUSIP',     cusip)]
        if bb_global and ('BB_GLOBAL', bb_global) in cache: return cache[('BB_GLOBAL', bb_global)]
        if ticker    and ('Ticker',    ticker)    in cache: return cache[('Ticker',    ticker)]
        return None

    positions['SecurityID'] = positions.apply(_resolve, axis=1)
    return positions


def test(xlsx_path: str = None) -> None:
    """Read positions from the 'Positions' tab, resolve SecurityID, write to 'SecurityID' tab."""
    from pathlib import Path
    import openpyxl

    if xlsx_path is None:
        xlsx_path = Path(__file__).parent / 'test_data' / 'positions.xlsx'
    xlsx_path = Path(xlsx_path)

    df = pd.read_excel(xlsx_path, sheet_name='Positions')
    print(f'Read {len(df)} rows from {xlsx_path.name} [Positions]')

    # lookup_security_ids expects 'CUSIP'; the sheet has 'Cusip'
    if 'Cusip' in df.columns and 'CUSIP' not in df.columns:
        df = df.rename(columns={'Cusip': 'CUSIP'})

    result = lookup_security_ids(df)

    resolved = result['SecurityID'].notna().sum()
    print(f'Resolved {resolved}/{len(result)} SecurityIDs')

    # Write result to 'SecurityID' tab, preserving other sheets
    with pd.ExcelWriter(xlsx_path, engine='openpyxl', mode='a',
                        if_sheet_exists='replace') as writer:
        result.to_excel(writer, sheet_name='SecurityID', index=False)

    print(f'Written to {xlsx_path.name} [SecurityID]')


if __name__ == '__main__':
    test()
