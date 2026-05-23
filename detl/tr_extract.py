"""
tr_extract.py — Fetch Treasury yield curve XML feed, save per-day CSV files,
insert into the treasury_yield DB table, and consolidate previous months into
yearly CSVs.

For each trading day in the fetched month:
  1. Saves one CSV: config['DATA_DIR'] / 'treasury.gov' / 'daily' / treasury_yield_YYYYMMDD.csv
  2. Upserts one row into the treasury_yield table (ON CONFLICT date DO UPDATE).

After fetch, consolidation runs automatically:
  - Daily files from months prior to the current month are merged into yearly CSVs:
      config['DATA_DIR'] / 'treasury.gov' / treasury_yield_YYYY.csv
  - Consolidated daily files are deleted.

Each CSV / DB row (wide format):
    date, BC_1MONTH, BC_2MONTH, BC_3MONTH, BC_6MONTH,
    BC_1YEAR, BC_2YEAR, BC_3YEAR, BC_5YEAR, BC_7YEAR,
    BC_10YEAR, BC_20YEAR, BC_30YEAR

Values are in percentage points as published (e.g., 4.32 = 4.32%).
Safe to re-run (upsert for DB, merge+dedup for yearly CSVs).

Table: see detl/sql/create_treasury_yield.sql

Usage:
    python detl/tr_extract.py                   # fetch current month + consolidate
    python detl/tr_extract.py --month 202605    # fetch specific month + consolidate
    python detl/tr_extract.py --consolidate     # consolidate only (no fetch)
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
import xml.etree.ElementTree as ET

import pandas as pd
import requests
from psycopg2.extras import execute_batch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from trg_config import config
from database2 import pg_connection

_URL = (
    'https://home.treasury.gov/resource-center/data-chart-center/interest-rates/'
    'pages/xml?data=daily_treasury_yield_curve&field_tdr_date_value_month={month}'
)

_NS_ATOM = 'http://www.w3.org/2005/Atom'
_NS_M    = 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata'
_NS_D    = 'http://schemas.microsoft.com/ado/2007/08/dataservices'

_MATURITIES = [
    'BC_1MONTH', 'BC_2MONTH', 'BC_3MONTH', 'BC_6MONTH',
    'BC_1YEAR',  'BC_2YEAR',  'BC_3YEAR',  'BC_5YEAR',
    'BC_7YEAR',  'BC_10YEAR', 'BC_20YEAR', 'BC_30YEAR',
]

_COLUMNS     = ['date'] + _MATURITIES
_DB_MAT_COLS = [m.lower() for m in _MATURITIES]

_DAILY_DIR  = config['DATA_DIR'] / 'treasury.gov' / 'daily'
_YEARLY_DIR = config['DATA_DIR'] / 'treasury.gov'

_INSERT_SQL = """
    INSERT INTO treasury_yield (date, {cols}, insert_time)
    VALUES (%(date)s, {placeholders}, NOW())
    ON CONFLICT (date) DO UPDATE SET
        {updates},
        insert_time = NOW()
""".format(
    cols         = ', '.join(_DB_MAT_COLS),
    placeholders = ', '.join(f'%({c})s' for c in _DB_MAT_COLS),
    updates      = ', '.join(f'{c} = EXCLUDED.{c}' for c in _DB_MAT_COLS),
)


# ── Fetch ──────────────────────────────────────────────────────────────────────

def fetch_xml(month: str) -> str:
    """Fetch the Atom XML feed for the given month (YYYYMM)."""
    url = _URL.format(month=month)
    print(f'Fetching: {url}')
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


# ── Parse ──────────────────────────────────────────────────────────────────────

def parse_xml(xml_text: str) -> pd.DataFrame:
    """Parse the Atom XML and return a DataFrame with one row per trading day."""
    root = ET.fromstring(xml_text)
    rows = []

    for entry in root.findall(f'{{{_NS_ATOM}}}entry'):
        props = entry.find(f'.//{{{_NS_M}}}properties')
        if props is None:
            continue

        date_el = props.find(f'{{{_NS_D}}}NEW_DATE')
        if date_el is None or not date_el.text:
            continue

        row = {'date': pd.to_datetime(date_el.text).date()}
        for mat in _MATURITIES:
            el = props.find(f'{{{_NS_D}}}{mat}')
            if el is not None and el.text:
                try:
                    row[mat] = float(el.text)
                except ValueError:
                    row[mat] = None
            else:
                row[mat] = None
        rows.append(row)

    df = pd.DataFrame(rows, columns=_COLUMNS)
    return df.sort_values('date').reset_index(drop=True)


# ── CSV ────────────────────────────────────────────────────────────────────────

def save_daily(df: pd.DataFrame) -> list[Path]:
    """Save one CSV per trading day into the daily/ folder."""
    _DAILY_DIR.mkdir(parents=True, exist_ok=True)

    saved = []
    for _, row in df.iterrows():
        date_str = row['date'].strftime('%Y%m%d')
        out_file = _DAILY_DIR / f'treasury_yield_{date_str}.csv'
        pd.DataFrame([row]).to_csv(out_file, index=False)
        saved.append(out_file)

    return saved


# ── DB ─────────────────────────────────────────────────────────────────────────

def insert_db(df: pd.DataFrame) -> int:
    """Upsert yield data into treasury_yield table. Returns number of rows upserted."""
    records = []
    for _, row in df.iterrows():
        rec = {'date': row['date']}
        for mat, db_col in zip(_MATURITIES, _DB_MAT_COLS):
            val = row[mat]
            rec[db_col] = None if pd.isna(val) else float(val)
        records.append(rec)

    with pg_connection() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, _INSERT_SQL, records)
        conn.commit()

    return len(records)


# ── Consolidate ────────────────────────────────────────────────────────────────

def consolidate() -> None:
    """
    Merge daily CSVs from previous months into yearly CSVs and delete the
    consolidated daily files.

    Only files whose month is strictly before the current month are processed,
    so the current month's daily files are never touched.
    """
    cutoff_month = datetime.now().strftime('%Y%m')   # e.g. '202605'

    # Collect eligible daily files grouped by year
    year_files: dict[str, list[Path]] = {}
    for f in sorted(_DAILY_DIR.glob('treasury_yield_????????.csv')):
        stem = f.stem                        # 'treasury_yield_YYYYMMDD'
        date_part = stem.split('_')[-1]      # 'YYYYMMDD'
        file_month = date_part[:6]           # 'YYYYMM'
        if file_month >= cutoff_month:
            continue
        year = date_part[:4]
        year_files.setdefault(year, []).append(f)

    if not year_files:
        print('Consolidate: no previous-month files to consolidate.')
        return

    for year, files in sorted(year_files.items()):
        yearly_file = _YEARLY_DIR / f'treasury_yield_{year}.csv'

        # Read existing yearly file if present (re-run safety)
        frames = []
        if yearly_file.exists():
            frames.append(pd.read_csv(yearly_file, parse_dates=['date']))

        # Read all daily files for this year
        for f in files:
            frames.append(pd.read_csv(f, parse_dates=['date']))

        combined = (
            pd.concat(frames, ignore_index=True)
            .drop_duplicates(subset='date')
            .sort_values('date')
            .reset_index(drop=True)
        )

        combined.to_csv(yearly_file, index=False)

        # Delete consolidated daily files
        for f in files:
            f.unlink()

        print(
            f'Consolidate {year}: {len(combined)} rows → {yearly_file.name}'
            f'  ({len(files)} daily file(s) deleted)'
        )


# ── Orchestrator ───────────────────────────────────────────────────────────────

def run(month: str) -> None:
    xml_text = fetch_xml(month)
    df       = parse_xml(xml_text)

    if df.empty:
        print(f'No data found for month={month}')
        return

    print(f'Parsed {len(df)} trading day(s)')

    saved = save_daily(df)
    print(f'CSV: {len(saved)} file(s) written to {saved[0].parent}')

    n = insert_db(df)
    print(f'DB:  {n} row(s) upserted into treasury_yield  (latest date: {df["date"].max()})')

    consolidate()


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Fetch Treasury yield curve XML and save to CSV + DB'
    )
    parser.add_argument(
        '--month', default=None, metavar='YYYYMM',
        help='Month to fetch in YYYYMM format (default: current month)',
    )
    parser.add_argument(
        '--consolidate', action='store_true',
        help='Consolidate previous months into yearly CSVs only (no fetch)',
    )
    args = parser.parse_args()

    if args.consolidate:
        consolidate()
    else:
        month = args.month or datetime.now().strftime('%Y%m')
        run(month)
