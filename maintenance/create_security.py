"""
create_security.py — Create new securities from an Excel file.

For each row in the sheet, creates a new security in security_info and adds
cross-reference entries (ISIN, CUSIP, Ticker) if provided.
Rows where the ISIN or CUSIP already exists in security_xref are skipped.

The Excel file is expected in maintenance/Excel/.

Sheet columns:
    security_name   (required)
    currency        (required)
    asset_class     (optional)
    asset_type      (optional)
    isin            (optional)
    cusip           (optional)
    ticker          (optional)

Usage:
    python maintenance/create_security.py
    python maintenance/create_security.py --file create_security.xlsx --sheet securities
    python maintenance/create_security.py --data-source MANUAL
    python maintenance/create_security.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'process2'))

from database2 import pg_connection
from security_utils import add_xref_if_missing, create_security

EXCEL_DIR           = Path(__file__).resolve().parent / 'Excel'
DEFAULT_FILE        = 'create_security.xlsx'
DEFAULT_SHEET       = 'securities'
DEFAULT_DATA_SOURCE = 'MANUAL'
OPTIONAL_COLS       = ['asset_class', 'asset_type', 'isin', 'cusip', 'ticker']


# ── Helpers ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger('create_security')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s', '%H:%M:%S')
    )
    logger.addHandler(handler)
    return logger


def _clean(val) -> str:
    """Convert pandas NaN / NaT / None → empty string."""
    try:
        if pd.isna(val):
            return ''
    except (TypeError, ValueError):
        pass
    return str(val).strip()


def _load_excel(filepath: Path, sheet: str) -> list[dict]:
    log = logging.getLogger('create_security')
    log.info(f"Reading sheet '{sheet}' from {filepath}")
    try:
        df = pd.read_excel(filepath, sheet_name=sheet)
    except ValueError as e:
        log.error(f"Could not read sheet '{sheet}': {e}")
        sys.exit(1)
    except Exception as e:
        log.error(f"Failed to open Excel file: {e}")
        sys.exit(1)

    if df.empty:
        log.warning('Sheet is empty — nothing to process.')
        sys.exit(0)

    log.info(f"  {len(df)} rows · {len(df.columns)} columns")

    for col in ('security_name', 'currency'):
        if col not in df.columns:
            log.error(f"Required column '{col}' not found in sheet '{sheet}'")
            sys.exit(1)

    for col in OPTIONAL_COLS:
        if col not in df.columns:
            df[col] = ''

    return [{col: _clean(row[col]) for col in df.columns} for _, row in df.iterrows()]


def _batch_check_existing(cur, rows: list[dict]) -> set[tuple]:
    """Return set of (ref_type, ref_id) pairs already present in security_xref."""
    isins   = [r['isin']   for r in rows if r.get('isin')]
    cusips  = [r['cusip']  for r in rows if r.get('cusip')]
    tickers = [r['ticker'] for r in rows if r.get('ticker')]
    existing: set[tuple] = set()

    if isins:
        cur.execute(
            'SELECT "REF_TYPE", "REF_ID" FROM security_xref WHERE "REF_TYPE" = %s AND "REF_ID" = ANY(%s)',
            ('ISIN', isins),
        )
        existing.update(cur.fetchall())

    if cusips:
        cur.execute(
            'SELECT "REF_TYPE", "REF_ID" FROM security_xref WHERE "REF_TYPE" = %s AND "REF_ID" = ANY(%s)',
            ('CUSIP', cusips),
        )
        existing.update(cur.fetchall())

    if tickers:
        cur.execute(
            'SELECT "REF_TYPE", "REF_ID" FROM security_xref WHERE "REF_TYPE" = %s AND "REF_ID" = ANY(%s)',
            ('Ticker', tickers),
        )
        existing.update(cur.fetchall())

    return existing


# ── Core logic ────────────────────────────────────────────────────────────────

def run(file: str, sheet: str, data_source: str, dry_run: bool) -> None:
    log = _setup_logger()

    excel_path = EXCEL_DIR / file
    if not excel_path.exists():
        log.error(f"File not found: {excel_path}")
        sys.exit(1)

    rows = _load_excel(excel_path, sheet)

    if dry_run:
        log.info('─' * 60)
        log.info('DRY RUN — no data will be written to the database')

    created = 0
    skipped = 0

    with pg_connection() as conn:
        with conn.cursor() as cur:
            existing = _batch_check_existing(cur, rows)

            for i, r in enumerate(rows, 1):
                security_name = r.get('security_name', '')
                currency      = r.get('currency',      '')
                asset_class   = r.get('asset_class',   '') or None
                asset_type    = r.get('asset_type',    '') or None
                isin          = r.get('isin',          '')
                cusip         = r.get('cusip',         '')
                ticker        = r.get('ticker',        '')

                if not security_name or not currency:
                    log.warning(f"Row {i}: SKIP — security_name and currency are required")
                    skipped += 1
                    continue

                if isin   and ('ISIN',   isin)   in existing:
                    log.warning(f"Row {i}: SKIP — ISIN '{isin}' already exists  ({security_name})")
                    skipped += 1
                    continue
                if cusip  and ('CUSIP',  cusip)  in existing:
                    log.warning(f"Row {i}: SKIP — CUSIP '{cusip}' already exists  ({security_name})")
                    skipped += 1
                    continue
                if ticker and ('Ticker', ticker) in existing:
                    log.warning(f"Row {i}: SKIP — Ticker '{ticker}' already exists  ({security_name})")
                    skipped += 1
                    continue

                if not isin and not cusip and not ticker:
                    log.warning(f"Row {i}: SKIP — no identifiers (isin/cusip/ticker) provided  ({security_name})")
                    skipped += 1
                    continue

                if dry_run:
                    log.info(
                        f"Row {i}: WOULD CREATE — name='{security_name}' currency='{currency}'"
                        f" asset_class='{asset_class}' isin='{isin}' cusip='{cusip}' ticker='{ticker}'"
                    )
                    created += 1
                    continue

                security_id = create_security(cur, security_name, currency, asset_class, asset_type, data_source)
                add_xref_if_missing(cur, security_id, 'ISIN',   isin,   data_source)
                add_xref_if_missing(cur, security_id, 'CUSIP',  cusip,  data_source)
                add_xref_if_missing(cur, security_id, 'Ticker', ticker, data_source)
                log.info(f"Row {i}: CREATED {security_id} — '{security_name}' isin='{isin}' cusip='{cusip}'")
                created += 1

        if not dry_run:
            conn.commit()

    log.info('─' * 60)
    log.info(f"Done.  Created: {created}  Skipped: {skipped}  Total: {len(rows)}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Create securities from an Excel file.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'Examples:\n'
            '  python maintenance/create_security.py\n'
            '  python maintenance/create_security.py --dry-run\n'
            '  python maintenance/create_security.py --file my_securities.xlsx --sheet Sheet1\n'
        ),
    )
    parser.add_argument(
        '--file',  default=DEFAULT_FILE,  metavar='FILENAME',
        help=f'Excel filename inside maintenance/Excel/ (default: {DEFAULT_FILE})',
    )
    parser.add_argument(
        '--sheet', default=DEFAULT_SHEET, metavar='SHEET',
        help=f'Sheet name to read (default: {DEFAULT_SHEET})',
    )
    parser.add_argument(
        '--data-source', default=DEFAULT_DATA_SOURCE, metavar='SOURCE',
        help=f'DataSource written to security_info and security_xref (default: {DEFAULT_DATA_SOURCE})',
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Preview what would be created without writing to the database',
    )
    args = parser.parse_args()
    run(args.file, args.sheet, args.data_source, args.dry_run)
