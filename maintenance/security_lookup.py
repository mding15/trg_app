"""
security_lookup.py — Resolve SecurityID from security_xref for positions in an Excel file.

Reads positions from an input sheet, looks up SecurityID via TRG_ID → ISIN → CUSIP → BB_GLOBAL →
Ticker priority, and writes the result (including unresolved rows) to an output sheet in
the same file.  Other sheets in the workbook are preserved.

Usage:
    python security_lookup.py
    python security_lookup.py --file path/to/positions.xlsx
    python security_lookup.py --input-sheet MySheet --output-sheet Results
    python security_lookup.py --dry-run

Options:
    --file          Path to the Excel workbook         (default: Excel/security_lookup.xlsx)
    --input-sheet   Sheet name to read positions from  (default: Positions)
    --output-sheet  Sheet name to write results to     (default: SecurityID)
    --dry-run       Print resolved/unresolved counts without writing the output sheet
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

DEFAULT_XLSX = Path(__file__).resolve().parent / "Excel" / "security_lookup.xlsx"

from database2 import pg_connection
from process2.security_lookup import lookup_security_ids


# ── Helpers ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("security_lookup")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


def _fetch_xref_ids(sec_ids: list[str]) -> pd.DataFrame:
    """Return DB_ISIN and DB_CUSIP from security_xref for the given SecurityIDs.
    Multiple values per SecurityID are joined comma-separated."""
    if not sec_ids:
        return pd.DataFrame(columns=['SecurityID', 'DB_ISIN', 'DB_CUSIP'])
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT "SecurityID", "REF_TYPE", "REF_ID"'
                ' FROM security_xref WHERE "SecurityID" = ANY(%s)'
                ' AND "REF_TYPE" IN (\'ISIN\', \'CUSIP\')',
                (sec_ids,),
            )
            rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(columns=['SecurityID', 'DB_ISIN', 'DB_CUSIP'])
    df = pd.DataFrame(rows, columns=['SecurityID', 'REF_TYPE', 'REF_ID'])
    pivot = (
        df.groupby(['SecurityID', 'REF_TYPE'])['REF_ID']
        .apply(', '.join)
        .unstack(fill_value=None)
        .reset_index()
    )
    pivot = pivot.rename(columns={'ISIN': 'DB_ISIN', 'CUSIP': 'DB_CUSIP'})
    for col in ('DB_ISIN', 'DB_CUSIP'):
        if col not in pivot.columns:
            pivot[col] = None
    return pivot[['SecurityID', 'DB_ISIN', 'DB_CUSIP']]


def _fetch_security_info(sec_ids: list[str]) -> pd.DataFrame:
    """Return SecurityName, AssetClass, AssetType from security_info for the given SecurityIDs."""
    if not sec_ids:
        return pd.DataFrame(columns=['SecurityID', 'SecurityName', 'AssetClass', 'AssetType'])
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT "SecurityID", "SecurityName", "AssetClass", "AssetType"'
                ' FROM security_info WHERE "SecurityID" = ANY(%s)',
                (sec_ids,),
            )
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=['SecurityID', 'SecurityName', 'AssetClass', 'AssetType'])


# ── Core logic ────────────────────────────────────────────────────────────────

def run(xlsx_path: Path, input_sheet: str, output_sheet: str, dry_run: bool) -> None:
    log = _setup_logger()

    if not xlsx_path.exists():
        log.error(f"File not found: {xlsx_path}")
        sys.exit(1)

    log.info(f"Reading '{input_sheet}' from {xlsx_path.name} …")
    try:
        df = pd.read_excel(xlsx_path, sheet_name=input_sheet)
    except Exception as e:
        log.error(f"Could not read sheet '{input_sheet}': {e}")
        sys.exit(1)

    log.info(f"  {len(df)} rows, {len(df.columns)} columns")

    if 'Cusip' in df.columns and 'CUSIP' not in df.columns:
        df = df.rename(columns={'Cusip': 'CUSIP'})
        log.info("  Renamed column 'Cusip' → 'CUSIP'")

    log.info("Looking up SecurityIDs …")
    result = lookup_security_ids(df)

    resolved   = result['SecurityID'].notna().sum()
    unresolved = len(result) - resolved
    log.info(f"  Resolved   : {resolved}")
    log.info(f"  Unresolved : {unresolved}")
    log.info(f"  Total      : {len(result)}")

    sec_ids = result['SecurityID'].dropna().unique().tolist()

    log.info("Fetching SecurityName, AssetClass, AssetType …")
    sec_info = _fetch_security_info(sec_ids)
    result = result.merge(sec_info, on='SecurityID', how='left')

    log.info("Fetching DB_ISIN, DB_CUSIP from security_xref …")
    xref_ids = _fetch_xref_ids(sec_ids)
    result = result.merge(xref_ids, on='SecurityID', how='left')
    log.info("─" * 60)

    if dry_run:
        log.info("DRY RUN — output sheet not written")
        log.info("─" * 60)
        return

    log.info(f"Writing '{output_sheet}' to {xlsx_path.name} …")
    with pd.ExcelWriter(xlsx_path, engine='openpyxl', mode='a',
                        if_sheet_exists='replace') as writer:
        result.to_excel(writer, sheet_name=output_sheet, index=False)

    log.info("─" * 60)
    log.info(f"Done.  {len(result)} rows written to [{output_sheet}]")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resolve SecurityID for positions in an Excel file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python security_lookup.py\n"
            "  python security_lookup.py --file path/to/positions.xlsx\n"
            "  python security_lookup.py --input-sheet MySheet\n"
            "  python security_lookup.py --output-sheet Results\n"
            "  python security_lookup.py --dry-run\n"
        ),
    )
    parser.add_argument("--file", dest="xlsx_path", default=str(DEFAULT_XLSX),
                        metavar="XLSX_PATH",
                        help="Path to the Excel workbook (default: Excel/security_lookup.xlsx)")
    parser.add_argument("--input-sheet", default="Positions", metavar="SHEET",
                        help="Sheet name to read positions from (default: Positions)")
    parser.add_argument("--output-sheet", default="SecurityID", metavar="SHEET",
                        help="Sheet name to write results to (default: SecurityID)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print counts without writing the output sheet")
    args = parser.parse_args()

    run(Path(args.xlsx_path), args.input_sheet, args.output_sheet, args.dry_run)


if __name__ == "__main__":
    main()
