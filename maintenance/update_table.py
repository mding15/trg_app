"""
update_table.py — Update rows in a PostgreSQL table from an Excel sheet.

Reads a named sheet from an Excel file in maintenance/Excel/, matches its
columns to the target DB table, and issues UPDATE statements row by row.
Columns listed via --key are used in the WHERE clause; all other matched
columns go into the SET clause.  Rows whose key is not found in the DB are
skipped and logged; the remaining rows continue to be updated.

Usage:
    python update_table.py current_security --key SecurityID
    python update_table.py current_security --key SecurityID --dry-run
    python update_table.py yh_stock_price   --key SecurityID:Date --file prices.xlsx --sheet Sheet1
    python update_table.py security_info                          # key inferred from TABLE_KEYS

Arguments:
    table       Target database table name                (required, positional)

Options:
    --key       Key column(s) for the WHERE clause, colon-separated (optional)
                e.g. --key SecurityID  or  --key SecurityID:Date
                If omitted, the key is looked up in TABLE_KEYS. If the table is
                not listed there, the script exits with an error.
    --file      Excel filename inside maintenance/Excel/  (default: <table>.xlsx)
    --sheet     Sheet name to read                        (default: <table>)
    --dry-run   Preview rows and column mapping without writing to DB
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
import psycopg2.sql as pgsql

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection

EXCEL_DIR = Path(__file__).resolve().parent / "Excel"

# Tables whose key can be omitted from --key; colon-separated composite keys
# are stored as lists.  Add more entries here as needed.
TABLE_KEYS: dict[str, list[str]] = {
    'security_info':      ['SecurityID'],
    'current_security':   ['SecurityID'],
    'mkt_data_source':    ['SecurityID'],
    'bond_info':          ['SecurityID'],
    'security_xref':      ['SecurityID', 'REF_TYPE'],
    'security_attribute': ['security_id'],
    'bond_price':         ['security_id', 'price_date'],
    
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("update_table")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


def _clean(val):
    """Convert pandas NaN / NaT / None → Python None; numpy scalars → Python natives."""
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(val, "item"):  # np.float64, np.int64, etc.
        return val.item()
    return val


def _get_db_columns(table: str) -> list[str]:
    """Return column names for *table* from information_schema, in ordinal order."""
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (table,))
            rows = cur.fetchall()
    if not rows:
        raise ValueError(
            f"Table '{table}' not found in the database (or it has no columns)."
        )
    return [r[0] for r in rows]


def _build_update(table: str, set_cols: list[str], key_cols: list[str]) -> pgsql.Composed:
    """Return a parameterised UPDATE Composed object safe against SQL injection."""
    return pgsql.SQL("UPDATE {table} SET {sets} WHERE {wheres}").format(
        table=pgsql.Identifier(table),
        sets=pgsql.SQL(", ").join(
            pgsql.SQL("{} = {}").format(pgsql.Identifier(c), pgsql.Placeholder())
            for c in set_cols
        ),
        wheres=pgsql.SQL(" AND ").join(
            pgsql.SQL("{} = {}").format(pgsql.Identifier(c), pgsql.Placeholder())
            for c in key_cols
        ),
    )


# ── Core logic ────────────────────────────────────────────────────────────────

def run(file: str | None, sheet: str | None, table: str, key_cols: list[str], dry_run: bool) -> None:
    log = _setup_logger()

    # ── Apply defaults ────────────────────────────────────────────────────────
    file_defaulted = file is None
    if file_defaulted:
        file = f"{table}.xlsx"
    if sheet is None:
        sheet = table

    # ── Load Excel ────────────────────────────────────────────────────────────
    excel_path = EXCEL_DIR / file
    if not excel_path.exists():
        hint = " (defaulted from table name — use --file to specify a different filename)" if file_defaulted else ""
        log.error(f"File not found: {excel_path}{hint}")
        sys.exit(1)

    log.info(f"Reading sheet '{sheet}' from {excel_path}")
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet)
    except ValueError as e:
        log.error(f"Could not read sheet '{sheet}': {e}")
        sys.exit(1)
    except Exception as e:
        log.error(f"Failed to open Excel file: {e}")
        sys.exit(1)

    if df.empty:
        log.warning("Sheet is empty — nothing to update.")
        return

    log.info(f"  {len(df)} rows · {len(df.columns)} columns")

    # ── Resolve key columns ───────────────────────────────────────────────────
    if not key_cols:
        if table in TABLE_KEYS:
            key_cols = TABLE_KEYS[table]
            log.info(f"  --key not specified — using registered key for '{table}': {key_cols}")
        else:
            log.error(
                f"--key is required for table '{table}' (not listed in TABLE_KEYS).\n"
                f"  Specify --key COL or --key COL1:COL2, or add '{table}' to TABLE_KEYS."
            )
            sys.exit(1)

    # ── Fetch DB columns ──────────────────────────────────────────────────────
    log.info(f"Fetching columns for table '{table}' from database …")
    try:
        db_cols = _get_db_columns(table)
    except ValueError as e:
        log.error(str(e))
        sys.exit(1)

    excel_cols = list(df.columns)
    matched    = [c for c in excel_cols if c in db_cols]
    extra_xl   = [c for c in excel_cols if c not in db_cols]

    # ── Validate key columns ──────────────────────────────────────────────────
    missing_keys = [k for k in key_cols if k not in matched]
    if missing_keys:
        log.error(
            f"Key column(s) {missing_keys} not found in both the Excel sheet and the DB table.\n"
            f"  Excel columns : {excel_cols}\n"
            f"  DB columns    : {db_cols}"
        )
        sys.exit(1)

    set_cols = [c for c in matched if c not in key_cols]

    if not set_cols:
        log.error(
            "No columns left to update after excluding key column(s). "
            "The Excel sheet must have at least one non-key column that exists in the DB table."
        )
        sys.exit(1)

    log.info(f"  Key column(s) ({len(key_cols):>2}): {key_cols}")
    log.info(f"  SET column(s) ({len(set_cols):>2}): {set_cols}")
    if extra_xl:
        log.warning(f"  Excel-only — skipped ({len(extra_xl):>2}): {extra_xl}")

    # ── Dry run ───────────────────────────────────────────────────────────────
    if dry_run:
        log.info("─" * 60)
        log.info("DRY RUN — no data will be written to the database")
        log.info(f"  Target table  : {table}")
        log.info(f"  Rows to update: {len(df)}")
        log.info(f"  Key column(s) : {key_cols}")
        log.info(f"  SET column(s) : {set_cols}")
        log.info("  Sample (up to 5 rows):")
        for i, row in df.head(5).iterrows():
            where_vals = {c: _clean(row[c]) for c in key_cols}
            set_vals   = {c: _clean(row[c]) for c in set_cols}
            log.info(f"    [{i}] WHERE {where_vals}  SET {set_vals}")
        log.info("─" * 60)
        return

    # ── Update ────────────────────────────────────────────────────────────────
    stmt    = _build_update(table, set_cols, key_cols)
    updated = 0
    skipped = 0
    errors  = 0

    log.info(f"Updating '{table}' …")
    with pg_connection() as conn:
        with conn.cursor() as cur:
            for i, row in df.iterrows():
                set_values = tuple(_clean(row[c]) for c in set_cols)
                key_values = tuple(_clean(row[c]) for c in key_cols)
                try:
                    cur.execute("SAVEPOINT row_sp")
                    cur.execute(stmt, set_values + key_values)
                    if cur.rowcount == 0:
                        cur.execute("RELEASE SAVEPOINT row_sp")
                        key_display = dict(zip(key_cols, key_values))
                        log.warning(f"  Row {i} skipped — key not found in DB: {key_display}")
                        skipped += 1
                    else:
                        cur.execute("RELEASE SAVEPOINT row_sp")
                        updated += 1
                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT row_sp")
                    log.warning(f"  Row {i} error — {str(e)[:160]}")
                    errors += 1
        conn.commit()

    log.info("─" * 60)
    log.info(f"Done.  Updated: {updated}  Skipped (key not found): {skipped}  Errors: {errors}  Total: {len(df)}")
    if skipped:
        log.warning(f"  {skipped} row(s) were skipped — key not found in the DB.")
    if errors:
        log.warning(f"  {errors} row(s) had errors — check warnings above.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Update rows in a PostgreSQL table from an Excel sheet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python update_table.py current_security --key SecurityID\n"
            "  python update_table.py current_security --key SecurityID --dry-run\n"
            "  python update_table.py yh_stock_price   --key SecurityID:Date\n"
            "  python update_table.py current_security --key SecurityID --file data.xlsx --sheet Sheet1\n"
        ),
    )
    parser.add_argument("table",     metavar="TABLE",
                        help="Target database table name")
    parser.add_argument("--key",     default=None, metavar="COL[:COL...]",
                        help="Key column(s) for the WHERE clause, colon-separated (e.g. SecurityID or SecurityID:Date). If omitted, looked up in TABLE_KEYS; error if not listed.")
    parser.add_argument("--file",    default=None, metavar="FILENAME",
                        help="Excel filename inside maintenance/Excel/ (default: <table>.xlsx)")
    parser.add_argument("--sheet",   default=None, metavar="SHEET",
                        help="Sheet name to read (default: <table>)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview column mapping and sample rows without writing to DB")
    args = parser.parse_args()

    key_cols = [k.strip() for k in args.key.split(":") if k.strip()] if args.key else []

    run(args.file, args.sheet, args.table, key_cols, args.dry_run)


if __name__ == "__main__":
    main()
