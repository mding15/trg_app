"""
upload_excel.py — Upload an Excel sheet to a PostgreSQL table.

Reads a named sheet from an Excel file in maintenance/Excel/, matches its
columns to the target DB table, and inserts rows one by one.  Bad rows are
skipped and logged; the remaining rows continue to be inserted.

Usage:
    python upload_excel.py benchmark
    python upload_excel.py benchmark --file benchmark.xlsx --sheet benchmark
    python upload_excel.py benchmark --dry-run

Arguments:
    table       Target database table name                (required, positional)

Options:
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


# ── Helpers ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("upload_excel")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


def _clean(val):
    """Convert pandas NaN / NaT / None → Python None for psycopg2."""
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
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


def _build_insert(table: str, columns: list[str]) -> pgsql.Composed:
    """Return a parameterised INSERT Composed object safe against SQL injection."""
    return pgsql.SQL("INSERT INTO {table} ({cols}) VALUES ({vals})").format(
        table=pgsql.Identifier(table),
        cols=pgsql.SQL(", ").join(map(pgsql.Identifier, columns)),
        vals=pgsql.SQL(", ").join([pgsql.Placeholder()] * len(columns)),
    )


# ── Core logic ────────────────────────────────────────────────────────────────

def run(file: str | None, sheet: str | None, table: str, dry_run: bool) -> None:
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
        # sheet not found
        log.error(f"Could not read sheet '{sheet}': {e}")
        sys.exit(1)
    except Exception as e:
        log.error(f"Failed to open Excel file: {e}")
        sys.exit(1)

    if df.empty:
        log.warning("Sheet is empty — nothing to upload.")
        return

    log.info(f"  {len(df)} rows · {len(df.columns)} columns")

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
    missing_db = [c for c in db_cols   if c not in excel_cols]

    if not matched:
        log.error(
            "No columns in common between the Excel sheet and the DB table. "
            f"Excel: {excel_cols}  |  DB: {db_cols}"
        )
        sys.exit(1)

    log.info(f"  Matched  ({len(matched):>2}): {matched}")
    if extra_xl:
        log.warning(f"  Excel-only — skipped ({len(extra_xl):>2}): {extra_xl}")
    if missing_db:
        log.info(f"  DB-only — will be NULL/default ({len(missing_db):>2}): {missing_db}")

    # ── Dry run ───────────────────────────────────────────────────────────────
    if dry_run:
        log.info("─" * 60)
        log.info("DRY RUN — no data will be written to the database")
        log.info(f"  Target table  : {table}")
        log.info(f"  Rows to insert: {len(df)}")
        log.info(f"  Columns       : {matched}")
        log.info("  Sample (up to 5 rows):")
        for i, row in df[matched].head(5).iterrows():
            values = {c: _clean(row[c]) for c in matched}
            log.info(f"    [{i}] {values}")
        log.info("─" * 60)
        return

    # ── Insert ────────────────────────────────────────────────────────────────
    stmt     = _build_insert(table, matched)
    inserted = 0
    skipped  = 0

    log.info(f"Inserting into '{table}' …")
    with pg_connection() as conn:
        with conn.cursor() as cur:
            for i, row in df.iterrows():
                values = tuple(_clean(row[c]) for c in matched)
                try:
                    cur.execute("SAVEPOINT row_sp")
                    cur.execute(stmt, values)
                    cur.execute("RELEASE SAVEPOINT row_sp")
                    inserted += 1
                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT row_sp")
                    log.warning(f"  Row {i} skipped — {str(e)[:160]}")
                    skipped += 1
        conn.commit()

    log.info("─" * 60)
    log.info(f"Done.  Inserted: {inserted}  Skipped: {skipped}  Total: {len(df)}")
    if skipped:
        log.warning(f"  {skipped} row(s) were skipped — check warnings above.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload an Excel sheet to a PostgreSQL table.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python upload_excel.py benchmark\n"
            "  python upload_excel.py benchmark --dry-run\n"
            "  python upload_excel.py benchmark --file data.xlsx --sheet Sheet1\n"
        ),
    )
    parser.add_argument("table",     metavar="TABLE",
                        help="Target database table name")
    parser.add_argument("--file",    default=None, metavar="FILENAME",
                        help="Excel filename inside maintenance/Excel/ (default: <table>.xlsx)")
    parser.add_argument("--sheet",   default=None, metavar="SHEET",
                        help="Sheet name to read (default: <table>)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview column mapping and sample rows without writing to DB")
    args = parser.parse_args()

    run(args.file, args.sheet, args.table, args.dry_run)


if __name__ == "__main__":
    main()
