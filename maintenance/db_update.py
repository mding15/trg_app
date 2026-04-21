"""
db_update.py — Update a database table from values in an Excel file.

For each row in the Excel sheet, non-empty values in non-key columns are written
to the database via UPDATE ... WHERE <keys>.  Empty cells (NaN, None, blank string)
are left untouched in the database.

Rules
-----
- Key columns      : specified via --keys (colon-separated); used in WHERE clause;
                     never updated themselves.  All key values must be non-empty —
                     rows with any missing key are silently skipped.
- Update columns   : all non-key Excel columns that match DB columns and are non-empty
                     for that row.  Rows where every non-key cell is empty are silently skipped.
- Key not found    : if the WHERE clause matches 0 DB rows, a warning is logged and
                     processing continues.

Row behaviour
-------------
    Any key cell empty on a row       → silently skipped
    All non-key cells empty on a row  → silently skipped
    Key not found in DB               → warning logged, continues
    Excel column not in DB            → logged as skipped, ignored
    Cell is NaN / None / ""           → not included in SET clause for that row

Usage
-----
    python maintenance/db_update.py security_attribute --keys security_id
    python maintenance/db_update.py security_attribute --keys security_id --dry-run
    python maintenance/db_update.py position_var --keys account_id:as_of_date:pos_id \\
        --file my_updates.xlsx --sheet Sheet1

Arguments
---------
    table       Target database table name  (required, positional)

Options
-------
    --keys      Colon-separated key column(s) for the WHERE clause  (required)
    --file      Excel filename inside maintenance/Excel/             (default: <table>.xlsx)
    --sheet     Sheet name to read                                   (default: <table>)
    --dry-run   Show what would be updated without touching the DB
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
    logger = logging.getLogger("db_update")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


def _is_empty(val) -> bool:
    """Return True if val should be treated as empty (NaN, None, blank string)."""
    try:
        if pd.isna(val):
            return True
    except (TypeError, ValueError):
        pass
    return str(val).strip() == ""


def _clean(val):
    """Convert empty values → None for psycopg2; leave everything else as-is."""
    return None if _is_empty(val) else val


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
    """Return a parameterised UPDATE statement safe against SQL injection."""
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


# ── Core ──────────────────────────────────────────────────────────────────────

def run(
    table: str,
    key_cols: list[str],
    file: str | None,
    sheet: str | None,
    dry_run: bool,
) -> None:
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
        hint = (
            " (defaulted from table name — use --file to specify a different filename)"
            if file_defaulted else ""
        )
        log.error(f"File not found: {excel_path}{hint}")
        sys.exit(1)

    log.info(f"Reading sheet '{sheet}' from {excel_path}")
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet, dtype=str)
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

    # ── Fetch DB columns ──────────────────────────────────────────────────────
    log.info(f"Fetching columns for table '{table}' from database …")
    try:
        db_cols = _get_db_columns(table)
    except ValueError as e:
        log.error(str(e))
        sys.exit(1)

    # ── Validate key columns ──────────────────────────────────────────────────
    missing_keys_xl = [k for k in key_cols if k not in df.columns]
    missing_keys_db = [k for k in key_cols if k not in db_cols]
    if missing_keys_xl:
        log.error(f"Key column(s) not found in Excel sheet: {missing_keys_xl}")
        sys.exit(1)
    if missing_keys_db:
        log.error(f"Key column(s) not found in DB table: {missing_keys_db}")
        sys.exit(1)

    # ── Determine candidate update columns ────────────────────────────────────
    excel_cols   = list(df.columns)
    matched      = [c for c in excel_cols if c in db_cols and c not in key_cols]
    extra_xl     = [c for c in excel_cols if c not in db_cols and c not in key_cols]

    if not matched:
        log.error(
            "No updatable columns in common between the Excel sheet and the DB table "
            f"(after excluding keys).  Excel: {excel_cols}  |  DB: {db_cols}"
        )
        sys.exit(1)

    log.info(f"  Key columns      : {key_cols}")
    log.info(f"  Update candidates: {matched}")
    if extra_xl:
        log.warning(f"  Excel-only — skipped: {extra_xl}")

    # ── Dry run ───────────────────────────────────────────────────────────────
    if dry_run:
        log.info("─" * 60)
        log.info("DRY RUN — no data will be written to the database")
        preview = 0
        for i, row in df.iterrows():
            if any(_is_empty(row[k]) for k in key_cols):
                continue
            set_cols = [c for c in matched if not _is_empty(row[c])]
            if not set_cols:
                continue
            key_vals = {k: row[k] for k in key_cols}
            set_vals = {c: row[c] for c in set_cols}
            log.info(f"  Row {i}: WHERE {key_vals} → SET {set_vals}")
            preview += 1
        log.info(f"  {preview} row(s) would be updated")
        log.info("─" * 60)
        return

    # ── Update ────────────────────────────────────────────────────────────────
    updated       = 0
    skipped_keys  = 0   # missing key values
    skipped_empty = 0   # no non-empty update values
    not_found     = 0   # WHERE matched 0 rows

    log.info(f"Updating '{table}' …")
    with pg_connection() as conn:
        with conn.cursor() as cur:
            for i, row in df.iterrows():

                # Skip rows with missing key values
                if any(_is_empty(row[k]) for k in key_cols):
                    skipped_keys += 1
                    continue

                # Find non-empty update columns for this row
                set_cols = [c for c in matched if not _is_empty(row[c])]
                if not set_cols:
                    skipped_empty += 1
                    continue

                stmt      = _build_update(table, set_cols, key_cols)
                set_vals  = tuple(_clean(row[c]) for c in set_cols)
                key_vals  = tuple(_clean(row[k]) for k in key_cols)

                try:
                    cur.execute("SAVEPOINT row_sp")
                    cur.execute(stmt, set_vals + key_vals)
                    if cur.rowcount == 0:
                        key_disp = dict(zip(key_cols, key_vals))
                        log.warning(f"  Row {i}: key not found in DB — {key_disp}")
                        not_found += 1
                    else:
                        updated += 1
                    cur.execute("RELEASE SAVEPOINT row_sp")
                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT row_sp")
                    log.warning(f"  Row {i} error — {str(e)[:160]}")

        conn.commit()

    log.info("─" * 60)
    log.info(f"Done.  Updated: {updated}  Not found: {not_found}  "
             f"Skipped (no keys): {skipped_keys}  Skipped (all empty): {skipped_empty}")
    if not_found:
        log.warning(f"  {not_found} row(s) had no matching DB record — check warnings above.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Update a PostgreSQL table from values in an Excel file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python db_update.py security_attribute --keys security_id\n"
            "  python db_update.py security_attribute --keys security_id --dry-run\n"
            "  python db_update.py position_var --keys account_id:as_of_date:pos_id\n"
            "      --file my_updates.xlsx --sheet Sheet1\n"
        ),
    )
    parser.add_argument("table",     metavar="TABLE",
                        help="Target database table name")
    parser.add_argument("--keys",    required=True, metavar="KEY1:KEY2:...",
                        help="Colon-separated key column(s) used in the WHERE clause")
    parser.add_argument("--file",    default=None, metavar="FILENAME",
                        help="Excel filename inside maintenance/Excel/ (default: <table>.xlsx)")
    parser.add_argument("--sheet",   default=None, metavar="SHEET",
                        help="Sheet name to read (default: <table>)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be updated without touching the DB")
    args = parser.parse_args()

    key_cols = [k.strip() for k in args.keys.split(":") if k.strip()]
    if not key_cols:
        parser.error("--keys must contain at least one column name")

    run(
        table    = args.table,
        key_cols = key_cols,
        file     = args.file,
        sheet    = args.sheet,
        dry_run  = args.dry_run,
    )


if __name__ == "__main__":
    main()
