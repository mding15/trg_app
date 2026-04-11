"""
dump_table.py — Dump a PostgreSQL table to a CSV file.

Fetches all columns from the target table and writes them to a timestamped
CSV file in maintenance/CSV/.  Use --limit to cap the number of rows
(default: 1000) or --all to dump the entire table.

Usage:
    python dump_table.py proc_positions
    python dump_table.py proc_positions --all
    python dump_table.py proc_positions --limit 500
    python dump_table.py proc_positions --dry-run

Arguments:
    table       Source database table name  (required, positional)

Options:
    --limit     Maximum rows to fetch       (default: 1000)
    --all       Fetch all rows (overrides --limit)
    --dry-run   Show row count and columns without writing the file
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection

CSV_DIR = Path(__file__).resolve().parent / "CSV"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("dump_table")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


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


def _fetch_rows(table: str, limit: int | None) -> pd.DataFrame:
    """Fetch all columns from *table*, optionally capped at *limit* rows."""
    sql = f"SELECT * FROM {table}"
    if limit is not None:
        sql += f" LIMIT {limit}"
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


# ── Core logic ────────────────────────────────────────────────────────────────

def run(table: str, limit: int | None, dry_run: bool) -> None:
    log = _setup_logger()

    # ── Validate table exists ─────────────────────────────────────────────────
    log.info(f"Fetching columns for table '{table}' from database …")
    try:
        db_cols = _get_db_columns(table)
    except ValueError as e:
        log.error(str(e))
        sys.exit(1)

    log.info(f"  {len(db_cols)} columns: {db_cols}")

    limit_msg = "all rows" if limit is None else f"up to {limit} rows"
    log.info(f"Querying '{table}' ({limit_msg}) …")

    # ── Dry run ───────────────────────────────────────────────────────────────
    if dry_run:
        with pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                total = cur.fetchone()[0]
        preview = total if limit is None else min(total, limit)
        log.info("─" * 60)
        log.info("DRY RUN — no file will be written")
        log.info(f"  Source table  : {table}")
        log.info(f"  Total rows    : {total}")
        log.info(f"  Rows to export: {preview}")
        log.info(f"  Columns ({len(db_cols)}): {db_cols}")
        log.info("─" * 60)
        return

    # ── Fetch ─────────────────────────────────────────────────────────────────
    df = _fetch_rows(table, limit)
    log.info(f"  Fetched {len(df)} rows")

    if df.empty:
        log.warning("Table returned no rows — CSV will not be written.")
        return

    # ── Write CSV ─────────────────────────────────────────────────────────────
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = CSV_DIR / f"{table}_{timestamp}.csv"

    df.to_csv(out_path, index=False)

    log.info("─" * 60)
    log.info(f"Done.  {len(df)} rows written to {out_path}")
    if limit is not None and len(df) == limit:
        log.warning(
            f"Output was capped at {limit} rows. Use --all to dump the full table."
        )


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dump a PostgreSQL table to a CSV file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python dump_table.py proc_positions\n"
            "  python dump_table.py proc_positions --all\n"
            "  python dump_table.py proc_positions --limit 500\n"
            "  python dump_table.py proc_positions --dry-run\n"
        ),
    )
    parser.add_argument("table", metavar="TABLE",
                        help="Source database table name")

    limit_group = parser.add_mutually_exclusive_group()
    limit_group.add_argument("--limit", type=int, default=1000, metavar="N",
                             help="Maximum number of rows to fetch (default: 1000)")
    limit_group.add_argument("--all", action="store_true",
                             help="Fetch all rows (overrides --limit)")

    parser.add_argument("--dry-run", action="store_true",
                        help="Show row count and columns without writing the file")
    args = parser.parse_args()

    limit = None if args.all else args.limit
    run(args.table, limit, args.dry_run)


if __name__ == "__main__":
    main()
