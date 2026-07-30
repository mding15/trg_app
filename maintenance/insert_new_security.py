"""
insert_new_security.py — CLI wrapper (Step 2 of the new-security workflow):
reads a CSV produced (and manually reviewed/edited) by
process_new_security.py and creates security_info/security_xref/option_info
rows in the database for its Option rows.

The actual logic (validation, per-row processing) lives in
security/new_security.py — this file is just the console-logging/argparse/
file-loading shell around it. See that module's docstring for full details.

Usage:
    python maintenance/insert_new_security.py --file new_securities_20260729_150730.csv
    python maintenance/insert_new_security.py --file new_securities_20260729_150730.csv --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection
from security.new_security import validate_and_normalize, process_rows
from _paths import CSV_DIR


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("insert_new_security")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


def _resolve_path(file: str) -> Path:
    p = Path(file)
    if p.exists():
        return p
    return CSV_DIR / file


def run(file: str, dry_run: bool) -> None:
    log = _setup_logger()

    path = _resolve_path(file)
    if not path.exists():
        log.error(f"File not found: {path}")
        sys.exit(1)
    df = pd.read_csv(path)
    try:
        df = validate_and_normalize(df)
    except ValueError as e:
        log.error(str(e))
        sys.exit(1)
    log.info(f"Loaded {len(df)} row(s) from {path}")

    if dry_run:
        log.info("─" * 60)
        log.info("DRY RUN — no data will be written to the database")

    with pg_connection() as conn:
        with conn.cursor() as cur:
            results = process_rows(cur, df, dry_run, log)
        if not dry_run:
            conn.commit()

    counts: dict[str, int] = {}
    for res in results:
        counts[res['status']] = counts.get(res['status'], 0) + 1

    log.info("─" * 60)
    log.info(
        f"Done.  Created: {counts.get('created', 0) + counts.get('would_create', 0)}  "
        f"Skipped (already exists): {counts.get('skipped_exists', 0)}  "
        f"Skipped (no underlying): {counts.get('skipped_no_underlying', 0)}  "
        f"Skipped (not option): {counts.get('skipped_not_option', 0)}  "
        f"Total: {len(df)}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create security_info/security_xref/option_info rows for Option rows in a reviewed process_new_security.py CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python maintenance/insert_new_security.py --file new_securities_20260729_150730.csv\n"
            "  python maintenance/insert_new_security.py --file new_securities_20260729_150730.csv --dry-run\n"
        ),
    )
    parser.add_argument(
        "--file", required=True, metavar="FILENAME",
        help="CSV filename inside data/maintenance/CSV/ (or a full path)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview what would be created without writing to the database",
    )
    args = parser.parse_args()

    run(args.file, args.dry_run)


if __name__ == "__main__":
    main()
