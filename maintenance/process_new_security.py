"""
process_new_security.py — CLI wrapper (Step 1 of the new-security workflow):
finds securities in position_var that aren't fully set up yet and dumps them
to a CSV file for review.

The actual logic (queries, parsing, column setup) lives in
security/new_security.py — this file is just the console-logging/argparse
shell around it. See that module's docstring for full details on what gets
flagged and how the output columns are derived.

Usage:
    python maintenance/process_new_security.py
    python maintenance/process_new_security.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from security.new_security import _fetch_new_securities, write_csv


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("process_new_security")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


def run(dry_run: bool) -> None:
    log = _setup_logger()

    log.info("Querying position_var for missing security_id / var_95 …")
    df = _fetch_new_securities()
    log.info(f"  Found {len(df)} row(s)")
    if not df.empty:
        log.info(f"  By reason:\n{df['reason'].value_counts().to_string()}")

    if dry_run:
        log.info("─" * 60)
        log.info("DRY RUN — no file will be written")
        log.info(f"  Rows: {len(df)}")
        if not df.empty:
            log.info(f"  Preview:\n{df.head(10).to_string(index=False)}")
        log.info("─" * 60)
        return

    if df.empty:
        log.info("No new securities found — CSV will not be written.")
        return

    out_path = write_csv(df)

    log.info("─" * 60)
    log.info(f"Done.  {len(df)} row(s) written to {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find securities in position_var missing security_id or var_95 (excluding cash) and dump them to a CSV file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python maintenance/process_new_security.py\n"
            "  python maintenance/process_new_security.py --dry-run\n"
        ),
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show the row count/preview without writing the file")
    args = parser.parse_args()

    run(args.dry_run)


if __name__ == "__main__":
    main()
