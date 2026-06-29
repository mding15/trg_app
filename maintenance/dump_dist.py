"""
dump_dist.py — Dump security PnL distributions from the VaR HDF store to CSV.

Reads a list of security IDs from a CSV file, fetches their distributions via
var_utils.get_dist(), and writes the result to maintenance/CSV/dist.{category}.csv.

Usage:
    python dump_dist.py
    python dump_dist.py --category IR
    python dump_dist.py --input maintenance/CSV/security_ids.csv --category SPREAD
    python dump_dist.py --dry-run
    python dump_dist.py --list

Options:
    --input     Path to CSV with security IDs (default: maintenance/CSV/security_ids.csv)
    --category  HDF distribution category     (default: PRICE)
    --dry-run   Show shape and coverage without writing the file
    --list      Dump all (Category, SecurityID) entries in the HDF to dist.list.csv
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils import var_utils

CSV_DIR = Path(__file__).resolve().parent / "CSV"
DEFAULT_INPUT = CSV_DIR / "security_ids.csv"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("dump_dist")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


# ── Core logic ────────────────────────────────────────────────────────────────

def run_list() -> None:
    log = _setup_logger()

    log.info("Listing all distributions in HDF …")
    df = var_utils.list_dist()

    log.info(f"  {len(df)} entries found  |  categories: {sorted(df['Category'].unique())}")

    CSV_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CSV_DIR / "dist.list.csv"
    df.to_csv(out_path, index=False)

    log.info("─" * 60)
    log.info(f"Done.  {len(df)} rows written to {out_path}")


def run(input_path: Path, category: str, dry_run: bool) -> None:
    log = _setup_logger()

    # ── Read security IDs ─────────────────────────────────────────────────────
    log.info(f"Reading security IDs from {input_path} …")
    if not input_path.exists():
        log.error(f"Input file not found: {input_path}")
        sys.exit(1)

    id_df = pd.read_csv(input_path)
    security_ids = id_df.iloc[:, 0].dropna().astype(str).tolist()
    log.info(f"  {len(security_ids)} security IDs loaded")

    if not security_ids:
        log.error("Input file contains no security IDs.")
        sys.exit(1)

    # ── Fetch distributions ───────────────────────────────────────────────────
    log.info(f"Fetching distributions from HDF (category={category!r}) …")
    dist = var_utils.get_dist(security_ids, category)

    n_found = len(dist.columns) if not dist.empty else 0
    missing = len(security_ids) - n_found

    if dist.empty:
        log.warning(f"No distributions found for category={category!r}.")
        if not dry_run:
            return
    else:
        log.info(f"  Requested: {len(security_ids)}  |  Returned: {n_found}  |  Missing: {missing}")
        log.info(f"  Shape: {dist.shape[0]} observations × {dist.shape[1]} securities")

    # ── Dry run ───────────────────────────────────────────────────────────────
    if dry_run:
        log.info("─" * 60)
        log.info("DRY RUN — no file will be written")
        log.info(f"  Input file   : {input_path}")
        log.info(f"  Category     : {category}")
        log.info(f"  Requested IDs: {len(security_ids)}")
        if not dist.empty:
            log.info(f"  Found in HDF : {n_found}")
            log.info(f"  Observations : {dist.shape[0]}")
            log.info(f"  Columns      : {list(dist.columns)}")
        log.info("─" * 60)
        return

    # ── Write CSV ─────────────────────────────────────────────────────────────
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CSV_DIR / f"dist.{category}.csv"

    dist.to_csv(out_path, index=True)

    log.info("─" * 60)
    log.info(f"Done.  {dist.shape[0]} rows × {dist.shape[1]} cols written to {out_path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dump security PnL distributions from the VaR HDF store to CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python dump_dist.py\n"
            "  python dump_dist.py --category IR\n"
            "  python dump_dist.py --input maintenance/CSV/my_ids.csv --category SPREAD\n"
            "  python dump_dist.py --dry-run\n"
            "  python dump_dist.py --list\n"
        ),
    )
    parser.add_argument(
        "--input", metavar="PATH", type=Path, default=DEFAULT_INPUT,
        help=f"CSV file with security IDs (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--category", metavar="CATEGORY", default="PRICE",
        help="HDF distribution category (default: PRICE)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show shape and coverage without writing the file",
    )
    parser.add_argument(
        "--list", action="store_true",
        help="Dump all (Category, SecurityID) entries in the HDF to dist.list.csv",
    )
    args = parser.parse_args()

    if args.list:
        run_list()
    else:
        run(args.input, args.category, args.dry_run)


if __name__ == "__main__":
    main()
