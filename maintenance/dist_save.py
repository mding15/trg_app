"""
save_dist.py — Save a return distribution CSV into the VaR HDF store.

Reads a distribution CSV from data/maintenance/CSV/, sets the target model, and
calls var_utils.save_dist() to write it into the model's .h5 file.

Usage:
    python save_dist.py dist.csv
    python save_dist.py dist.csv --model M_20251231
    python save_dist.py dist.csv --model M_20251231 --category PRICE

Arguments:
    file        CSV filename inside data/maintenance/CSV/   (required)

Options:
    --model     Model ID to write to, e.g. M_20251231  (default: default model)
    --category  Distribution category                  (default: PRICE)
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils import var_utils
from _paths import CSV_DIR


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("save_dist")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


def run(file: str, model: str | None, category: str) -> None:
    log = _setup_logger()

    # ── Set model ─────────────────────────────────────────────────────────────
    var_utils.set_model_id(model)

    # ── Load CSV ──────────────────────────────────────────────────────────────
    csv_path = CSV_DIR / file
    if not csv_path.exists():
        log.error(f"File not found: {csv_path}")
        sys.exit(1)

    log.info(f"Reading {csv_path}")
    dist = pd.read_csv(csv_path, index_col=0)
    log.info(f"  {len(dist)} rows · {len(dist.columns)} securities: {dist.columns.tolist()}")

    # ── Save ──────────────────────────────────────────────────────────────────
    log.info(f"Saving distribution (category={category}) ...")
    var_utils.save_dist(dist, category)
    log.info("Done.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Save a return distribution CSV into the VaR HDF store.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python save_dist.py dist.csv\n"
            "  python save_dist.py dist.csv --model M_20251231\n"
            "  python save_dist.py dist.csv --model M_20251231 --category PRICE\n"
        ),
    )
    parser.add_argument("file",       metavar="FILE",
                        help="CSV filename inside data/maintenance/CSV/")
    parser.add_argument("--model",    default=None, metavar="MODEL_ID",
                        help="Model ID to write to (default: default model)")
    parser.add_argument("--category", default="PRICE", metavar="CATEGORY",
                        help="Distribution category (default: PRICE)")
    args = parser.parse_args()

    run(args.file, args.model, args.category)


if __name__ == "__main__":
    main()
