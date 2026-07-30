"""
check_parser_vs_security_info.py — Validate security_name_parser.py against
the security_info table: for every row, compare the parser's derived
security_type to the table's own AssetClass and AssetType, and write a CSV
for review.

security_info has no maturity/coupon/strike/underlying data, so only the
security_type classification can be checked here — not the detailed
extracted fields (those are only tested via process_new_security.py's
output against position_var).

Note on what "match" means: the parser's security_type is 'Bond' when
AssetClass='Bond' (a direct pass-through — always matches AssetClass, never
AssetType, since AssetType for bonds varies: Bond/Treasury/ETF/Fund) and
'Option' when AssetType='Option' OR the name independently looks like an
option (matches AssetType, not AssetClass, since AssetClass for options is
'Derivative'). For every other type, security_type currently just echoes
AssetClass/AssetType back (no independent parser exists yet), so those
rows will trivially "match" — this is a scaffold for future type-specific
parsers, not a meaningful test of parser accuracy for those rows yet.

Usage:
    python maintenance/check_parser_vs_security_info.py
    python maintenance/check_parser_vs_security_info.py --dry-run
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
from _paths import CSV_DIR
from security.security_name_parser import parse_security


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("check_parser_vs_security_info")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


def _fetch_security_info() -> pd.DataFrame:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT "SecurityID", "SecurityName", "AssetClass", "AssetType" '
                'FROM security_info'
            )
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def _apply_parser(df: pd.DataFrame) -> pd.DataFrame:
    def _row(r):
        is_option = (r['AssetType'] == 'Option')
        result = parse_security(r['SecurityName'], r['AssetClass'], r['AssetType'], is_option)
        return pd.Series({
            'parsed_security_type': result['security_type'],
            'parsed_ok':            result['parsed'],
        })

    parsed = df.apply(_row, axis=1)
    df = pd.concat([df, parsed], axis=1)
    df['matches_asset_class'] = df['parsed_security_type'] == df['AssetClass']
    df['matches_asset_type']  = df['parsed_security_type'] == df['AssetType']
    df['match'] = df['matches_asset_class'] | df['matches_asset_type']
    return df


def run(dry_run: bool) -> None:
    log = _setup_logger()

    log.info("Fetching security_info …")
    df = _fetch_security_info()
    log.info(f"  {len(df)} row(s)")

    df = _apply_parser(df)
    n_match = int(df['match'].sum())
    n_mismatch = len(df) - n_match
    log.info("─" * 60)
    log.info(f"Match (security_type == AssetClass or AssetType): {n_match} / {len(df)}")
    log.info(f"Mismatch: {n_mismatch}")
    if n_mismatch:
        log.info(f"Mismatches by (AssetClass, AssetType, parsed_security_type):")
        mism = df[~df['match']]
        log.info(
            mism.groupby(['AssetClass', 'AssetType', 'parsed_security_type'], dropna=False)
                .size().sort_values(ascending=False).to_string()
        )

    if dry_run:
        log.info("─" * 60)
        log.info("DRY RUN — no file will be written")
        return

    CSV_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = CSV_DIR / f"parser_vs_security_info_{timestamp}.csv"
    df.to_csv(out_path, index=False)

    log.info("─" * 60)
    log.info(f"Done.  {len(df)} row(s) written to {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare security_name_parser's security_type to security_info's AssetClass/AssetType.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python maintenance/check_parser_vs_security_info.py\n"
            "  python maintenance/check_parser_vs_security_info.py --dry-run\n"
        ),
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show the match/mismatch summary without writing the file")
    args = parser.parse_args()

    run(args.dry_run)


if __name__ == "__main__":
    main()
