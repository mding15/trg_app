"""
update_current_security.py — Add missing securities to current_security.

Finds security_ids present in proc_positions (latest as_of_date per account)
that are not in current_security, fetches their attributes from
security_info_view, and inserts them.

Usage:
    python maintenance/update_current_security.py
    python maintenance/update_current_security.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection

# ── SQL ────────────────────────────────────────────────────────────────────────

_SQL_MISSING = """
SELECT DISTINCT p.security_id
FROM proc_positions p
JOIN (
    SELECT account_id, MAX(as_of_date) AS latest_date
    FROM proc_positions
    GROUP BY account_id
) t ON p.account_id = t.account_id
   AND p.as_of_date = t.latest_date
WHERE p.security_id NOT IN (
    SELECT "SecurityID" FROM current_security
)
ORDER BY p.security_id
"""

_SQL_SEC_INFO = """
SELECT "SecurityID", "SecurityName", "Currency", "AssetClass", "AssetType",
       "ISIN", "CUSIP", "BB_UNIQUE", "BB_GLOBAL", "Ticker"
FROM security_info_view
WHERE "SecurityID" = ANY(%s)
"""

_SQL_INSERT = """
INSERT INTO current_security
    ("SecurityID", "SecurityName", "Currency", "AssetClass", "AssetType",
     "ISIN", "CUSIP", "BB_UNIQUE", "BB_GLOBAL", "Ticker", "insert_time")
VALUES
    (%(SecurityID)s, %(SecurityName)s, %(Currency)s, %(AssetClass)s, %(AssetType)s,
     %(ISIN)s, %(CUSIP)s, %(BB_UNIQUE)s, %(BB_GLOBAL)s, %(Ticker)s, NOW())
ON CONFLICT ("SecurityID") DO NOTHING
"""


# ── logging ────────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    log = logging.getLogger('update_current_security')
    log.setLevel(logging.DEBUG)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s', '%H:%M:%S'))
    log.addHandler(h)
    return log


# ── core ───────────────────────────────────────────────────────────────────────

def run(dry_run: bool = False) -> None:
    log = _setup_logger()

    with pg_connection() as conn:
        with conn.cursor() as cur:

            # Step 1: find missing security_ids
            cur.execute(_SQL_MISSING)
            missing_ids = [row[0] for row in cur.fetchall()]

    if not missing_ids:
        log.info('No missing securities — current_security is up to date.')
        return

    log.info(f'Found {len(missing_ids)} security_id(s) missing from current_security:')
    for sid in missing_ids:
        log.info(f'  {sid}')

    with pg_connection() as conn:
        with conn.cursor() as cur:

            # Step 2: fetch from security_info_view
            cur.execute(_SQL_SEC_INFO, (missing_ids,))
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

    found_df = pd.DataFrame(rows, columns=cols)
    found_ids = set(found_df['SecurityID'].tolist())

    not_found = [sid for sid in missing_ids if sid not in found_ids]
    if not_found:
        log.warning(f'{len(not_found)} security_id(s) not found in security_info_view (will be skipped):')
        for sid in not_found:
            log.warning(f'  {sid}')

    if found_df.empty:
        log.warning('Nothing to insert.')
        return

    if dry_run:
        log.info('─' * 60)
        log.info('DRY RUN — no rows will be inserted')
        log.info(f'  Would insert {len(found_df)} row(s) into current_security:')
        for _, row in found_df.iterrows():
            log.info(f'  {row["SecurityID"]:20s}  {str(row["AssetClass"] or ""):15s}  {row["SecurityName"] or ""}')
        log.info('─' * 60)
        return

    # Step 3: insert
    records = found_df.where(pd.notna(found_df), None).to_dict(orient='records')
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(_SQL_INSERT, records)
        conn.commit()

    log.info('─' * 60)
    log.info(f'Inserted {len(records)} row(s) into current_security.')


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Add missing proc_positions securities to current_security.'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Show what would be inserted without writing to the database',
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)
