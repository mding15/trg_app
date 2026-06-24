"""
account_limit.py — Initialise account_limit rows for a new account by cloning the
                   default template stored under account_id = 0.

Behaviour
---------
- Reads all rows from account_limit WHERE account_id = 0 (the default template).
- If the target account already has ANY rows in account_limit, the script logs a
  warning and exits without touching the database.
- Otherwise it inserts one row per template row with the new account_id.

Usage:
    python account/account_limit.py --account-id 1012
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection

TEMPLATE_ACCOUNT_ID = 0


# ── Logging ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger('account_limit')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s', '%H:%M:%S')
    )
    logger.addHandler(handler)
    return logger


# ── DB helpers ────────────────────────────────────────────────────────────────

def _fetch_template(cur) -> list[tuple]:
    """Return (limit_category, limit_value) rows for account_id = 0."""
    cur.execute(
        'SELECT limit_category, limit_value FROM account_limit WHERE account_id = %s',
        (TEMPLATE_ACCOUNT_ID,),
    )
    return cur.fetchall()


def _count_existing(cur, account_id: int) -> int:
    cur.execute(
        'SELECT COUNT(*) FROM account_limit WHERE account_id = %s',
        (account_id,),
    )
    return cur.fetchone()[0]


# ── Core logic ────────────────────────────────────────────────────────────────

def initial_limit_value(account_id: int, log: logging.Logger | None = None) -> None:
    if log is None:
        log = logging.getLogger('account_limit')

    with pg_connection() as conn:
        with conn.cursor() as cur:

            # Load template
            template = _fetch_template(cur)
            if not template:
                log.error(
                    f'No rows found in account_limit for template account_id={TEMPLATE_ACCOUNT_ID}. '
                    'Populate the default template first.'
                )
                sys.exit(1)
            log.info(f'Template (account_id={TEMPLATE_ACCOUNT_ID}): {len(template)} row(s)')

            # Check target account
            existing = _count_existing(cur, account_id)
            if existing:
                log.warning(
                    f'account_id={account_id} already has {existing} row(s) in account_limit — skipping.'
                )
                return

            cur.executemany(
                'INSERT INTO account_limit (account_id, limit_category, limit_value) VALUES (%s, %s, %s)',
                [(account_id, cat, val) for cat, val in template],
            )
        conn.commit()

    log.info(f'Inserted {len(template)} row(s) into account_limit for account_id={account_id}')


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Initialise account_limit for a new account by cloning the account_id=0 template.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'Examples:\n'
            '  python account/account_limit.py --account-id 1012\n'
        ),
    )
    parser.add_argument(
        '--account-id', '-a', required=True, type=int, metavar='ACCOUNT_ID',
        help='Target account_id to initialise',
    )
    args = parser.parse_args()

    log = _setup_logger()
    log.info(f'account_limit  account_id={args.account_id}')

    initial_limit_value(args.account_id, log)

    log.info('─' * 60)
    log.info('Done.')


if __name__ == '__main__':
    main()
