"""
account_parameters.py — Initialise account_parameters for a new account by cloning
                        the default template stored under account_id = 0.

Behaviour
---------
- Reads the row from account_parameters WHERE account_id = 0 (the default template).
- If the target account already has a row in account_parameters, logs a warning and exits.
- Otherwise inserts one row for the new account_id with all template values.

Usage:
    python account/account_parameters.py --account-id 1012
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection

TEMPLATE_ACCOUNT_ID = 0
EXCLUDE_COLS = {'account_id', 'updated_at'}


# ── Logging ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger('account_parameters')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s', '%H:%M:%S')
    )
    logger.addHandler(handler)
    return logger


# ── Core logic ────────────────────────────────────────────────────────────────

def initial_parameters(account_id: int, log: logging.Logger | None = None) -> None:
    if log is None:
        log = logging.getLogger('account_parameters')

    with pg_connection() as conn:
        with conn.cursor() as cur:

            # Load template row
            cur.execute(
                'SELECT * FROM account_parameters WHERE account_id = %s',
                (TEMPLATE_ACCOUNT_ID,),
            )
            row = cur.fetchone()
            if not row:
                log.error(
                    f'No row found in account_parameters for template account_id={TEMPLATE_ACCOUNT_ID}. '
                    'Populate the default template first.'
                )
                sys.exit(1)

            all_cols = [desc[0] for desc in cur.description]
            copy_cols = [c for c in all_cols if c not in EXCLUDE_COLS]
            col_idx   = {c: i for i, c in enumerate(all_cols)}
            values    = [row[col_idx[c]] for c in copy_cols]

            log.info(f'Template (account_id={TEMPLATE_ACCOUNT_ID}): {len(copy_cols)} column(s)')

            # Check target account
            cur.execute(
                'SELECT COUNT(*) FROM account_parameters WHERE account_id = %s',
                (account_id,),
            )
            if cur.fetchone()[0]:
                log.warning(
                    f'account_id={account_id} already has a row in account_parameters — skipping.'
                )
                return

            col_list     = ', '.join(['account_id'] + copy_cols)
            placeholders = ', '.join(['%s'] * (1 + len(copy_cols)))
            cur.execute(
                f'INSERT INTO account_parameters ({col_list}) VALUES ({placeholders})',
                [account_id] + values,
            )
        conn.commit()

    log.info(f'Inserted account_parameters row for account_id={account_id}')


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Initialise account_parameters for a new account by cloning the account_id=0 template.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'Examples:\n'
            '  python account/account_parameters.py --account-id 1012\n'
        ),
    )
    parser.add_argument(
        '--account-id', '-a', required=True, type=int, metavar='ACCOUNT_ID',
        help='Target account_id to initialise',
    )
    args = parser.parse_args()

    log = _setup_logger()
    log.info(f'account_parameters  account_id={args.account_id}')

    initial_parameters(args.account_id, log)

    log.info('─' * 60)
    log.info('Done.')


if __name__ == '__main__':
    main()
