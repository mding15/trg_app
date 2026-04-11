# -*- coding: utf-8 -*-
"""
delete_user.py — Delete a user and perform cascading Postgres cleanup.

Usage:
    python maintenance/delete_user.py <username>

Log output is written to both the console and:
    <repo_root>/log/delete_user.log
"""

import sys
import logging
from pathlib import Path

# ensure trg_app root is on the path when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database2 import pg_connection

# ── logger setup (console + file, single formatter) ───────────────────────────

_log_dir = Path(__file__).resolve().parents[2] / 'log'
_log_dir.mkdir(exist_ok=True)

logger = logging.getLogger('delete_user')
logger.setLevel(logging.INFO)

_formatter = logging.Formatter(
    '%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
for _handler in [
    logging.StreamHandler(sys.stdout),
    logging.FileHandler(_log_dir / 'delete_user.log', encoding='utf-8'),
]:
    _handler.setFormatter(_formatter)
    logger.addHandler(_handler)


# ── helpers ────────────────────────────────────────────────────────────────────

def _placeholders(lst):
    """Return a '(%s, %s, ...)' placeholder string for a list."""
    return '(' + ', '.join(['%s'] * len(lst)) + ')'


def _delete(cur, table, column, values):
    """DELETE FROM <table> WHERE <column> IN <values>; logs row count."""
    cur.execute(
        f'DELETE FROM {table} WHERE {column} IN {_placeholders(values)}',
        tuple(values),
    )
    logger.info(f'Deleted {cur.rowcount} row(s) from {table}')


# ── main function ──────────────────────────────────────────────────────────────

def delete_user(username):
    """
    Delete a user and perform cascading cleanup — pure SQL via database2.

    Always (user-level):
      1. Collect all references needed for client cleanup BEFORE deletion.
      2. Remove user from account_access.
      3. NULL out owner_id on any accounts owned by this user (with warning).
      4. Delete the user row.

    If the client has no remaining users after deletion (client-level):
      5. Delete portfolio_info, port_parameters, port_positions, port_limit.
      6. Delete user_entitilement for the client's portfolio groups.
      7. Delete portfolio_group rows.
      8. Delete account_access rows for the client's accounts.
      9. Delete account rows for the client.
      10. Delete the client row.
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:

            # ── gather everything BEFORE any deletion ───────────────���──────────
            cur.execute(
                'SELECT user_id, client_id FROM "user" WHERE username = %s',
                (username,),
            )
            row = cur.fetchone()
            if not row:
                logger.error(f'User not found: {username}')
                return
            user_id, client_id = row

            cur.execute(
                'SELECT client_name FROM client WHERE client_id = %s',
                (client_id,),
            )
            client_name = cur.fetchone()[0]

            cur.execute(
                'SELECT pgroup_id FROM portfolio_group WHERE client_id = %s',
                (client_id,),
            )
            pgroup_id_list = [r[0] for r in cur.fetchall()]

            port_id_list = []
            if pgroup_id_list:
                cur.execute(
                    f'SELECT port_id FROM portfolio_info WHERE port_group_id IN {_placeholders(pgroup_id_list)}',
                    tuple(pgroup_id_list),
                )
                port_id_list = [r[0] for r in cur.fetchall()]

            cur.execute(
                'SELECT COUNT(*) FROM "user" WHERE client_id = %s AND user_id != %s',
                (client_id, user_id),
            )
            remaining_count = cur.fetchone()[0]

            logger.info(
                f'Starting deletion: {username} (id={user_id}), '
                f'client="{client_name}" (id={client_id})'
            )

            # ── Step 1: remove from account_access ────────────────────────────
            cur.execute('DELETE FROM account_access WHERE user_id = %s', (user_id,))
            logger.info(f'Removed {cur.rowcount} account_access row(s) for user {user_id}')

            # ── Step 2: NULL out owner_id on owned accounts (with warning) ────
            cur.execute(
                'SELECT account_id FROM account WHERE owner_id = %s',
                (user_id,),
            )
            owned_account_ids = [r[0] for r in cur.fetchall()]
            if owned_account_ids:
                logger.warning(
                    f'User {username} (id={user_id}) is owner of account(s): '
                    f'{owned_account_ids}. Setting owner_id to NULL.'
                )
                cur.execute(
                    'UPDATE account SET owner_id = NULL WHERE owner_id = %s',
                    (user_id,),
                )

            # ── Step 3: delete the user ────────────────────────────────────────
            cur.execute('DELETE FROM "user" WHERE user_id = %s', (user_id,))
            logger.info(f'Deleted user: {username} (id={user_id})')

            # ── Step 4: check whether client cleanup is needed ─────────────────
            if remaining_count > 0:
                logger.info(
                    f'Client "{client_name}" (id={client_id}) still has '
                    f'{remaining_count} user(s). Skipping client cleanup.'
                )
                conn.commit()
                return

            # ── Step 5–10: client-level cleanup ───────────────────────────────
            logger.info(
                f'No remaining users for client "{client_name}" (id={client_id}). '
                f'Starting client cleanup.'
            )

            # portfolios and related tables
            if port_id_list:
                for table in ('portfolio_info', 'port_parameters', 'port_positions', 'port_limit'):
                    _delete(cur, table, 'port_id', port_id_list)

            # entitlements + portfolio groups
            if pgroup_id_list:
                _delete(cur, 'user_entitilement', 'port_group_id', pgroup_id_list)
                _delete(cur, 'portfolio_group',   'pgroup_id',     pgroup_id_list)

            # account_access + accounts
            cur.execute(
                'SELECT account_id FROM account WHERE client_id = %s',
                (client_id,),
            )
            account_ids = [r[0] for r in cur.fetchall()]
            if account_ids:
                _delete(cur, 'account_access', 'account_id', account_ids)
                _delete(cur, 'account',        'account_id', account_ids)

            # client
            cur.execute('DELETE FROM client WHERE client_id = %s', (client_id,))
            logger.info(f'Deleted client: "{client_name}" (id={client_id})')

        conn.commit()


# ── CLI entry point ────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Delete a TRG user and cascade cleanup.')
    parser.add_argument('username', help='Email / username of the user to delete')
    args = parser.parse_args()

    delete_user(args.username)
