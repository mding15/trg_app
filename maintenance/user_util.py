"""
user_util.py — User management utilities.

Commands:
    reset-password   Overwrite a user's password.
    verify-password  Verify a given password against the stored hash.

Usage:
    python user_util.py reset-password <username> <new_password>
    python user_util.py reset-password <username> <new_password> --dry-run
    python user_util.py verify-password <username> <password>

Options:
    --dry-run   Show user info and what would change without writing to DB.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import bcrypt

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection


# ── logger ────────────────────────────────────────────────────────────────────

_log_dir = Path(__file__).resolve().parents[2] / 'log'
_log_dir.mkdir(exist_ok=True)

logger = logging.getLogger('user_util')
logger.setLevel(logging.INFO)

_formatter = logging.Formatter(
    '%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
for _handler in [
    logging.StreamHandler(sys.stdout),
    logging.FileHandler(_log_dir / 'user_util.log', encoding='utf-8'),
]:
    _handler.setFormatter(_formatter)
    logger.addHandler(_handler)


# ── reset-password ────────────────────────────────────────────────────────────

def reset_password(username: str, new_password: str, dry_run: bool = False) -> None:
    """Overwrite a user's password with a new bcrypt hash."""

    # ── Look up user ──────────────────────────────────────────────────────────
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT user_id, username, email, role FROM "user" WHERE username = %s',
                (username,),
            )
            row = cur.fetchone()

    if not row:
        logger.error(f"User not found: '{username}'")
        sys.exit(1)

    user_id, db_username, email, role = row
    logger.info(f"Found user: username='{db_username}'  email='{email}'  role='{role}'  id={user_id}")

    # ── Dry run ───────────────────────────────────────────────────────────────
    if dry_run:
        logger.info('─' * 60)
        logger.info('DRY RUN — no changes will be written to the database')
        logger.info(f"  Target user : {db_username} (id={user_id})")
        logger.info(f"  Action      : reset password")
        logger.info('─' * 60)
        return

    # ── Confirmation ──────────────────────────────────────────────────────────
    try:
        answer = input(
            f"Reset password for '{db_username}' (email={email}, role={role})? [y/N] "
        ).strip().lower()
    except (KeyboardInterrupt, EOFError):
        print()
        logger.info('Aborted.')
        return

    if answer != 'y':
        logger.info('Aborted.')
        return

    # ── Hash and update ───────────────────────────────────────────────────────
    hashed = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt(rounds=12)).decode('utf-8')

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'UPDATE "user" SET password = %s WHERE user_id = %s',
                (hashed, user_id),
            )
        conn.commit()

    logger.info(f"Password reset successfully for '{db_username}' (id={user_id})")


# ── verify-password ──────────────────────────────────────────────────────────

def verify_password(username: str, password: str) -> None:
    """Verify a given password against the stored bcrypt hash."""

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT user_id, username, email, role, password FROM "user" WHERE username = %s',
                (username,),
            )
            row = cur.fetchone()

    if not row:
        logger.error(f"User not found: '{username}'")
        sys.exit(1)

    user_id, db_username, email, role, stored_hash = row
    logger.info(f"Found user: username='{db_username}'  email='{email}'  role='{role}'  id={user_id}")

    if bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8')):
        logger.info('Password verification: MATCH')
    else:
        logger.warning('Password verification: NO MATCH')


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='User management utilities.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'Examples:\n'
            '  python user_util.py reset-password alice@example.com NewPass123!\n'
            '  python user_util.py reset-password alice@example.com NewPass123! --dry-run\n'
            '  python user_util.py verify-password alice@example.com NewPass123!\n'
        ),
    )
    subparsers = parser.add_subparsers(dest='command')

    rp = subparsers.add_parser(
        'reset-password',
        help='Overwrite a user\'s password',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    rp.add_argument('username', help='Username of the user')
    rp.add_argument('password', help='New password (plain text — will be bcrypt-hashed)')
    rp.add_argument('--dry-run', action='store_true',
                    help='Show user info without writing to the database')

    vp = subparsers.add_parser(
        'verify-password',
        help='Verify a given password against the stored hash',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    vp.add_argument('username', help='Username of the user')
    vp.add_argument('password', help='Password to verify')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == 'reset-password':
        reset_password(args.username, args.password, args.dry_run)
    elif args.command == 'verify-password':
        verify_password(args.username, args.password)


if __name__ == '__main__':
    main()
