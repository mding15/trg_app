"""
backfill_tracked_client_id.py — One-time backfill of portfolio_info.client_id
for existing 'tracked' portfolios, plus the physical file's location.

Background: tracked portfolios belong to an account, not to whoever uploaded
them. Before this fix, client_id (and the file's save folder) was taken from
the *uploader's* own client_id instead of the account's. This script finds
every tracked portfolio_info row whose client_id doesn't match its account's
true client_id, moves the file from CLIENT_DIR/<old client_id>/ to
CLIENT_DIR/<correct client_id>/ (renaming on collision, same as a fresh
upload would), and updates client_id on the row to match.

Rows with no account_id, or where the file can't be found at either the old
or new location, are left untouched and logged for manual review.

Usage:
    python maintenance/backfill_tracked_client_id.py --dry-run          # preview only
    python maintenance/backfill_tracked_client_id.py                    # run for real, all accounts
    python maintenance/backfill_tracked_client_id.py --account-id 1016  # single account
    python maintenance/backfill_tracked_client_id.py --account-id 1016 --dry-run
"""
from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database2 import pg_connection
from dashboard.upload_portfolio import get_portfolio_file_path

logger = logging.getLogger(__name__)


# ── logging setup ──────────────────────────────────────────────────────────────

def _setup_logger() -> None:
    log_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'log')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir,
        f'backfill_tracked_client_id_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    )
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s  %(levelname)-8s  %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout),
        ],
    )


# ── data fetch ────────────────────────────────────────────────────────────────

def _fetch_mismatched_rows(account_id: int | None) -> list[dict]:
    """Tracked portfolio_info rows whose client_id disagrees with their account's client_id."""
    sql = """
        SELECT pi.port_id, pi.filename, pi.client_id AS old_client_id,
               pi.account_id, a.client_id AS correct_client_id
        FROM portfolio_info pi
        JOIN account a ON a.account_id = pi.account_id
        WHERE pi.port_type = 'tracked'
          AND pi.client_id IS DISTINCT FROM a.client_id
    """
    params: tuple = ()
    if account_id is not None:
        sql += " AND pi.account_id = %s"
        params = (account_id,)
    sql += " ORDER BY pi.port_id"

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [
        {
            'port_id':           r[0],
            'filename':          r[1],
            'old_client_id':     r[2],
            'account_id':        r[3],
            'correct_client_id': r[4],
        }
        for r in rows
    ]


def _fetch_orphaned_tracked_rows() -> list[dict]:
    """Tracked rows with no account_id at all — can't be reconciled by this script."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT port_id, filename, client_id FROM portfolio_info "
                "WHERE port_type = 'tracked' AND account_id IS NULL"
            )
            rows = cur.fetchall()
    return [{'port_id': r[0], 'filename': r[1], 'client_id': r[2]} for r in rows]


# ── file move ─────────────────────────────────────────────────────────────────

def _versioned_dest(dest) -> object:
    """If dest exists, return dest with _v1, _v2, ... inserted before the extension."""
    if not dest.exists():
        return dest
    stem, ext = dest.stem, dest.suffix
    version = 1
    while True:
        candidate = dest.parent / (f'{stem}_v{version}{ext}' if ext else f'{stem}_v{version}')
        if not candidate.exists():
            return candidate
        version += 1


def _move_file(old_client_id: int, correct_client_id: int, filename: str, dry_run: bool):
    """Move the file to the correct client folder. Returns (new_filename, note)."""
    old_path = get_portfolio_file_path(old_client_id, filename)
    new_path = get_portfolio_file_path(correct_client_id, filename)

    if not old_path.exists():
        if new_path.exists():
            return new_path.name, 'file already at correct location'
        return None, 'FILE NOT FOUND at old or new location'

    new_path = _versioned_dest(new_path)
    if dry_run:
        return new_path.name, f'would move {old_path} -> {new_path}'

    new_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(old_path), str(new_path))
    return new_path.name, f'moved {old_path} -> {new_path}'


def _update_client_id(port_id: int, client_id: int, filename: str, dry_run: bool) -> None:
    if dry_run:
        return
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'UPDATE portfolio_info SET client_id = %s, filename = %s WHERE port_id = %s',
                (client_id, filename, port_id),
            )
        conn.commit()


# ── main ──────────────────────────────────────────────────────────────────────

def backfill(account_id: int | None, dry_run: bool) -> None:
    _setup_logger()
    mode = '[DRY RUN] ' if dry_run else ''
    logger.info(f"=== {mode}Backfill tracked portfolio_info.client_id started ===")

    rows = _fetch_mismatched_rows(account_id)
    logger.info(f"Found {len(rows)} tracked row(s) with a client_id/account mismatch")

    moved = 0
    skipped = 0
    for r in rows:
        new_filename, note = _move_file(
            r['old_client_id'], r['correct_client_id'], r['filename'], dry_run
        )
        if new_filename is None:
            logger.warning(
                f"  {mode}port_id={r['port_id']}  account_id={r['account_id']}  "
                f"old_client_id={r['old_client_id']} -> correct_client_id={r['correct_client_id']}  "
                f"filename={r['filename']}: {note} — SKIPPED, left as-is"
            )
            skipped += 1
            continue

        _update_client_id(r['port_id'], r['correct_client_id'], new_filename, dry_run)
        logger.info(
            f"  {mode}port_id={r['port_id']}  account_id={r['account_id']}  "
            f"old_client_id={r['old_client_id']} -> correct_client_id={r['correct_client_id']}  "
            f"{note}"
        )
        moved += 1

    orphans = _fetch_orphaned_tracked_rows() if account_id is None else []
    if orphans:
        logger.warning(f"{len(orphans)} tracked row(s) have no account_id — cannot reconcile, left as-is:")
        for o in orphans:
            logger.warning(f"    port_id={o['port_id']}  filename={o['filename']}  client_id={o['client_id']}")

    logger.info(f"=== {mode}Backfill completed: {moved} fixed, {skipped} skipped ===")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Backfill portfolio_info.client_id (and move the file) for tracked "
                    "portfolios whose client_id doesn't match their account's client_id.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python maintenance/backfill_tracked_client_id.py --dry-run\n"
            "  python maintenance/backfill_tracked_client_id.py\n"
            "  python maintenance/backfill_tracked_client_id.py --account-id 1016 --dry-run\n"
        ),
    )
    parser.add_argument('--account-id', metavar='ACCOUNT_ID', type=int, default=None,
                        help='Process a single account_id; default: all accounts')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview what would be moved/updated without touching files or the database')
    args = parser.parse_args()

    backfill(account_id=args.account_id, dry_run=args.dry_run)
