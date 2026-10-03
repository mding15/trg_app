"""
run_tracked_account.py — Run the daily pipeline for one tracked account, right away.

Used after a tracked portfolio upload (dashboard/upload_portfolio.py) and by
POST /api/maint/account/<id>/recalc, so the account's dashboard tables don't have
to wait for the nightly scheduler run. Only the account-level jobs run; the
security-level jobs (update_current_security, calc_option_price, the *_pnl jobs,
calc_beta, purge_pnl_stat) are skipped, so a security that has never been priced
gets no VaR / beta / stress P&L until the nightly run fills them in.

Steps:
    1. as_of_date from proc_asof_date (or --date).
    2. Take a Postgres advisory lock for the account, so overlapping runs for the
       same account (e.g. two quick uploads) run one after another.
    3. tracked_proc_positions — reprice the latest tracked upload into proc_positions.
    4. If the account rolls up into parent accounts: populate_parent_positions for
       every parent under the topmost ancestor (siblings' intermediate parents
       included, so grandparents merge completely). Parents are merged from
       whatever child rows exist for as_of_date, so mid-day they can be partial
       until the nightly run.
    5. For the account, then each ancestor (nearest first):
       calculate_var → calc_alternative_var → calc_stress_test → dashboard_process.

Stops at the first failing step (non-zero exit from the CLI). Every step deletes and
re-inserts per (as_of_date, account_id), so this is safe to re-run and doesn't
conflict with the nightly run.

Usage:
    python process2/run_tracked_account.py --account-id 7
    python process2/run_tracked_account.py --account-id 7 --date 2026-10-02
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from database2 import pg_connection, get_proc_asof_date

SCRIPT = Path(__file__).resolve()
REPO_ROOT = SCRIPT.parents[1]
LOCK_CLASS = 7301          # advisory lock namespace: (LOCK_CLASS, account_id)
DEFAULT_TIMEOUT = 30 * 60  # seconds, for launch()


# ── logging setup ──────────────────────────────────────────────────────────────

def _setup_logger(account_id: int, as_of_date) -> logging.Logger:
    log_dir = REPO_ROOT.parent / 'log'
    log_dir.mkdir(exist_ok=True)
    run_ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'run_tracked_account_{account_id}_{as_of_date}_{run_ts}.log'

    logger = logging.getLogger(f'run_tracked_account_{account_id}')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger


# ── account hierarchy ─────────────────────────────────────────────────────────

def _load_parent_of() -> dict[int, int]:
    """Return {account_id: parent_account_id} for all accounts that have a parent."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT account_id, parent_account_id FROM account WHERE parent_account_id IS NOT NULL')
            return {a: p for a, p in cur.fetchall()}


def _ancestors(account_id: int, parent_of: dict[int, int]) -> list[int]:
    """Parent, grandparent, … (nearest first). Stops on a cycle."""
    chain: list[int] = []
    cur = parent_of.get(account_id)
    while cur is not None and cur not in chain and cur != account_id:
        chain.append(cur)
        cur = parent_of.get(cur)
    return chain


def _parents_under(top: int, parent_of: dict[int, int]) -> list[int]:
    """All parent accounts in the subtree rooted at top (including top)."""
    children: dict[int, list[int]] = {}
    for child, parent in parent_of.items():
        children.setdefault(parent, []).append(child)
    out, stack = [], [top]
    while stack:
        n = stack.pop()
        if n in children and n not in out:
            out.append(n)
            stack.extend(children[n])
    return out


# ── main ───────────────────────────────────────────────────────────────────────

def run_tracked_account(account_id: int, as_of_date: str | None = None) -> None:
    """Run the account-level pipeline for account_id. Raises on the first failure."""
    # Deferred imports: these pull in the VaR engine, config, HDF access, etc.
    from process2.tracked_proc_positions import process_tracked_positions
    from process2 import populate_parent_positions
    from process2.calculate_var import calculate_var
    from process2 import calc_alternative_var
    from process2.calc_stress_test import calculate_stress_test
    from dashboard import dashboard_process

    as_of_date = as_of_date or get_proc_asof_date()
    logger = _setup_logger(account_id, as_of_date)
    logger.info(f'=== run_tracked_account: account_id={account_id}  as_of_date={as_of_date} ===')

    with pg_connection() as lock_conn:
        lock_conn.autocommit = True
        with lock_conn.cursor() as cur:
            cur.execute('SELECT pg_try_advisory_lock(%s, %s)', (LOCK_CLASS, account_id))
            if not cur.fetchone()[0]:
                logger.info('Another run for this account is in progress — waiting for it to finish …')
                cur.execute('SELECT pg_advisory_lock(%s, %s)', (LOCK_CLASS, account_id))
        try:
            logger.info('--- Step 1: tracked_proc_positions ---')
            n = process_tracked_positions(as_of_date, account_id)
            if not n:
                raise RuntimeError(f'tracked_proc_positions produced no rows for account_id={account_id} '
                                   f'(no tracked portfolio, or it has no positions)')

            parent_of = _load_parent_of()
            ancestors = _ancestors(account_id, parent_of)
            if ancestors:
                to_merge = _parents_under(ancestors[-1], parent_of)
                logger.info(f'--- Step 2: populate_parent_positions  ancestors={ancestors}  '
                            f're-merging parents={sorted(to_merge)} ---')
                populate_parent_positions.run(as_of_date, to_merge)
            else:
                logger.info('--- Step 2: skipped (no parent accounts) ---')

            for acct in [account_id, *ancestors]:
                logger.info(f'--- Step 3: account_id={acct}: calculate_var ---')
                calculate_var(feed_source=None, as_of_date=as_of_date, account_id=acct)
                logger.info(f'--- Step 3: account_id={acct}: calc_alternative_var ---')
                calc_alternative_var.run(as_of_date, acct)
                logger.info(f'--- Step 3: account_id={acct}: calc_stress_test ---')
                calculate_stress_test(as_of_date, acct)
                logger.info(f'--- Step 3: account_id={acct}: dashboard_process ---')
                dashboard_process.run(as_of_date, acct)
        except Exception:
            logger.exception('FAILED')
            raise
        finally:
            with lock_conn.cursor() as cur:
                cur.execute('SELECT pg_advisory_unlock(%s, %s)', (LOCK_CLASS, account_id))

    logger.info(f'=== Done: account_id={account_id}  as_of_date={as_of_date} ===')


def launch(account_id: int, as_of_date: str | None = None, timeout: int = DEFAULT_TIMEOUT) -> tuple[bool, str]:
    """Run this script in a subprocess (keeps the VaR/HDF work out of the API process).

    Blocks until it finishes; call it from a background thread. Returns (ok, message).
    """
    cmd = [sys.executable, str(SCRIPT), '--account-id', str(account_id)]
    if as_of_date:
        cmd += ['--date', str(as_of_date)]
    try:
        proc = subprocess.run(cmd, cwd=REPO_ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                              text=True, encoding='utf-8', errors='replace', timeout=timeout)
    except subprocess.TimeoutExpired:
        return False, f'account calculation timed out after {timeout // 60} min'
    if proc.returncode == 0:
        return True, 'account calculation completed'
    lines = [l for l in (proc.stderr or '').splitlines() if l.strip()]
    return False, f"account calculation failed: {lines[-1][:500] if lines else f'exit code {proc.returncode}'}"


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Run the account-level daily pipeline for one tracked account.')
    parser.add_argument('--account-id', type=int, required=True, metavar='ACCOUNT_ID')
    parser.add_argument('--date', default=None, metavar='YYYY-MM-DD',
                        help='As-of date (default: read from proc_asof_date table)')
    args = parser.parse_args()

    run_tracked_account(args.account_id, args.date)
