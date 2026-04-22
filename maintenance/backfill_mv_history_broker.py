"""
backfill_mv_history_broker.py — One-time backfill of broker/broker_account in db_mv_history.

Rebuilds db_mv_history from proc_positions_hist, grouping by
(security_id, broker, broker_account) so that position-level return
calculations can key on broker as well as security_id.

For leaf accounts: groups proc_positions_hist by
    (account_id, as_of_date, security_id, broker, broker_account), sums market_value.

For parent accounts: collects all leaf descendant positions for each date,
    fills missing broker/broker_account with 'Unknown', then groups by
    (security_id, broker, broker_account) under the parent account_id.

Usage:
    python maintenance/backfill_mv_history_broker.py                          # all accounts, all dates
    python maintenance/backfill_mv_history_broker.py --account-id 5           # single account (leaf or parent)
    python maintenance/backfill_mv_history_broker.py --date 2025-09-30        # all accounts, single date
    python maintenance/backfill_mv_history_broker.py --account-id 5 --date 2025-09-30
    python maintenance/backfill_mv_history_broker.py --dry-run                # preview without writing
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from database2 import pg_connection

logger = logging.getLogger(__name__)


# ── logging setup ──────────────────────────────────────────────────────────────

def _setup_logger() -> None:
    log_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'log')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir,
        f'backfill_mv_history_broker_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
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


# ── parent account helpers ────────────────────────────────────────────────────

def _load_parent_map() -> dict[int, list[int]]:
    """Return {parent_account_id: [child_account_id, ...]}."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT account_id, parent_account_id FROM account WHERE parent_account_id IS NOT NULL'
            )
            rows = cur.fetchall()
    parent_map: dict[int, list[int]] = {}
    for child_id, parent_id in rows:
        parent_map.setdefault(parent_id, []).append(child_id)
    return parent_map


def _get_leaf_descendants(account_id: int, parent_map: dict[int, list[int]]) -> list[int]:
    """Recursively collect all leaf descendants of account_id."""
    children = parent_map.get(account_id, [])
    if not children:
        return [account_id]
    leaves: list[int] = []
    for child in children:
        leaves.extend(_get_leaf_descendants(child, parent_map))
    return leaves


# ── data fetchers ─────────────────────────────────────────────────────────────

def _fetch_dates_for_account(account_id: int, leaf_ids: list[int]) -> list:
    """
    Return all distinct as_of_dates in proc_positions_hist for the given
    leaf_ids, ordered ascending.
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT as_of_date
                FROM proc_positions_hist
                WHERE account_id = ANY(%s)
                ORDER BY as_of_date
                """,
                (leaf_ids,),
            )
            return [row[0] for row in cur.fetchall()]


def _fetch_all_dates(filter_date=None) -> list:
    """Return all distinct as_of_dates in proc_positions_hist, optionally filtered."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            if filter_date:
                cur.execute(
                    "SELECT DISTINCT as_of_date FROM proc_positions_hist "
                    "WHERE as_of_date = %s ORDER BY as_of_date",
                    (filter_date,),
                )
            else:
                cur.execute(
                    "SELECT DISTINCT as_of_date FROM proc_positions_hist ORDER BY as_of_date"
                )
            return [row[0] for row in cur.fetchall()]


def _fetch_positions(leaf_ids: list[int], as_of_date) -> pd.DataFrame:
    """
    Fetch proc_positions_hist rows for the given leaf_ids and as_of_date.
    Returns a DataFrame with columns: account_id, security_id, broker,
    broker_account, market_value.
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT account_id, security_id,
                       COALESCE(NULLIF(broker, ''), 'Unknown')         AS broker,
                       COALESCE(NULLIF(broker_account, ''), 'Unknown') AS broker_account,
                       COALESCE(market_value, 0.0)                     AS market_value
                FROM proc_positions_hist
                WHERE account_id = ANY(%s) AND as_of_date = %s
                """,
                (leaf_ids, as_of_date),
            )
            rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(columns=['account_id', 'security_id', 'broker', 'broker_account', 'market_value'])
    return pd.DataFrame(rows, columns=['account_id', 'security_id', 'broker', 'broker_account', 'market_value'])


# ── mv_rows builders ──────────────────────────────────────────────────────────

def _build_leaf_mv_rows(df: pd.DataFrame, account_id: int) -> list[dict]:
    """
    Group by (security_id, broker, broker_account) for a single leaf account_id
    and return mv_rows dicts.
    """
    acc_df = df[df['account_id'] == account_id].copy()
    if acc_df.empty:
        return []
    agg = (
        acc_df.groupby(['security_id', 'broker', 'broker_account'], as_index=False)['market_value']
        .sum()
    )
    return [
        {
            'security_id':    row['security_id'],
            'broker':         row['broker'],
            'broker_account': row['broker_account'],
            'market_value':   float(row['market_value']),
        }
        for _, row in agg.iterrows()
        if row['security_id']
    ]


def _build_parent_mv_rows(df: pd.DataFrame) -> list[dict]:
    """
    Merge all leaf positions by (security_id, broker, broker_account) for a
    parent account. broker/broker_account NULLs were already filled with 'Unknown'
    in the SQL fetch, so no additional fillna is needed.
    """
    if df.empty:
        return []
    agg = (
        df.groupby(['security_id', 'broker', 'broker_account'], as_index=False)['market_value']
        .sum()
    )
    return [
        {
            'security_id':    row['security_id'],
            'broker':         row['broker'],
            'broker_account': row['broker_account'],
            'market_value':   float(row['market_value']),
        }
        for _, row in agg.iterrows()
        if row['security_id']
    ]


# ── DB write ──────────────────────────────────────────────────────────────────

def _delete_mv_history(account_id: int, as_of_date, dry_run: bool) -> int:
    if dry_run:
        return 0
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'DELETE FROM db_mv_history WHERE account_id = %s AND as_of_date = %s',
                (account_id, as_of_date),
            )
            count = cur.rowcount
        conn.commit()
    return count


def _write_mv_history(account_id: int, as_of_date, mv_rows: list[dict], dry_run: bool) -> int:
    if dry_run or not mv_rows:
        return len(mv_rows)
    sql = """
        INSERT INTO db_mv_history
            (account_id, as_of_date, security_id, broker, broker_account, market_value)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (account_id, as_of_date, security_id, broker, broker_account)
        DO UPDATE SET market_value = EXCLUDED.market_value
    """
    rows = [
        (account_id, as_of_date, r['security_id'], r['broker'], r['broker_account'], r['market_value'])
        for r in mv_rows
    ]
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
    return len(rows)


# ── main ──────────────────────────────────────────────────────────────────────

def backfill(account_id: int | None = None, filter_date=None, dry_run: bool = False) -> None:
    _setup_logger()
    mode = '[DRY RUN] ' if dry_run else ''
    logger.info(f"=== {mode}Backfill db_mv_history broker started ===")

    parent_map = _load_parent_map()
    all_parent_ids = set(parent_map.keys())

    if account_id is not None:
        # Single account mode
        if account_id in all_parent_ids:
            leaf_ids   = _get_leaf_descendants(account_id, parent_map)
            leaf_accts = []
            parent_accts = [account_id]
        else:
            leaf_ids   = [account_id]
            leaf_accts = [account_id]
            parent_accts = []

        dates = _fetch_dates_for_account(account_id, leaf_ids)
        if filter_date:
            dates = [d for d in dates if str(d) == str(filter_date)]

        logger.info(
            f"Single account mode: account_id={account_id}  "
            f"leaf_ids={leaf_ids}  dates={len(dates)}"
        )

        total_inserted = 0
        for date in dates:
            df = _fetch_positions(leaf_ids, date)

            for lid in leaf_accts:
                mv_rows = _build_leaf_mv_rows(df, lid)
                deleted = _delete_mv_history(lid, date, dry_run)
                inserted = _write_mv_history(lid, date, mv_rows, dry_run)
                logger.info(
                    f"  {mode}account_id={lid}  date={date}  "
                    f"deleted={deleted}  inserted={inserted}"
                )
                total_inserted += inserted

            for pid in parent_accts:
                p_leaf_ids = _get_leaf_descendants(pid, parent_map)
                p_df = df[df['account_id'].isin(p_leaf_ids)]
                mv_rows = _build_parent_mv_rows(p_df)
                deleted = _delete_mv_history(pid, date, dry_run)
                inserted = _write_mv_history(pid, date, mv_rows, dry_run)
                logger.info(
                    f"  {mode}parent_account_id={pid}  date={date}  "
                    f"leaf_ids={p_leaf_ids}  deleted={deleted}  inserted={inserted}"
                )
                total_inserted += inserted

    else:
        # Full run — all accounts, all dates
        dates = _fetch_all_dates(filter_date)
        logger.info(f"Full run: {len(dates)} date(s) to process")

        # Determine all leaf accounts from proc_positions_hist
        with pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT DISTINCT account_id FROM proc_positions_hist')
                all_leaf_ids = [row[0] for row in cur.fetchall()]

        leaf_accts   = [aid for aid in all_leaf_ids if aid not in all_parent_ids]
        parent_accts = sorted(all_parent_ids)

        logger.info(
            f"Leaf accounts: {leaf_accts}  "
            f"Parent accounts: {parent_accts}"
        )

        total_inserted = 0

        for date in dates:
            # Fetch all leaf positions for this date in one query
            df = _fetch_positions(all_leaf_ids, date)

            if df.empty:
                logger.warning(f"  No positions found in proc_positions_hist for date={date} — skipping.")
                continue

            # Leaf accounts
            for lid in leaf_accts:
                mv_rows = _build_leaf_mv_rows(df, lid)
                if not mv_rows:
                    continue
                deleted = _delete_mv_history(lid, date, dry_run)
                inserted = _write_mv_history(lid, date, mv_rows, dry_run)
                logger.info(
                    f"  {mode}account_id={lid}  date={date}  "
                    f"deleted={deleted}  inserted={inserted}"
                )
                total_inserted += inserted

            # Parent accounts
            for pid in parent_accts:
                p_leaf_ids = _get_leaf_descendants(pid, parent_map)
                p_df = df[df['account_id'].isin(p_leaf_ids)]
                if p_df.empty:
                    logger.warning(
                        f"  parent_account_id={pid}  date={date}: "
                        f"no leaf positions found — skipping."
                    )
                    continue
                mv_rows = _build_parent_mv_rows(p_df)
                deleted = _delete_mv_history(pid, date, dry_run)
                inserted = _write_mv_history(pid, date, mv_rows, dry_run)
                logger.info(
                    f"  {mode}parent_account_id={pid}  date={date}  "
                    f"leaf_ids={p_leaf_ids}  deleted={deleted}  inserted={inserted}"
                )
                total_inserted += inserted

        logger.info(f"Total rows inserted: {total_inserted}")

    logger.info(f"=== {mode}Backfill completed ===")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Backfill broker/broker_account in db_mv_history from proc_positions_hist.'
    )
    parser.add_argument('--account-id', metavar='ACCOUNT_ID', type=int, default=None,
                        help='Process a single account_id (leaf or parent); default: all accounts')
    parser.add_argument('--date', metavar='YYYY-MM-DD', default=None,
                        help='Process a single as_of_date; default: all dates in proc_positions_hist')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview what would be written without modifying the database')
    args = parser.parse_args()

    backfill(account_id=args.account_id, filter_date=args.date, dry_run=args.dry_run)
