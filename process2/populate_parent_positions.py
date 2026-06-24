"""
populate_parent_positions.py — Pre-populate proc_positions rows for parent accounts.

Run this before calculate_var.py. For each parent account, merges positions from
its direct children (which may themselves be merged parents) and writes the result
into proc_positions. calculate_var.py then treats parent accounts identically to
leaf accounts with no special-casing.

Usage:
    python populate_parent_positions.py                                         # all parents, latest date
    python populate_parent_positions.py --date 2025-09-30
    python populate_parent_positions.py --parent-account-ids 1001:1002
    python populate_parent_positions.py --date 2025-09-30 --parent-account-ids 1001:1002
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values

from database2 import pg_connection


# ── logging ───────────────────────────────────────────────────────────────────

def _setup_logger(as_of_date) -> logging.Logger:
    log_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'log')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir,
        f'populate_parent_positions_{as_of_date}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    )
    logger = logging.getLogger(f'populate_parent_positions_{as_of_date}')
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


# ── parent map ────────────────────────────────────────────────────────────────

def _load_parent_map() -> dict[int, list[int]]:
    """Return {parent_account_id: [child_account_id, ...]} for all accounts that have a parent."""
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
    """Recursively return all leaf descendants of account_id."""
    children = parent_map.get(account_id, [])
    if not children:
        return [account_id]
    leaves: list[int] = []
    for child in children:
        leaves.extend(_get_leaf_descendants(child, parent_map))
    return leaves


def _sorted_parents(parent_map: dict[int, list[int]]) -> list[int]:
    """
    Return parent IDs in topological order: children before parents.
    Ensures intermediate parent rows are in all_positions before grandparents are processed.
    """
    order: list[int] = []
    visited: set[int] = set()

    def _visit(pid: int) -> None:
        if pid in visited:
            return
        visited.add(pid)
        for child in parent_map.get(pid, []):
            if child in parent_map:
                _visit(child)
        order.append(pid)

    for pid in sorted(parent_map.keys()):
        _visit(pid)
    return order


# ── DB read ───────────────────────────────────────────────────────────────────

def fetch_leaf_positions(as_of_date, leaf_ids: list[int]) -> pd.DataFrame:
    """
    Read proc_positions rows for the given leaf_ids and as_of_date.
    Only fetches accounts that are actual descendants of a parent — not all leaf accounts.
    feed_source is ignored — positions for all feeds are returned.
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM proc_positions
                WHERE as_of_date = %s
                  AND account_id = ANY(%s)
                """,
                (as_of_date, leaf_ids),
            )
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=cols)
    return df.drop(columns=[c for c in ('id', 'insert_time') if c in df.columns])


# ── merge ─────────────────────────────────────────────────────────────────────

_GROUP_KEYS = ['security_id', 'broker', 'broker_account']
_SUM_COLS   = {'market_value', 'quantity', 'total_cost'}


def _merge_for_parent(
    all_positions: pd.DataFrame,
    child_ids: list[int],
    parent_account_id: int,
) -> pd.DataFrame:
    """
    Merge proc_positions rows for direct children into a single set of rows for the parent.

    child_ids may be leaf account IDs or intermediate parent IDs already merged into
    all_positions — the caller controls the scope.

    Aggregation rules:
      market_value, quantity, total_cost  — sum
      expected_return                     — weighted average by market_value
      excluded                            — True only if ALL child rows are excluded
      exclude_reason                      — cleared (set to None)
      feed_source                         — set to None (spans multiple feeds)
      account_id                          — set to parent_account_id
      position_id                         — regenerated as "1", "2", ...
      all other columns                   — first value
    """
    df = all_positions[all_positions['account_id'].isin(child_ids)].copy()
    if df.empty:
        return df

    df['broker']         = df['broker'].fillna('Unknown')
    df['broker_account'] = df['broker_account'].fillna('Unknown')

    df['market_value'] = pd.to_numeric(df['market_value'], errors='coerce').fillna(0.0)
    if 'quantity' in df.columns:
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0.0)
    if 'total_cost' in df.columns:
        df['total_cost'] = pd.to_numeric(df['total_cost'], errors='coerce').fillna(0.0)
    if 'expected_return' in df.columns:
        df['expected_return'] = pd.to_numeric(df['expected_return'], errors='coerce').fillna(0.0)
        df['_er_mv'] = df['market_value'] * df['expected_return']

    if 'excluded' in df.columns:
        excluded_agg = (
            df.groupby(_GROUP_KEYS)['excluded']
            .apply(lambda s: all(x is True for x in s))
            .rename('excluded')
            .reset_index()
        )
    else:
        excluded_agg = None

    _skip = {*_GROUP_KEYS, 'account_id', 'position_id', 'excluded', 'exclude_reason',
             'expected_return', '_er_mv'}
    agg_dict: dict[str, str] = {}
    for col in df.columns:
        if col in _skip:
            continue
        agg_dict[col] = 'sum' if col in _SUM_COLS else 'first'
    if '_er_mv' in df.columns:
        agg_dict['_er_mv'] = 'sum'

    merged = df.groupby(_GROUP_KEYS).agg(agg_dict).reset_index()

    if '_er_mv' in merged.columns:
        mv_denom = merged['market_value'].replace(0.0, float('nan'))
        merged['expected_return'] = merged['_er_mv'] / mv_denom
        merged = merged.drop(columns=['_er_mv'])

    if excluded_agg is not None:
        merged = merged.merge(excluded_agg, on=_GROUP_KEYS, how='left')
    else:
        merged['excluded'] = False

    merged['position_id']    = [str(i + 1) for i in range(len(merged))]
    merged['account_id']     = parent_account_id
    merged['exclude_reason'] = None
    merged['feed_source']    = None

    return merged.reset_index(drop=True)


# ── DB write ──────────────────────────────────────────────────────────────────

def _delete_parent_rows(parent_ids: list[int], as_of_date, cur) -> None:
    cur.execute(
        'DELETE FROM proc_positions WHERE account_id = ANY(%s) AND as_of_date = %s',
        (parent_ids, as_of_date),
    )


def _insert_rows(df: pd.DataFrame, cur) -> int:
    if df.empty:
        return 0
    df = df.replace({np.nan: None, pd.NaT: None})
    cols    = list(df.columns)
    col_sql = ', '.join(f'"{c}"' for c in cols)
    sql     = f'INSERT INTO proc_positions ({col_sql}) VALUES %s'
    records = [tuple(row) for row in df.itertuples(index=False, name=None)]
    execute_values(cur, sql, records)
    return len(records)


# ── main ──────────────────────────────────────────────────────────────────────

def run(as_of_date=None, parent_account_ids: list[int] | None = None) -> None:
    if as_of_date is None:
        with pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT MAX(as_of_date) FROM proc_positions')
                result = cur.fetchone()[0]
        if result is None:
            raise ValueError('No rows found in proc_positions.')
        as_of_date = result.isoformat()

    logger = _setup_logger(as_of_date)
    logger.info(f'=== populate_parent_positions: as_of_date={as_of_date} ===')

    parent_map = _load_parent_map()
    if not parent_map:
        logger.info('No parent accounts configured — nothing to do.')
        return

    all_parent_ids = list(parent_map.keys())

    if parent_account_ids is not None:
        unknown = [p for p in parent_account_ids if p not in parent_map]
        if unknown:
            logger.warning(f'Requested parent_account_ids not found in account table: {unknown}')
        all_parent_ids = [p for p in parent_account_ids if p in parent_map]
        if not all_parent_ids:
            logger.warning('No valid parent accounts to process — exiting.')
            return
        logger.info(f'Filtering to requested parent accounts: {sorted(all_parent_ids)}')
    else:
        logger.info(f'Parent accounts: {sorted(all_parent_ids)}')

    # Only fetch leaf accounts that feed into a parent — not all leaf accounts
    relevant_leaf_ids = list({
        leaf
        for parent_id in all_parent_ids
        for leaf in _get_leaf_descendants(parent_id, parent_map)
    })
    logger.info(f'Relevant leaf accounts: {sorted(relevant_leaf_ids)}')

    all_positions = fetch_leaf_positions(as_of_date, relevant_leaf_ids)
    if all_positions.empty:
        logger.warning(f'No leaf positions found for as_of_date={as_of_date}. Exiting.')
        return
    table_cols = list(all_positions.columns)  # actual proc_positions columns
    logger.info(f'Loaded {len(all_positions)} leaf rows from proc_positions.')

    merged_frames: list[pd.DataFrame] = []

    for parent_id in _sorted_parents(parent_map):
        if parent_id not in all_parent_ids:
            continue
        child_ids = parent_map[parent_id]
        merged = _merge_for_parent(all_positions, child_ids, parent_id)
        if merged.empty:
            logger.warning(
                f'parent_account_id={parent_id}: no positions found for '
                f'child_ids={child_ids}. Skipping.'
            )
            continue
        # Make merged rows available for any grandparent processed later
        all_positions = pd.concat([all_positions, merged], ignore_index=True)
        merged_frames.append(merged)
        logger.info(
            f'parent_account_id={parent_id}: {len(merged)} rows merged from child_ids={child_ids}'
        )

    if not merged_frames:
        logger.warning('No parent rows produced — nothing to insert.')
        return

    all_merged     = pd.concat(merged_frames, ignore_index=True)
    as_of_date_val = pd.to_datetime(as_of_date).date()
    inserted_ids   = all_merged['account_id'].unique().tolist()

    # Restrict to columns that actually exist in proc_positions
    insert_df = all_merged[[c for c in table_cols if c in all_merged.columns]]

    with pg_connection() as conn:
        with conn.cursor() as cur:
            _delete_parent_rows(inserted_ids, as_of_date_val, cur)
            n = _insert_rows(insert_df, cur)
        conn.commit()

    logger.info(f'Inserted {n} parent rows for account_ids={sorted(inserted_ids)}.')
    logger.info('=== Done ===')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Pre-populate proc_positions rows for parent accounts.'
    )
    parser.add_argument('--date', metavar='YYYY-MM-DD',
                        help='as_of_date to process (default: latest in proc_positions)')
    parser.add_argument('--parent-account-ids', metavar='ID1:ID2:...',
                        help='Colon-separated parent account IDs to process (default: all)')
    args = parser.parse_args()

    parent_ids = None
    if args.parent_account_ids:
        parent_ids = [int(x) for x in args.parent_account_ids.split(':') if x.strip()]

    run(args.date, parent_ids)
