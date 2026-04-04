"""
calculate_var.py — Calculate VaR for all accounts of a given feed_source and store results in position_var.

Main function: calculate_var(feed_source, as_of_date=None)

Steps:
    1. Preprocess positions from proc_positions (column mapping, security info, prices).
    2. For each account: run VaR, re-attach excluded positions (VaR columns = NULL).
    3. Insert results into position_var per account (delete + re-insert).
    Failed accounts are logged and skipped.

Usage:
    python calculate_var.py mssb              # uses latest as_of_date for feed_source=mssb
    python calculate_var.py mssb 2025-09-30   # uses specified as_of_date
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from api import app
from database2 import pg_connection
from engine import VaR_engine as engine
from process2.db_position_var import fetch_latest_as_of_date, insert_results
from process2.preprocess_var import preprocess_var


# ── logging setup ─────────────────────────────────────────────────────────────

def _setup_logger(feed_source: str, as_of_date) -> logging.Logger:
    log_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'log')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir,
        f'calculate_var_{feed_source}_{as_of_date}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    )
    logger = logging.getLogger(f'calculate_var_{feed_source}_{as_of_date}')
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


# ── results builder ────────────────────────────────────────────────────────────

def build_results(positions: pd.DataFrame, DATA: dict) -> pd.DataFrame:
    """
    Join input positions with DATA['Positions'] and DATA['VaR'] on pos_id.
    Only new columns (not already in positions) are brought in from each DataFrame.
    Result is stored in DATA['Results'] and returned.
    """
    result = positions.copy()

    engine_pos = DATA.get('Positions')
    if engine_pos is not None:
        new_cols = ['pos_id'] + [c for c in engine_pos.columns if c not in result.columns]
        result = result.merge(engine_pos[new_cols], on='pos_id', how='left')

    var_df = DATA.get('VaR')
    if var_df is not None:
        new_cols = ['pos_id'] + [c for c in var_df.columns if c not in result.columns]
        result = result.merge(var_df[new_cols], on='pos_id', how='left')

    DATA['Results'] = result
    return result


# ── output utility ─────────────────────────────────────────────────────────────

def write_data_to_excel(DATA: dict, filename: str = 'DATA.xlsx') -> None:
    """Write each element of DATA to a tab in a single Excel workbook in the output subfolder."""
    output_dir = os.path.join(os.path.dirname(__file__), 'output')
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)

    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
        for key, value in DATA.items():
            if isinstance(value, pd.DataFrame):
                index = value.index.name is not None
                value.to_excel(writer, sheet_name=key[:31], index=index)
            elif isinstance(value, dict) and any(isinstance(v, pd.DataFrame) for v in value.values()):
                for sub_key, sub_df in value.items():
                    if isinstance(sub_df, pd.DataFrame):
                        sub_df.to_excel(writer, sheet_name=f'{key}_{sub_key}'[:31], index=False)
            elif isinstance(value, dict):
                pd.DataFrame([value]).to_excel(writer, sheet_name=key[:31], index=False)
            else:
                pd.DataFrame([{key: value}]).to_excel(writer, sheet_name=key[:31], index=False)


# ── Parent account hierarchy ───────────────────────────────────────────────────

def _load_parent_map() -> dict[int, list[int]]:
    """
    Return {parent_account_id: [child_account_id, ...]} for all accounts that have a parent.
    Parent accounts are virtual — they have no proc_positions rows of their own.
    """
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
    """
    Recursively collect all leaf descendants of account_id (up to 5 levels deep).
    A leaf is any account that does not appear as a key in parent_map.
    Since we always resolve back to leaf positions, parent processing order does not matter.
    """
    children = parent_map.get(account_id, [])
    if not children:
        return [account_id]
    leaves: list[int] = []
    for child in children:
        leaves.extend(_get_leaf_descendants(child, parent_map))
    return leaves


# Columns summed across child positions when merging by SecurityID
_SUM_COLS = {'MarketValue', 'Quantity'}
# Columns weighted-averaged by MarketValue
_WAVG_COLS = {'ExpectedReturn'}
# Columns that are cleared for virtual parent accounts
_CLEAR_COLS = {'broker_account', 'exclude_reason'}


def _merge_positions_for_parent(
    all_positions: pd.DataFrame,
    leaf_ids: list[int],
    parent_account_id: int,
) -> pd.DataFrame:
    """
    Build consolidated positions for a parent account by merging all leaf descendant
    positions grouped by SecurityID.

    Aggregation rules:
      MarketValue, Quantity  — sum
      ExpectedReturn         — weighted average by MarketValue
      excluded               — True only if ALL child rows for that SecurityID are excluded;
                               active (False) if active in any child (option a)
      broker_account,
      exclude_reason         — set to None (parent is virtual, no broker account)
      account_id             — set to parent_account_id
      pos_id                 — regenerated as "1", "2", ...
      all other columns      — first value (static fields, same per SecurityID)
    """
    df = all_positions[all_positions['account_id'].isin(leaf_ids)].copy()
    if df.empty:
        return df

    df['MarketValue'] = pd.to_numeric(df['MarketValue'], errors='coerce').fillna(0.0)
    if 'Quantity' in df.columns:
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0.0)
    if 'ExpectedReturn' in df.columns:
        df['ExpectedReturn'] = pd.to_numeric(df['ExpectedReturn'], errors='coerce').fillna(0.0)
        df['_er_mv'] = df['MarketValue'] * df['ExpectedReturn']

    # excluded: True only if every occurrence of that SecurityID is explicitly True
    excluded_agg = (
        df.groupby('SecurityID')['excluded']
        .apply(lambda s: all(x is True for x in s))
        .rename('excluded')
        .reset_index()
    )

    # Build groupby aggregation — skip columns handled separately
    _skip = {'SecurityID', 'account_id', 'pos_id', 'excluded',
             'broker_account', 'exclude_reason', 'ExpectedReturn', '_er_mv'}
    agg_dict: dict[str, str] = {}
    for col in df.columns:
        if col in _skip:
            continue
        agg_dict[col] = 'sum' if col in _SUM_COLS else 'first'
    if '_er_mv' in df.columns:
        agg_dict['_er_mv'] = 'sum'

    merged = df.groupby('SecurityID').agg(agg_dict).reset_index()

    # Resolve weighted-average ExpectedReturn
    if '_er_mv' in merged.columns:
        mv_denom = merged['MarketValue'].replace(0.0, float('nan'))
        merged['ExpectedReturn'] = merged['_er_mv'] / mv_denom
        merged = merged.drop(columns=['_er_mv'])

    # Attach excluded aggregation
    merged = merged.merge(excluded_agg, on='SecurityID', how='left')

    # Regenerate pos_id, set parent identity, clear broker fields
    merged['pos_id']          = [str(i + 1) for i in range(len(merged))]
    merged['account_id']      = parent_account_id
    merged['broker_account']  = None
    merged['exclude_reason']  = None

    return merged.reset_index(drop=True)


# ── main ───────────────────────────────────────────────────────────────────────

def calculate_var(feed_source: str, as_of_date=None):
    """
    Run VaR for every account_id in proc_positions for the given feed_source and as_of_date.
    If as_of_date is not provided, the latest as_of_date for the feed_source is used.
    """
    if as_of_date is None:
        as_of_date = fetch_latest_as_of_date(feed_source)

    logger = _setup_logger(feed_source, as_of_date)
    logger.info(f"=== Start calculating VaR for feed_source={feed_source!r} as_of_date={as_of_date} ===")

    try:
        params, all_positions = preprocess_var(as_of_date, feed_source)
    except Exception as e:
        logger.error(f"preprocess_var failed: {e}")
        raise
    account_ids   = all_positions['account_id'].unique()
    total_inserted = 0

    for account_id in account_ids:
        acc_pos  = all_positions[all_positions['account_id'] == account_id]
        excluded = acc_pos[acc_pos['excluded'] == True]
        active   = acc_pos[acc_pos['excluded'] != True]

        try:
            with app.app_context():
                DATA = engine.calc_VaR(active, params)

            result = build_results(active, DATA)

            if not excluded.empty:
                new_cols     = [c for c in result.columns if c not in excluded.columns]
                excluded_out = excluded.reindex(columns=result.columns)
                for col in new_cols:
                    excluded_out[col] = None
                result = pd.concat([result, excluded_out], ignore_index=True)

            n = insert_results(result, as_of_date)
            total_inserted += n
            logger.info(f"account_id={account_id}: {len(active)} positions calculated, "
                        f"{len(excluded)} excluded, {n} rows inserted")

        except Exception as e:
            logger.error(f"account_id={account_id}: FAILED — {e}")
            continue

    logger.info(f"Leaf accounts: {total_inserted} rows inserted across {len(account_ids)} account(s)")

    # ── Parent account VaR ────────────────────────────────────────────────────
    parent_map = _load_parent_map()
    if not parent_map:
        logger.info("No parent accounts configured — skipping consolidation.")
    else:
        all_parents = sorted(parent_map.keys())
        logger.info(f"Processing {len(all_parents)} parent account(s): {all_parents}")

        for parent_id in all_parents:
            leaf_ids = _get_leaf_descendants(parent_id, parent_map)
            logger.info(f"parent_account_id={parent_id}: leaf_ids={leaf_ids}")

            merged = _merge_positions_for_parent(all_positions, leaf_ids, parent_id)
            if merged.empty:
                logger.warning(
                    f"parent_account_id={parent_id}: no positions found for "
                    f"leaf_ids={leaf_ids} in feed_source={feed_source!r}. Skipping."
                )
                continue

            excluded = merged[merged['excluded'] == True]
            active   = merged[merged['excluded'] != True]

            try:
                with app.app_context():
                    DATA = engine.calc_VaR(active, params)

                result = build_results(active, DATA)

                if not excluded.empty:
                    new_cols     = [c for c in result.columns if c not in excluded.columns]
                    excluded_out = excluded.reindex(columns=result.columns)
                    for col in new_cols:
                        excluded_out[col] = None
                    result = pd.concat([result, excluded_out], ignore_index=True)

                n = insert_results(result, as_of_date)
                total_inserted += n
                logger.info(
                    f"parent_account_id={parent_id}: {len(active)} positions calculated, "
                    f"{len(excluded)} excluded, {n} rows inserted"
                )

            except Exception as e:
                logger.error(f"parent_account_id={parent_id}: FAILED — {e}")
                continue

    logger.info(f"Total: {total_inserted} rows inserted")
    logger.info("=== Done ===")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python calculate_var.py <feed_source> [as_of_date]')
        print('  e.g. python calculate_var.py mssb')
        print('  e.g. python calculate_var.py mssb 2025-09-30')
        sys.exit(1)
    _feed_source  = sys.argv[1]
    _as_of_date   = sys.argv[2] if len(sys.argv) > 2 else None
    calculate_var(_feed_source, _as_of_date)
