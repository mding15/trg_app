"""
calculate_var.py — Calculate VaR for all accounts and store results in position_var.

Main function: calculate_var(feed_source=None, as_of_date=None, account_id=None)

Steps:
    1. Preprocess positions from proc_positions (column mapping, security info, prices).
    2. For each account: run VaR, re-attach excluded positions (VaR columns = NULL).
    3. Insert results into position_var per account (delete + re-insert).
    Failed accounts are logged and skipped.

Usage:
    python calculate_var.py                                          # all feeds, latest date
    python calculate_var.py --feed-source mssb                      # mssb only, latest date
    python calculate_var.py --date 2025-09-30                        # all feeds, specific date
    python calculate_var.py --feed-source mssb --date 2025-09-30    # mssb, specific date
    python calculate_var.py --feed-source mssb --account-id 5       # one leaf account
    python calculate_var.py --feed-source mssb --date 2025-09-30 --account-id 5
    python calculate_var.py --feed-source mssb --date 2025-09-30 --account-id 12
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

def _setup_logger(feed_source: str | None, as_of_date, account_id=None) -> logging.Logger:
    log_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'log')
    os.makedirs(log_dir, exist_ok=True)
    fs_part        = f'{feed_source}_' if feed_source is not None else ''
    account_suffix = f'_account{account_id}' if account_id is not None else ''
    log_file = os.path.join(
        log_dir,
        f'calculate_var_{fs_part}{as_of_date}{account_suffix}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    )
    logger = logging.getLogger(f'calculate_var_{fs_part}{as_of_date}{account_suffix}')
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


# ── Beta helpers ───────────────────────────────────────────────────────────────

def load_account_beta_keys(account_ids: list[int]) -> dict[int, str]:
    """
    Return {account_id: beta_key} for all given account_ids in a single query,
    using the most-recently updated row per account.
    """
    if not account_ids:
        return {}
    query = """
        SELECT DISTINCT ON (account_id) account_id, beta_key
        FROM account_parameters
        WHERE account_id = ANY(%s) AND beta_key IS NOT NULL
        ORDER BY account_id, updated_at DESC
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (list(account_ids),))
            rows = cur.fetchall()
    return {row[0]: row[1] for row in rows}


def fetch_betas_bulk(
    beta_keys: list[str],
    security_ids: list[str],
) -> dict[str, dict[str, float]]:
    """
    Return {beta_key: {security_id: beta}} for the given beta_keys,
    filtered to only the securities present in security_ids.
    A single query replaces repeated per-account fetches.
    """
    if not beta_keys or not security_ids:
        return {}
    query = """
        SELECT beta_key, security_id, beta
        FROM sec_beta
        WHERE beta_key = ANY(%s) AND security_id = ANY(%s)
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (list(beta_keys), list(security_ids)))
            rows = cur.fetchall()
    result: dict[str, dict[str, float]] = {}
    for bk, sid, beta in rows:
        result.setdefault(bk, {})[sid] = beta
    return result


def add_beta_to_result(
    result: pd.DataFrame,
    betas: dict,
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Map betas onto result by SecurityID. Missing securities get NULL.
    Applies to all rows (active and excluded).
    """
    result['beta'] = result['SecurityID'].map(betas) if betas else None
    matched = result['beta'].notna().sum()
    logger.info(f"Beta: {matched}/{len(result)} positions matched (beta_key has {len(betas)} securities)")
    return result


# ── VaR runner helper ─────────────────────────────────────────────────────────

def _run_var(
    label: str,
    active: pd.DataFrame,
    excluded: pd.DataFrame,
    params: dict,
    as_of_date,
    betas: dict,
    logger: logging.Logger,
) -> int:
    """
    Run VaR engine on active positions, re-attach excluded rows with NULL VaR columns,
    add beta for all rows, insert into position_var, and return the number of rows inserted.
    betas — pre-fetched {security_id: beta} for this account (may be empty).
    Raises on engine or DB failure — caller decides whether to skip or abort.
    """
    with app.app_context():
        DATA = engine.calc_VaR(active, params)

    result = build_results(active, DATA)

    if not excluded.empty:
        new_cols     = [c for c in result.columns if c not in excluded.columns]
        excluded_out = excluded.reindex(columns=result.columns)
        for col in new_cols:
            excluded_out[col] = None
        result = pd.concat([result, excluded_out], ignore_index=True)

    # add beta to all rows (active + excluded)
    result = add_beta_to_result(result, betas, logger)

    n = insert_results(result, as_of_date)
    logger.info(f"{label}: {len(active)} positions calculated, {len(excluded)} excluded, {n} rows inserted")
    return n


# ── main ───────────────────────────────────────────────────────────────────────

def calculate_var(feed_source: str | None = None, as_of_date=None, account_id: int | None = None):
    """
    Run VaR for the given feed_source and as_of_date.

    feed_source=None — process all feed sources combined.
    account_id=None  — process all leaf accounts then all parent accounts (full run).
    account_id=<leaf>   — process that single leaf account only.
    account_id=<parent> — fetch leaf descendants' positions, merge, run VaR for parent only.

    If as_of_date is not provided, the latest as_of_date across proc_positions is used.
    """
    if as_of_date is None:
        as_of_date = fetch_latest_as_of_date(feed_source)

    logger = _setup_logger(feed_source, as_of_date, account_id)

    # ── Single account mode ───────────────────────────────────────────────────
    if account_id is not None:
        parent_map = _load_parent_map()

        if account_id in parent_map:
            # ── Parent account ────────────────────────────────────────────────
            leaf_ids = _get_leaf_descendants(account_id, parent_map)
            logger.info(
                f"=== Start VaR: feed_source={feed_source!r} as_of_date={as_of_date} "
                f"parent account_id={account_id} leaf_ids={leaf_ids} ==="
            )
            try:
                params, all_positions = preprocess_var(as_of_date, feed_source, account_ids=leaf_ids)
            except Exception as e:
                logger.error(f"preprocess_var failed: {e}")
                raise

            merged = _merge_positions_for_parent(all_positions, leaf_ids, account_id)
            if merged.empty:
                logger.warning(
                    f"No positions found for parent account_id={account_id} "
                    f"leaf_ids={leaf_ids} — nothing to insert."
                )
                return

            # Pre-fetch beta for this parent account
            sec_ids          = all_positions['SecurityID'].dropna().unique().tolist()
            acct_beta_keys   = load_account_beta_keys([account_id])
            unique_beta_keys = list(set(acct_beta_keys.values()))
            betas_bulk       = fetch_betas_bulk(unique_beta_keys, sec_ids)
            betas            = betas_bulk.get(acct_beta_keys.get(account_id), {})

            excluded = merged[merged['excluded'] == True]
            active   = merged[merged['excluded'] != True]
            try:
                n = _run_var(f"parent_account_id={account_id}", active, excluded, params, as_of_date, betas, logger)
            except Exception as e:
                logger.error(f"parent_account_id={account_id}: FAILED — {e}")
                raise

        else:
            # ── Leaf account ──────────────────────────────────────────────────
            logger.info(
                f"=== Start VaR: feed_source={feed_source!r} as_of_date={as_of_date} "
                f"leaf account_id={account_id} ==="
            )
            try:
                params, all_positions = preprocess_var(as_of_date, feed_source, account_ids=[account_id])
            except Exception as e:
                logger.error(f"preprocess_var failed: {e}")
                raise

            # Pre-fetch beta for this leaf account
            sec_ids          = all_positions['SecurityID'].dropna().unique().tolist()
            acct_beta_keys   = load_account_beta_keys([account_id])
            unique_beta_keys = list(set(acct_beta_keys.values()))
            betas_bulk       = fetch_betas_bulk(unique_beta_keys, sec_ids)
            betas            = betas_bulk.get(acct_beta_keys.get(account_id), {})

            acc_pos  = all_positions[all_positions['account_id'] == account_id]
            excluded = acc_pos[acc_pos['excluded'] == True]
            active   = acc_pos[acc_pos['excluded'] != True]
            try:
                n = _run_var(f"account_id={account_id}", active, excluded, params, as_of_date, betas, logger)
            except Exception as e:
                logger.error(f"account_id={account_id}: FAILED — {e}")
                raise

        logger.info(f"Total: {n} rows inserted")
        logger.info("=== Done ===")
        return

    # ── Full run (no account_id specified) ────────────────────────────────────
    logger.info(f"=== Start VaR: feed_source={feed_source!r} as_of_date={as_of_date} (all accounts) ===")

    try:
        params, all_positions = preprocess_var(as_of_date, feed_source)
    except Exception as e:
        logger.error(f"preprocess_var failed: {e}")
        raise

    leaf_account_ids = all_positions['account_id'].unique()

    # Load parent map early so we can include parent account IDs in the beta pre-fetch
    parent_map  = _load_parent_map()
    all_parents = sorted(parent_map.keys()) if parent_map else []

    # Pre-fetch all betas in one pass: one DB call for account→beta_key, one for betas
    all_acct_ids     = [int(x) for x in leaf_account_ids] + all_parents
    sec_ids          = all_positions['SecurityID'].dropna().unique().tolist()
    acct_beta_keys   = load_account_beta_keys(all_acct_ids)           # {account_id: beta_key}
    unique_beta_keys = list(set(acct_beta_keys.values()))
    betas_bulk       = fetch_betas_bulk(unique_beta_keys, sec_ids)     # {beta_key: {sec_id: beta}}
    logger.info(
        f"Beta pre-fetch: {len(acct_beta_keys)} accounts with beta_key, "
        f"{len(unique_beta_keys)} unique key(s), "
        f"{sum(len(v) for v in betas_bulk.values())} security-beta mappings loaded"
    )

    def _account_betas(acct_id: int) -> dict:
        """Return the pre-fetched {security_id: beta} dict for acct_id."""
        bk = acct_beta_keys.get(acct_id)
        return betas_bulk.get(bk, {}) if bk else {}

    total_inserted = 0

    for acct_id in leaf_account_ids:
        acc_pos  = all_positions[all_positions['account_id'] == acct_id]
        excluded = acc_pos[acc_pos['excluded'] == True]
        active   = acc_pos[acc_pos['excluded'] != True]
        try:
            total_inserted += _run_var(
                f"account_id={acct_id}", active, excluded, params, as_of_date,
                _account_betas(acct_id), logger,
            )
        except Exception as e:
            logger.error(f"account_id={acct_id}: FAILED — {e}")
            continue

    logger.info(f"Leaf accounts: {total_inserted} rows inserted across {len(leaf_account_ids)} account(s)")

    # ── Parent account VaR ────────────────────────────────────────────────────
    if not parent_map:
        logger.info("No parent accounts configured — skipping consolidation.")
    else:
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
                total_inserted += _run_var(
                    f"parent_account_id={parent_id}", active, excluded, params, as_of_date,
                    _account_betas(parent_id), logger,
                )
            except Exception as e:
                logger.error(f"parent_account_id={parent_id}: FAILED — {e}")
                continue

    logger.info(f"Total: {total_inserted} rows inserted")
    logger.info("=== Done ===")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Calculate VaR and store results in position_var.')
    parser.add_argument('--feed-source', default=None, metavar='FEED_SOURCE',
                        help='Feed source to process (e.g. mssb); default: all feed sources')
    parser.add_argument('--date', metavar='YYYY-MM-DD',
                        help='as_of_date to process (default: latest in proc_positions)')
    parser.add_argument('--account-id', metavar='ACCOUNT_ID', type=int,
                        help='Process a single account_id (leaf or parent); default: all accounts')
    args = parser.parse_args()

    calculate_var(args.feed_source, args.date, args.account_id)
