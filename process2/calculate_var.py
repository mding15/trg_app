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

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from api import app
from engine import VaR_engine as engine
from process2.db_position_var import fetch_latest_as_of_date, insert_results
from process2.preprocess_var import preprocess_var


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


# ── main ───────────────────────────────────────────────────────────────────────

def calculate_var(feed_source: str, as_of_date=None):
    """
    Run VaR for every account_id in proc_positions for the given feed_source and as_of_date.
    If as_of_date is not provided, the latest as_of_date for the feed_source is used.
    """
    if as_of_date is None:
        as_of_date = fetch_latest_as_of_date(feed_source)
        print(f'as_of_date not provided, using latest for feed_source={feed_source!r}: {as_of_date}')

    print(f"=== Calculating VaR for feed_source={feed_source!r} as_of_date={as_of_date} ===")

    params, all_positions = preprocess_var(as_of_date, feed_source)
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
            print(f"account_id={account_id}: {len(active)} positions calculated, "
                  f"{len(excluded)} excluded, {n} rows inserted")

        except Exception as e:
            print(f"account_id={account_id}: FAILED — {e}")
            continue

    print(f"Total: {total_inserted} rows inserted across {len(account_ids)} account(s)")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python calculate_var.py <feed_source> [as_of_date]')
        print('  e.g. python calculate_var.py mssb')
        print('  e.g. python calculate_var.py mssb 2025-09-30')
        sys.exit(1)
    _feed_source  = sys.argv[1]
    _as_of_date   = sys.argv[2] if len(sys.argv) > 2 else None
    calculate_var(_feed_source, _as_of_date)
