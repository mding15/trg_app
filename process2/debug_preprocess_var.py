"""
debug_preprocess_var.py — Dump intermediate steps of preprocess_var() to Excel.

Edit the three variables below, then run:
    python process2/debug_preprocess_var.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from process2.db_position_var import fetch_proc_positions
from process2.preprocess_var import _map_columns, build_params
from process2.update_position_price import update_position_price
from process2.update_security_info import update_security_info

# ── configure here ─────────────────────────────────────────────────────────────

AS_OF_DATE  = '2026-04-30'
FEED_SOURCE = 'mssb'
ACCOUNT_ID  = 1003          # single integer

# ──────────────────────────────────────────────────────────────────────────────


def _write_sheet(writer: pd.ExcelWriter, df: pd.DataFrame, name: str) -> None:
    df.to_excel(writer, sheet_name=name, index=False)


def main() -> None:
    account_ids = [ACCOUNT_ID]

    # Step 1
    step1 = fetch_proc_positions(AS_OF_DATE, FEED_SOURCE, account_ids)
    if step1.empty:
        raise ValueError(
            f'No proc_positions rows found for as_of_date={AS_OF_DATE}, '
            f'feed_source={FEED_SOURCE!r}, account_id={ACCOUNT_ID}'
        )
    print(f'Step 1: {len(step1)} rows, {len(step1.columns)} columns')

    # Step 2
    step2 = _map_columns(step1)
    print(f'Step 2: {len(step2)} rows, {len(step2.columns)} columns')

    # Step 3
    step3 = update_security_info(step2, asof_date=AS_OF_DATE)
    print(f'Step 3: {len(step3)} rows, {len(step3.columns)} columns')

    # Step 4
    step4 = update_position_price(step3, AS_OF_DATE)
    print(f'Step 4: {len(step4)} rows, {len(step4.columns)} columns')

    # Step 6 (params)
    params = build_params(AS_OF_DATE)
    params_df = pd.DataFrame(list(params.items()), columns=['Parameter', 'Value'])

    # Write Excel
    output_dir = os.path.join(os.path.dirname(__file__), 'output')
    os.makedirs(output_dir, exist_ok=True)
    filename = f'debug_preprocess_var_{AS_OF_DATE}_{FEED_SOURCE}.xlsx'
    out_path = os.path.join(output_dir, filename)

    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        _write_sheet(writer, step1,    '1_fetch_proc_positions')
        _write_sheet(writer, step2,    '2_map_columns')
        _write_sheet(writer, step3,    '3_update_security_info')
        _write_sheet(writer, step4,    '4_update_position_price')
        _write_sheet(writer, params_df, '6_params')

    print(f'\nSaved → {out_path}')


if __name__ == '__main__':
    main()
