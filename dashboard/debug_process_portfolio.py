"""
debug_process_portfolio.py — Dump intermediate steps of process_portfolio() to Excel.

Edit PORT_ID below, then run:
    python trg_app/dashboard/debug_process_portfolio.py
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from api import app
from dashboard.upload_portfolio import get_portfolio_file_path
from database2 import pg_connection

# ── configure here ─────────────────────────────────────────────────────────────

PORT_ID = 5363

# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)


def _dict_to_df(d: dict) -> pd.DataFrame:
    rows = []
    for k, v in d.items():
        if not isinstance(v, (str, int, float, bool, type(None))):
            v = str(v)
        rows.append((k, v))
    return pd.DataFrame(rows, columns=['Parameter', 'Value'])


def _write_sheet(writer: pd.ExcelWriter, df: pd.DataFrame, name: str) -> None:
    df.to_excel(writer, sheet_name=name, index=False)


def main() -> None:
    # Deferred to avoid circular imports via database.model_aux → api.app
    from preprocess import read_portfolio, scrubbing_portfolio as scrub
    from engine import VaR_engine
    from process2.calculate_var import build_results, add_beta_to_result, fetch_betas_bulk

    # Look up portfolio from DB
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT port_name, filename, client_id FROM portfolio_info WHERE port_id = %s',
                (PORT_ID,),
            )
            row = cur.fetchone()
    if not row:
        raise ValueError(f'portfolio not found: port_id={PORT_ID}')
    port_name, filename, client_id = row
    print(f'Portfolio: port_name={port_name!r}, filename={filename}, client_id={client_id}')

    file_path = get_portfolio_file_path(client_id, filename)
    if not file_path.exists():
        raise FileNotFoundError(f'file not found: {file_path}')

    with app.app_context():
        # ── Step 1: read input file ───────────────────────────────────────────
        params, positions, limit = read_portfolio.read_input_file(file_path)
        step1_positions = positions.copy()
        step1_params    = dict(params)
        step1_limit     = dict(limit)
        print(f'Step 1: {len(step1_positions)} rows, {len(step1_positions.columns)} cols')

        # ── Step 2: scrub ─────────────────────────────────────────────────────
        positions['port_id']    = PORT_ID
        params['port_id']       = PORT_ID
        params['PortfolioName'] = port_name
        limit['port_id']        = PORT_ID

        params_scrubbed, positions_scrubbed, unknown_positions = scrub.scrubbing_portfolio(
            params, positions, limit
        )
        positions_scrubbed['pos_id'] = positions_scrubbed['ID']
        unknown_positions['pos_id']  = unknown_positions['ID']
        print(
            f'Step 2: {len(positions_scrubbed)} active rows, '
            f'{len(unknown_positions)} unknown rows'
        )

        # ── Step 3: VaR engine ────────────────────────────────────────────────
        DATA = VaR_engine.calc_VaR(positions_scrubbed, params_scrubbed)
        if 'Error' in DATA:
            raise RuntimeError(f"VaR engine error: {DATA['Error']}")
        var_positions = DATA.get('Positions')
        var_result    = DATA.get('VaR')
        print(f"Step 3: DATA keys={list(DATA.keys())}")

        # ── Step 4: build_results ─────────────────────────────────────────────
        result = build_results(positions_scrubbed, DATA)
        print(f'Step 4: {len(result)} rows, {len(result.columns)} cols')

        # ── Step 5: re-attach unknown_positions ───────────────────────────────
        if not unknown_positions.empty:
            excluded_out = unknown_positions.reindex(columns=result.columns)
            for col in [c for c in result.columns if c not in unknown_positions.columns]:
                excluded_out[col] = None
            result_with_excluded = pd.concat([result, excluded_out], ignore_index=True)
        else:
            result_with_excluded = result.copy()
        print(f'Step 5: {len(result_with_excluded)} rows (including excluded)')

        # ── Step 6: add beta ──────────────────────────────────────────────────
        sec_ids      = result_with_excluded['SecurityID'].dropna().unique().tolist()
        beta_key     = 'SP500_1Y'
        betas_bulk   = fetch_betas_bulk([beta_key], sec_ids)
        betas        = betas_bulk.get(beta_key, {})
        result_final = add_beta_to_result(result_with_excluded.copy(), betas, logger)
        print(f'Step 6: beta matched {result_final["beta"].notna().sum()}/{len(result_final)} rows')

    # ── Write Excel ───────────────────────────────────────────────────────────
    output_dir = Path(__file__).parent / 'output'
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_name = port_name.replace('/', '_').replace('\\', '_').replace(' ', '_')
    out_path  = output_dir / f'debug_process_portfolio_{PORT_ID}_{safe_name}.xlsx'

    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        _write_sheet(writer, step1_positions,              '1_positions_raw')
        _write_sheet(writer, _dict_to_df(step1_params),   '1_params')
        _write_sheet(writer, _dict_to_df(step1_limit),    '1_limit')
        _write_sheet(writer, positions_scrubbed,           '2_positions_scrubbed')
        _write_sheet(writer, _dict_to_df(params_scrubbed),'2_params_scrubbed')
        _write_sheet(writer, unknown_positions,            '2_unknown_positions')
        if var_positions is not None:
            _write_sheet(writer, var_positions,            '3_var_positions')
        if var_result is not None:
            _write_sheet(writer, var_result,               '3_var_result')
        _write_sheet(writer, result,                       '4_result')
        _write_sheet(writer, result_with_excluded,         '5_result_with_excluded')
        _write_sheet(writer, result_final,                 '6_result_final')

    print(f'\nSaved → {out_path}')


if __name__ == '__main__':
    main()
