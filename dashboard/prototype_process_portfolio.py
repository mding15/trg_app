"""
prototype_process_portfolio.py — Dump intermediate steps of process_portfolio() to Excel,
using update_security_info() and update_position_price() from preprocess_var() in step 2
instead of scrubbing_portfolio().

Edit PORT_ID below, then run:
    python trg_app/dashboard/prototype_process_portfolio.py
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

PORT_ID = 5365

# ──────────────────────────────────────────────────────────────────────────────

# Maps template (Excel) column names → VaR engine column names expected by
# update_security_info() and update_position_price().
# Columns already matching the engine convention are omitted (SecurityName,
# ISIN, Ticker, Quantity, Currency).
_TEMPLATE_TO_ENGINE = {
    'ID':           'pos_id',
    'Cusip':        'CUSIP',
    'Market Value': 'MarketValue',
    'Asset Class':  'AssetClass',
}

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


def _lookup_security_ids(positions: pd.DataFrame) -> pd.DataFrame:
    """
    Resolve SecurityID from security_xref using ISIN → CUSIP → Ticker priority.
    Positions that cannot be resolved are left with SecurityID = None.
    Mirrors _load_security_cache() in process_mssb_positions.py but read-only
    (no security creation).
    """
    positions = positions.copy()

    isins   = [v for v in positions['ISIN'].dropna().unique()   if v]
    cusips  = [v for v in positions['CUSIP'].dropna().unique()  if v]
    tickers = [v for v in positions['Ticker'].dropna().unique() if v]

    cache: dict[tuple, str] = {}

    with pg_connection() as conn:
        with conn.cursor() as cur:
            if isins:
                cur.execute(
                    'SELECT "REF_ID", "SecurityID" FROM security_xref'
                    ' WHERE "REF_TYPE" = %s AND "REF_ID" = ANY(%s)',
                    ('ISIN', isins),
                )
                for ref_id, sec_id in cur.fetchall():
                    cache[('ISIN', ref_id)] = sec_id

            if cusips:
                cur.execute(
                    'SELECT "REF_ID", "SecurityID" FROM security_xref'
                    ' WHERE "REF_TYPE" = %s AND "REF_ID" = ANY(%s)',
                    ('CUSIP', cusips),
                )
                for ref_id, sec_id in cur.fetchall():
                    cache.setdefault(('CUSIP', ref_id), sec_id)

            if tickers:
                cur.execute(
                    'SELECT "REF_ID", "SecurityID" FROM security_xref'
                    ' WHERE "REF_TYPE" = %s AND "REF_ID" = ANY(%s)',
                    ('Ticker', tickers),
                )
                for ref_id, sec_id in cur.fetchall():
                    cache.setdefault(('Ticker', ref_id), sec_id)

    def _resolve(row) -> str | None:
        isin   = row.get('ISIN')
        cusip  = row.get('CUSIP')
        ticker = row.get('Ticker')
        if isin   and ('ISIN',   isin)   in cache: return cache[('ISIN',   isin)]
        if cusip  and ('CUSIP',  cusip)  in cache: return cache[('CUSIP',  cusip)]
        if ticker and ('Ticker', ticker) in cache: return cache[('Ticker', ticker)]
        return None

    positions['SecurityID'] = positions.apply(_resolve, axis=1)
    return positions


def main() -> None:
    # Deferred to avoid circular imports via database.model_aux → api.app
    from preprocess import read_portfolio
    from engine import VaR_engine
    from process2.update_security_info import update_security_info
    from process2.update_position_price import update_position_price
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

    # ── Step 1: read input file ───────────────────────────────────────────
    params, positions, limit = read_portfolio.read_input_file(file_path)
    step1_positions = positions.copy()
    step1_params    = dict(params)
    step1_limit     = dict(limit)
    print(f'Step 1: {len(step1_positions)} rows, {len(step1_positions.columns)} cols')

    # ── Step 2a: map template columns → engine convention ─────────────────
    positions_mapped = step1_positions.copy().rename(columns=_TEMPLATE_TO_ENGINE)
    # SecurityID, LastPrice, LastPriceDate are absent from the template;
    # update_security_info fills enrichment data from the DB using SecurityID.
    for col in ('SecurityID', 'LastPrice', 'LastPriceDate'):
        if col not in positions_mapped.columns:
            positions_mapped[col] = None
    print(f'Step 2a: {len(positions_mapped)} rows, {len(positions_mapped.columns)} cols')

    asof_date = step1_params.get('AsofDate')

    # ── Step 2b: security lookup (ISIN → CUSIP → Ticker) ─────────────────
    positions_lookup = _lookup_security_ids(positions_mapped)
    resolved = positions_lookup['SecurityID'].notna().sum()
    print(f'Step 2b: {resolved}/{len(positions_lookup)} positions resolved to SecurityID')

    # ── Step 2c: update_security_info ─────────────────────────────────────
    positions_sec = update_security_info(positions_lookup, asof_date=asof_date)
    print(f'Step 2c: {len(positions_sec)} rows, {len(positions_sec.columns)} cols')

    # ── Step 2d: update_position_price ────────────────────────────────────
    positions_priced = update_position_price(positions_sec, asof_date)
    active_positions   = positions_priced[positions_priced['excluded'] != True].reset_index(drop=True)
    excluded_positions = positions_priced[positions_priced['excluded'] == True].reset_index(drop=True)
    print(
        f'Step 2d: {len(positions_priced)} rows — '
        f'{len(active_positions)} active, {len(excluded_positions)} excluded'
    )

    # params for VaR engine: use step1_params + port_id + PortfolioName
    params_for_var = dict(step1_params)
    # params_for_var['port_id']       = PORT_ID
    # params_for_var['PortfolioName'] = port_name

    # ── Step 3: VaR engine (active positions only) ────────────────────────
    DATA = VaR_engine.calc_VaR(active_positions, params_for_var)
    if 'Error' in DATA:
        raise RuntimeError(f"VaR engine error: {DATA['Error']}")
    var_positions = DATA.get('Positions')
    var_result    = DATA.get('VaR')
    print(f"Step 3: DATA keys={list(DATA.keys())}")

    # ── Step 4: build_results ─────────────────────────────────────────────
    result = build_results(active_positions, DATA)
    print(f'Step 4: {len(result)} rows, {len(result.columns)} cols')

    # ── Step 5: re-attach excluded positions ──────────────────────────────
    if not excluded_positions.empty:
        excluded_out = excluded_positions.reindex(columns=result.columns)
        for col in [c for c in result.columns if c not in excluded_positions.columns]:
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
    out_path  = output_dir / f'prototype_process_portfolio_{PORT_ID}_{safe_name}.xlsx'

    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        _write_sheet(writer, step1_positions,              '1_positions_raw')
        _write_sheet(writer, _dict_to_df(step1_params),   '1_params')
        _write_sheet(writer, _dict_to_df(step1_limit),    '1_limit')
        _write_sheet(writer, positions_mapped,             '2a_mapped')
        _write_sheet(writer, positions_lookup,             '2b_security_lookup')
        _write_sheet(writer, positions_sec,                '2c_security_info')
        _write_sheet(writer, positions_priced,             '2d_position_price')
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
