"""
run_spy_var.py — Run VaR for a single SPY position and save results to Excel.

Reads live data from the DB. Does NOT write back to the DB.

Usage:
    python run_spy_var.py              # uses latest as_of_date from proc_positions
    python run_spy_var.py 2025-09-30   # uses specified as_of_date
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from api import app
from engine import VaR_engine as engine
from process2.db_position_var import fetch_latest_as_of_date, fetch_proc_positions
from process2.preprocess_var import preprocess_var
from process2.calculate_var import build_results, write_data_to_excel

_TICKER         = 'SPY'
_BROKER_ACCOUNT = '508066168'


def run_spy_var(as_of_date=None):
    if as_of_date is None:
        as_of_date = fetch_latest_as_of_date()
        print(f'as_of_date not provided, using latest: {as_of_date}')

    # ── diagnostic: check raw DB rows before preprocessing ────────────────────
    raw = fetch_proc_positions(as_of_date)
    raw_match = raw[(raw['ticker'] == _TICKER) & (raw['broker_account'] == _BROKER_ACCOUNT)]
    print(f'Raw proc_positions rows matching filter: {len(raw_match)}')
    if not raw_match.empty:
        show_cols = [c for c in ['position_id', 'account_id', 'ticker', 'broker_account', 'quantity', 'market_value'] if c in raw_match.columns]
        print(raw_match[show_cols].to_string(index=False))

    print(f'\n=== Preprocessing positions for as_of_date={as_of_date} ===')
    params, all_positions = preprocess_var(as_of_date)

    mask = (
        (all_positions['Ticker']         == _TICKER) &
        (all_positions['broker_account'] == _BROKER_ACCOUNT)
    )
    positions = all_positions[mask].reset_index(drop=True)

    if positions.empty:
        print(f'No position found for Ticker={_TICKER}, broker_account={_BROKER_ACCOUNT}')
        sys.exit(1)

    print(f'Found {len(positions)} matching position(s)')
    print(positions[['Ticker', 'broker_account', 'Quantity', 'MarketValue']].to_string(index=False))

    # Override AsofDate to match the actual row date
    params['AsofDate'] = params['ReportDate']

    print(f'\n=== Running VaR engine ===')
    with app.app_context():
        DATA = engine.calc_VaR(positions, params)

    build_results(positions, DATA)   # stores result in DATA['Results']

    filename = f'spy_var_{as_of_date}.xlsx'
    write_data_to_excel(DATA, filename=filename)
    output_path = os.path.join(os.path.dirname(__file__), 'output', filename)
    print(f'\nSaved: {output_path}')


if __name__ == '__main__':
    if len(sys.argv) > 1:
        run_spy_var(sys.argv[1])
    else:
        run_spy_var()
