"""
run_equity_model.py — Equity model maintenance tasks.

Task 1 (write-prices): Read historical adjusted-close prices from a CSV file and
write them into the 'Prices' tab of Excel/EquityModel.xlsx. The CSV uses Ticker as
the security identifier; the mapping to SecurityID is derived from the 'Securities'
tab of the workbook.  The Prices tab is fully replaced on each run.

Task 2 (run-model): Read Securities and Prices tabs from EquityModel.xlsx and call
models.equity_model.run_model() to run the full equity model pipeline.

Usage:
    python run_equity_model.py write-prices --file CSV/hist_price_20260513_165602.csv
    python run_equity_model.py run-model
    python run_equity_model.py run-model --model-id M_20251231
    python run_equity_model.py run-model --model-id M_20251231 --submodel-id Equity.5
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import openpyxl

SCRIPT_DIR    = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))  # trg_app/ — required for models.equity_model imports
WORKBOOK_PATH = SCRIPT_DIR / 'Excel' / 'EquityModel.xlsx'


# ── logging ────────────────────────────────────────────────────────────────────

def _setup_logger(task: str) -> logging.Logger:
    log_dir = SCRIPT_DIR.parent.parent / 'log'
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f'run_equity_model_{task}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

    logger = logging.getLogger(f'run_equity_model_{task}')
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


# ── helpers ────────────────────────────────────────────────────────────────────

def _read_securities(wb: openpyxl.Workbook) -> list[dict]:
    """Read the Securities tab. Returns list of {SecurityID, Ticker, ...} dicts in tab order."""
    ws = wb['Securities']
    headers = [ws.cell(1, c).value for c in range(1, ws.max_column + 1)]
    rows = []
    for r in range(2, ws.max_row + 1):
        row = {headers[c - 1]: ws.cell(r, c).value for c in range(1, ws.max_column + 1)}
        if row.get('SecurityID'):
            rows.append(row)
    return rows


# ── task: write-prices ─────────────────────────────────────────────────────────

def write_prices(csv_path: Path, logger: logging.Logger) -> None:
    """
    Read adjclose prices from csv_path and replace the Prices tab in EquityModel.xlsx.

    Columns in the Prices tab: Date, then one column per SecurityID in Securities tab order.
    Rows: one per distinct date in the CSV, sorted ascending.
    """
    logger.info(f"=== write-prices started: csv='{csv_path}' workbook='{WORKBOOK_PATH}' ===")

    # ── Read workbook ──────────────────────────────────────────────────────────
    if not WORKBOOK_PATH.exists():
        raise FileNotFoundError(f"Workbook not found: {WORKBOOK_PATH}")
    wb = openpyxl.load_workbook(WORKBOOK_PATH)

    if 'Securities' not in wb.sheetnames:
        raise ValueError("'Securities' tab not found in workbook")
    securities = _read_securities(wb)
    ticker_to_sec_id = {s['Ticker']: s['SecurityID'] for s in securities if s.get('Ticker')}
    sec_ids = [s['SecurityID'] for s in securities]  # ordered
    logger.info(f"Read {len(securities)} securities from 'Securities' tab")

    # ── Read CSV ───────────────────────────────────────────────────────────────
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    df = pd.read_csv(csv_path, usecols=['date', 'adjclose', 'ticker'], parse_dates=['date'])
    logger.info(f"Read {len(df)} rows from CSV  ({df['ticker'].nunique()} tickers, "
                f"date range {df['date'].min().date()} → {df['date'].max().date()})")

    # Warn about any tickers in CSV not found in Securities tab
    csv_tickers = set(df['ticker'].unique())
    unmapped = csv_tickers - set(ticker_to_sec_id)
    if unmapped:
        logger.warning(f"Tickers in CSV with no Securities mapping (will be skipped): {sorted(unmapped)}")

    # ── Pivot: rows=date, cols=SecurityID ─────────────────────────────────────
    df['security_id'] = df['ticker'].map(ticker_to_sec_id)
    df = df.dropna(subset=['security_id'])
    prices = (
        df.pivot(index='date', columns='security_id', values='adjclose')
          .sort_index()
          .reindex(columns=sec_ids)   # enforce Securities tab column order
    )
    logger.info(f"Pivoted price matrix: {len(prices)} dates × {len(prices.columns)} securities")

    # ── Replace Prices tab ─────────────────────────────────────────────────────
    if 'Prices' in wb.sheetnames:
        del wb['Prices']
    ws = wb.create_sheet('Prices')

    # Header row
    ws.cell(1, 1, 'Date')
    for col_idx, sec_id in enumerate(sec_ids, start=2):
        ws.cell(1, col_idx, sec_id)

    # Data rows
    for row_idx, (date, row) in enumerate(prices.iterrows(), start=2):
        ws.cell(row_idx, 1, date.date())
        for col_idx, sec_id in enumerate(sec_ids, start=2):
            val = row.get(sec_id)
            if pd.notna(val):
                ws.cell(row_idx, col_idx, float(val))

    wb.save(WORKBOOK_PATH)

    logger.info(f"Wrote {len(prices)} rows × {len(sec_ids)} security columns to 'Prices' tab")
    logger.info(f"Saved: '{WORKBOOK_PATH}'")
    logger.info("=== write-prices done ===")


# ── task: run-model ────────────────────────────────────────────────────────────

def run_equity_model(model_id: str | None, submodel_id: str | None, logger: logging.Logger) -> None:
    """
    Read Securities and Prices tabs from EquityModel.xlsx and call
    equity_model.run_model() to run the full equity model pipeline.

    securities: DataFrame built from the Securities tab
                (columns: SecurityID, SecurityName, Currency, AssetClass, AssetType, Ticker)
    hist_prices: DataFrame from the Prices tab
                 (DatetimeIndex named 'Date', SecurityID columns, float values)
    """
    from models import equity_model

    logger.info(
        f"=== run-model started: workbook='{WORKBOOK_PATH}' "
        f"model_id={model_id or '(default)'} submodel_id={submodel_id or '(auto)'} ==="
    )

    # ── Read workbook ──────────────────────────────────────────────────────────
    if not WORKBOOK_PATH.exists():
        raise FileNotFoundError(f"Workbook not found: {WORKBOOK_PATH}")
    wb = openpyxl.load_workbook(WORKBOOK_PATH, data_only=True)

    for required in ('Securities', 'Prices'):
        if required not in wb.sheetnames:
            raise ValueError(f"'{required}' tab not found in workbook")

    # ── Build securities DataFrame ─────────────────────────────────────────────
    sec_rows = _read_securities(wb)
    securities = pd.DataFrame(sec_rows)
    logger.info(f"Read {len(securities)} securities from 'Securities' tab")

    # ── Build hist_prices DataFrame ────────────────────────────────────────────
    ws = wb['Prices']
    headers = [ws.cell(1, c).value for c in range(1, ws.max_column + 1)]  # ['Date', 'T1...', ...]
    price_rows = []
    for r in range(2, ws.max_row + 1):
        row = [ws.cell(r, c).value for c in range(1, ws.max_column + 1)]
        if row[0] is not None:
            price_rows.append(row)

    hist_prices = pd.DataFrame(price_rows, columns=headers)
    hist_prices['Date'] = pd.to_datetime(hist_prices['Date'])
    hist_prices = hist_prices.set_index('Date')
    hist_prices = hist_prices.astype(float)
    logger.info(
        f"Read Prices tab: {len(hist_prices)} dates × {len(hist_prices.columns)} securities "
        f"({hist_prices.index.min().date()} → {hist_prices.index.max().date()})"
    )

    # ── Run model ──────────────────────────────────────────────────────────────
    logger.info("Calling equity_model.run_model() ...")
    DATA = equity_model.run_model(securities, hist_prices, model_id, submodel_id)

    params = DATA.get('Parameters', {})
    logger.info(
        f"run_model() complete: model_id={params.get('Model ID')} "
        f"submodel_id={params.get('Submodel ID')}"
    )
    logger.info("=== run-model done ===")


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Equity model maintenance tasks')
    sub = parser.add_subparsers(dest='task', required=True)

    p_prices = sub.add_parser('write-prices', help='Write adjusted-close prices into the Prices tab')
    p_prices.add_argument('--file', required=True, metavar='PATH',
                          help='Path to hist_price CSV (e.g. CSV/hist_price_20260513_165602.csv)')

    p_model = sub.add_parser('run-model', help='Run the equity model pipeline')
    p_model.add_argument('--model-id', default=None, metavar='MODEL_ID',
                         help='Model ID (default: from var_utils.get_default_model_id())')
    p_model.add_argument('--submodel-id', default=None, metavar='SUBMODEL_ID',
                         help='Submodel ID (default: auto-generated as Equity.<timestamp>)')

    args = parser.parse_args()

    if args.task == 'write-prices':
        _logger = _setup_logger('write_prices')
        write_prices(Path(args.file), _logger)

    elif args.task == 'run-model':
        _logger = _setup_logger('run_model')
        run_equity_model(args.model_id, args.submodel_id, _logger)
