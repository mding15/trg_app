"""
dump_mkt_data_info.py — Dump market data statistics for all categories to Excel.

For each category in the mkt_data security list (excluding TEST):
  - Fetches historical prices in batches via mkt_timeseries.get()
  - Calculates statistics via utils.stat_utils.hist_stat()
  - Writes one Excel tab per category

Output: maintenance/Excel/mkt_data_info_<YYYYMMDD_HHMMSS>.xlsx

Usage:
    python maintenance/dump_mkt_data_info.py
    python maintenance/dump_mkt_data_info.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

# Flask app context required by mkt_timeseries / mkt_data (SQLAlchemy)
from api import app
app.app_context().push()

from mkt_data import mkt_timeseries
from utils.stat_utils import hist_stat

EXCEL_DIR  = SCRIPT_DIR / 'Excel'
BATCH_SIZE = 50


# ── logging ────────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    log = logging.getLogger('dump_mkt_data_info')
    log.setLevel(logging.DEBUG)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s', '%H:%M:%S'))
    log.addHandler(h)
    return log


# ── core ───────────────────────────────────────────────────────────────────────

def run(dry_run: bool = False) -> None:
    log = _setup_logger()

    sec_list = mkt_timeseries.get_mkt_data_sec_list()
    categories = sorted(set(sec_list['Category'].unique()) - {'TEST'})
    log.info(f'Categories: {categories}')

    if dry_run:
        log.info('─' * 60)
        log.info('DRY RUN — no file will be written')
        for cat in categories:
            n = sec_list[sec_list['Category'] == cat].shape[0]
            log.info(f'  {cat:20s}  {n} securities')
        log.info('─' * 60)
        return

    category_dfs: dict[str, pd.DataFrame] = {}

    for cat in categories:
        log.info(f'Processing category: {cat}')
        sec_ids = sec_list[sec_list['Category'] == cat]['SecurityID'].tolist()
        batch_stats: list[pd.DataFrame] = []

        for i in range(0, len(sec_ids), BATCH_SIZE):
            batch = sec_ids[i: i + BATCH_SIZE]
            log.info(f'  batch {i}–{i + len(batch) - 1}  ({len(batch)} securities)')
            try:
                prices = mkt_timeseries.get(batch, category=cat)
                if prices.empty:
                    log.warning(f'  no price data for batch {i}')
                    continue
                stat = hist_stat(prices)
                stat.index.name = 'SecurityID'
                batch_stats.append(stat.reset_index())
            except Exception as e:
                log.warning(f'  batch {i} failed: {e}')

        if batch_stats:
            df = pd.concat(batch_stats, ignore_index=True)
            df = df[df['Length'] > 0].copy()
            log.info(f'  {cat}: {len(df)} securities with data')
            category_dfs[cat] = df
        else:
            log.warning(f'  {cat}: no data — sheet skipped')

    if not category_dfs:
        log.warning('No data collected — Excel file not written.')
        return

    EXCEL_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path  = EXCEL_DIR / f'mkt_data_info_{timestamp}.xlsx'

    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        for cat, df in category_dfs.items():
            sheet_name = cat[:31]  # Excel tab name limit
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            log.info(f'  wrote sheet "{sheet_name}"  ({len(df)} rows)')

    log.info('─' * 60)
    log.info(f'Done.  {len(category_dfs)} sheet(s) written to {out_path}')


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Dump market data statistics for all categories to Excel.'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Show category/security counts without writing the file',
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)
