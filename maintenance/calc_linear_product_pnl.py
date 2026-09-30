"""
calc_linear_product_pnl.py — Linear product P&L distributions for the securities
in an upload-template portfolio file.

Same calculation as process2/calc_linear_product_pnl.py (shared code:
build_linear_pnl) — price distribution from the VaR HDF as $1 P&L, all-zero
P&L for Cash — but the securities come from the input file instead of
current_security:
    1. Read '1. Positions' and resolve SecurityID the same way as an upload
       (process2/security_lookup.py: TRG_ID → ISIN → CUSIP → BB_GLOBAL → Ticker).
    2. Keep linear products by security_info AssetClass/AssetType (same rule as
       process2: Equity, Alternative, Commodity, REIT, Cash; Bond Fund/ETF).
    3. Skip securities that already have PNL/{SecurityID} in security_pnl.h5,
       unless --overwrite.
    4. Build P&L; non-Cash securities with no price distribution are skipped.

Writes (never to the database):
    security_pnl.h5 — PNL/{SecurityID} for generated securities (not with --dry-run)
    data/maintenance/CSV/<input_stem>_security_sensitivity_<timestamp>.csv
    data/maintenance/CSV/<input_stem>_security_pnl_stat_<timestamp>.csv
        — the rows process2 would write to security_sensitivity /
          security_pnl_stat (same columns, pnl_type 'LINEAR')
    data/maintenance/CSV/<input_stem>_linear_pnl_<timestamp>.csv
        — one row per position: security, class, and pnl_status
          (generated / exists (skipped) / not linear / no price distribution /
          unresolved SecurityID)
The CSVs are written in dry-run mode too.

Usage:
    python maintenance/calc_linear_product_pnl.py                        # input_template.xlsx
    python maintenance/calc_linear_product_pnl.py --file my_port.xlsx    # relative to data/maintenance/Excel
    python maintenance/calc_linear_product_pnl.py --date 2026-09-25      # override the file's As of Date
    python maintenance/calc_linear_product_pnl.py --dry-run              # CSVs only, no HDF write
    python maintenance/calc_linear_product_pnl.py --overwrite            # regenerate existing PNL too
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dashboard.process_uploaded_portfolio import _TEMPLATE_TO_ENGINE   # same column mapping as an upload
from database2 import pg_connection
from preprocess import read_portfolio
from process2.security_lookup import lookup_security_ids
from process2.calc_linear_product_pnl import PNL_FILE, PNL_TYPE, build_linear_pnl, is_linear
from process2.db_pnl_stat import pnl_stat_rows, security_sensitivity_rows
from utils import hdf_utils
from _paths import CSV_DIR, EXCEL_DIR

DEFAULT_FILE = 'input_template.xlsx'

_STATUS_COLS = ['pos_id', 'security_id', 'security_name', 'ticker', 'market_value',
                'asset_class', 'asset_type', 'pnl_status']


# ── Helpers ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger('calc_linear_product_pnl')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s', '%H:%M:%S')
    )
    logger.addHandler(handler)
    return logger


def _read_positions(in_path: Path) -> tuple[dict, pd.DataFrame]:
    """Input file positions with SecurityID resolved, as an upload would."""
    params, positions, _ = read_portfolio.read_input_file(in_path)
    positions = positions.rename(columns=_TEMPLATE_TO_ENGINE)
    if 'SecurityID' not in positions.columns:
        positions['SecurityID'] = None
    return params, lookup_security_ids(positions)


def _security_classes(security_ids: list[str]) -> pd.DataFrame:
    """security_info AssetClass/AssetType, index SecurityID."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT "SecurityID", "AssetClass", "AssetType" FROM security_info WHERE "SecurityID" = ANY(%s)',
                (security_ids,),
            )
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=['SecurityID', 'AssetClass', 'AssetType']).set_index('SecurityID')


def _existing_pnl_ids() -> set[str]:
    """SecurityIDs that already have PNL/{SecurityID} in security_pnl.h5."""
    if not PNL_FILE.exists():
        return set()
    with pd.HDFStore(PNL_FILE, mode='r') as store:
        return {k.split('/')[-1] for k in store.keys() if k.startswith('/PNL/')}


# ── Core logic ────────────────────────────────────────────────────────────────

def run(file_name: str, asof_date: date | None, dry_run: bool = False, overwrite: bool = False) -> None:
    log = _setup_logger()

    in_path = Path(file_name)
    if not in_path.is_absolute():
        in_path = EXCEL_DIR / in_path
    log.info(f'Input: {in_path}')

    params, positions = _read_positions(in_path)
    if asof_date is None:
        asof_date = params.get('AsofDate')
    log.info(f'as_of_date={asof_date}  positions={len(positions)}  '
             f'SecurityID resolved={positions["SecurityID"].notna().sum()}')

    # ── Classify each security ────────────────────────────────────────────────
    sec_ids = positions['SecurityID'].dropna().unique().tolist()
    classes = _security_classes(sec_ids)
    existing = _existing_pnl_ids()

    status = {}                 # SecurityID → pnl_status
    todo = []                   # linear securities to build
    for sid in sec_ids:
        ac = classes['AssetClass'].get(sid)
        at = classes['AssetType'].get(sid)
        if not is_linear(ac, at):
            status[sid] = f'not linear ({ac}/{at})'
        elif sid in existing and not overwrite:
            status[sid] = 'exists (skipped)'
        else:
            todo.append({'SecurityID': sid, 'AssetClass': ac})

    # ── Build P&L (shared with process2) ──────────────────────────────────────
    pnl = pd.DataFrame()
    sens = stats = None
    if todo:
        pnl, sens, stats, no_dist = build_linear_pnl(pd.DataFrame(todo))
        for sid in no_dist:
            status[sid] = 'no price distribution'
        for sid in pnl.columns:
            status[sid] = 'generated (zero, cash)' if classes['AssetClass'].get(sid) == 'Cash' else 'generated'

    # ── Per-position status ───────────────────────────────────────────────────
    report = pd.DataFrame({
        'pos_id':        positions['pos_id'],
        'security_id':   positions['SecurityID'],
        'security_name': positions.get('SecurityName'),
        'ticker':        positions.get('Ticker'),
        'market_value':  positions.get('MarketValue'),
        'asset_class':   positions['SecurityID'].map(classes['AssetClass']),
        'asset_type':    positions['SecurityID'].map(classes['AssetType']),
        'pnl_status':    positions['SecurityID'].map(status).fillna('unresolved SecurityID'),
    })[_STATUS_COLS]

    log.info('─' * 60)
    for st, n in report['pnl_status'].value_counts().items():
        log.info(f'  {st:<40} {n} position(s)')
    for _, r in report[report['pnl_status'].isin(['no price distribution', 'unresolved SecurityID'])].iterrows():
        log.warning(f"  {r['pnl_status']}: pos_id={r['pos_id']}  security_id={r['security_id']}  {r['security_name']}")

    # ── Write CSVs and HDF ────────────────────────────────────────────────────
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    log.info('─' * 60)

    status_path = CSV_DIR / f'{in_path.stem}_linear_pnl_{timestamp}.csv'
    report.to_csv(status_path, index=False)
    log.info(f'Position status written to {status_path}')

    if pnl.empty:
        log.info('No P&L generated — security_sensitivity/security_pnl_stat CSVs and HDF not written.')
        return

    sens_path = CSV_DIR / f'{in_path.stem}_security_sensitivity_{timestamp}.csv'
    security_sensitivity_rows(sens, asof_date).to_csv(sens_path, index=False)
    log.info(f'security_sensitivity rows written to {sens_path}')

    stat_path = CSV_DIR / f'{in_path.stem}_security_pnl_stat_{timestamp}.csv'
    pnl_stat_rows(stats, asof_date, PNL_TYPE).to_csv(stat_path, index=False)
    log.info(f'security_pnl_stat rows written to {stat_path}')

    if dry_run:
        log.info(f'Dry run — {pnl.shape[1]} P&L distribution(s) not saved to {PNL_FILE.name}.')
    else:
        hdf_utils.save(pnl, 'PNL', PNL_FILE)
        log.info(f'Saved {pnl.shape[1]} P&L distribution(s) to {PNL_FILE}: {", ".join(pnl.columns)}')


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Linear product P&L distributions for the securities in a portfolio file.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='Examples:\n  python maintenance/calc_linear_product_pnl.py --dry-run\n'
               '  python maintenance/calc_linear_product_pnl.py --file my_port.xlsx\n',
    )
    parser.add_argument('--file', default=DEFAULT_FILE,
                        help=f'input Excel file, relative to data/maintenance/Excel (default: {DEFAULT_FILE})')
    parser.add_argument('--date', type=date.fromisoformat, default=None, metavar='YYYY-MM-DD',
                        help="override the file's As of Date (used for the CSV rows' as_of_date)")
    parser.add_argument('--dry-run', action='store_true',
                        help='write the CSVs only; do not save P&L to security_pnl.h5')
    parser.add_argument('--overwrite', action='store_true',
                        help='also regenerate securities that already have a P&L distribution')
    args = parser.parse_args()

    run(file_name=args.file, asof_date=args.date, dry_run=args.dry_run, overwrite=args.overwrite)


if __name__ == '__main__':
    main()
