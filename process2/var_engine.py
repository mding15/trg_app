"""
var_engine.py — Calculate VaR and risk metrics for each position in a portfolio.

For each position:
    1. PnL vector = MarketValue × security_pnl[SecurityID]  (from security_pnl.h5)
    2. Metrics: std, mg_std, var_95, var_99, es_95, es_99,
                mg_var_95, mg_var_99, mg_es_95, mg_es_99

All marginal metrics are computed relative to the portfolio total (sum of all
valid position PnL vectors). Positions whose SecurityID is not found in
security_pnl.h5 are included in the output with NaN metrics.

Input columns expected by calc_var():
    pos_id, SecurityID, MarketValue  (plus any others, which are passed through)

Output of calc_var():
    DataFrame indexed by pos_id with 10 metric columns only.
    Caller merges back into the positions DataFrame to retain all other columns.

Usage:
    python process2/var_engine.py --account-id 1003
    python process2/var_engine.py --account-id 1003 --date 2026-05-19
    python process2/var_engine.py --account-id 1003 --test
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from trg_config import config
from database2 import pg_connection, get_proc_asof_date
from utils import hdf_utils, var_utils

PNL_FILE     = config['VaR_DIR'] / 'security_pnl.h5'
PNL_CATEGORY = 'PNL'

_METRICS = [
    'std', 'mg_std',
    'var_95', 'var_99',
    'es_95',  'es_99',
    'mg_var_95', 'mg_var_99',
    'mg_es_95',  'mg_es_99',
]


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _load_positions(account_id: int, as_of_date) -> pd.DataFrame:
    """Return proc_positions rows for the given account and as_of_date."""
    sql = """
        SELECT position_id AS pos_id, security_id AS "SecurityID",
               security_name AS "SecurityName", market_value AS "MarketValue"
        FROM proc_positions
        WHERE account_id = %s AND as_of_date = %s
        ORDER BY position_id
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (account_id, as_of_date))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


# ── HDF helper ─────────────────────────────────────────────────────────────────

def _load_security_pnl(security_ids: list[str]) -> pd.DataFrame:
    """Read PnL distribution matrix (scenarios × securities) from security_pnl.h5."""
    return hdf_utils.read(security_ids, PNL_CATEGORY, PNL_FILE)


# ── Core calculation ───────────────────────────────────────────────────────────

def calc_var(positions: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate VaR and risk metrics for each position.

    Parameters
    ----------
    positions : DataFrame
        Must contain columns: pos_id, SecurityID, MarketValue.
        pos_id must be a plain column (not the index).

    Returns
    -------
    DataFrame indexed by pos_id with 10 metric columns only.
    Positions whose SecurityID is absent from security_pnl.h5 have NaN metrics.
    Caller is responsible for merging these metrics back into the positions DataFrame.
    """
    # Output scaffold — all positions, metrics initialised to NaN
    out = positions.set_index('pos_id')[[]].copy()
    for col in _METRICS:
        out[col] = float('nan')

    if positions.empty:
        return out

    # Load PnL distributions for all unique securities in one read
    security_ids = positions['SecurityID'].dropna().unique().tolist()
    sec_pnl = _load_security_pnl(security_ids)   # scenarios × SecurityIDs

    # Build position PnL matrix (only positions with available data)
    pos_pnl_cols = {}
    for _, row in positions.iterrows():
        pid = row['pos_id']
        sid = row['SecurityID']
        mv  = row['MarketValue']
        if sid in sec_pnl.columns and mv is not None and not pd.isna(mv):
            pos_pnl_cols[pid] = sec_pnl[sid] * float(mv)

    if not pos_pnl_cols:
        return out

    pnl = pd.DataFrame(pos_pnl_cols)   # scenarios × pos_ids (valid only)

    # ── Metrics ───────────────────────────────────────────────────────────────

    std = pnl.std()

    mg_std_df = var_utils.calc_marginal_vol(pnl)
    mg_std = (
        mg_std_df.drop(index='total', errors='ignore')['mgVol']
        if mg_std_df is not None
        else pd.Series(dtype=float)
    )

    var_95 = var_utils.calc_VaR(pnl, 0.95)['VaR']
    var_99 = var_utils.calc_VaR(pnl, 0.99)['VaR']

    es_95 = var_utils.calc_tVaR(pnl, 0.95)['tVaR']
    es_99 = var_utils.calc_tVaR(pnl, 0.99)['tVaR']

    mg_var_95 = var_utils.calc_marginal_VaR(pnl, CL=0.95).drop('Total', errors='ignore')
    mg_var_99 = var_utils.calc_marginal_VaR(pnl, CL=0.99).drop('Total', errors='ignore')

    mg_es_95 = var_utils.calc_marginal_tVaR(pnl, CL=0.95)
    mg_es_99 = var_utils.calc_marginal_tVaR(pnl, CL=0.99)

    # Assemble and update (index-aligned; NaN positions retain NaN)
    metrics = pd.DataFrame({
        'std':       std,
        'mg_std':    mg_std,
        'var_95':    var_95,
        'var_99':    var_99,
        'es_95':     es_95,
        'es_99':     es_99,
        'mg_var_95': mg_var_95,
        'mg_var_99': mg_var_99,
        'mg_es_95':  mg_es_95,
        'mg_es_99':  mg_es_99,
    })
    out.update(metrics)
    return out


# ── Orchestrator ───────────────────────────────────────────────────────────────

def run(account_id: int, as_of_date=None) -> pd.DataFrame:
    """
    Load positions from proc_positions and compute VaR metrics.

    Parameters
    ----------
    account_id : int
    as_of_date : date | str | None — defaults to proc_asof_date table value

    Returns
    -------
    DataFrame indexed by pos_id with position columns + all metric columns.
    """
    if as_of_date is None:
        as_of_date = date.fromisoformat(get_proc_asof_date())
    elif isinstance(as_of_date, str):
        as_of_date = date.fromisoformat(as_of_date)

    print(f'engine.run  account_id={account_id}  as_of_date={as_of_date}')

    positions = _load_positions(account_id, as_of_date)
    print(f'Positions: {len(positions)}')

    if positions.empty:
        print('No positions found.')
        return pd.DataFrame()

    metrics = calc_var(positions)
    result  = positions.set_index('pos_id').join(metrics)
    n_ok  = result['std'].notna().sum()
    n_nan = result['std'].isna().sum()
    print(f'Metrics computed: {n_ok} with PnL data, {n_nan} without (NaN)')
    return result


# ── Test ───────────────────────────────────────────────────────────────────────

def test(account_id: int = 1003):
    """Run on account_id using the latest as_of_date in proc_positions; save result to CSV."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT MAX(as_of_date) FROM proc_positions WHERE account_id = %s',
                (account_id,),
            )
            row = cur.fetchone()

    if not row or row[0] is None:
        print(f'No positions found for account_id={account_id}')
        return

    as_of_date = row[0]
    print(f'Latest as_of_date for account_id={account_id}: {as_of_date}')

    result = run(account_id, as_of_date)
    if result.empty:
        return

    print(result.to_string())

    out_dir = Path(__file__).resolve().parent / 'test_output'
    out_dir.mkdir(exist_ok=True)
    ts = as_of_date.strftime('%Y%m%d') if hasattr(as_of_date, 'strftime') else str(as_of_date).replace('-', '')
    out_file = out_dir / f'engine_var_{account_id}_{ts}.csv'
    result.to_csv(out_file)
    print(f'Saved: {out_file}')


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Calculate VaR and risk metrics per position'
    )
    parser.add_argument(
        '--account-id', type=int, required=True, metavar='ACCOUNT_ID',
        help='Account ID to process',
    )
    parser.add_argument(
        '--date', default=None, metavar='YYYY-MM-DD',
        help='As-of date (default: read from proc_asof_date table)',
    )
    parser.add_argument(
        '--test', action='store_true',
        help='Run test mode: use latest as_of_date from proc_positions and save result to CSV',
    )
    args = parser.parse_args()

    if args.test:
        test(account_id=args.account_id)
    else:
        result = run(account_id=args.account_id, as_of_date=args.date)
        if not result.empty:
            print(result.to_string())
