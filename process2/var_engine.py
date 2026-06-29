"""
var_engine.py — Calculate VaR and risk metrics for each position in a portfolio.

For each position:
    1. PnL vector = MarketValue × security_pnl[SecurityID]  (from security_pnl.h5)
    2. Metrics: std, mg_std, var_95, var_99, es_95, es_99,
                mg_var_95, mg_var_99, mg_es_95, mg_es_99
    3. Sensitivity metrics (when as_of_date provided):
       - Copied from security_sensitivity: tenor, iv, ir_tenor, yield, duration,
         convexity, spread_duration, spread_convexity, skewness, kurtosis
       - Position-level: delta, gamma, vega (sensitivity × mv);
         ir_pv01 (duration × mv × 0.0001), sp_pv01 (spread_duration × mv × 0.0001)
       - delta_var: marginal VaR from position PNL for positions with non-NULL delta
       - ir_duration_var: marginal VaR from IR_PNL (pct return) × market_value
       - sp_duration_var: marginal VaR from SPREAD_PNL (pct return) × market_value
       - weight: mv / total_mv; vol: std / mv * sqrt(252)

All marginal metrics are computed relative to the portfolio total (sum of all
valid position PnL vectors). Positions whose SecurityID is not found in
security_pnl.h5 are included in the output with NaN metrics.

Input columns expected by calc_var():
    pos_id, SecurityID, MarketValue  (plus any others, which are passed through)

Output of calc_var():
    DataFrame indexed by pos_id with metric columns.
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

_SENS_COLS = [
    'tenor', 'iv', 'ir_tenor', 'yield', 'duration', 'convexity',
    'spread_duration', 'spread_convexity', 'skewness', 'kurtosis',
]

_SENS_METRICS = [
    'tenor', 'iv', 'ir_tenor', 'yield', 'duration', 'convexity',
    'spread_duration', 'spread_convexity', 'skewness', 'kurtosis',
    'delta', 'gamma', 'vega', 'ir_pv01', 'sp_pv01',
    'weight',
]


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _load_security_sensitivity(security_ids: list, as_of_date) -> pd.DataFrame:
    """Fetch sensitivity rows for the given SecurityIDs and as_of_date."""
    if not security_ids:
        return pd.DataFrame()
    sql = """
        SELECT security_id, tenor, delta, gamma, vega, iv, ir_tenor,
               yield, duration, convexity, spread_duration, spread_convexity,
               skewness, kurtosis
        FROM security_sensitivity
        WHERE security_id = ANY(%s)
          AND as_of_date = %s
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (security_ids, as_of_date))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


def _calc_partial_var(
    pos: pd.DataFrame,
    mv_map: pd.Series,
    hdf_category: str,
    total_pnl: pd.Series,
) -> tuple[pd.Series, pd.Series]:
    """
    Build a position-level PNL matrix from HDF (hdf_category) scaled by market value,
    then return (standalone_var, marginal_var) at 95% CL indexed by pos_id.

    pos          : DataFrame indexed by pos_id with a 'SecurityID' column
    mv_map       : Series of market values indexed by pos_id (filtered to relevant positions)
    hdf_category : 'IR_PNL' or 'SPREAD_PNL' (stores percentage returns)
    total_pnl    : portfolio total PNL Series used as marginal VaR context

    pos_pnl = sec_pnl (pct return) × market_value

    Returns (empty, empty) if the HDF category is unavailable or yields no data.
    """
    _empty = pd.Series(dtype=float)

    sec_ids = pos.loc[mv_map.index, 'SecurityID'].dropna().unique().tolist()
    try:
        sec_pnl = hdf_utils.read(sec_ids, hdf_category, PNL_FILE)
    except Exception:
        return _empty, _empty

    pos_cols = {}
    for pid in mv_map.index:
        sid    = pos.at[pid, 'SecurityID']
        mv_val = mv_map.at[pid]
        if sid in sec_pnl.columns and not pd.isna(mv_val):
            pos_cols[pid] = sec_pnl[sid] * float(mv_val)

    if not pos_cols:
        return _empty, _empty

    pnl_df     = pd.DataFrame(pos_cols)
    standalone = var_utils.calc_VaR(pnl_df, 0.95)['VaR']
    marginal   = var_utils.calc_marginal_VaR(pnl_df, sum_pl=total_pnl, CL=0.95).drop('Total', errors='ignore')
    return standalone, marginal


def calc_ir_var(
    pos: pd.DataFrame,
    mv_map: pd.Series,
    total_pnl: pd.Series,
) -> tuple[pd.Series, pd.Series]:
    """IR VaR: IR_PNL (pct return) × market_value. Returns (standalone, marginal)."""
    return _calc_partial_var(pos, mv_map, 'IR_PNL', total_pnl)


def calc_sp_var(
    pos: pd.DataFrame,
    mv_map: pd.Series,
    total_pnl: pd.Series,
) -> tuple[pd.Series, pd.Series]:
    """Spread VaR: SPREAD_PNL (pct return) × market_value. Returns (standalone, marginal)."""
    return _calc_partial_var(pos, mv_map, 'SPREAD_PNL', total_pnl)


def calc_sensitivity_metrics(positions: pd.DataFrame, as_of_date) -> pd.DataFrame:
    """
    Fetch security-level sensitivities and compute position-level metrics.

    Called before calc_var() so the result can be passed in as sensitivity_metrics.
    Does not require pnl or std — those stay inside calc_var().

    positions  : DataFrame with pos_id, SecurityID, MarketValue columns
    as_of_date : date used to look up security_sensitivity rows

    Returns a DataFrame indexed by pos_id with _SENS_METRICS columns.
    All columns default to NaN; only populated where data exists.
    Computes: tenor, iv, ir_tenor, yield, duration, convexity,
              spread_duration, spread_convexity, skewness, kurtosis  (copied from DB)
              delta, gamma, vega  (sensitivity × mv)
              ir_pv01             (duration × mv × 0.0001)
              sp_pv01             (spread_duration × mv × 0.0001)
              weight              (mv / total_mv)
    """
    out = positions.set_index('pos_id')[[]].copy()
    for col in _SENS_METRICS:
        out[col] = float('nan')

    # Load security sensitivities and join to positions
    sec_ids = positions['SecurityID'].dropna().unique().tolist()
    sens = _load_security_sensitivity(sec_ids, as_of_date)

    pos = positions.set_index('pos_id').copy()
    if not sens.empty:
        pos = pos.join(sens.set_index('security_id'), on='SecurityID')

    mv = pos['MarketValue'].apply(pd.to_numeric, errors='coerce')

    # Copy security-level sensitivity columns to position level
    for col in _SENS_COLS:
        if col in pos.columns:
            out[col] = pd.to_numeric(pos[col], errors='coerce')

    # Position-level computed fields (sensitivity × mv)
    for col in ('delta', 'gamma', 'vega'):
        if col in pos.columns:
            out[col] = pd.to_numeric(pos[col], errors='coerce') * mv

    if 'duration' in pos.columns:
        out['ir_pv01'] = pd.to_numeric(pos['duration'], errors='coerce') * mv * 0.0001

    if 'spread_duration' in pos.columns:
        out['sp_pv01'] = pd.to_numeric(pos['spread_duration'], errors='coerce') * mv * 0.0001

    # weight = mv / total_mv
    total_mv = mv.sum()
    if total_mv and total_mv != 0:
        out['weight'] = mv / total_mv

    return out


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

def _load_security_pnl(
    security_ids: list[str],
    category: str = PNL_CATEGORY,
    adhoc_pnl: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Read PnL distribution matrix (scenarios × securities) from security_pnl.h5.
    If adhoc_pnl is provided, its columns take priority; only the remaining
    security_ids are fetched from HDF and concatenated."""
    if adhoc_pnl is not None:
        remaining_ids = [sid for sid in security_ids if sid not in adhoc_pnl.columns]
        if not remaining_ids:
            return adhoc_pnl
        return pd.concat([adhoc_pnl, hdf_utils.read(remaining_ids, category, PNL_FILE)], axis=1)
    return hdf_utils.read(security_ids, category, PNL_FILE)


# ── Core calculation ───────────────────────────────────────────────────────────

def calc_var(
    positions: pd.DataFrame,
    category: str = PNL_CATEGORY,
    adhoc_pnl: pd.DataFrame | None = None,
    sensitivity_metrics: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Calculate VaR and risk metrics for each position.

    Parameters
    ----------
    positions : DataFrame
        Must contain columns: pos_id, SecurityID, MarketValue.
        pos_id must be a plain column (not the index).
    category : str
        HDF category to load distributions from (default: 'PNL').
        Pass 'ALT' to use unadjusted alternative distributions.
    adhoc_pnl : DataFrame | None
        Optional pre-computed PnL distributions (scenarios × security_ids).
        Passed through to _load_security_pnl(): adhoc columns take priority,
        remaining security IDs are fetched from HDF. Indices assumed aligned.
    sensitivity_metrics : DataFrame | None
        Pre-computed output of calc_sensitivity_metrics(). When provided,
        it is joined into the output and the following are also computed using
        the internal pnl matrix: vol, delta_var, ir_duration_var, sp_duration_var.
        When None, only the 10 core VaR metrics are returned.

    Returns
    -------
    DataFrame indexed by pos_id with metric columns.
    Positions whose SecurityID is absent from all sources have NaN metrics.
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
    sec_pnl = _load_security_pnl(security_ids, category, adhoc_pnl)

    # Build position PnL matrix (only positions with available data)
    pos_pnl_cols = {}
    unmatched = []
    for _, row in positions.iterrows():
        pid = row['pos_id']
        sid = row['SecurityID']
        mv  = row['MarketValue']
        if sid in sec_pnl.columns and mv is not None and not pd.isna(mv):
            pos_pnl_cols[pid] = sec_pnl[sid] * float(mv)
        elif pd.notna(sid):
            unmatched.append((pid, sid))

    if unmatched:
        print(f'No security_pnl match for {len(unmatched)} position(s):')
        for pid, sid in unmatched:
            print(f'  pos_id={pid}  SecurityID={sid}')

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

    # Extended metrics — computed here because they need pnl and std
    if sensitivity_metrics is not None:
        pos = positions.set_index('pos_id')
        mv  = pos['MarketValue'].apply(pd.to_numeric, errors='coerce')

        # Initialise all partial-VaR columns so they always appear in the output
        # (insert_results filters to df.columns, so absent columns are silently skipped)
        _partial_cols = (
            'delta_var', 'ir_duration_var', 'ir_var', 'sp_duration_var', 'spread_var',
            'mg_delta_var', 'mg_ir_duration_var', 'mg_ir_var', 'mg_sp_duration_var', 'mg_spread_var',
        )
        for col in _partial_cols:
            out[col] = float('nan')

        # vol = std / mv * sqrt(252)
        out['vol'] = out['std'] / mv * (252 ** 0.5)

        total_pnl = pnl.sum(axis=1).rename('Total')

        # delta VaR: positions with non-NULL delta that have PNL data
        delta_pids = sensitivity_metrics.index[
            sensitivity_metrics['delta'].notna() & sensitivity_metrics.index.isin(pnl.columns)
        ]
        if len(delta_pids) > 0:
            delta_pnl = pnl[delta_pids]
            out['delta_var']    = var_utils.calc_VaR(delta_pnl, 0.95)['VaR']
            out['mg_delta_var'] = var_utils.calc_marginal_VaR(delta_pnl, sum_pl=total_pnl, CL=0.95).drop('Total', errors='ignore')

        ir_pos_ids = sensitivity_metrics['ir_pv01'].dropna().index
        if len(ir_pos_ids) > 0:
            ir_sa, ir_mg              = calc_ir_var(pos, mv.loc[ir_pos_ids.intersection(mv.index)], total_pnl)
            out['ir_duration_var']    = ir_sa
            out['mg_ir_duration_var'] = ir_mg
            out['ir_var']             = ir_sa
            out['mg_ir_var']          = ir_mg

        sp_pos_ids = sensitivity_metrics['sp_pv01'].dropna().index
        if len(sp_pos_ids) > 0:
            sp_sa, sp_mg              = calc_sp_var(pos, mv.loc[sp_pos_ids.intersection(mv.index)], total_pnl)
            out['sp_duration_var']    = sp_sa
            out['mg_sp_duration_var'] = sp_mg
            out['spread_var']         = sp_sa
            out['mg_spread_var']      = sp_mg

        out = out.join(sensitivity_metrics)

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

    sens    = calc_sensitivity_metrics(positions, as_of_date)
    metrics = calc_var(positions, sensitivity_metrics=sens)
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

    out_dir = config['TEST_DIR'] / 'src' / 'process2'
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
