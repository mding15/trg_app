"""
dashboard/adhoc_positions_calc.py — Calculation helpers for ad-hoc (uploaded) portfolio summaries.

Parallel to positions_calc.py but reads from port_position_var (keyed by port_id)
instead of position_var (keyed by account_id + as_of_date).
"""
from __future__ import annotations

import math

import pandas as pd
import psycopg2.extras

from database2 import pg_connection


def get_positions(conn, port_id: int) -> pd.DataFrame:
    """Fetch all position rows for a port_id from port_position_var."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT security_id, market_value, expected_return, beta,
                   mg_var_95, mg_es_95, mg_std, total_cost, asset_class, as_of_date
            FROM port_position_var
            WHERE port_id = %s
            """,
            (port_id,),
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_positions_batch(conn, port_ids: list) -> dict:
    """Fetch positions for multiple port_ids in one query. Returns {port_id: DataFrame}."""
    if not port_ids:
        return {}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT port_id, security_id, market_value, expected_return, beta,
                   mg_var_95, mg_es_95, mg_std, total_cost, asset_class, as_of_date
            FROM port_position_var
            WHERE port_id = ANY(%s)
            """,
            (port_ids,),
        )
        rows = cur.fetchall()
    if not rows:
        return {}
    df_all = pd.DataFrame(rows)
    return {pid: grp.drop(columns='port_id').reset_index(drop=True)
            for pid, grp in df_all.groupby('port_id')}


def compute_portfolio_summary(port_id: int, df: pd.DataFrame) -> dict:
    """
    Compute portfolio-level summary for an ad-hoc uploaded portfolio.
    Input df should be fetched via get_positions().

    Risk metric formulas (mirror positions_calc.py):
        var_1d_95      = SUM(mg_var_95)
        var_1d_99      = var_1d_95 * 1.41
        es_1d_95       = SUM(mg_es_95)
        es_99          = es_1d_95 * 1.41
        volatility     = SUM(mg_std) / aum * sqrt(252) * 100  [stored as %]
        expectedReturn = SUM(market_value * expected_return) / aum * 100  [stored as %]
        sharpe_vol     = total_ret / vol_ratio
        sharpe_var     = total_ret / (var_1d_95 / aum) / sqrt(252)
        sharpe_es      = total_ret / (es_1d_95 / aum) / sqrt(252)
        beta           = MV-weighted; defaults: Cash->0, Fixed Income->0.5, other->1.0
    """
    as_of_date = df['as_of_date'].max()

    mv    = pd.to_numeric(df['market_value'], errors='coerce').fillna(0.0)
    aum   = float(mv.sum())
    count = df['security_id'].nunique()

    def _sum_col(col):
        s = pd.to_numeric(df[col], errors='coerce').sum()
        return float(s) if not pd.isna(s) else None

    sum_mvar  = _sum_col('mg_var_95')
    sum_mtvar = _sum_col('mg_es_95')
    sum_mstd  = _sum_col('mg_std')

    er            = pd.to_numeric(df['expected_return'], errors='coerce')
    sum_mv_er_val = (mv * er).sum()
    sum_mv_er     = float(sum_mv_er_val) if not pd.isna(sum_mv_er_val) else None

    # ── VaR / ES ──────────────────────────────────────────────────────────────
    var_1d_95 = round(float(sum_mvar),  2) if sum_mvar  is not None else None
    var_1d_99 = round(var_1d_95 * 1.41, 2) if var_1d_95 is not None else None
    es_1d_95  = round(float(sum_mtvar), 2) if sum_mtvar is not None else None
    es_99     = round(es_1d_95  * 1.41, 2) if es_1d_95  is not None else None

    # ── Volatility & Expected Return ───────────────────────────────────────────
    if sum_mstd is not None and aum > 0:
        vol_ratio  = float(sum_mstd) / aum * math.sqrt(252)
        volatility = round(vol_ratio * 100, 4)
    else:
        vol_ratio  = None
        volatility = None

    if sum_mv_er is not None and aum > 0:
        total_return = float(sum_mv_er) / aum  # decimal, used in sharpe calculations
    else:
        total_return = None

    # ── Sharpe ratios ──────────────────────────────────────────────────────────
    if total_return is not None and vol_ratio:
        sharpe_vol = round(total_return / vol_ratio, 4)
    else:
        sharpe_vol = None

    if total_return is not None and var_1d_95 and aum > 0:
        sharpe_var = round(total_return / (var_1d_95 / aum) / math.sqrt(252), 4)
    else:
        sharpe_var = None

    if total_return is not None and es_1d_95 and aum > 0:
        sharpe_es = round(total_return / (es_1d_95 / aum) / math.sqrt(252), 4)
    else:
        sharpe_es = None

    # ── Beta ───────────────────────────────────────────────────────────────────
    default_beta     = df['asset_class'].map({'Cash': 0.0, 'Fixed Income': 0.5}).fillna(1.0)
    beta_series      = pd.to_numeric(df['beta'], errors='coerce').fillna(default_beta)
    mv_weighted_beta = round(float((mv * beta_series).sum() / aum), 4) if aum != 0 else None

    return {
        'asOfDate':       as_of_date.strftime('%Y-%m-%d') if hasattr(as_of_date, 'strftime') else str(as_of_date),
        'aum':            round(aum, 2),
        'numPositions':   int(count) if count else 0,
        'var1d95':        var_1d_95,
        'var1d99':        var_1d_99,
        'es1d95':         es_1d_95,
        'es99':           es_99,
        'volatility':     volatility,
        'sharpeVol':      sharpe_vol,
        'sharpeVar':      sharpe_var,
        'sharpeES':       sharpe_es,
        'beta':           mv_weighted_beta,
        'expectedReturn': round(total_return * 100, 4) if total_return is not None else None,
    }


def test(port_id: int = 5393):
    with pg_connection() as conn:
        df = get_positions(conn, port_id)
    if df.empty:
        print(f'No positions found for port_id={port_id}')
        return
    print(f'Fetched {len(df)} rows for port_id={port_id}')
    summary = compute_portfolio_summary(port_id, df)
    for k, v in summary.items():
        print(f'  {k}: {v}')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Compute ad-hoc portfolio summary')
    parser.add_argument('--port-id', type=int, default=5393, help='port_id to test (default: 5393)')
    args = parser.parse_args()
    test(args.port_id)
