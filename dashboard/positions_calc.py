"""
positions_calc.py — Calculation helpers for dashboard positions.

Reads raw positions from position_var (current day).
Reads historical market values from db_mv_history (for return calculations).
"""
from __future__ import annotations

import math

import pandas as pd
import psycopg2.extras

from database2 import pg_connection


# ── position_var helpers ───────────────────────────────────────────────────────

def get_latest_feed_dates(conn, n: int = 2) -> list:
    """Return the N most recent distinct as_of_dates in position_var, newest first."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT as_of_date FROM position_var ORDER BY as_of_date DESC LIMIT %s",
            (n,),
        )
        return [row[0] for row in cur.fetchall()]


def get_account_ids_on_date(conn, as_of_date) -> list:
    """Return all distinct account_ids in position_var for the given as_of_date."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT account_id FROM position_var WHERE as_of_date = %s ORDER BY account_id",
            (as_of_date,),
        )
        return [row[0] for row in cur.fetchall()]


def get_positions_on_date(conn, as_of_date, account_id: int) -> pd.DataFrame:
    """Fetch all position rows for a specific (as_of_date, account_id) from position_var."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT security_id, ticker, security_name, market_value, currency,
                   "class" AS asset_class, region, sector,
                   marginal_tvar, marginal_var, marginal_std, expected_return, beta,
                   total_cost, broker, broker_account
            FROM position_var
            WHERE as_of_date = %s AND account_id = %s
            """,
            (as_of_date, account_id),
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ── db_mv_history helpers ─────────────────────────────────────────────────────

def mv_map_from_history(conn, account_id: int, ref_date) -> dict[tuple, float]:
    """Return {(security_id, broker, broker_account): market_value} for the closest as_of_date <= ref_date."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT security_id, broker, broker_account, market_value FROM db_mv_history
            WHERE account_id = %s AND as_of_date = (
                SELECT MAX(as_of_date) FROM db_mv_history
                WHERE account_id = %s AND as_of_date <= %s
            )
            """,
            (account_id, account_id, ref_date),
        )
        rows = cur.fetchall()
    return {
        (r["security_id"], r["broker"], r["broker_account"]): float(r["market_value"])
        for r in rows if r["market_value"] is not None
    }


def total_mv_from_history(conn, account_id, ref_date):
    """Return total market value for the closest as_of_date <= ref_date."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT SUM(market_value) FROM db_mv_history
            WHERE account_id = %s AND as_of_date = (
                SELECT MAX(as_of_date) FROM db_mv_history
                WHERE account_id = %s AND as_of_date <= %s
            )
            """,
            (account_id, account_id, ref_date),
        )
        result = cur.fetchone()[0]
    return float(result) if result is not None else None


def prev_date_from_history(conn, account_id, before_date):
    """Return the most recent as_of_date strictly before before_date in db_mv_history."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(as_of_date) FROM db_mv_history
            WHERE account_id = %s AND as_of_date < %s
            """,
            (account_id, before_date),
        )
        result = cur.fetchone()[0]
    return result


def earliest_total_mv_from_history(conn, account_id):
    """Return the total market value at the earliest available date in db_mv_history (since inception)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT SUM(market_value) FROM db_mv_history
            WHERE account_id = %s AND as_of_date = (
                SELECT MIN(as_of_date) FROM db_mv_history WHERE account_id = %s
            )
            """,
            (account_id, account_id),
        )
        result = cur.fetchone()[0]
    return float(result) if result is not None else None


# ── Return math ───────────────────────────────────────────────────────────────

def pct_return(current_mv: float, base_mv_map: dict, security_id: str):
    """Position-level percentage return vs base map, or None if unavailable."""
    base = base_mv_map.get(security_id)
    if base is None or base == 0:
        return None
    return round((current_mv - base) / abs(base) * 100, 2)


def pnl(current_mv: float, base_mv_map: dict, security_id: str):
    """Position-level absolute P&L vs base map, or None if unavailable."""
    base = base_mv_map.get(security_id)
    if base is None:
        return None
    return round(current_mv - base, 2)


def pct_return_total(current, base):
    """Portfolio-level percentage return, or None if base unavailable."""
    if base is None or base == 0:
        return None
    return round((current - base) / abs(base) * 100, 2)


# ── Main computation functions ────────────────────────────────────────────────

def compute_portfolio_summary(account_id: int, as_of_date, df: pd.DataFrame) -> dict:
    """
    Compute portfolio-level summary for account_id.
    Current AUM and risk metrics are derived from df (pre-fetched from position_var);
    return metrics are calculated from db_mv_history.

    Risk metric formulas:
        var_1d_95  = SUM(marginal_var)
        var_1d_99  = var_1d_95 * 1.41
        var_10d_99 = var_1d_99 * sqrt(10)
        es_1d_95   = SUM(marginal_tvar)
        es_99      = es_1d_95 * 1.41
        volatility = SUM(marginal_std) / aum * sqrt(252)  [stored as %]
        total_ret  = SUM(market_value * expected_return) / aum
        sharpe_vol = total_ret / volatility_ratio
        beta       = 1.2  [hardcoded placeholder]
        max_drawdown = -12.5  [hardcoded placeholder, %]
        unrealized_gain = aum * 10%  [placeholder]
        top_five_conc = top-5 security MV / aum * 100  [%]
    """
    import datetime
    latest = pd.to_datetime(as_of_date).date() if not isinstance(as_of_date, datetime.date) else as_of_date
    first_of_month  = latest.replace(day=1)
    first_of_year   = latest.replace(month=1, day=1)
    one_year_ago    = latest.replace(year=latest.year - 1)
    three_years_ago = latest.replace(year=latest.year - 3)

    mv = pd.to_numeric(df['market_value'], errors='coerce').fillna(0.0)
    count = df['security_id'].nunique()
    aum   = float(mv.sum())

    def _sum_col(col):
        s = pd.to_numeric(df[col], errors='coerce').sum()
        return float(s) if not pd.isna(s) else None

    sum_mvar  = _sum_col('marginal_var')
    sum_mtvar = _sum_col('marginal_tvar')
    sum_mstd  = _sum_col('marginal_std')
    er            = pd.to_numeric(df['expected_return'], errors='coerce')
    sum_mv_er_val = (mv * er).sum()
    sum_mv_er     = float(sum_mv_er_val) if not pd.isna(sum_mv_er_val) else None

    top5_mvs = (
        df.groupby('security_id')['market_value']
        .apply(lambda x: pd.to_numeric(x, errors='coerce').fillna(0.0).sum())
        .nlargest(5)
        .tolist()
    )

    # MV-weighted beta: null beta defaults to 0 (Cash), 0.5 (Fixed Income), 1 (all others)
    default_beta = df['asset_class'].map({'Cash': 0.0, 'Fixed Income': 0.5}).fillna(1.0)
    beta_series  = pd.to_numeric(df['beta'], errors='coerce').fillna(default_beta)
    total_mv_signed = mv.sum()  # mv is signed (already computed above)
    mv_weighted_beta = round(float((mv * beta_series).sum() / total_mv_signed), 4) if total_mv_signed != 0 else None

    with pg_connection() as conn:
        # Historical reference AUMs for return calculations
        prev = prev_date_from_history(conn, account_id, latest)

        prev_aum      = total_mv_from_history(conn, account_id, prev)            if prev else None
        mtd_aum       = total_mv_from_history(conn, account_id, first_of_month)
        ytd_aum       = total_mv_from_history(conn, account_id, first_of_year)
        one_yr_aum    = total_mv_from_history(conn, account_id, one_year_ago)
        three_yr_aum  = total_mv_from_history(conn, account_id, three_years_ago)
        si_aum        = earliest_total_mv_from_history(conn, account_id)

    day_pnl = round(aum - prev_aum, 2) if prev_aum is not None else None

    # ── VaR / ES ────────────────────────────────────────────────────────────────
    var_1d_95  = round(float(sum_mvar),  2) if sum_mvar  is not None else None
    var_1d_99  = round(var_1d_95 * 1.41, 2) if var_1d_95  is not None else None
    var_10d_99 = round(var_1d_99 * math.sqrt(10), 2) if var_1d_99 is not None else None
    es_1d_95   = round(float(sum_mtvar), 2) if sum_mtvar is not None else None
    es_99      = round(es_1d_95  * 1.41, 2) if es_1d_95  is not None else None

    # ── Volatility & Sharpe ─────────────────────────────────────────────────────
    if sum_mstd is not None and aum > 0:
        vol_ratio  = float(sum_mstd) / aum * math.sqrt(252)   # decimal, e.g. 0.15
        volatility = round(vol_ratio * 100, 4)                 # stored as %, e.g. 15.0
    else:
        vol_ratio  = None
        volatility = None

    if sum_mv_er is not None and aum > 0:
        total_return = float(sum_mv_er) / aum                  # decimal
    else:
        total_return = None

    if total_return is not None and vol_ratio:
        sharpe_vol = round(total_return / vol_ratio, 4)
    else:
        sharpe_vol = None

    if total_return is not None and var_1d_95 and aum > 0:
        sharpe_var = round(total_return / (var_1d_95 / aum) / math.sqrt(252), 4)
    else:
        sharpe_var = None

    # ── Other metrics ───────────────────────────────────────────────────────────
    tc = pd.to_numeric(df['total_cost'], errors='coerce').replace(0, float('nan'))
    if tc.notna().any():
        ug_series = mv - tc  # NaN where total_cost is NULL — skipped by sum
        unrealized_gain = round(float(ug_series.sum(skipna=True)), 2)
    else:
        unrealized_gain = None
    top_five_conc   = round(sum(top5_mvs) / aum * 100, 2) if aum > 0 else None

    return {
        "asOfDate":       latest.strftime("%Y-%m-%d"),
        "aum":            round(aum, 2),
        "numPositions":   int(count) if count else 0,
        "dayPnL":         day_pnl,
        "dayReturn":        pct_return_total(aum, prev_aum),
        "mtdReturn":        pct_return_total(aum, mtd_aum),
        "ytdReturn":        pct_return_total(aum, ytd_aum),
        "oneYearReturn":    pct_return_total(aum, one_yr_aum),
        "threeYearReturn":  pct_return_total(aum, three_yr_aum),
        "siReturn":         pct_return_total(aum, si_aum),
        "unrealizedGain": unrealized_gain,
        "var1d95":        var_1d_95,
        "var1d99":        var_1d_99,
        "var10d99":       var_10d_99,
        "es1d95":         es_1d_95,
        "es99":           es_99,
        "volatility":     volatility,
        "sharpeVol":      sharpe_vol,
        "sharpeVar":      sharpe_var,
        "beta":           mv_weighted_beta,
        "maxDrawdown":    -12.5,
        "topFiveConc":    top_five_conc,
    }


def compute_positions(account_id: int, as_of_date, df: pd.DataFrame) -> list[dict]:
    """
    Compute position-level data for account_id, aggregated by security_id.
    Current positions are derived from df (pre-fetched from position_var);
    returns are calculated from db_mv_history.
    """
    import datetime
    latest = pd.to_datetime(as_of_date).date() if not isinstance(as_of_date, datetime.date) else as_of_date
    first_of_month = latest.replace(day=1)
    first_of_year  = latest.replace(month=1, day=1)
    one_year_ago   = latest.replace(year=latest.year - 1)

    with pg_connection() as conn:
        prev = prev_date_from_history(conn, account_id, latest)

        prev_mv   = mv_map_from_history(conn, account_id, prev)           if prev else {}
        mtd_mv    = mv_map_from_history(conn, account_id, first_of_month)
        ytd_mv    = mv_map_from_history(conn, account_id, first_of_year)
        one_yr_mv = mv_map_from_history(conn, account_id, one_year_ago)

    df['market_value']  = pd.to_numeric(df['market_value'],  errors='coerce').fillna(0.0)
    df['marginal_tvar'] = pd.to_numeric(df['marginal_tvar'], errors='coerce')
    df['total_cost']    = pd.to_numeric(df['total_cost'],    errors='coerce').replace(0, float('nan'))

    df_agg = (
        df.groupby(['security_id', 'broker', 'broker_account'])
        .agg(
            security_name=('security_name', 'first'),
            asset_class=  ('asset_class',   'first'),
            currency=     ('currency',      'first'),
            ticker=       ('ticker',        'first'),
            market_value=   ('market_value',   'sum'),
            marginal_tvar=  ('marginal_tvar',  'sum'),
            total_cost=     ('total_cost',     lambda x: x.sum(min_count=1)),
        )
        .reset_index()
    )

    total_mv = float(df_agg['market_value'].sum())

    positions = []
    for _, row in df_agg.iterrows():
        sid    = row['security_id']
        broker = row['broker'] if pd.notna(row['broker']) else None
        ba     = row['broker_account'] if pd.notna(row['broker_account']) else None
        key    = (sid, broker, ba)
        mv     = float(row['market_value'])
        weight = round(mv / total_mv * 100, 2) if total_mv else None

        tc = row['total_cost']
        ug = round(mv - float(tc), 2) if pd.notna(tc) else None

        positions.append({
            "security_id":    sid,
            "name":           row['security_name'],
            "assetClass":     row['asset_class'],
            "ticker":         row['ticker'],
            "currency":       row['currency'],
            "marketValue":    round(mv, 2),
            "weight":         weight,
            "dayPnL":         pnl(mv, prev_mv, key),
            "dayReturn":      pct_return(mv, prev_mv, key),
            "mtdReturn":      pct_return(mv, mtd_mv, key),
            "ytdReturn":      pct_return(mv, ytd_mv, key),
            "oneYearReturn":  pct_return(mv, one_yr_mv, key),
            "varContrib":     float(row['marginal_tvar']) if pd.notna(row['marginal_tvar']) else None,
            "unrealizedGain": ug,
            "broker":         broker,
            "brokerAccount":  ba,
        })

    return positions
