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
                   asset_class, marginal_tvar
            FROM position_var
            WHERE as_of_date = %s AND account_id = %s
            """,
            (as_of_date, account_id),
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ── db_mv_history helpers ─────────────────────────────────────────────────────

def mv_map_from_history(conn, account_id: int, ref_date) -> dict[str, float]:
    """Return {security_id: market_value} for the closest as_of_date <= ref_date."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT security_id, market_value FROM db_mv_history
            WHERE account_id = %s AND as_of_date = (
                SELECT MAX(as_of_date) FROM db_mv_history
                WHERE account_id = %s AND as_of_date <= %s
            )
            """,
            (account_id, account_id, ref_date),
        )
        rows = cur.fetchall()
    return {r["security_id"]: float(r["market_value"]) for r in rows if r["market_value"] is not None}


def total_mv_from_history(conn, account_id: int, ref_date) -> float | None:
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


def prev_date_from_history(conn, account_id: int, before_date) -> object | None:
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


def pct_return_total(current: float, base: float | None) -> float | None:
    """Portfolio-level percentage return, or None if base unavailable."""
    if base is None or base == 0:
        return None
    return round((current - base) / abs(base) * 100, 2)


# ── Main computation functions ────────────────────────────────────────────────

def compute_portfolio_summary(account_id: int, as_of_date=None) -> dict:
    """
    Compute portfolio-level summary for account_id.
    Current AUM and risk metrics come from position_var;
    return metrics are calculated from db_mv_history.

    as_of_date: if provided, use that date; otherwise use the latest date in position_var.

    Risk metric formulas:
        var_1d_95  = SUM(marginal_var)
        var_1d_99  = var_1d_95 * 1.41
        var_10d_99 = var_1d_99 * sqrt(10)
        es_1d_95   = SUM(marginal_tvar)
        es_99      = es_1d_95 * 1.41
        volatility = SUM(marginal_std) / aum * sqrt(252)  [stored as %]
        total_ret  = SUM(market_value * expected_return) / aum
        sharpe     = total_ret / volatility_ratio
        beta       = 1.2  [hardcoded placeholder]
        max_drawdown = -12.5  [hardcoded placeholder, %]
        unrealized_gain = aum * 10%  [placeholder]
        top_five_conc = top-5 security MV / aum * 100  [%]
    """
    with pg_connection() as conn:
        if as_of_date is not None:
            import datetime
            latest = pd.to_datetime(as_of_date).date() if not isinstance(as_of_date, datetime.date) else as_of_date
        else:
            dates = get_latest_feed_dates(conn, n=1)
            if not dates:
                return {}
            latest = dates[0]
        first_of_month = latest.replace(day=1)
        first_of_year  = latest.replace(month=1, day=1)
        one_year_ago   = latest.replace(year=latest.year - 1)

        # AUM and position count
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(DISTINCT security_id), SUM(market_value)
                FROM position_var
                WHERE as_of_date = %s AND account_id = %s
                """,
                (latest, account_id),
            )
            count, aum_raw = cur.fetchone()
        aum = float(aum_raw) if aum_raw is not None else 0.0

        # Risk aggregates from position_var
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT SUM(marginal_var),
                       SUM(marginal_tvar),
                       SUM(marginal_std),
                       SUM(market_value * expected_return)
                FROM position_var
                WHERE as_of_date = %s AND account_id = %s
                """,
                (latest, account_id),
            )
            risk_row = cur.fetchone()
        sum_mvar, sum_mtvar, sum_mstd, sum_mv_er = risk_row if risk_row else (None, None, None, None)

        # Top-5 concentration (group by security_id, take highest 5 MVs)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT SUM(market_value) AS mv
                FROM position_var
                WHERE as_of_date = %s AND account_id = %s
                GROUP BY security_id
                ORDER BY mv DESC
                LIMIT 5
                """,
                (latest, account_id),
            )
            top5_mvs = [float(r[0]) for r in cur.fetchall() if r[0] is not None]

        # Historical reference AUMs for return calculations
        prev = prev_date_from_history(conn, account_id, latest)

        prev_aum   = total_mv_from_history(conn, account_id, prev)           if prev else None
        mtd_aum    = total_mv_from_history(conn, account_id, first_of_month)
        ytd_aum    = total_mv_from_history(conn, account_id, first_of_year)
        one_yr_aum = total_mv_from_history(conn, account_id, one_year_ago)

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
        sharpe = round(total_return / vol_ratio, 4)
    else:
        sharpe = None

    # ── Other metrics ───────────────────────────────────────────────────────────
    unrealized_gain = round(aum * 0.10, 2) if aum else None
    top_five_conc   = round(sum(top5_mvs) / aum * 100, 2) if aum > 0 else None

    return {
        "asOfDate":       latest.strftime("%Y-%m-%d"),
        "aum":            round(aum, 2),
        "numPositions":   int(count) if count else 0,
        "dayPnL":         day_pnl,
        "dayReturn":      pct_return_total(aum, prev_aum),
        "mtdReturn":      pct_return_total(aum, mtd_aum),
        "ytdReturn":      pct_return_total(aum, ytd_aum),
        "oneYearReturn":  pct_return_total(aum, one_yr_aum),
        "unrealizedGain": unrealized_gain,
        "var1d95":        var_1d_95,
        "var1d99":        var_1d_99,
        "var10d99":       var_10d_99,
        "es1d95":         es_1d_95,
        "es99":           es_99,
        "volatility":     volatility,
        "sharpe":         sharpe,
        "beta":           1.2,
        "maxDrawdown":    -12.5,
        "topFiveConc":    top_five_conc,
    }


def compute_positions(account_id: int, as_of_date=None) -> list[dict]:
    """
    Compute position-level data for account_id, aggregated by security_id.
    Current positions come from position_var; returns are calculated from db_mv_history.

    as_of_date: if provided, use that date; otherwise use the latest date in position_var.
    """
    with pg_connection() as conn:
        if as_of_date is not None:
            import datetime
            latest = pd.to_datetime(as_of_date).date() if not isinstance(as_of_date, datetime.date) else as_of_date
        else:
            dates = get_latest_feed_dates(conn, n=1)
            if not dates:
                return []
            latest = dates[0]
        first_of_month = latest.replace(day=1)
        first_of_year  = latest.replace(month=1, day=1)
        one_year_ago   = latest.replace(year=latest.year - 1)

        df = get_positions_on_date(conn, latest, account_id)
        if df.empty:
            return []

        prev = prev_date_from_history(conn, account_id, latest)

        prev_mv   = mv_map_from_history(conn, account_id, prev)           if prev else {}
        mtd_mv    = mv_map_from_history(conn, account_id, first_of_month)
        ytd_mv    = mv_map_from_history(conn, account_id, first_of_year)
        one_yr_mv = mv_map_from_history(conn, account_id, one_year_ago)

    df['market_value']  = pd.to_numeric(df['market_value'],  errors='coerce').fillna(0.0)
    df['marginal_tvar'] = pd.to_numeric(df['marginal_tvar'], errors='coerce')

    df_agg = (
        df.groupby('security_id')
        .agg(
            security_name=('security_name', 'first'),
            asset_class=  ('asset_class',   'first'),
            currency=     ('currency',      'first'),
            ticker=       ('ticker',        'first'),
            market_value= ('market_value',  'sum'),
            marginal_tvar=('marginal_tvar', 'sum'),
        )
        .reset_index()
    )

    total_mv = float(df_agg['market_value'].sum())

    positions = []
    for _, row in df_agg.iterrows():
        sid = row['security_id']
        mv  = float(row['market_value'])
        weight = round(mv / total_mv * 100, 2) if total_mv else None

        positions.append({
            "security_id":   sid,
            "name":          row['security_name'],
            "assetClass":    row['asset_class'],
            "ticker":        row['ticker'],
            "currency":      row['currency'],
            "marketValue":   round(mv, 2),
            "weight":        weight,
            "dayPnL":        pnl(mv, prev_mv, sid),
            "dayReturn":     pct_return(mv, prev_mv, sid),
            "mtdReturn":     pct_return(mv, mtd_mv, sid),
            "ytdReturn":     pct_return(mv, ytd_mv, sid),
            "oneYearReturn": pct_return(mv, one_yr_mv, sid),
            "varContrib":    float(row['marginal_tvar']) if pd.notna(row['marginal_tvar']) else None,
        })

    return positions
