"""
positions_db.py — Create and access the three dashboard DB tables.

Tables:
    db_mv_history        — daily per-security market value snapshots
    db_portfolio_summary — pre-computed portfolio summary per (account_id, as_of_date)
    db_positions         — pre-computed positions per (account_id, as_of_date, security_id)
"""
from __future__ import annotations

import logging

from database2 import pg_connection

logger = logging.getLogger(__name__)


# ── Write ─────────────────────────────────────────────────────────────────────

def delete_mv_history(account_id: int, as_of_date) -> int:
    """Delete all db_mv_history rows for (account_id, as_of_date). Returns row count deleted."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM db_mv_history WHERE account_id = %s AND as_of_date = %s",
                (account_id, as_of_date),
            )
            count = cur.rowcount
        conn.commit()
    return count


def write_mv_history(account_id: int, as_of_date, mv_rows: list[dict]) -> None:
    """Upsert rows into db_mv_history. mv_rows: list of {security_id, broker, broker_account, market_value}."""
    sql = """
        INSERT INTO db_mv_history (account_id, as_of_date, security_id, broker, broker_account, market_value)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (account_id, as_of_date, security_id, broker, broker_account)
        DO UPDATE SET market_value = EXCLUDED.market_value
    """
    rows = [
        (account_id, as_of_date, r["security_id"], r.get("broker"), r.get("broker_account"), r["market_value"])
        for r in mv_rows if r.get("security_id")
    ]
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()


def delete_portfolio_summary(account_id: int, as_of_date) -> int:
    """Delete db_portfolio_summary rows for (account_id, as_of_date). Returns row count deleted."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM db_portfolio_summary WHERE account_id = %s AND as_of_date = %s",
                (account_id, as_of_date),
            )
            count = cur.rowcount
        conn.commit()
    return count


def write_portfolio_summary(account_id: int, summary: dict) -> None:
    """Upsert a portfolio summary row into db_portfolio_summary.

    Required keys: asOfDate, aum, numPositions, dayPnL, dayReturn,
                   mtdReturn, ytdReturn, oneYearReturn
    Optional keys (risk metrics, default None):
        unrealizedGain, var1d95, var1d99, var10d99, es1d95, es99,
        volatility, sharpe, beta, maxDrawdown, topFiveConc
    """
    sql = """
        INSERT INTO db_portfolio_summary
            (account_id, as_of_date, aum, num_positions, day_pnl,
             day_return, mtd_return, ytd_return, one_year_return,
             unrealized_gain, var_1d_95, var_1d_99, var_10d_99,
             es_1d_95, es_99,
             volatility, sharpe, beta, max_drawdown, top_five_conc,
             three_year_return, si_return, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, NOW())
        ON CONFLICT (account_id, as_of_date) DO UPDATE SET
            aum               = EXCLUDED.aum,
            num_positions     = EXCLUDED.num_positions,
            day_pnl           = EXCLUDED.day_pnl,
            day_return        = EXCLUDED.day_return,
            mtd_return        = EXCLUDED.mtd_return,
            ytd_return        = EXCLUDED.ytd_return,
            one_year_return   = EXCLUDED.one_year_return,
            unrealized_gain   = EXCLUDED.unrealized_gain,
            var_1d_95         = EXCLUDED.var_1d_95,
            var_1d_99         = EXCLUDED.var_1d_99,
            var_10d_99        = EXCLUDED.var_10d_99,
            es_1d_95          = EXCLUDED.es_1d_95,
            es_99             = EXCLUDED.es_99,
            volatility        = EXCLUDED.volatility,
            sharpe            = EXCLUDED.sharpe,
            beta              = EXCLUDED.beta,
            max_drawdown      = EXCLUDED.max_drawdown,
            top_five_conc     = EXCLUDED.top_five_conc,
            three_year_return = EXCLUDED.three_year_return,
            si_return         = EXCLUDED.si_return,
            updated_at        = NOW()
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                account_id,
                summary["asOfDate"],
                summary["aum"],
                summary["numPositions"],
                summary["dayPnL"],
                summary["dayReturn"],
                summary["mtdReturn"],
                summary["ytdReturn"],
                summary["oneYearReturn"],
                summary.get("unrealizedGain"),
                summary.get("var1d95"),
                summary.get("var1d99"),
                summary.get("var10d99"),
                summary.get("es1d95"),
                summary.get("es99"),
                summary.get("volatility"),
                summary.get("sharpe"),
                summary.get("beta"),
                summary.get("maxDrawdown"),
                summary.get("topFiveConc"),
                summary.get("threeYearReturn"),
                summary.get("siReturn"),
            ))
        conn.commit()


def delete_positions(account_id: int, as_of_date) -> int:
    """Delete db_positions rows for (account_id, as_of_date). Returns row count deleted."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM db_positions WHERE account_id = %s AND as_of_date = %s",
                (account_id, as_of_date),
            )
            count = cur.rowcount
        conn.commit()
    return count


def write_positions(account_id: int, as_of_date, positions: list[dict]) -> None:
    """Upsert position rows into db_positions."""
    sql = """
        INSERT INTO db_positions
            (account_id, as_of_date, security_id, ticker, name, asset_class, currency,
             market_value, weight, day_pnl, day_return, mtd_return,
             ytd_return, one_year_return, var_contrib, unrealized_gain,
             broker, broker_account, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (account_id, as_of_date, security_id, broker, broker_account) DO UPDATE SET
            ticker          = EXCLUDED.ticker,
            name            = EXCLUDED.name,
            asset_class     = EXCLUDED.asset_class,
            currency        = EXCLUDED.currency,
            market_value    = EXCLUDED.market_value,
            weight          = EXCLUDED.weight,
            day_pnl         = EXCLUDED.day_pnl,
            day_return      = EXCLUDED.day_return,
            mtd_return      = EXCLUDED.mtd_return,
            ytd_return      = EXCLUDED.ytd_return,
            one_year_return = EXCLUDED.one_year_return,
            var_contrib     = EXCLUDED.var_contrib,
            unrealized_gain = EXCLUDED.unrealized_gain,
            updated_at      = NOW()
    """
    rows = [(
        account_id, as_of_date,
        p["security_id"], p["ticker"], p["name"], p["assetClass"], p["currency"],
        p["marketValue"], p["weight"], p["dayPnL"], p["dayReturn"],
        p["mtdReturn"], p["ytdReturn"], p["oneYearReturn"], p["varContrib"],
        p.get("unrealizedGain"), p.get("broker"), p.get("brokerAccount"),
    ) for p in positions]
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()


# ── User -> account mapping ───────────────────────────────────────────────────

def get_account_ids_for_user(username: str) -> list[int]:
    """
    Return all account_ids the given username has access to, ordered by account_id.
    Joins: user (username -> user_id) -> account_access (user_id -> account_id).
    Returns an empty list if the user is not found or has no accounts assigned.
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT aa.account_id
                FROM "user" u
                JOIN account_access aa ON aa.user_id = u.user_id
                WHERE u.username = %s
                ORDER BY aa.account_id
                """,
                (username,),
            )
            result = [row[0] for row in cur.fetchall()]
            logger.debug("get_account_ids_for_user(%r) -> %r", username, result)
            return result


def get_accounts_for_user(username: str) -> list[dict]:
    """
    Return all accounts the given username has access to.
    Default account (is_default=True) is returned first, then by account_id.
    Joins: user -> account_access -> account.
    Returns a list of {account_id, account_name, short_name, is_default} dicts.
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.account_id, a.account_name, a.short_name, aa.is_default
                FROM "user" u
                JOIN account_access aa ON aa.user_id = u.user_id
                JOIN account a ON a.account_id = aa.account_id
                WHERE u.username = %s
                ORDER BY aa.is_default DESC, a.account_id
                """,
                (username,),
            )
            result = [
                {
                    "account_id":   row[0],
                    "account_name": row[1],
                    "short_name":   row[2],
                    "is_default":   row[3],
                }
                for row in cur.fetchall()
            ]
            logger.debug("get_accounts_for_user(%r) -> %r", username, result)
            return result


def user_has_account_access(username: str, account_id: int) -> bool:
    """Return True if the given username has access to the given account_id."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM "user" u
                JOIN account_access aa ON aa.user_id = u.user_id
                WHERE u.username = %s AND aa.account_id = %s
                LIMIT 1
                """,
                (username, account_id),
            )
            return cur.fetchone() is not None


# ── Read (internal) ───────────────────────────────────────────────────────────

def get_mv_history_dates(account_id: int) -> set:
    """Return the set of as_of_dates already written to db_mv_history for account_id."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT as_of_date FROM db_mv_history WHERE account_id = %s",
                (account_id,),
            )
            return {row[0] for row in cur.fetchall()}


# ── Read (API) ────────────────────────────────────────────────────────────────

def read_portfolio_summary(account_id: int) -> dict:
    """Return the latest portfolio summary for account_id, including all risk metrics."""
    from dashboard.metric_utils import resolve_metric

    ps_sql = """
        SELECT as_of_date, aum, num_positions, day_pnl, day_return,
               mtd_return, ytd_return, one_year_return,
               unrealized_gain, var_1d_95, var_1d_99, var_10d_99,
               es_1d_95, es_99,
               volatility, sharpe, beta, max_drawdown, top_five_conc,
               three_year_return, si_return
        FROM db_portfolio_summary
        WHERE account_id = %s
        ORDER BY as_of_date DESC
        LIMIT 1
    """
    ap_sql = """
        SELECT risk_measure, risk_horizon
        FROM account_parameters
        WHERE account_id = %s
        ORDER BY updated_at DESC
        LIMIT 1
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ps_sql, (account_id,))
            row = cur.fetchone()
            cur.execute(ap_sql, (account_id,))
            ap = cur.fetchone()

    if not row:
        return {}

    aum = row[1]

    def _pct(value):
        if value is None or aum is None or aum == 0:
            return None
        return value / aum * 100

    result = {
        "asOfDate":       row[0].strftime("%Y-%m-%d"),
        "aum":            aum,
        "numPositions":   row[2],
        "dayPnL":         row[3],
        "dayReturn":      row[4],
        "mtdReturn":      row[5],
        "ytdReturn":      row[6],
        "oneYearReturn":  row[7],
        "unrealizedGain": row[8],
        "var1d95":        row[9],
        "var1d95Pct":     _pct(row[9]),
        "var1d99":        row[10],
        "var1d99Pct":     _pct(row[10]),
        "var10d99":       row[11],
        "var10d99Pct":    _pct(row[11]),
        "es1d95":         row[12],
        "es1d95Pct":      _pct(row[12]),
        "es99":           row[13],
        "es99Pct":        _pct(row[13]),
        "volatility":     row[14],
        "sharpe":         row[15],
        "beta":           row[16],
        "maxDrawdown":      row[17],
        "topFiveConc":      row[18],
        "threeYearReturn":  row[19],
        "siReturn":         row[20],
    }

    # Resolve configured strip-tile metric from account_parameters
    measure, horizon = (ap[0], ap[1]) if ap else (None, None)
    field, label = resolve_metric(measure, horizon)
    result["riskMetric"] = {
        "label": label or f"{measure} {horizon}" if measure else "—",
        "value": result.get(field) if field else None,
    }

    return result


# ── Asset allocation ──────────────────────────────────────────────────────────

def delete_asset_allocation(account_id: int, as_of_date) -> int:
    """Delete db_asset_allocation rows for (account_id, as_of_date). Returns row count."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM db_asset_allocation WHERE account_id = %s AND as_of_date = %s",
                (account_id, as_of_date),
            )
            count = cur.rowcount
        conn.commit()
    return count


def write_asset_allocation(account_id: int, as_of_date, rows: list[dict]) -> None:
    """Upsert asset-class rows into db_asset_allocation.

    Each row dict keys: assetClass, marketValue, weight, bmkWeight,
                        periodReturn, varContrib
    """
    sql = """
        INSERT INTO db_asset_allocation
            (account_id, as_of_date, asset_class, market_value, weight,
             bmk_weight, period_return, var_contrib, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (account_id, as_of_date, asset_class) DO UPDATE SET
            market_value   = EXCLUDED.market_value,
            weight         = EXCLUDED.weight,
            bmk_weight     = EXCLUDED.bmk_weight,
            period_return  = EXCLUDED.period_return,
            var_contrib    = EXCLUDED.var_contrib,
            updated_at     = NOW()
    """
    data = [
        (account_id, as_of_date,
         r["assetClass"], r.get("marketValue"), r.get("weight"),
         r.get("bmkWeight"), r.get("periodReturn"), r.get("varContrib"))
        for r in rows
    ]
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, data)
        conn.commit()


def read_asset_allocation(account_id: int) -> list[dict]:
    """Return asset allocation for the latest as_of_date for account_id."""
    sql = """
        SELECT asset_class, market_value, weight, bmk_weight, period_return, var_contrib
        FROM db_asset_allocation
        WHERE account_id = %s
          AND as_of_date = (
              SELECT MAX(as_of_date) FROM db_asset_allocation WHERE account_id = %s
          )
        ORDER BY weight DESC NULLS LAST
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (account_id, account_id))
            rows = cur.fetchall()
    return [
        {
            "assetClass":    r[0],
            "marketValue":   r[1],
            "weight":        r[2],
            "bmkWeight":     r[3],
            "periodReturn":  r[4],
            "varContrib":    r[5],
        }
        for r in rows
    ]


# ── Stress test results ────────────────────────────────────────────────────────

def delete_stress_results(account_id: int, as_of_date) -> int:
    """Delete db_stress_results rows for (account_id, as_of_date). Returns row count."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM db_stress_results WHERE account_id = %s AND as_of_date = %s",
                (account_id, as_of_date),
            )
            count = cur.rowcount
        conn.commit()
    return count


def write_stress_results(account_id: int, as_of_date, rows: list[dict]) -> None:
    """Upsert stress-test result rows into db_stress_results.

    Each row dict keys: scenarioId (int), pnlUsd, pnlPct
    """
    sql = """
        INSERT INTO db_stress_results
            (account_id, as_of_date, scenario_id, pnl_usd, pnl_pct, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (account_id, as_of_date, scenario_id) DO UPDATE SET
            pnl_usd    = EXCLUDED.pnl_usd,
            pnl_pct    = EXCLUDED.pnl_pct,
            updated_at = NOW()
    """
    data = [
        (account_id, as_of_date, r["scenarioId"], r.get("pnlUsd"), r.get("pnlPct"))
        for r in rows
    ]
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, data)
        conn.commit()


# ── Risk alerts ────────────────────────────────────────────────────────────────

def delete_risk_alerts(account_id: int, as_of_date) -> int:
    """Delete db_risk_alerts rows for (account_id, as_of_date). Returns row count."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM db_risk_alerts WHERE account_id = %s AND as_of_date = %s",
                (account_id, as_of_date),
            )
            count = cur.rowcount
        conn.commit()
    return count


def write_risk_alerts(account_id: int, as_of_date, alerts: list[dict]) -> None:
    """Upsert risk alert rows into db_risk_alerts.

    Each alert dict keys: msg (str), level (str: warning / info / critical)
    Seq is assigned automatically as 1-based position in the list.
    """
    sql = """
        INSERT INTO db_risk_alerts
            (account_id, as_of_date, seq, msg, level, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (account_id, as_of_date, seq) DO UPDATE SET
            msg        = EXCLUDED.msg,
            level      = EXCLUDED.level,
            updated_at = NOW()
    """
    data = [
        (account_id, as_of_date, idx + 1, a["msg"], a["level"])
        for idx, a in enumerate(alerts)
    ]
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, data)
        conn.commit()


def read_risk_alerts(account_id: int) -> list[dict]:
    """Return risk alerts for the latest as_of_date, ordered by seq."""
    sql = """
        SELECT msg, level
        FROM db_risk_alerts
        WHERE account_id = %s
          AND as_of_date = (
              SELECT MAX(as_of_date) FROM db_risk_alerts WHERE account_id = %s
          )
        ORDER BY seq
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (account_id, account_id))
            rows = cur.fetchall()
    return [{"msg": r[0], "level": r[1]} for r in rows]


def count_risk_alerts(account_id: int) -> int:
    """Count risk alerts for the latest as_of_date (used for activeAlerts in risk summary)."""
    sql = """
        SELECT COUNT(*)
        FROM db_risk_alerts
        WHERE account_id = %s
          AND as_of_date = (
              SELECT MAX(as_of_date) FROM db_risk_alerts WHERE account_id = %s
          )
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (account_id, account_id))
            row = cur.fetchone()
    return row[0] if row else 0


# ── Account config ─────────────────────────────────────────────────────────────

def read_var_limit(account_id: int) -> float | None:
    """Return var_limit_pct from account_limit for the given account_id.

    NOTE: verify the column name matches your account_limit table definition.
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT limit_value FROM account_limit WHERE account_id = %s AND limit_category = 'var_limit_pct'",
                (account_id,),
            )
            row = cur.fetchone()
    return float(row[0]) if row and row[0] is not None else None


# ── Risk page parameters ──────────────────────────────────────────────────────

def read_risk_parameters(account_id: int) -> list[dict]:
    """Return portfolio settings rows for the Risk page parameters table."""
    ap_sql = """
        SELECT risk_horizon, risk_measure, base_currency, benchmark, exp_return
        FROM account_parameters
        WHERE account_id = %s
        ORDER BY updated_at DESC
        LIMIT 1
    """
    ps_sql = """
        SELECT MAX(as_of_date)
        FROM db_portfolio_summary
        WHERE account_id = %s
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ap_sql, (account_id,))
            ap = cur.fetchone()
            cur.execute(ps_sql, (account_id,))
            ps = cur.fetchone()

    def _fmt_date(d):
        if d is None:
            return None
        return d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]

    def _val(v):
        return v if v is not None else "—"

    as_of_date    = _fmt_date(ps[0]) if ps else None
    risk_horizon  = ap[0] if ap else None
    risk_measure  = ap[1] if ap else None
    base_currency = ap[2] if ap else None
    benchmark     = ap[3] if ap else None
    exp_return    = ap[4] if ap else None

    return [
        {"label": "As of Date",    "value": _val(as_of_date),    "badge": False},
        {"label": "Risk Horizon",  "value": _val(risk_horizon),  "badge": True},
        {"label": "Risk Measure",  "value": _val(risk_measure),  "badge": True},
        {"label": "Base Currency", "value": _val(base_currency), "badge": True},
        {"label": "Benchmark",     "value": _val(benchmark),     "badge": True},
        {"label": "Exp. Return",   "value": _val(exp_return),    "badge": True},
    ]


# ── Risk page measures ────────────────────────────────────────────────────────

def read_risk_measures(account_id: int) -> list[dict]:
    """Return the four Risk Measures tiles from the latest db_portfolio_summary row."""
    sql = """
        SELECT aum, es_1d_95, volatility, beta
        FROM db_portfolio_summary
        WHERE account_id = %s
        ORDER BY as_of_date DESC
        LIMIT 1
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (account_id,))
            row = cur.fetchone()

    def _mm(v):
        return str(round(float(v) / 1e6, 1)) if v is not None else "—"

    def _es(v):
        """Format ES value: K if <1M, 0.0M if 1M–10M, 0M if >=10M."""
        if v is None:
            return "—", None
        f = float(v)
        if f < 1_000_000:
            return str(round(f / 1_000)), "K"
        elif f < 10_000_000:
            return str(round(f / 1_000_000, 1)), "M"
        else:
            return str(round(f / 1_000_000)), "M"

    def _pct(v):
        return str(round(float(v), 1)) if v is not None else "—"

    def _ratio(v):
        return str(round(float(v), 2)) if v is not None else "—"

    aum      = row[0] if row else None
    es_1d_95 = row[1] if row else None
    vol      = row[2] if row else None
    beta     = row[3] if row else None

    es_value, es_unit = _es(es_1d_95)

    return [
        {"label": "Market Value", "value": _mm(aum),    "unit": "M",     "sub": None},
        {"label": "ES 95% 1D",   "value": es_value,     "unit": es_unit, "sub": None},
        {"label": "Volatility",  "value": _pct(vol),    "unit": "%",     "sub": "annualised"},
        {"label": "Beta",        "value": _ratio(beta), "unit": None,    "sub": "vs S&P 500"},
    ]


# ── Chart data (computed from db_mv_history) ───────────────────────────────────

def compute_chart_data(account_id: int, range_key: str) -> list[dict]:
    """
    Compute portfolio total-value time series from db_mv_history.

    Returns a list of {label, value, bmk} dicts where:
    - value is the total market value in dollars (sum over all securities)
    - bmk is the SP500 benchmark value scaled so its first point equals the
      portfolio's first value, then grows proportionally to benchmark_hist
    - label format: "Mon DD" for 1M/3M, "Mon 'YY" for all other ranges

    range_key: "1M" | "3M" | "1Y" | "3Y" | "ALL"
    """
    import pandas as pd
    from datetime import date, timedelta
    from dashboard.portfolio_chart import (
        _fetch_benchmark_ids, _fetch_benchmark_hist,
        _align_benchmark, _scale,
    )

    today = date.today()
    range_days = {"1M": 30, "3M": 90, "1Y": 365, "3Y": 365 * 3}
    days = range_days.get(range_key.upper())
    label_fmt = "%b %d" if range_key.upper() in ["1M", "3M"] else "%b '%y"

    if days is not None:
        cutoff = today - timedelta(days=days)
        sql = """
            SELECT as_of_date, SUM(market_value) AS total_mv
            FROM db_mv_history
            WHERE account_id = %s AND as_of_date >= %s
            GROUP BY as_of_date
            ORDER BY as_of_date
        """
        params = (account_id, cutoff)
    else:
        sql = """
            SELECT as_of_date, SUM(market_value) AS total_mv
            FROM db_mv_history
            WHERE account_id = %s
            GROUP BY as_of_date
            ORDER BY as_of_date
        """
        params = (account_id,)

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    if not rows:
        return []

    # ── Portfolio series ──────────────────────────────────────────────────────
    port_dates  = [r[0] for r in rows]
    port_values = [float(r[1]) if r[1] is not None else None for r in rows]
    port_idx    = pd.DatetimeIndex(port_dates)
    port_first  = next((v for v in port_values if v is not None), None)

    # ── SP500 benchmark ───────────────────────────────────────────────────────
    bmk_values = [None] * len(rows)
    if port_first is not None:
        key_to_id = _fetch_benchmark_ids()
        sp500_id  = key_to_id.get("sp500")
        if sp500_id is not None:
            from_date = port_dates[0]
            to_date   = port_dates[-1]
            # Fetch 14 days before from_date so forward-fill covers the first point
            bmk_hist  = _fetch_benchmark_hist([sp500_id], from_date - timedelta(days=14), to_date)
            if sp500_id in bmk_hist:
                aligned   = _align_benchmark(bmk_hist[sp500_id], port_idx)
                first_val = aligned.dropna()
                if not first_val.empty:
                    bmk_values = _scale(aligned, float(first_val.iloc[0]), port_first)

    # ── Assemble response ─────────────────────────────────────────────────────
    return [
        {
            "label":      port_dates[i].strftime(label_fmt),
            "as_of_date": port_dates[i].strftime("%Y-%m-%d"),
            "value":      port_values[i],
            "bmk":        bmk_values[i],
        }
        for i in range(len(rows))
    ]


def read_positions(account_id: int) -> list[dict]:
    """Return positions for the latest as_of_date for account_id."""
    sql = """
        SELECT id, ticker, name, asset_class, currency, market_value, weight,
               day_pnl, day_return, mtd_return, ytd_return, one_year_return, var_contrib,
               unrealized_gain, broker, broker_account
        FROM db_positions
        WHERE account_id = %s
          AND as_of_date = (
              SELECT MAX(as_of_date) FROM db_positions WHERE account_id = %s
          )
        ORDER BY market_value DESC NULLS LAST
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (account_id, account_id))
            rows = cur.fetchall()

    return [
        {
            "id":             r[0],
            "ticker":         r[1],
            "name":           r[2],
            "assetClass":     r[3],
            "currency":       r[4],
            "marketValue":    r[5],
            "weight":         r[6],
            "dayPnL":         r[7],
            "dayReturn":      r[8],
            "mtdReturn":      r[9],
            "ytdReturn":      r[10],
            "oneYearReturn":  r[11],
            "varContrib":     r[12],
            "unrealizedGain": r[13],
            "broker":         r[14],
            "brokerAccount":  r[15],
        }
        for r in rows
    ]


def get_broker_summary(account_id: int) -> list[dict]:
    """
    Return per-(broker, broker_account) summary for account_id from db_positions.
    Columns: broker, brokerAccount, marketValue, dayReturn, var1d.
    dayReturn is computed as SUM(day_pnl) / SUM(market_value) * 100.
    Rows ordered by market_value descending.
    """
    sql = """
        SELECT
            COALESCE(broker, '—')         AS broker,
            COALESCE(broker_account, '—') AS broker_account,
            SUM(market_value)             AS market_value,
            SUM(day_pnl)                  AS day_pnl,
            SUM(var_contrib)              AS var1d
        FROM db_positions
        WHERE account_id = %s
          AND as_of_date = (
              SELECT MAX(as_of_date) FROM db_positions WHERE account_id = %s
          )
        GROUP BY broker, broker_account
        ORDER BY broker, broker_account
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (account_id, account_id))
            rows = cur.fetchall()

    result = []
    for broker, broker_account, mv, day_pnl, var1d in rows:
        mv = float(mv) if mv is not None else 0.0
        day_return = (
            round(float(day_pnl) / mv * 100, 2)
            if mv and day_pnl is not None else None
        )
        result.append({
            "broker":        broker,
            "brokerAccount": broker_account,
            "marketValue":   round(mv, 2),
            "dayReturn":     day_return,
            "var1d":         round(float(var1d), 2) if var1d is not None else None,
        })
    return result


def get_top_risk_contributors(account_id: int, n: int = 10) -> list[dict]:
    """
    Return up to N positions by % VaR contribution from db_positions.

    Selection logic:
      neg_quota = floor(n * 0.4)
      negatives — positions where pct <= -0.5%, sorted by most negative first;
                  up to neg_quota are selected.
      positives — remaining n - num_neg_actual slots, sorted by pct descending.

    Output order: positives (largest → smallest), then negatives (least negative → most negative).
    Each entry: {name, pct}  where pct = var_contrib / total_var_contrib * 100.
    Returns [] if no data or total_var_contrib is zero.
    """
    import math

    sql = """
        SELECT COALESCE(NULLIF(MIN(ticker), ''), MIN(name)) AS label,
               SUM(var_contrib) AS var_contrib
        FROM db_positions
        WHERE account_id = %s
          AND as_of_date = (
              SELECT MAX(as_of_date) FROM db_positions WHERE account_id = %s
          )
          AND var_contrib IS NOT NULL
        GROUP BY security_id
        HAVING SUM(var_contrib) IS NOT NULL
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (account_id, account_id))
            rows = cur.fetchall()

    if not rows:
        return []

    entries = [{"name": row[0] or "", "var_contrib": float(row[1])} for row in rows]

    total = sum(e["var_contrib"] for e in entries)
    if total == 0:
        return []

    for e in entries:
        e["pct"] = round(e["var_contrib"] / total * 100, 2)

    neg_quota = math.floor(n * 0.4)

    # Negatives: must be <= -0.5%, pick the most negative ones up to quota
    negatives = sorted(
        (e for e in entries if e["pct"] <= -0.5),
        key=lambda x: x["pct"],   # ascending: most negative first
    )
    selected_neg = negatives[:neg_quota]

    # Positives: fill remaining slots
    num_pos = n - len(selected_neg)
    positives = sorted(
        (e for e in entries if e["pct"] > 0),
        key=lambda x: x["pct"],
        reverse=True,
    )
    selected_pos = positives[:num_pos]

    # Output: positives descending, then negatives least-negative → most-negative
    selected_neg_out = sorted(selected_neg, key=lambda x: x["pct"], reverse=True)

    return [
        {"name": e["name"], "pct": e["pct"]}
        for e in selected_pos + selected_neg_out
    ]
