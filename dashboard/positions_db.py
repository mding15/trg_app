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
    """Upsert rows into db_mv_history. mv_rows: list of {security_id, market_value}."""
    sql = """
        INSERT INTO db_mv_history (account_id, as_of_date, security_id, market_value)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (account_id, as_of_date, security_id)
        DO UPDATE SET market_value = EXCLUDED.market_value
    """
    rows = [(account_id, as_of_date, r["security_id"], r["market_value"])
            for r in mv_rows if r.get("security_id")]
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
             volatility, sharpe, beta, max_drawdown, top_five_conc, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (account_id, as_of_date) DO UPDATE SET
            aum              = EXCLUDED.aum,
            num_positions    = EXCLUDED.num_positions,
            day_pnl          = EXCLUDED.day_pnl,
            day_return       = EXCLUDED.day_return,
            mtd_return       = EXCLUDED.mtd_return,
            ytd_return       = EXCLUDED.ytd_return,
            one_year_return  = EXCLUDED.one_year_return,
            unrealized_gain  = EXCLUDED.unrealized_gain,
            var_1d_95        = EXCLUDED.var_1d_95,
            var_1d_99        = EXCLUDED.var_1d_99,
            var_10d_99       = EXCLUDED.var_10d_99,
            es_1d_95         = EXCLUDED.es_1d_95,
            es_99            = EXCLUDED.es_99,
            volatility       = EXCLUDED.volatility,
            sharpe           = EXCLUDED.sharpe,
            beta             = EXCLUDED.beta,
            max_drawdown     = EXCLUDED.max_drawdown,
            top_five_conc    = EXCLUDED.top_five_conc,
            updated_at       = NOW()
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
             ytd_return, one_year_return, var_contrib, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (account_id, as_of_date, security_id) DO UPDATE SET
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
            updated_at      = NOW()
    """
    rows = [(
        account_id, as_of_date,
        p["security_id"], p["ticker"], p["name"], p["assetClass"], p["currency"],
        p["marketValue"], p["weight"], p["dayPnL"], p["dayReturn"],
        p["mtdReturn"], p["ytdReturn"], p["oneYearReturn"], p["varContrib"],
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
    sql = """
        SELECT as_of_date, aum, num_positions, day_pnl, day_return,
               mtd_return, ytd_return, one_year_return,
               unrealized_gain, var_1d_95, var_1d_99, var_10d_99,
               es_1d_95, es_99,
               volatility, sharpe, beta, max_drawdown, top_five_conc
        FROM db_portfolio_summary
        WHERE account_id = %s
        ORDER BY as_of_date DESC
        LIMIT 1
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (account_id,))
            row = cur.fetchone()

    if not row:
        return {}

    aum = row[1]

    def _pct(value):
        if value is None or aum is None or aum == 0:
            return None
        return value / aum * 100

    return {
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
        "maxDrawdown":    row[17],
        "topFiveConc":    row[18],
    }


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

def read_risk_parameters(account_id: int) -> dict:
    """Return portfolio parameters for the Risk page parameters table.

    Reads account_run_parameters for all run-time fields (name, dates, tail
    measure, risk horizon, benchmark, return frequency, expected return,
    base currency). AUM / portfolio size is fetched separately from
    db_portfolio_summary because account_run_parameters has no market value.
    """
    arp_sql = """
        SELECT "PortfolioName", "AsofDate", "ReportDate", "TailMeasure",
               "RiskHorizon", "Benchmark", "ReturnFrequency", "ExpectedReturn",
               "BaseCurrency"
        FROM account_run_parameters
        WHERE account_id = %s
    """
    ps_sql = """
        SELECT aum, as_of_date
        FROM db_portfolio_summary
        WHERE account_id = %s
        ORDER BY as_of_date DESC
        LIMIT 1
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(arp_sql, (account_id,))
            arp = cur.fetchone()
            cur.execute(ps_sql, (account_id,))
            ps = cur.fetchone()

    def _fmt_date(d):
        if d is None:
            return None
        return d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]

    aum     = ps[0] if ps else None
    size_mm = round(float(aum) / 1e6, 1) if aum else None

    return {
        "portfolioName":   arp[0] if arp else None,
        "portfolioSizeMm": size_mm,
        "asOfDate":        _fmt_date(arp[1]) if arp else None,
        "reportDate":      _fmt_date(arp[2]) if arp else None,
        "tailMeasure":     arp[3] if arp else None,
        "varVolWindow":    arp[4] if arp else None,
        "benchmark":       arp[5] if arp else None,
        "returnFrequency": arp[6] if arp else None,
        "expectedReturns": arp[7] if arp else None,
        "baseCurrency":    arp[8] if arp else None,
    }


# ── Chart data (computed from db_mv_history) ───────────────────────────────────

def compute_chart_data(account_id: int, range_key: str) -> list[dict]:
    """
    Compute portfolio total-value time series from db_mv_history.

    Returns a list of {label, value, bmk} dicts where:
    - value is the total market value in dollars (sum over all securities)
    - bmk is None (benchmark data not yet available)
    - label format: "Mon DD" for 1M, "Mon 'YY" for all other ranges

    range_key: "1M" | "3M" | "1Y" | "3Y" | "ALL"
    """
    from datetime import date, timedelta

    today = date.today()
    range_days = {"1M": 30, "3M": 90, "1Y": 365, "3Y": 365 * 3}
    days = range_days.get(range_key.upper())
    label_fmt = "%b %d" if range_key.upper() == "1M" else "%b '%y"

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

    return [
        {
            "label": row[0].strftime(label_fmt),
            "value": float(row[1]) if row[1] is not None else None,
            "bmk":   None,
        }
        for row in rows
    ]


def read_positions(account_id: int) -> list[dict]:
    """Return positions for the latest as_of_date for account_id."""
    sql = """
        SELECT security_id, ticker, name, asset_class, currency, market_value, weight,
               day_pnl, day_return, mtd_return, ytd_return, one_year_return, var_contrib
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
            "security_id":   r[0],
            "ticker":        r[1],
            "name":          r[2],
            "assetClass":    r[3],
            "currency":      r[4],
            "marketValue":   r[5],
            "weight":        r[6],
            "dayPnL":        r[7],
            "dayReturn":     r[8],
            "mtdReturn":     r[9],
            "ytdReturn":     r[10],
            "oneYearReturn": r[11],
            "varContrib":    r[12],
        }
        for r in rows
    ]
