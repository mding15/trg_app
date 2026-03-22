"""
positions_db.py — Create and access the three dashboard DB tables.

Tables:
    db_mv_history        — daily per-security market value snapshots
    db_portfolio_summary — pre-computed portfolio summary per (account_id, as_of_date)
    db_positions         — pre-computed positions per (account_id, as_of_date, security_id)
"""
from __future__ import annotations

from database2 import pg_connection


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
    """Upsert a portfolio summary row into db_portfolio_summary."""
    sql = """
        INSERT INTO db_portfolio_summary
            (account_id, as_of_date, aum, num_positions, day_pnl,
             day_return, mtd_return, ytd_return, one_year_return, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (account_id, as_of_date) DO UPDATE SET
            aum             = EXCLUDED.aum,
            num_positions   = EXCLUDED.num_positions,
            day_pnl         = EXCLUDED.day_pnl,
            day_return      = EXCLUDED.day_return,
            mtd_return      = EXCLUDED.mtd_return,
            ytd_return      = EXCLUDED.ytd_return,
            one_year_return = EXCLUDED.one_year_return,
            updated_at      = NOW()
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
            return [row[0] for row in cur.fetchall()]


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
    """Return the latest portfolio summary for account_id."""
    sql = """
        SELECT as_of_date, aum, num_positions, day_pnl, day_return,
               mtd_return, ytd_return, one_year_return
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

    return {
        "asOfDate":      row[0].strftime("%Y-%m-%d"),
        "aum":           row[1],
        "numPositions":  row[2],
        "dayPnL":        row[3],
        "dayReturn":     row[4],
        "mtdReturn":     row[5],
        "ytdReturn":     row[6],
        "oneYearReturn": row[7],
    }


def read_positions(account_id: int) -> list[dict]:
    """Return positions for the latest as_of_date for account_id."""
    sql = """
        SELECT security_id, name, asset_class, currency, market_value, weight,
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
            "name":          r[1],
            "assetClass":    r[2],
            "currency":      r[3],
            "marketValue":   r[4],
            "weight":        r[5],
            "dayPnL":        r[6],
            "dayReturn":     r[7],
            "mtdReturn":     r[8],
            "ytdReturn":     r[9],
            "oneYearReturn": r[10],
            "varContrib":    r[11],
        }
        for r in rows
    ]
