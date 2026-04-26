"""
breakdown_db.py — DB operations for the db_portfolio_breakdown table.

Functions:
    delete_breakdowns(account_id, as_of_date)
    write_breakdowns(account_id, as_of_date, rows)
"""
from __future__ import annotations

from database2 import pg_connection


def delete_breakdowns(account_id: int, as_of_date) -> int:
    """Delete db_portfolio_breakdown rows for (account_id, as_of_date). Returns row count deleted."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM db_portfolio_breakdown WHERE account_id = %s AND as_of_date = %s",
                (account_id, as_of_date),
            )
            count = cur.rowcount
        conn.commit()
    return count


def write_breakdowns(account_id: int, as_of_date, rows: list[dict]) -> None:
    """Upsert breakdown rows into db_portfolio_breakdown.

    Each row dict keys: breakdown_type, category, weight, var_contrib.
    """
    sql = """
        INSERT INTO db_portfolio_breakdown
            (account_id, as_of_date, breakdown_type, category,
             weight, var_contrib, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (account_id, as_of_date, breakdown_type, category) DO UPDATE SET
            weight      = EXCLUDED.weight,
            var_contrib = EXCLUDED.var_contrib,
            updated_at  = NOW()
    """
    data = [
        (
            account_id, as_of_date,
            r['breakdown_type'], r['category'],
            r.get('weight'), r.get('var_contrib'),
        )
        for r in rows
    ]
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, data)
        conn.commit()
