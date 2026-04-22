"""
concentration_db.py — DB operations for the db_concentrations table.

Functions:
    delete_concentrations(account_id, as_of_date)
    write_concentrations(account_id, as_of_date, rows)
    read_concentrations(account_id)
"""
from __future__ import annotations

from database2 import pg_connection

# Display order matches the mock data and UI
_CATEGORY_ORDER = ['Asset Class', 'Region', 'Currency', 'Industry', 'Single Name']


def delete_concentrations(account_id: int, as_of_date) -> int:
    """Delete db_concentrations rows for (account_id, as_of_date). Returns row count deleted."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM db_concentrations WHERE account_id = %s AND as_of_date = %s",
                (account_id, as_of_date),
            )
            count = cur.rowcount
        conn.commit()
    return count


def write_concentrations(account_id: int, as_of_date, rows: list[dict]) -> None:
    """
    Upsert concentration rows into db_concentrations.

    Each row dict keys: category, category_name, max_weight, limit_value, ratio.
    """
    sql = """
        INSERT INTO db_concentrations
            (account_id, as_of_date, category, category_name,
             max_weight, limit_value, ratio, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (account_id, as_of_date, category) DO UPDATE SET
            category_name = EXCLUDED.category_name,
            max_weight    = EXCLUDED.max_weight,
            limit_value   = EXCLUDED.limit_value,
            ratio         = EXCLUDED.ratio,
            updated_at    = NOW()
    """
    data = [
        (
            account_id, as_of_date,
            r['category'], r.get('category_name'),
            r.get('max_weight'), r.get('limit_value'), r.get('ratio'),
        )
        for r in rows
    ]
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, data)
        conn.commit()


def read_concentrations(account_id: int) -> list[dict]:
    """
    Return concentration rows for the latest as_of_date for account_id,
    in canonical category order. Format: [{category, ratio}, ...].
    """
    sql = """
        SELECT category, ratio
        FROM db_concentrations
        WHERE account_id = %s
          AND as_of_date = (
              SELECT MAX(as_of_date) FROM db_concentrations WHERE account_id = %s
          )
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (account_id, account_id))
            rows = cur.fetchall()

    if not rows:
        return []

    row_map = {r[0]: r[1] for r in rows}
    return [
        {"category": cat, "ratio": row_map[cat]}
        for cat in _CATEGORY_ORDER
        if cat in row_map
    ]
