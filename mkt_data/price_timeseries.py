"""
price_timeseries.py — Current price lookups from the database.

Functions:
    get_current_price(security_ids, as_of_date) -> dict {SecurityID: (Close, Date)}
"""
from __future__ import annotations

import os
import sys

from database2 import pg_connection

# Cache keyed by (frozenset(security_ids), as_of_date).
_cache: dict[tuple, dict] = {}


def get_current_price(security_ids: list[str], as_of_date) -> dict[str, tuple]:
    """
    Batch-fetch closing prices from current_price for the given SecurityIDs.
    Uses the most recent price on or before as_of_date.
    Returns {SecurityID: (Close, Date)}.

    Results are cached per (security_ids, as_of_date) — repeated calls with
    the same arguments do not hit the database again.
    """
    if not security_ids:
        return {}

    key = (frozenset(security_ids), as_of_date)
    if key in _cache:
        return _cache[key]

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON ("SecurityID") "SecurityID", "Close", "Date"
                FROM current_price
                WHERE "Date" <= %s AND "SecurityID" = ANY(%s)
                ORDER BY "SecurityID", "Date" DESC
                """,
                (as_of_date, list(security_ids)),
            )
            result = {row[0]: (float(row[1]), row[2]) for row in cur.fetchall()}

    _cache[key] = result
    return result
