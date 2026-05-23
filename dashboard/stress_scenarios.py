"""
stress_scenarios.py — DB-backed stress scenario results for the API.

get_stress_scenarios(account_id) — query st_account_summary + st_scenarios + position_var
                                   and return the list expected by the frontend.

Response shape per item:
    {
        "name":      str,
        "period":    str,
        "type":      str,   # "Historical" | "Hypothetical"
        "impactUSD": float, # st_pnl / 1_000_000
        "impactPct": float, # st_pnl / total_account_market_value * 100
    }
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database2 import pg_connection


def get_stress_scenarios(account_id: int) -> list[dict]:
    """
    Return stress scenario results for account_id, or [] if no data is available.

    Uses the latest as_of_date present in st_account_summary for the account.
    Market value is summed from position_var for the same date.
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:

            # Latest date with results for this account
            cur.execute(
                'SELECT MAX(as_of_date) FROM st_account_summary WHERE account_id = %s',
                (account_id,),
            )
            row = cur.fetchone()
            if not row or row[0] is None:
                return []
            as_of_date = row[0]

            # Total account market value from position_var
            cur.execute(
                'SELECT SUM(market_value) FROM position_var WHERE account_id = %s AND as_of_date = %s',
                (account_id, as_of_date),
            )
            mv_row = cur.fetchone()
            total_mv = float(mv_row[0]) if mv_row and mv_row[0] else None

            # Stress PnL joined with scenario metadata
            cur.execute(
                """
                SELECT s.name, s.period, s.type, sa.st_pnl
                FROM st_account_summary sa
                JOIN st_scenarios s ON s.scenario_id = sa.scenario_id
                WHERE sa.account_id = %s
                  AND sa.as_of_date = %s
                  AND s.is_active = TRUE
                ORDER BY sa.st_pnl ASC
                """,
                (account_id, as_of_date),
            )
            rows = cur.fetchall()

    if not rows:
        return []

    results = []
    for name, period, scenario_type, st_pnl in rows:
        pnl = float(st_pnl) if st_pnl is not None else 0.0
        impact_usd = pnl / 1_000_000
        impact_pct = (pnl / total_mv * 100) if total_mv else None
        results.append({
            'name':      name,
            'period':    period,
            'type':      scenario_type,
            'impactUSD': impact_usd,
            'impactPct': impact_pct,
        })

    return results
