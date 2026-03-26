"""
stress_test.py — Stress-test scenario seeding and results.

Functions:
    seed_scenarios(as_of_date)      — seed scenario_definition + account_scenario,
                                      then trigger calculate_stress_test for each account
    calculate_stress_test(account_id, as_of_date)
                                    — STUB: write placeholder P&L to db_stress_results
    read_stress_results(account_id) — read latest stress results joined with scenario names

CLI:
    python stress_test.py                        # seed with today's date
    python stress_test.py --date 2026-03-25      # seed with a specific date
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import date

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database2 import pg_connection
from dashboard.positions_db import write_stress_results
from dashboard.static_data import STRESS_SCENARIOS

logger = logging.getLogger(__name__)

_SEED_ACCOUNT_IDS = [1001, 1003]

# Name-keyed lookup for the stub calculator
_STATIC_PNL = {s["name"]: s for s in STRESS_SCENARIOS}


# ── Seeding ────────────────────────────────────────────────────────────────────

def seed_scenarios(as_of_date=None) -> None:
    """
    Seed scenario_definition and account_scenario from STRESS_SCENARIOS static data,
    then call calculate_stress_test for each account in _SEED_ACCOUNT_IDS.

    Scenarios already present in scenario_definition (matched by name) are skipped.
    account_scenario entries that already exist are skipped (ON CONFLICT DO NOTHING).

    Args:
        as_of_date: date for db_stress_results rows. Defaults to today.
    """
    if as_of_date is None:
        as_of_date = date.today()

    # Step 1: Insert new scenarios, collect name -> scenario_id mapping
    scenario_ids: dict[str, int] = {}
    with pg_connection() as conn:
        with conn.cursor() as cur:
            for s in STRESS_SCENARIOS:
                cur.execute(
                    "SELECT scenario_id FROM scenario_definition WHERE name = %s",
                    (s["name"],),
                )
                row = cur.fetchone()
                if row:
                    scenario_ids[s["name"]] = row[0]
                    logger.info(f"Scenario '{s['name']}' already exists (id={row[0]}), skipping.")
                else:
                    cur.execute(
                        """
                        INSERT INTO scenario_definition (name, period, severity)
                        VALUES (%s, %s, %s)
                        RETURNING scenario_id
                        """,
                        (s["name"], s["period"], s["severity"]),
                    )
                    sid = cur.fetchone()[0]
                    scenario_ids[s["name"]] = sid
                    logger.info(f"Inserted scenario '{s['name']}' (id={sid}).")
        conn.commit()

    # Step 2: Map all scenarios to each seed account
    with pg_connection() as conn:
        with conn.cursor() as cur:
            for account_id in _SEED_ACCOUNT_IDS:
                for sid in scenario_ids.values():
                    cur.execute(
                        """
                        INSERT INTO account_scenario (account_id, scenario_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (account_id, sid),
                    )
        conn.commit()
    logger.info(
        f"Mapped {len(scenario_ids)} scenario(s) to accounts {_SEED_ACCOUNT_IDS}."
    )

    # Step 3: Populate stress results for each account
    for account_id in _SEED_ACCOUNT_IDS:
        calculate_stress_test(account_id, as_of_date)


# ── Calculation stub ───────────────────────────────────────────────────────────

def calculate_stress_test(account_id: int, as_of_date) -> None:
    """
    STUB: Write placeholder stress-test P&L to db_stress_results.

    For each scenario assigned to account_id in account_scenario, the static
    pnlUsd / pnlPct from STRESS_SCENARIOS is used as placeholder data.

    Replace this implementation with real calculation logic when ready.
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT sd.scenario_id, sd.name
                FROM account_scenario acs
                JOIN scenario_definition sd ON sd.scenario_id = acs.scenario_id
                WHERE acs.account_id = %s AND sd.is_active = TRUE
                """,
                (account_id,),
            )
            rows = cur.fetchall()

    if not rows:
        logger.warning(
            f"calculate_stress_test: no active scenarios for account_id={account_id}."
        )
        return

    stress_rows = [
        {
            "scenarioId": scenario_id,
            "pnlUsd":     _STATIC_PNL.get(name, {}).get("pnlUsd"),
            "pnlPct":     _STATIC_PNL.get(name, {}).get("pnlPct"),
        }
        for scenario_id, name in rows
    ]

    write_stress_results(account_id, as_of_date, stress_rows)
    logger.info(
        f"calculate_stress_test: wrote {len(stress_rows)} row(s) "
        f"for account_id={account_id} on {as_of_date}."
    )


# ── Read ───────────────────────────────────────────────────────────────────────

def read_stress_results(account_id: int) -> list[dict]:
    """Return stress results for the latest as_of_date, joined with scenario_definition."""
    sql = """
        SELECT sd.name, sd.period, sr.pnl_usd, sr.pnl_pct, sd.severity
        FROM db_stress_results sr
        JOIN scenario_definition sd ON sd.scenario_id = sr.scenario_id
        WHERE sr.account_id = %s
          AND sr.as_of_date = (
              SELECT MAX(as_of_date) FROM db_stress_results WHERE account_id = %s
          )
        ORDER BY ABS(sr.pnl_pct) DESC NULLS LAST
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (account_id, account_id))
            rows = cur.fetchall()
    return [
        {
            "name":     r[0],
            "period":   r[1],
            "pnlUsd":   r[2],
            "pnlPct":   r[3],
            "severity": r[4],
        }
        for r in rows
    ]


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )
    as_of = date.today()
    if "--date" in sys.argv:
        idx = sys.argv.index("--date")
        as_of = date.fromisoformat(sys.argv[idx + 1])
    seed_scenarios(as_of)
    print("Done.")
