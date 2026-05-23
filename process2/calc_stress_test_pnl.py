"""
calc_stress_test.py

Calculates stress test PnL at the security level and writes results to st_security_pnl.

Price model:  pnl = sum(beta_i * shock_i / 100) across all factors in the model
Spread model: placeholder — Phase 1 not implemented

Run: python calc_stress_test.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from database2 import pg_connection


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_active_scenarios(conn):
    """Return list of active scenario_ids from st_scenarios."""
    with conn.cursor() as cur:
        cur.execute("SELECT scenario_id FROM st_scenarios WHERE is_active = true ORDER BY scenario_id")
        return [row[0] for row in cur.fetchall()]


def load_models(conn, model_type):
    """
    Return dict: model_id -> [factor_symbol, ...]  (non-null f1..f10, in order)
    for the given model_type ('Price' or 'Spread').
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT model_id, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10
            FROM st_model
            WHERE model_type = %s
            ORDER BY model_id
        """, (model_type,))
        return {
            row[0]: [f for f in row[1:] if f is not None]
            for row in cur.fetchall()
        }


def load_shocks(conn, scenario_id):
    """
    Return dict: factor_symbol -> shock as decimal (e.g. -50 → -0.50)
    for a given scenario_id.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT factor_symbol, shock
            FROM st_shock
            WHERE scenario_id = %s
        """, (scenario_id,))
        return {row[0]: row[1] / 100.0 for row in cur.fetchall()}


def load_betas(conn, model_ids):
    """
    Return DataFrame (security_id, model_id, b1..b10) for the given model_ids.
    """
    placeholders = ','.join(['%s'] * len(model_ids))
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT security_id, model_id, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10
            FROM st_model_beta
            WHERE model_id IN ({placeholders})
        """, tuple(model_ids))
        cols = ['security_id', 'model_id', 'b1', 'b2', 'b3', 'b4',
                'b5', 'b6', 'b7', 'b8', 'b9', 'b10']
        return pd.DataFrame(cur.fetchall(), columns=cols)


# ---------------------------------------------------------------------------
# Calculations
# ---------------------------------------------------------------------------

def calc_price_pnl(betas_df, models, shocks):
    """
    For each security, compute pnl = sum(beta_i * shock_i) across its model factors.

    betas_df : DataFrame with columns security_id, model_id, b1..b10
    models   : dict model_id -> [factor_symbol, ...]
    shocks   : dict factor_symbol -> shock_decimal

    Returns list of (security_id, pnl).
    """
    results = []
    for _, row in betas_df.iterrows():
        factor_symbols = models.get(row['model_id'], [])
        pnl = 0.0
        for i, symbol in enumerate(factor_symbols):
            beta = row[f'b{i + 1}']
            shock = shocks.get(symbol)
            if beta is not None and shock is not None:
                pnl += beta * shock
        results.append((row['security_id'], pnl))
    return results


def calc_spread_pnl(betas_df, models, shocks):
    """Spread model — Phase 1 placeholder."""
    # TODO: implement Spread model PnL calculation
    return []


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def delete_scenario_pnl(conn, scenario_id):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM st_security_pnl WHERE scenario_id = %s", (scenario_id,))
        print(f"  Deleted {cur.rowcount} existing rows for scenario {scenario_id}")


def insert_pnl(conn, scenario_id, results):
    if not results:
        print(f"  No rows to insert for scenario {scenario_id}")
        return
    data = [(scenario_id, sec_id, pnl) for sec_id, pnl in results]
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO st_security_pnl (scenario_id, security_id, pnl) VALUES (%s, %s, %s)",
            data,
        )
    print(f"  Inserted {len(data)} rows for scenario {scenario_id}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run():
    with pg_connection() as conn:
        scenarios = load_active_scenarios(conn)
        print(f"Active scenarios: {scenarios}")

        price_models = load_models(conn, 'Price')
        print(f"Price models ({len(price_models)}): {list(price_models.keys())}")

        spread_models = load_models(conn, 'Spread')
        print(f"Spread models ({len(spread_models)}): {list(spread_models.keys())}")

        price_betas_df = load_betas(conn, list(price_models.keys()))
        print(f"Securities with Price betas: {len(price_betas_df)}")

        spread_betas_df = load_betas(conn, list(spread_models.keys()))
        print(f"Securities with Spread betas: {len(spread_betas_df)}")

        for scenario_id in scenarios:
            print(f"\nScenario {scenario_id}:")
            shocks = load_shocks(conn, scenario_id)

            if not shocks:
                print(f"  WARNING: no shocks found for scenario {scenario_id} — skipping.")
                continue

            price_results = calc_price_pnl(price_betas_df, price_models, shocks)
            spread_results = calc_spread_pnl(spread_betas_df, spread_models, shocks)

            all_results = price_results + spread_results

            delete_scenario_pnl(conn, scenario_id)
            insert_pnl(conn, scenario_id, all_results)
            conn.commit()

        print("\nDone.")


if __name__ == '__main__':
    run()
