"""
breakdown_calc.py — Compute portfolio breakdowns from position_var data.

For each dimension, groups positions by category, then computes:
    weight      = category market value / total market value  (decimal)
    var_contrib = category marginal_tvar / total marginal_tvar  (decimal)

NULL / blank category values are grouped into 'Other'.

Dimensions and their position_var columns:
    asset_class → "class" (aliased as asset_class in get_positions_on_date)
    region      → region
    currency    → currency
    industry    → sector
"""
from __future__ import annotations

import pandas as pd


_DIMENSIONS: list[tuple[str, str]] = [
    ('asset_class', 'asset_class'),
    ('region',      'region'),
    ('currency',    'currency'),
    ('industry',    'sector'),
]


def compute_breakdowns(account_id: int, as_of_date, df: pd.DataFrame) -> list[dict]:
    """
    Compute breakdown rows for all dimensions from a position_var DataFrame.

    df must contain: market_value, asset_class, region, currency, sector.

    Returns a flat list of dicts:
        breakdown_type, category, weight, var_contrib
    """
    df = df.copy()
    df['market_value']  = pd.to_numeric(df['market_value'],  errors='coerce').fillna(0.0)
    df['marginal_tvar'] = pd.to_numeric(df['marginal_tvar'], errors='coerce').fillna(0.0)

    total_mv   = float(df['market_value'].sum())
    total_tvar = float(df['marginal_tvar'].sum())

    results = []

    for breakdown_type, col in _DIMENSIONS:
        if col not in df.columns:
            continue

        tmp = df[[col, 'market_value', 'marginal_tvar']].copy()
        tmp[col] = tmp[col].replace('', None).fillna('Other')

        agg = (
            tmp.groupby(col, as_index=False)
            .agg(mv=('market_value', 'sum'), tvar=('marginal_tvar', 'sum'))
        )

        for _, row in agg.iterrows():
            weight     = round(float(row['mv'])   / total_mv,   6) if total_mv   else None
            var_contrib = round(float(row['tvar']) / total_tvar, 6) if total_tvar else None

            results.append({
                'breakdown_type': breakdown_type,
                'category':       row[col],
                'weight':         weight,
                'var_contrib':    var_contrib,
            })

    return results
