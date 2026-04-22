"""
concentration_calc.py — Compute concentration ratios from position_var data.

For each category, finds the largest constituent by market-value weight,
then computes ratio = max_weight% / limit% using limits from account_limit.

Categories and their position_var columns:
    Asset Class  → asset_class  (position_var "class", aliased in fetch)
    Region       → region
    Currency     → currency
    Industry     → sector
    Single Name  → security_id
"""
from __future__ import annotations

import pandas as pd

from database2 import pg_connection


_CATEGORIES: list[tuple[str, str, str]] = [
    # (display label,  df column,    account_limit key)
    ('Asset Class',  'asset_class', 'con_limit_asset_pct'),
    ('Region',       'region',      'con_limit_region_pct'),
    ('Currency',     'currency',    'con_limit_currency_pct'),
    ('Industry',     'sector',      'con_limit_industry_pct'),
    ('Single Name',  'security_id', 'con_limit_name_pct'),
]


def load_limits(conn, account_id: int) -> dict[str, float]:
    """
    Return {limit_category: limit_value} for all concentration limits
    for account_id from account_limit. Values are stored as % (e.g. 20 = 20%).
    Missing categories are absent from the dict.
    """
    keys = [cat[2] for cat in _CATEGORIES]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT limit_category, limit_value
            FROM account_limit
            WHERE account_id = %s AND limit_category = ANY(%s)
            """,
            (account_id, keys),
        )
        return {row[0]: float(row[1]) for row in cur.fetchall()}


def compute_concentrations(
    account_id: int,
    as_of_date,
    df: pd.DataFrame,
    limits: dict[str, float],
) -> list[dict]:
    """
    Compute concentration ratios for account_id from a position_var DataFrame.

    df must contain: market_value, asset_class, region, currency, sector,
                     security_id, ticker, security_name.

    NULL / blank category values are grouped into 'Other'.
    limit_value is in % (e.g. 20 means 20%).
    ratio = max_weight_pct / limit_value_pct; NULL if no limit configured.

    Returns a list of dicts:
        category, category_name, max_weight, limit_value, ratio
    """
    df = df.copy()
    df['market_value'] = pd.to_numeric(df['market_value'], errors='coerce').fillna(0.0)
    total_mv = float(df['market_value'].sum())

    results = []

    for label, col, limit_key in _CATEGORIES:
        if col not in df.columns:
            continue

        tmp = df[[col, 'market_value', 'ticker', 'security_name']].copy()
        tmp[col] = tmp[col].replace('', None).fillna('Other')

        agg = tmp.groupby(col)['market_value'].sum().reset_index()
        agg['weight_pct'] = (agg['market_value'] / total_mv * 100.0) if total_mv > 0 else 0.0

        if agg.empty:
            continue

        max_idx      = agg['weight_pct'].idxmax()
        max_row      = agg.loc[max_idx]
        max_weight   = round(float(max_row['weight_pct']), 4)
        max_col_val  = max_row[col]

        # For Single Name resolve a readable label (ticker preferred, then security_name)
        if col == 'security_id' and max_col_val != 'Other':
            match = tmp[tmp['security_id'] == max_col_val]
            if not match.empty:
                ticker = match.iloc[0].get('ticker')
                name   = match.iloc[0].get('security_name')
                category_name = ticker if ticker else (name if name else max_col_val)
            else:
                category_name = max_col_val
        else:
            category_name = max_col_val

        limit_val = limits.get(limit_key)
        ratio = round(max_weight / limit_val, 4) if limit_val else None

        results.append({
            'category':      label,
            'category_name': category_name,
            'max_weight':    max_weight,
            'limit_value':   float(limit_val) if limit_val is not None else None,
            'ratio':         ratio,
        })

    return results
