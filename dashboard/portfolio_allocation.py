"""
portfolio_allocation.py — Compute /api/portfolio/allocation data from position_var.

Two-step process:
  Step 1: Fetch a flat DataFrame from position_var (one row per position).
  Step 2: Aggregate into the nested levels + holdings structure expected by the frontend.

Slices and their dimension mapping:
  asset    — class (L1) › sc1 (L2) › ticker (L3, leaf)
  broker   — broker (L1) › class (L2, leaf)
  region   — region (L1) › class (L2, leaf)
  industry — sector (L1) › ticker (L2, leaf)
  currency — currency (L1) › class (L2, leaf)
"""
from __future__ import annotations

import pandas as pd
import psycopg2.extras

from database2 import pg_connection


VALID_SLICES = ("asset", "broker", "region", "industry", "currency")


# ── Step 1: fetch flat data ───────────────────────────────────────────────────

def _latest_as_of_date(conn, account_id: int):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(as_of_date) FROM position_var WHERE account_id = %s",
            (account_id,),
        )
        row = cur.fetchone()
    return row[0] if row and row[0] is not None else None


def _fetch_flat(conn, account_id: int, as_of_date) -> pd.DataFrame:
    """Return one row per position with all columns needed across all slices."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                COALESCE(ticker,        security_id, '(unnamed)')  AS ticker,
                COALESCE(class,         '(unclassified)')          AS asset_class,
                COALESCE(sc1,           '(unclassified)')          AS sc1,
                COALESCE(broker,        '(unknown)')               AS broker,
                COALESCE(region,        '(unknown)')               AS region,
                COALESCE(country,       '(unknown)')               AS country,
                COALESCE(sector,        '(unknown)')               AS sector,
                COALESCE(currency,      '(unknown)')               AS currency,
                COALESCE(market_value,  0)                         AS market_value,
                COALESCE(marginal_tvar, 0)                         AS marginal_tvar
            FROM position_var
            WHERE account_id = %s AND as_of_date = %s
            """,
            (account_id, as_of_date),
        )
        rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df['market_value']  = pd.to_numeric(df['market_value'],  errors='coerce').fillna(0.0)
    df['marginal_tvar'] = pd.to_numeric(df['marginal_tvar'], errors='coerce').fillna(0.0)
    return df


# ── Step 2: helpers ───────────────────────────────────────────────────────────

def _fmt_mv(v: float) -> str:
    """Format absolute market value as a short string: '12.9M', '450.3K'."""
    abs_v = abs(v)
    if abs_v >= 1e6:
        return f"{v / 1e6:.1f}M"
    if abs_v >= 1e3:
        return f"{v / 1e3:.1f}K"
    return f"{v:.0f}"


def _pct_of(value: float, total: float) -> float:
    if total == 0:
        return 0.0
    return round(value / total * 100, 2)


def _fmt_pct(v: float) -> str:
    return f"{v:.1f}%"


# ── Step 2: generic slice builder ─────────────────────────────────────────────

def _build_slice(
    df: pd.DataFrame,
    dim1: str,
    dim2: str,
    dim3: str | None,
    slice_name: str,
    l1_subtitle: str,
    l2_subtitle_tmpl: str,
    l3_subtitle_tmpl: str | None,
    holdings_col1: str,
    holdings_col2: str,
    dim4: str | None = None,
    l4_subtitle_tmpl: str | None = None,
    holdings_col3: str | None = None,
) -> dict:
    """
    Build levels + holdings for one slice.

    dim1  → L1 grouping column
    dim2  → L2 grouping column
    dim3  → L3 grouping column, or None if only two levels
    dim4  → L4 label column (leaf), or None if only three levels
    """
    total_mv  = float(df['market_value'].sum())
    total_var = float(df['marginal_tvar'].sum())

    levels:   dict = {}
    holdings: dict = {}

    # ── L1 aggregation ────────────────────────────────────────────────────────
    df1 = (
        df.groupby(dim1, sort=False)[['market_value', 'marginal_tvar']]
        .sum()
        .reset_index()
    )
    # Stable keys derived from position in sorted groups
    key1_map: dict[str, str] = {
        row[dim1]: f"{slice_name}-L1-{i}" for i, row in df1.iterrows()
    }

    all_rows = []
    for _, row in df1.iterrows():
        label = row[dim1]
        all_rows.append({
            "label": label,
            "mv":    _pct_of(float(row['market_value']), total_mv),
            "var":   _pct_of(float(row['marginal_tvar']), total_var),
            "child": key1_map[label],
        })
    levels["all"] = {
        "subtitle":    l1_subtitle,
        "parent":      None,
        "parentLabel": None,
        "rows":        all_rows,
    }

    h_rows = []
    for _, row in df1.iterrows():
        mv = float(row['market_value'])
        h_rows.append([row[dim1], _fmt_mv(mv), _fmt_pct(_pct_of(mv, total_mv)), "—", "—"])
    holdings[f"all_{slice_name}"] = {
        "cols": [holdings_col1, "Mkt Val", "Alloc %", "1D Ret", "YTD"],
        "rows": h_rows,
        "foot": ["Total", _fmt_mv(total_mv), "100%", "—", "—"],
    }

    # ── L2 aggregation ────────────────────────────────────────────────────────
    df12 = (
        df.groupby([dim1, dim2], sort=False)[['market_value', 'marginal_tvar']]
        .sum()
        .reset_index()
    )
    key2_map: dict[tuple, str] = {
        (row[dim1], row[dim2]): f"{slice_name}-L2-{i}" for i, row in df12.iterrows()
    }

    for dim1_val, key1 in key1_map.items():
        subset1 = df12[df12[dim1] == dim1_val]
        sub_mv  = float(subset1['market_value'].sum())
        sub_var = float(subset1['marginal_tvar'].sum())

        l2_rows = []
        for _, row in subset1.iterrows():
            dim2_val = row[dim2]
            child    = key2_map[(dim1_val, dim2_val)] if dim3 else None
            l2_rows.append({
                "label": dim2_val,
                "mv":    _pct_of(float(row['market_value']), sub_mv),
                "var":   _pct_of(float(row['marginal_tvar']), sub_var),
                "child": child,
            })
        levels[key1] = {
            "subtitle":    l2_subtitle_tmpl.format(dim1_val=dim1_val),
            "parent":      "all",
            "parentLabel": dim1_val,
            "rows":        l2_rows,
        }

        h2_rows = []
        for _, row in subset1.iterrows():
            mv = float(row['market_value'])
            h2_rows.append([row[dim2], _fmt_mv(mv), _fmt_pct(_pct_of(mv, sub_mv)), "—", "—"])
        holdings[key1] = {
            "cols": [holdings_col2, "Mkt Val", "Alloc %", "1D Ret", "YTD"],
            "rows": h2_rows,
            "foot": ["Total", _fmt_mv(sub_mv), "100%", "—", "—"],
        }

    # ── L3 aggregation ────────────────────────────────────────────────────────
    if dim3:
        df123 = (
            df.groupby([dim1, dim2, dim3], sort=False)[['market_value', 'marginal_tvar']]
            .sum()
            .reset_index()
        )
        key3_map: dict[tuple, str] = {
            (row[dim1], row[dim2], row[dim3]): f"{slice_name}-L3-{i}"
            for i, row in df123.iterrows()
        }

        for (dim1_val, dim2_val), key2 in key2_map.items():
            subset2 = df123[(df123[dim1] == dim1_val) & (df123[dim2] == dim2_val)]
            sub_mv  = float(subset2['market_value'].sum())
            sub_var = float(subset2['marginal_tvar'].sum())

            l3_rows = []
            for _, row in subset2.iterrows():
                dim3_val = row[dim3]
                child    = key3_map[(dim1_val, dim2_val, dim3_val)] if dim4 else None
                l3_rows.append({
                    "label": dim3_val,
                    "mv":    _pct_of(float(row['market_value']), sub_mv),
                    "var":   _pct_of(float(row['marginal_tvar']), sub_var),
                    "child": child,
                })
            levels[key2] = {
                "subtitle":    l3_subtitle_tmpl.format(dim1_val=dim1_val, dim2_val=dim2_val),
                "parent":      key1_map[dim1_val],
                "parentLabel": dim2_val,
                "rows":        l3_rows,
            }

            h3_col = holdings_col3 or "Sub-class"
            h3_rows = []
            for _, row in subset2.iterrows():
                mv = float(row['market_value'])
                h3_rows.append([row[dim3], _fmt_mv(mv), _fmt_pct(_pct_of(mv, sub_mv)), "—", "—"])
            holdings[key2] = {
                "cols": [h3_col, "Mkt Val", "Alloc %", "1D Ret", "YTD"],
                "rows": h3_rows,
                "foot": ["Total", _fmt_mv(sub_mv), "100%", "—", "—"],
            }

        # ── L4 aggregation ────────────────────────────────────────────────────
        if dim4:
            df1234 = (
                df.groupby([dim1, dim2, dim3, dim4], sort=False)[['market_value', 'marginal_tvar']]
                .sum()
                .reset_index()
            )

            for (dim1_val, dim2_val, dim3_val), key3 in key3_map.items():
                subset3 = df1234[
                    (df1234[dim1] == dim1_val) &
                    (df1234[dim2] == dim2_val) &
                    (df1234[dim3] == dim3_val)
                ]
                sub_mv  = float(subset3['market_value'].sum())
                sub_var = float(subset3['marginal_tvar'].sum())

                l4_rows = []
                for _, row in subset3.iterrows():
                    l4_rows.append({
                        "label": row[dim4],
                        "mv":    _pct_of(float(row['market_value']), sub_mv),
                        "var":   _pct_of(float(row['marginal_tvar']), sub_var),
                        "child": None,
                    })
                levels[key3] = {
                    "subtitle":    l4_subtitle_tmpl.format(
                                       dim1_val=dim1_val, dim2_val=dim2_val, dim3_val=dim3_val),
                    "parent":      key2_map[(dim1_val, dim2_val)],
                    "parentLabel": dim3_val,
                    "rows":        l4_rows,
                }

                h4_rows = []
                for _, row in subset3.iterrows():
                    mv = float(row['market_value'])
                    h4_rows.append([row[dim4], _fmt_mv(mv), _fmt_pct(_pct_of(mv, sub_mv)), "—", "—"])
                holdings[key3] = {
                    "cols": ["Security", "Mkt Val", "Alloc %", "1D Ret", "YTD"],
                    "rows": h4_rows,
                    "foot": ["Total", _fmt_mv(sub_mv), "100%", "—", "—"],
                }

    return {"levels": levels, "holdings": holdings}


# ── Public API ────────────────────────────────────────────────────────────────

def get_portfolio_allocation(account_id: int, slice_key: str, as_of_date=None) -> dict:
    """
    Return allocation drill-down data for one slice.
    Returns {} if no data is available.
    """
    if slice_key not in VALID_SLICES:
        return {}

    with pg_connection() as conn:
        if as_of_date is None:
            as_of_date = _latest_as_of_date(conn, account_id)
        if as_of_date is None:
            return {}
        df = _fetch_flat(conn, account_id, as_of_date)

    if df.empty:
        return {}

    if slice_key == "asset":
        return _build_slice(
            df, "asset_class", "sc1", "ticker",
            slice_name="asset",
            l1_subtitle="Asset class \u00b7 click to drill down",
            l2_subtitle_tmpl="{dim1_val} \u2014 sub-classes \u00b7 click to drill down",
            l3_subtitle_tmpl="{dim1_val} \u00b7 {dim2_val} \u2014 securities",
            holdings_col1="Asset class",
            holdings_col2="Sub-class",
        )

    if slice_key == "broker":
        return _build_slice(
            df, "broker", "asset_class", "sc1",
            slice_name="broker",
            l1_subtitle="Broker \u00b7 click to drill down",
            l2_subtitle_tmpl="{dim1_val} \u2014 by asset class",
            l3_subtitle_tmpl="{dim1_val} \u00b7 {dim2_val} \u2014 sub-classes",
            holdings_col1="Broker",
            holdings_col2="Asset class",
            holdings_col3="Sub-class",
            dim4="ticker",
            l4_subtitle_tmpl="{dim1_val} \u00b7 {dim2_val} \u00b7 {dim3_val} \u2014 securities",
        )

    if slice_key == "region":
        return _build_slice(
            df, "region", "country", "ticker",
            slice_name="region",
            l1_subtitle="Region \u00b7 click to drill down",
            l2_subtitle_tmpl="{dim1_val} \u2014 by country",
            l3_subtitle_tmpl="{dim1_val} \u00b7 {dim2_val} \u2014 securities",
            holdings_col1="Region",
            holdings_col2="Country",
        )

    if slice_key == "industry":
        return _build_slice(
            df, "sector", "ticker", None,
            slice_name="industry",
            l1_subtitle="Sector \u00b7 click to drill down",
            l2_subtitle_tmpl="{dim1_val} \u2014 securities",
            l3_subtitle_tmpl=None,
            holdings_col1="Sector",
            holdings_col2="Security",
        )

    # slice_key == "currency"
    return _build_slice(
        df, "currency", "asset_class", "ticker",
        slice_name="currency",
        l1_subtitle="Currency \u00b7 click to drill down",
        l2_subtitle_tmpl="{dim1_val} \u2014 by asset class",
        l3_subtitle_tmpl="{dim1_val} \u00b7 {dim2_val} \u2014 securities",
        holdings_col1="Currency",
        holdings_col2="Asset class",
    )
