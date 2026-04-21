import pprint
from pathlib import Path

import pandas as pd
import psycopg2.extras

from database2 import pg_connection

_HERE = Path(__file__).parent


def _fetch_positions(conn, account_id: int, as_of_date) -> pd.DataFrame:
    """Fetch ticker, class, sc1, market_value, marginal_var from position_var."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                COALESCE(ticker, security_id, '(unnamed)')  AS ticker,
                COALESCE(class,  '(unclassified)')          AS class,
                COALESCE(sc1,    '(unclassified)')          AS sc1,
                COALESCE(market_value,  0)                  AS market_value,
                COALESCE(marginal_var,  0)                  AS marginal_var
            FROM position_var
            WHERE account_id = %s
              AND as_of_date  = %s
            """,
            (account_id, as_of_date),
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _latest_as_of_date(conn, account_id: int):
    """Return the most recent as_of_date in position_var for the given account."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(as_of_date)
            FROM position_var
            WHERE account_id = %s
            """,
            (account_id,),
        )
        row = cur.fetchone()
    return row[0] if row and row[0] is not None else None


def get_alloc_drilldown_data(account_id: int, as_of_date=None) -> dict:
    with pg_connection() as conn:
        if as_of_date is None:
            as_of_date = _latest_as_of_date(conn, account_id)
        if as_of_date is None:
            return {}

        df = _fetch_positions(conn, account_id, as_of_date)

    if df.empty:
        return {}

    # ── Key generation ────────────────────────────────────────────────────────
    df['key1'] = "L1-" + df.groupby('class', sort=False).ngroup().astype(str)
    df['key2'] = "L2-" + df.groupby(['class', 'sc1'], sort=False).ngroup().astype(str)

    total_mv  = df['market_value'].sum()
    total_var = df['marginal_var'].sum()

    def pct_mv(v):
        return float(round(v / total_mv  * 100, 2))

    def pct_var(v):
        return float(round(v / total_var * 100, 2))

    ALLOC_DRILLDOWN = {}

    # ── Level "all": one row per asset class ──────────────────────────────────
    df_class   = df.groupby('class', sort=False)[['market_value', 'marginal_var']].sum()
    class_key1 = df.drop_duplicates('class').set_index('class')['key1']

    all_rows = []
    for cls, row in df_class.iterrows():
        all_rows.append({
            "label": cls,
            "mv":    pct_mv(row['market_value']),
            "var":   pct_var(row['marginal_var']),
            "child": class_key1[cls],
        })

    ALLOC_DRILLDOWN["all"] = {
        "subtitle": "Asset class · click a bar to drill down",
        "rows": all_rows,
    }

    # ── Level L1-X: one entry per class; rows = sc1 sub-classes ──────────────
    df_sc1 = (
        df.groupby(['class', 'sc1', 'key1', 'key2'], sort=False)[['market_value', 'marginal_var']]
        .sum()
        .reset_index()
    )

    for key1_val in df['key1'].unique():
        class_name = df.loc[df['key1'] == key1_val, 'class'].iloc[0]
        subset = df_sc1[df_sc1['key1'] == key1_val]

        rows = []
        for _, row in subset.iterrows():
            rows.append({
                "label": row['sc1'],
                "mv":    pct_mv(row['market_value']),
                "var":   pct_var(row['marginal_var']),
                "child": row['key2'],
            })

        ALLOC_DRILLDOWN[key1_val] = {
            "subtitle":    f"{class_name} \u203a subclass \u00b7 click to drill down",
            "parent":      "all",
            "parentLabel": class_name,
            "rows":        rows,
        }

    # ── Level L2-X: one entry per (class, sc1); rows = individual tickers ────
    for key2_val in df['key2'].unique():
        subset   = df[df['key2'] == key2_val]
        sc1_name = subset['sc1'].iloc[0]
        key1_val = subset['key1'].iloc[0]

        rows = []
        for _, row in subset.iterrows():
            rows.append({
                "label": row['ticker'],
                "mv":    pct_mv(row['market_value']),
                "var":   pct_var(row['marginal_var']),
                "child": None,
            })

        ALLOC_DRILLDOWN[key2_val] = {
            "subtitle":    f"{sc1_name} \u00b7 individual securities",
            "parent":      key1_val,
            "parentLabel": sc1_name,
            "rows":        rows,
        }

    return ALLOC_DRILLDOWN


if __name__ == "__main__":
    import sys
    _account_id  = int(sys.argv[1]) if len(sys.argv) > 1 else 1003
    _as_of_date  = sys.argv[2]      if len(sys.argv) > 2 else None

    data = get_alloc_drilldown_data(_account_id, _as_of_date)

    output_path = _HERE / "test_data/alloc_drilldown_output.py"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("ALLOC_DRILLDOWN = ")
        pprint.pprint(data, stream=f, sort_dicts=False)

    print(f"Written to {output_path}  (account_id={_account_id}, as_of_date={_as_of_date})")
