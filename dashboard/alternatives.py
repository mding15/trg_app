"""
alternatives.py — DB queries for the Alternatives page.
"""
import math
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database2 import pg_connection

_SC1_COLORS = [
    "#7c5ce4",  # purple
    "#4e8fdd",  # blue
    "#22d47e",  # green
    "#f39c12",  # orange
    "#e05252",  # red
    "#00cec9",  # teal
    "#fd79a8",  # pink
    "#a29bfe",  # lavender
]


def _fmt_month(d) -> str:
    """Format a date as "Mon 'YY" (e.g., "Jun '24")."""
    return d.strftime("%b '") + d.strftime("%y")


def build_subclasses(positions: list) -> dict:
    """
    Derive {"subclasses": [...], "drill": {...}} from an already-fetched positions list.

    Expects each position dict to contain: sc1, name, market_value, mg_var_95.
    subclasses alloc/varc are % of portfolio total; drill alloc/varc are % of sc1 total.
    """
    if not positions:
        return {"subclasses": [], "drill": {}}

    groups = {}
    for p in positions:
        sc1 = p.get("sc1")
        if sc1 is None:
            continue
        if sc1 not in groups:
            groups[sc1] = []
        groups[sc1].append({
            "name":  p.get("name") or p.get("ticker", ""),
            "mv":    float(p["market_value"]) if p.get("market_value") is not None else 0.0,
            "var95": float(p["mg_var_95"])    if p.get("mg_var_95")    is not None else 0.0,
        })

    if not groups:
        return {"subclasses": [], "drill": {}}

    sc1_names = sorted(groups)
    sc1_totals = {
        sc1: {"mv": sum(p["mv"] for p in pos), "var": sum(p["var95"] for p in pos)}
        for sc1, pos in groups.items()
    }
    total_mv  = sum(t["mv"]  for t in sc1_totals.values())
    total_var = sum(t["var"] for t in sc1_totals.values())

    subclasses = [
        {
            "name":  sc1,
            "alloc": round(sc1_totals[sc1]["mv"]  / total_mv  * 100, 1) if total_mv  else 0.0,
            "varc":  round(sc1_totals[sc1]["var"] / total_var * 100, 1) if total_var else 0.0,
            "color": _SC1_COLORS[i % len(_SC1_COLORS)],
        }
        for i, sc1 in enumerate(sc1_names)
    ]

    drill = {
        sc1: [
            {
                "name":  p["name"],
                "alloc": round(p["mv"]    / sc1_totals[sc1]["mv"]  * 100, 1) if sc1_totals[sc1]["mv"]  else 0.0,
                "varc":  round(p["var95"] / sc1_totals[sc1]["var"] * 100, 1) if sc1_totals[sc1]["var"] else 0.0,
            }
            for p in groups[sc1]
        ]
        for sc1 in sc1_names
    }

    return {"subclasses": subclasses, "drill": drill}


def get_alt_history(account_id: int) -> dict:
    """
    Return {labels, series} for the Alternatives historical chart.

    Each series is one sc1 sub-class; values are market value in $M
    at each as_of_date where class = 'Alternatives'.
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pv.as_of_date, pv.sc1, SUM(pv.market_value)
                FROM position_var pv
                WHERE pv."class" = 'Alternatives'
                  AND pv.account_id = %s
                GROUP BY pv.as_of_date, pv.sc1
                ORDER BY pv.as_of_date, pv.sc1
                """,
                (account_id,),
            )
            rows = cur.fetchall()

    if not rows:
        return {"labels": [], "series": []}

    dates    = sorted({r[0] for r in rows})
    sc1_names = sorted({r[1] for r in rows if r[1] is not None})

    mv_map = {(r[0], r[1]): float(r[2]) for r in rows if r[2] is not None}

    return {
        "labels": [_fmt_month(d) for d in dates],
        "series": [
            {
                "name":   name,
                "color":  _SC1_COLORS[i % len(_SC1_COLORS)],
                "values": [mv_map.get((d, name), 0.0) for d in dates],
            }
            for i, name in enumerate(sc1_names)
        ],
    }


def get_alt_positions(account_id: int) -> list:
    """
    Return a list of alternative positions for the most recent as_of_date.

    Column names match the query aliases exactly so no mapping is needed.
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    dp.ticker,
                    dp."name",
                    pv.class,
                    pv.sc1,
                    pv.region,
                    pv.country,
                    pv.industry,
                    pv.currency,
                    dp.market_value,
                    dp.weight,
                    am.proxy_name,
                    am.proxy_correl,
                    am.unadj_vol,
                    am.adj_vol,
                    am.liq_adj,
                    pv.mg_var_95,
                    pv.expected_return AS ret_unadj,
                    pv.expected_return AS ret_liqadj,
                    dp.unrealized_gain,
                    dp.ytd_return
                FROM db_positions dp
                LEFT JOIN position_var pv
                       ON pv.account_id  = dp.account_id
                      AND pv.as_of_date  = dp.as_of_date
                      AND pv.security_id = dp.security_id
                LEFT JOIN alternative_model am
                       ON am.security_id = dp.security_id
                WHERE dp.asset_class = 'Alternatives'
                  AND dp.account_id  = %s
                  AND dp.as_of_date  = (
                      SELECT MAX(as_of_date)
                      FROM db_positions
                      WHERE asset_class = 'Alternatives'
                        AND account_id  = %s
                  )
                """,
                (account_id, account_id),
            )
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()

    return [dict(zip(cols, row)) for row in rows]


def get_alt_gauges(account_id: int) -> dict:
    """
    Build gauge data for the Alternatives page from the database.
    Returns {"var": {...}, "sharpe": {...}}.
    All values rounded to 4 decimal places; missing data returns None fields.
    """
    def _f(v):
        return round(float(v), 4) if v is not None else None

    # ── Query 1: alternative_var — account-level aggregates ───────────────────
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT SUM(market_value)    AS market_value,
                       SUM(mg_std)          AS std,
                       SUM(unadj_mg_std)    AS unadj_std,
                       SUM(mg_var_95)       AS var_95,
                       SUM(unadj_mg_var_95) AS unadj_var_95
                FROM alternative_var
                WHERE account_id = %s
                  AND as_of_date = (
                      SELECT MAX(as_of_date) FROM alternative_var WHERE account_id = %s
                  )
                """,
                (account_id, account_id),
            )
            cols = [d[0] for d in cur.description]
            row  = cur.fetchone()
    av = dict(zip(cols, row)) if row else {}

    # ── Query 2: account_limit — var limit + sharpe target in one query ───────
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT limit_category, limit_value
                FROM account_limit
                WHERE account_id = %s
                  AND limit_category IN ('var_alt_limit_dollar', 'target_alt_sharpe_vol')
                """,
                (account_id,),
            )
            rows = cur.fetchall()
    limits = {r[0]: float(r[1]) for r in rows if r[1] is not None}

    # ── Query 3: stat_static_data — risk-free rate ────────────────────────────
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT "Value" FROM stat_static_data WHERE "Name" = %s',
                ('Riskfree Rate',),
            )
            rf_row = cur.fetchone()
    rf = float(rf_row[0]) if rf_row and rf_row[0] is not None else None

    # ── Query 4: position_var — weighted expected return ──────────────────────
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT SUM(market_value * expected_return) / NULLIF(SUM(market_value), 0)
                FROM position_var
                WHERE account_id = %s
                  AND as_of_date = (
                      SELECT MAX(as_of_date) FROM position_var WHERE account_id = %s
                  )
                """,
                (account_id, account_id),
            )
            er_row = cur.fetchone()
    exp_ret = float(er_row[0]) if er_row and er_row[0] is not None else None

    # ── VaR gauge ─────────────────────────────────────────────────────────────
    unadj_var  = _f(av.get('unadj_var_95'))
    liqadj_var = _f(av.get('var_95'))
    var_limit  = _f(limits.get('var_alt_limit_dollar'))

    var_candidates = [v for v in [unadj_var, liqadj_var, var_limit] if v is not None]
    var_max  = round(max(var_candidates) * 1.4, 4) if var_candidates else None
    var_band = round(var_limit * 0.05, 4)           if var_limit is not None else None

    # ── Sharpe gauge ──────────────────────────────────────────────────────────
    mv        = float(av.get('market_value') or 0)
    std       = float(av.get('std')          or 0)
    unadj_std = float(av.get('unadj_std')    or 0)

    unadj_vol  = (unadj_std / mv * math.sqrt(252)) if mv else None
    adj_vol    = (std       / mv * math.sqrt(252)) if mv else None

    unadj_ratio  = None
    liqadj_ratio = None
    if exp_ret is not None and rf is not None:
        excess       = exp_ret - rf
        unadj_ratio  = _f(excess / unadj_vol)  if unadj_vol  else None
        liqadj_ratio = _f(excess / adj_vol)    if adj_vol    else None

    sharpe_target = _f(limits.get('target_alt_sharpe_vol'))

    sharpe_candidates = [v for v in [unadj_ratio, liqadj_ratio, sharpe_target] if v is not None]
    sharpe_max  = round(max(sharpe_candidates) * 1.4, 4) if sharpe_candidates else None
    sharpe_band = round(sharpe_target * 0.05, 4)          if sharpe_target is not None else None

    return {
        "var": {
            "unadj_var":  unadj_var,
            "liqadj_var": liqadj_var,
            "limit":      var_limit,
            "max":        var_max,
            "band":       var_band,
        },
        "sharpe": {
            "unadj_ratio":  unadj_ratio,
            "liqadj_ratio": liqadj_ratio,
            "target":       sharpe_target,
            "max":          sharpe_max,
            "band":         sharpe_band,
        },
    }


def test(account_id: int = 1003):
    import csv, os
    from datetime import datetime

    rows = get_alt_positions(account_id)
    if not rows:
        print(f"No rows returned for account_id={account_id}")
        return

    out_dir = os.path.join(os.path.dirname(__file__), "test_output")
    os.makedirs(out_dir, exist_ok=True)
    ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = os.path.join(out_dir, f"alt_positions_{account_id}_{ts}.csv")

    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved {len(rows)} rows → {os.path.abspath(out)}")


if __name__ == "__main__":
    test()
