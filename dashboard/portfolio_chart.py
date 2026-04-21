"""
portfolio_chart.py — Build portfolio vs benchmark chart data from the database.

Response format (matches PORTFOLIO_CHART static shape):
    {
        "labels":    [...],
        "portfolio": [...],
        "sp500":     [...],
        "blend6040": [...],
        "msci":      [...],
    }

Resampling rules by range_key (falls back to weekly if raw daily count < 100):
    1M  → daily      label: "Mon D"
    3M  → weekly     label: "Mon D"
    1Y  → monthly    label: "Mon 'YY"
    3Y  → quarterly  label: "Mon 'YY"
    ALL → yearly     label: "YYYY"

Benchmark values are scaled so the first point equals the portfolio's first MV,
then grow proportionally to the benchmark_hist index.
Benchmark dates are forward-filled to match portfolio dates.
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from database2 import pg_connection

# Maps API response keys → benchmark_name in the benchmark table
BENCHMARK_KEYS: dict[str, str] = {
    "sp500":     "SP500",
    "blend6040": "60/40 Blend",
    "msci":      "MSCI World",
}

_FALLBACK_WEEKLY  = 100   # fall back to weekly when raw daily count < this
_FALLBACK_MONTHLY = 252   # fall back to monthly (3Y/ALL) when raw daily count < this


# ── DB helpers ────────────────────────────────────────────────────────────────

def _pg_fetch(sql: str, params: tuple) -> list:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def _fetch_portfolio_mv(account_id: int, cutoff: date | None) -> pd.Series:
    """Return daily total MV series for account_id indexed by date."""
    if cutoff:
        rows = _pg_fetch(
            "SELECT as_of_date, SUM(market_value) FROM db_mv_history "
            "WHERE account_id = %s AND as_of_date >= %s "
            "GROUP BY as_of_date ORDER BY as_of_date",
            (account_id, cutoff),
        )
    else:
        rows = _pg_fetch(
            "SELECT as_of_date, SUM(market_value) FROM db_mv_history "
            "WHERE account_id = %s "
            "GROUP BY as_of_date ORDER BY as_of_date",
            (account_id,),
        )
    if not rows:
        return pd.Series(dtype=float)
    return pd.Series(
        [float(r[1]) if r[1] is not None else None for r in rows],
        index=pd.DatetimeIndex([r[0] for r in rows]),
    )


def _fetch_benchmark_ids() -> dict[str, int | None]:
    """Return {api_key: benchmark_id} for all BENCHMARK_KEYS."""
    names = list(BENCHMARK_KEYS.values())
    rows  = _pg_fetch(
        "SELECT benchmark_id, benchmark_name FROM benchmark WHERE benchmark_name = ANY(%s)",
        (names,),
    )
    name_to_id = {r[1]: r[0] for r in rows}
    return {key: name_to_id.get(name) for key, name in BENCHMARK_KEYS.items()}


def _fetch_benchmark_hist(
    benchmark_ids: list[int], from_date: date, to_date: date
) -> dict[int, pd.Series]:
    """Return {benchmark_id: Series(value, index=DatetimeIndex)} for the date range."""
    if not benchmark_ids:
        return {}
    rows = _pg_fetch(
        "SELECT benchmark_id, date, value FROM benchmark_hist "
        "WHERE benchmark_id = ANY(%s) AND date BETWEEN %s AND %s "
        "ORDER BY benchmark_id, date",
        (benchmark_ids, from_date, to_date),
    )
    buckets: dict[int, dict] = {}
    for bid, d, v in rows:
        buckets.setdefault(bid, {})[d] = float(v) if v is not None else None
    return {
        bid: pd.Series(vals, index=pd.DatetimeIndex(list(vals.keys())))
        for bid, vals in buckets.items()
    }


# ── Resampling & alignment ────────────────────────────────────────────────────

def _resample(series: pd.Series, freq: str) -> pd.Series:
    if freq == "D":
        return series.dropna()
    return series.resample(freq).last().dropna()


def _align_benchmark(bmk: pd.Series, target_idx: pd.DatetimeIndex) -> pd.Series:
    """Forward-fill bmk onto target_idx."""
    if bmk.empty:
        return pd.Series([None] * len(target_idx), index=target_idx, dtype=object)
    combined = bmk.reindex(target_idx.union(bmk.index).sort_values()).ffill()
    return combined.reindex(target_idx)


def _scale(bmk_aligned: pd.Series, bmk_first: float, port_first: float) -> list[float | None]:
    """Scale benchmark so its first value = port_first, proportional thereafter."""
    out = []
    for v in bmk_aligned:
        if v is None or pd.isna(v):
            out.append(None)
        else:
            out.append(round(float(v) / bmk_first * port_first, 2))
    return out


# ── Label formatting ──────────────────────────────────────────────────────────

def _fmt(d: date, fmt: str) -> str:
    """Format date, stripping leading zero from day number."""
    return d.strftime(fmt).replace(" 0", " ")


# ── Public API ────────────────────────────────────────────────────────────────

def get_portfolio_chart_data(account_id: int, range_key: str) -> dict | None:
    """
    Build chart data for account_id and range_key.
    Returns None if no portfolio data exists for the account.
    """
    rk    = range_key.upper()
    today = date.today()

    # ── Cutoff date ───────────────────────────────────────────────────────────
    cutoff_days = {"1M": 30, "3M": 90, "1Y": 365, "3Y": 365 * 3}.get(rk)
    cutoff      = (today - timedelta(days=cutoff_days)) if cutoff_days else None

    # ── Fetch raw daily portfolio MV ──────────────────────────────────────────
    port_daily = _fetch_portfolio_mv(account_id, cutoff)
    if port_daily.empty:
        return None

    raw_count = len(port_daily)

    # ── Frequency and label format ────────────────────────────────────────────
    freq      = {"1M": "D",  "3M": "W", "1Y": "ME", "3Y": "QE", "ALL": "YE"}.get(rk, "D")
    label_fmt = {"1M": "%b %d", "3M": "%b %d", "1Y": "%b '%y",
                 "3Y": "%b '%y", "ALL": "%Y"}.get(rk, "%b %d")

    # Fall back to coarser/finer frequency when data is sparse
    if rk == "1Y" and raw_count < _FALLBACK_WEEKLY:
        freq, label_fmt = "W", "%b %d"
    elif rk in ("3Y", "ALL"):
        if raw_count < _FALLBACK_WEEKLY:
            freq, label_fmt = "W", "%b %d"
        elif raw_count < _FALLBACK_MONTHLY:
            freq, label_fmt = "ME", "%b '%y"

    # ── Resample portfolio ────────────────────────────────────────────────────
    port = _resample(port_daily, freq)
    if port.empty:
        return None

    port_idx   = port.index
    from_date  = port_idx[0].date()
    to_date    = port_idx[-1].date()
    port_first = float(port.iloc[0])

    # ── Fetch and align benchmarks ────────────────────────────────────────────
    # Fetch 14 days before from_date so forward-fill has prior values when the
    # benchmark has no entry on the exact first portfolio date.
    key_to_id  = _fetch_benchmark_ids()
    valid_ids  = [bid for bid in key_to_id.values() if bid is not None]
    bmk_from   = from_date - timedelta(days=14)
    bmk_hist   = _fetch_benchmark_hist(valid_ids, bmk_from, to_date)

    # ── Assemble response ─────────────────────────────────────────────────────
    result: dict = {
        "labels":    [_fmt(d.date(), label_fmt) for d in port_idx],
        "portfolio": [round(float(v), 2) if v is not None and not pd.isna(v) else None
                      for v in port],
    }

    for api_key, bid in key_to_id.items():
        if bid is None or bid not in bmk_hist:
            result[api_key] = [None] * len(port_idx)
            continue

        bmk_aligned = _align_benchmark(bmk_hist[bid], port_idx)
        first_valid = bmk_aligned.dropna()
        if first_valid.empty:
            result[api_key] = [None] * len(port_idx)
            continue

        result[api_key] = _scale(bmk_aligned, float(first_valid.iloc[0]), port_first)

    return result
