from dashboard.positions_db import read_portfolio_summary, read_account_limit_multi
from dashboard.metric_utils import resolve_metric
from database2 import pg_connection


def _fmt_var(v):
    """Return (value_str, unit) for a raw dollar value."""
    if v is None:
        return "—", ""
    f = float(v)
    if abs(f) >= 1_000_000:
        return f"{f / 1e6:.1f}", "M"
    return f"{f / 1e3:.1f}", "K"


def _target_label(v) -> str:
    """Return a combined label string for the gauge tick mark (e.g. '25.0M' or '850K')."""
    if v is None:
        return ""
    f = float(v)
    if abs(f) >= 1_000_000:
        return f"{f / 1e6:.1f}M"
    return f"{f / 1e3:.0f}K"


def _read_benchmark_volatility(benchmark_id) -> float | None:
    """Return the most recent annualised volatility from benchmark_metrics, or None."""
    if benchmark_id is None:
        return None
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT volatility FROM benchmark_metrics "
                "WHERE benchmark_id = %s ORDER BY date DESC LIMIT 1",
                (benchmark_id,),
            )
            row = cur.fetchone()
    return float(row[0]) if row and row[0] is not None else None


def _read_gauge_measure(account_id):
    """Return (gauge_measure, risk_horizon, benchmark_id) from account_parameters + benchmark."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ap.gauge_measure, ap.risk_horizon, b.benchmark_id "
                "FROM account_parameters ap "
                "LEFT JOIN benchmark b ON b.benchmark_name = ap.benchmark "
                "WHERE ap.account_id = %s ORDER BY ap.updated_at DESC LIMIT 1",
                (account_id,),
            )
            row = cur.fetchone()
    return (row[0], row[1], row[2]) if row else (None, None, None)


def build_gauge_data(account_id: int) -> dict:
    """Shared gauge data used by both the Summary and Risk pages."""
    ps = read_portfolio_summary(account_id)
    aum = ps.get("aum")

    # Resolve gauge metric from account_parameters.gauge_measure
    measure, horizon, benchmark_id = _read_gauge_measure(account_id)
    field, _ = resolve_metric(measure, horizon)
    gauge_raw = ps.get(field) if field else None
    var_value = gauge_raw if gauge_raw is not None else 16_900_000

    limits = read_account_limit_multi(
        account_id, ["var_limit_dollar", "target_sharpe_var", "target_sharpe_vol", "es_limit_dollar", "target_sharpe_es"]
    )
    var_limit = limits.get("var_limit_dollar", 25_000_000)
    var_band  = var_limit * 0.05

    sharpe_var_raw = ps.get("sharpeVar")
    sharpe_var     = sharpe_var_raw if sharpe_var_raw is not None else 0.21

    vol_raw = ps.get("volatility")
    vol     = vol_raw if vol_raw is not None else 10.0

    sharpe_vol_raw = ps.get("sharpeVol")
    sharpe_vol     = sharpe_vol_raw if sharpe_vol_raw is not None else 0.21

    sharpe_var_target = limits.get("target_sharpe_var", 0.25)
    sharpe_vol_target = limits.get("target_sharpe_vol", 0.25)

    sharpe_var_band = sharpe_var_target * 0.20
    sharpe_vol_band = sharpe_vol_target * 0.20
    sharpe_var_max  = max(sharpe_var_target, sharpe_var) * 1.15
    sharpe_vol_max  = max(sharpe_vol_target, sharpe_vol) * 1.15

    _vol_bmk_raw = _read_benchmark_volatility(benchmark_id)
    vol_bmk  = (_vol_bmk_raw * 100) if _vol_bmk_raw is not None else 7.5
    vol_band = vol_bmk * 0.20
    vol_max  = max(vol_bmk, float(vol)) * 1.15

    reading_value, reading_unit = _fmt_var(var_value)

    # SR (ES) vs BMK gauge
    sharpe_es_raw    = ps.get("sharpeES")
    sharpe_es        = sharpe_es_raw if sharpe_es_raw is not None else 0.15
    sharpe_es_target = limits.get("target_sharpe_es", 0.12)
    sharpe_es_band   = sharpe_es_target * 0.20
    sharpe_es_max    = max(sharpe_es_target, sharpe_es) * 1.15

    # ES 95% vs Limit gauge
    es_raw   = ps.get("es1d95")
    es_value = es_raw if es_raw is not None else 24_100_000
    es_limit = limits.get("es_limit_dollar", var_limit * 1.30)
    es_band  = es_limit * 0.05
    es_reading_value, es_reading_unit = _fmt_var(es_value)

    # Beta vs S&P gauge
    beta_raw   = ps.get("beta")
    beta_value = float(beta_raw) if beta_raw is not None else 1.2
    beta_bmk   = 1.0
    beta_band  = 0.10
    beta_max   = max(beta_value, beta_bmk) * 1.5

    return {
        # VaR vs Limit gauge
        "var_value":         var_value,
        "var_limit":         var_limit,
        "var_band":          var_band,
        "var_reading_value": reading_value,
        "var_reading_unit":  reading_unit,
        "var_target_label":  _target_label(var_limit),
        # SR (VaR) vs BMK gauge
        "sharpe_var_value":  sharpe_var,
        "sharpe_var_target": sharpe_var_target,
        "sharpe_var_band":   sharpe_var_band,
        "sharpe_var_max":    sharpe_var_max,
        # SR (ES) vs BMK gauge
        "sharpe_es_value":   sharpe_es,
        "sharpe_es_target":  sharpe_es_target,
        "sharpe_es_band":    sharpe_es_band,
        "sharpe_es_max":     sharpe_es_max,
        # ES 95% vs Limit gauge
        "es_value":          es_value,
        "es_limit":          es_limit,
        "es_band":           es_band,
        "es_reading_value":  es_reading_value,
        "es_reading_unit":   es_reading_unit,
        "es_target_label":   _target_label(es_limit),
        # Vol vs BMK gauge
        "vol_value":         float(vol),
        "vol_bmk":           vol_bmk,
        "vol_band":          vol_band,
        "vol_max":           vol_max,
        # SR (Vol) vs BMK gauge
        "sharpe_vol_value":  sharpe_vol,
        "sharpe_vol_target": sharpe_vol_target,
        "sharpe_vol_band":   sharpe_vol_band,
        "sharpe_vol_max":    sharpe_vol_max,
        # Beta vs S&P gauge
        "beta_value":        beta_value,
        "beta_bmk":          beta_bmk,
        "beta_band":         beta_band,
        "beta_max":          beta_max,
    }
