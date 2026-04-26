from dashboard.positions_db import read_portfolio_summary, read_var_limit
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


def _read_gauge_measure(account_id):
    """Return (gauge_measure, risk_horizon) from account_parameters."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT gauge_measure, risk_horizon FROM account_parameters WHERE account_id = %s ORDER BY updated_at DESC LIMIT 1",
                (account_id,),
            )
            row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)


def build_gauge_data(account_id: int) -> dict:
    """Shared gauge data used by both the Summary and Risk pages."""
    ps = read_portfolio_summary(account_id)
    sharpe = ps.get("sharpeVol")
    aum    = ps.get("aum")

    # Resolve gauge metric from account_parameters.gauge_measure
    measure, horizon = _read_gauge_measure(account_id)
    field, _ = resolve_metric(measure, horizon)
    gauge_raw = ps.get(field) if field else None
    var_value = gauge_raw if gauge_raw is not None else 16_900_000

    var_limit_raw = read_var_limit(account_id)
    var_limit = float(var_limit_raw) if var_limit_raw is not None else 25_000_000
    var_band     = var_limit * 0.05
    sharpe_value = sharpe if sharpe is not None else 0.21

    reading_value, reading_unit = _fmt_var(var_value)

    return {
        "sharpe_value":      sharpe_value,
        "sharpe_target":     0.15,
        "sharpe_band":       0.05,
        "sharpe_max":        max(0.50, sharpe_value * 1.25),
        "var_value":         var_value,
        "var_limit":         var_limit,
        "var_band":          var_band,
        "var_reading_value": reading_value,
        "var_reading_unit":  reading_unit,
        "var_target_label":  _target_label(var_limit),
    }
