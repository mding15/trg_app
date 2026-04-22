# Maps (measure, horizon) → (db_field_in_read_portfolio_summary, display_label)
METRIC_MAP = {
    ("ES 95%",  "1D"):  ("es1d95",   "ES 95% 1D"),
    ("ES 99%",  "1D"):  ("es99",     "ES 99% 1D"),
    ("VaR 95%", "1D"):  ("var1d95",  "VaR 95% 1D"),
    ("VaR 99%", "1D"):  ("var1d99",  "VaR 99% 1D"),
    ("VaR 99%", "10D"): ("var10d99", "VaR 99% 10D"),
}


def resolve_metric(measure: str | None, horizon: str | None) -> tuple[str | None, str | None]:
    """Return (db_field, display_label) for the given measure/horizon pair, or (None, None)."""
    if not measure or not horizon:
        return None, None
    return METRIC_MAP.get((measure, horizon), (None, None))
