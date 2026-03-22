"""
static_data.py — Hard-coded data for dashboard endpoints that are not yet
sourced from the database.
"""

# ── Summary page ──────────────────────────────────────────────────────────────

METRICS = [
    {
        "label":       "Total Market Value",
        "value":       "$24.7M",
        "sub":         None,
        "change":      "▲ 3.2% MTD",
        "pos":         True,
        "accentColor": "#2b36d9",   # indigo_a700
    },
    {
        "label":       "Unrealized Gain",
        "value":       "$3.18M",
        "sub":         None,
        "change":      "▲ $192K today",
        "pos":         True,
        "accentColor": "#04ff1d",   # green_a700
    },
    {
        "label":       "Tail VaR (95%, 1D)",
        "value":       "$1.24M",
        "sub":         "95% confidence",
        "change":      "▼ $48K vs yesterday",
        "pos":         False,
        "accentColor": "#ff2e2e",   # red_500
    },
    {
        "label":       "Portfolio Volatility",
        "value":       "12.4%",
        "sub":         "annualized",
        "change":      "▼ 0.6% vs last month",
        "pos":         False,
        "accentColor": "#f59e0b",   # amber
    },
    {
        "label":       "Portfolio Beta",
        "value":       "0.86",
        "sub":         "vs S&P 500",
        "change":      None,
        "pos":         None,
        "accentColor": "#fd527b",   # pink_a200
    },
    {
        "label":       "Sharpe Ratio",
        "value":       "1.42",
        "sub":         None,
        "change":      "▲ 0.08 vs last qtr",
        "pos":         True,
        "accentColor": "#a084c9",   # purple
    },
]

CHART_DATA = {
    "1M": [
        {"label": "Jan 23",  "value": 23.1, "bmk": 22.8},
        {"label": "Jan 30",  "value": 23.5, "bmk": 23.1},
        {"label": "Feb 6",   "value": 24.0, "bmk": 23.5},
        {"label": "Feb 20",  "value": 24.7, "bmk": 24.0},
    ],
    "3M": [
        {"label": "Nov '25", "value": 21.8, "bmk": 21.4},
        {"label": "Dec '25", "value": 22.4, "bmk": 21.9},
        {"label": "Dec 15",  "value": 22.9, "bmk": 22.4},
        {"label": "Jan '26", "value": 23.5, "bmk": 22.9},
        {"label": "Jan 15",  "value": 23.9, "bmk": 23.3},
        {"label": "Feb '26", "value": 24.3, "bmk": 23.7},
        {"label": "Feb 20",  "value": 24.7, "bmk": 24.0},
    ],
    "1Y": [
        {"label": "Feb '25", "value": 18.2, "bmk": 17.9},
        {"label": "Mar '25", "value": 19.0, "bmk": 18.5},
        {"label": "Apr '25", "value": 19.8, "bmk": 19.1},
        {"label": "May '25", "value": 20.5, "bmk": 19.8},
        {"label": "Jun '25", "value": 20.1, "bmk": 19.6},
        {"label": "Jul '25", "value": 21.0, "bmk": 20.3},
        {"label": "Aug '25", "value": 21.8, "bmk": 21.0},
        {"label": "Sep '25", "value": 22.3, "bmk": 21.5},
        {"label": "Oct '25", "value": 22.9, "bmk": 22.0},
        {"label": "Nov '25", "value": 23.2, "bmk": 22.4},
        {"label": "Dec '25", "value": 23.7, "bmk": 22.9},
        {"label": "Jan '26", "value": 24.1, "bmk": 23.3},
        {"label": "Feb '26", "value": 24.7, "bmk": 24.0},
    ],
    "3Y": [
        {"label": "Feb '23", "value": 14.2, "bmk": 14.0},
        {"label": "Aug '23", "value": 15.8, "bmk": 15.4},
        {"label": "Feb '24", "value": 17.1, "bmk": 16.7},
        {"label": "Aug '24", "value": 18.5, "bmk": 18.1},
        {"label": "Feb '25", "value": 18.2, "bmk": 17.8},
        {"label": "Aug '25", "value": 21.8, "bmk": 21.2},
        {"label": "Feb '26", "value": 24.7, "bmk": 24.0},
    ],
    "ALL": [
        {"label": "2019",    "value":  8.4, "bmk":  8.3},
        {"label": "2020",    "value":  9.1, "bmk":  9.2},
        {"label": "2021",    "value": 11.3, "bmk": 11.0},
        {"label": "2022",    "value": 10.2, "bmk": 10.5},
        {"label": "2023",    "value": 14.2, "bmk": 13.8},
        {"label": "2024",    "value": 18.5, "bmk": 17.8},
        {"label": "H1 '25", "value": 20.5, "bmk": 19.8},
        {"label": "H2 '25", "value": 23.2, "bmk": 22.5},
        {"label": "Feb '26", "value": 24.7, "bmk": 24.0},
    ],
}

PORTFOLIO = [
    {"assetClass": "Fixed Income", "value": "$8.2M",  "weight": "33.2%", "ret": "+4.1%",  "pos": True,  "var": "$0.31M"},
    {"assetClass": "Equity",       "value": "$9.6M",  "weight": "38.9%", "ret": "+18.4%", "pos": True,  "var": "$0.72M"},
    {"assetClass": "Commodity",    "value": "$2.4M",  "weight": "9.7%",  "ret": "+9.3%",  "pos": True,  "var": "$0.19M"},
    {"assetClass": "Alternative",  "value": "$2.8M",  "weight": "11.3%", "ret": "+6.7%",  "pos": True,  "var": "$0.18M"},
    {"assetClass": "Cash",         "value": "$1.7M",  "weight": "6.9%",  "ret": "+0.8%",  "pos": True,  "var": "$0.02M"},
]

RISK = [
    {"factor": "Market Risk",   "pct": 82, "color": "#ff2e2e", "textColor": "#ff2e2e"},
    {"factor": "Interest Rate", "pct": 65, "color": "#2b36d9", "textColor": "#2b36d9"},
    {"factor": "Credit Risk",   "pct": 48, "color": "#fd527b", "textColor": "#fd527b"},
    {"factor": "Liquidity",     "pct": 31, "color": "#04ff1d", "textColor": "#04ff1d"},
    {"factor": "Currency",      "pct": 22, "color": "#a084c9", "textColor": "#a084c9"},
]

ASSET_ALLOCATION = [
    {"name": "Equity",       "port": 38.9, "bmk": 40.0, "color": "#2b36d9"},
    {"name": "Fixed Income", "port": 33.2, "bmk": 35.0, "color": "#04ff1d"},
    {"name": "Alternative",  "port": 11.3, "bmk": 10.0, "color": "#a084c9"},
    {"name": "Commodity",    "port":  9.7, "bmk": 10.0, "color": "#f59e0b"},
    {"name": "Cash",         "port":  6.9, "bmk":  5.0, "color": "#fd527b"},
]

# ── Risk page ─────────────────────────────────────────────────────────────────

RISK_SUMMARY_V2 = {
    "var1d95":      1_240_000,
    "var1d95Pct":   5.020,
    "var1d99":      1_800_000,
    "var1d99Pct":   7.287,
    "var10d99":     5_700_000,
    "var10d99Pct":  23.077,
    "es99":         2_350_000,
    "es99Pct":      9.514,
    "varLimitPct":  10.0,
    "volatility":   12.4,
    "sharpe":       1.42,
    "beta":         0.86,
    "maxDrawdown":  -8.3,
    "topFiveConc":  54.1,
    "activeAlerts": 2,
}

RISK_CONTRIBUTIONS = [
    {"ticker": "T10Y",  "name": "US Treasury 4.25% 2034",     "varContrib": 3.21, "esContrib":  3.60},
    {"ticker": "HFLS",  "name": "Apex L/S Equity Hedge Fund", "varContrib": 3.28, "esContrib":  3.75},
    {"ticker": "IEGA",  "name": "iShares EUR Corp Bond ETF",  "varContrib": 1.87, "esContrib":  2.21},
    {"ticker": "GLD",   "name": "SPDR Gold Shares",           "varContrib": 4.10, "esContrib":  4.65},
    {"ticker": "ASML",  "name": "ASML Holding NV",            "varContrib": 3.89, "esContrib":  4.05},
    {"ticker": "MSFT",  "name": "Microsoft Corp.",            "varContrib": 5.91, "esContrib":  6.08},
    {"ticker": "AAPL",  "name": "Apple Inc.",                 "varContrib": 6.82, "esContrib":  7.15},
    {"ticker": "NVDA",  "name": "NVIDIA Corp.",               "varContrib": 9.21, "esContrib": 10.43},
]

FACTOR_EXPOSURES_V2 = [
    {"factor": "Equity Beta",   "contribution": 42.1},
    {"factor": "Interest Rate", "contribution": 18.2},
    {"factor": "FX",            "contribution": 14.3},
    {"factor": "Credit Spread", "contribution": 11.8},
    {"factor": "Idiosyncratic", "contribution":  7.7},
    {"factor": "Commodity",     "contribution":  5.9},
]

STRESS_SCENARIOS = [
    {"name": "Global Financial Crisis", "period": "Sep–Dec 2008", "pnlUsd": -6_720_000, "pnlPct": -27.2, "severity": "high"},
    {"name": "COVID-19 Crash",          "period": "Feb–Mar 2020", "pnlUsd": -3_980_000, "pnlPct": -16.1, "severity": "high"},
    {"name": "2022 Rate Shock",         "period": "Jan–Oct 2022", "pnlUsd": -2_850_000, "pnlPct": -11.5, "severity": "high"},
    {"name": "Tech Selloff −20%",       "period": "Hypothetical", "pnlUsd": -2_210_000, "pnlPct":  -8.9, "severity": "medium"},
    {"name": "China Hard Landing",      "period": "Hypothetical", "pnlUsd": -1_830_000, "pnlPct":  -7.4, "severity": "medium"},
    {"name": "EM Currency Crisis",      "period": "Hypothetical", "pnlUsd": -1_170_000, "pnlPct":  -4.7, "severity": "medium"},
    {"name": "USD +10% (FX Shock)",     "period": "Hypothetical", "pnlUsd":   -870_000, "pnlPct":  -3.5, "severity": "low"},
    {"name": "Oil +50% Spike",          "period": "Hypothetical", "pnlUsd":   -420_000, "pnlPct":  -1.7, "severity": "low"},
]

RISK_ALERTS = [
    {"msg": "NVDA single-position VaR contribution at 9.2% (limit: 10%)", "level": "warning"},
    {"msg": "EM equity exposure at 5.8% vs 5.0% policy limit",            "level": "warning"},
]
