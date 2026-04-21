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

# ── Risk page — Asset Allocation vs VaR drill-down chart ──────────────────────
# Source: power_bi/demo.xlsx — dm_fact_d_MgPositions joined with dm_d_Positions
# Fields: class_name, sc1, ticker, mv_weight (fraction), var_contrib (fraction of total VaR)

ASSET_ALLOCATION_DRILLDOWN = [
    {"class_name": "Equity",       "sc1": "EQ Fund",          "ticker": "SSDRF",   "mv_weight": 0.0200, "var_contrib": 0.0701},
    {"class_name": "Equity",       "sc1": "EQ ETF",           "ticker": "CSTNL",   "mv_weight": 0.0250, "var_contrib": 0.1003},
    {"class_name": "Equity",       "sc1": "EQ ETF",           "ticker": "MFMER",   "mv_weight": 0.0200, "var_contrib": 0.0541},
    {"class_name": "Equity",       "sc1": "EQ ETF",           "ticker": "LOMIZ",   "mv_weight": 0.0250, "var_contrib": 0.0031},
    {"class_name": "Equity",       "sc1": "EQ Fund",          "ticker": "JPMMZ",   "mv_weight": 0.0200, "var_contrib": 0.0649},
    {"class_name": "Equity",       "sc1": "EQ Fund",          "ticker": "TIESI",   "mv_weight": 0.0200, "var_contrib": 0.0479},
    {"class_name": "Fixed Income", "sc1": "FI Fund",          "ticker": "IMSCF",   "mv_weight": 0.0200, "var_contrib": 0.0109},
    {"class_name": "Fixed Income", "sc1": "FI ETF",           "ticker": "EUEIC",   "mv_weight": 0.0900, "var_contrib": 0.0074},
    {"class_name": "Fixed Income", "sc1": "FI ETF",           "ticker": "IHHSF",   "mv_weight": 0.0200, "var_contrib": 0.0323},
    {"class_name": "Fixed Income", "sc1": "FI Fund",          "ticker": "SCHHH",   "mv_weight": 0.1000, "var_contrib": 0.0392},
    {"class_name": "Fixed Income", "sc1": "FI Fund",          "ticker": "AGACZ",   "mv_weight": 0.1000, "var_contrib": 0.0560},
    {"class_name": "Fixed Income", "sc1": "FI ETF",           "ticker": "NUHGZ",   "mv_weight": 0.1900, "var_contrib": 0.1184},
    {"class_name": "Fixed Income", "sc1": "FI ETF",           "ticker": "AEUUU",   "mv_weight": 0.1900, "var_contrib": 0.3079},
    {"class_name": "Fixed Income", "sc1": "FI Fund",          "ticker": "MOTTZ",   "mv_weight": 0.0200, "var_contrib": 0.0206},
    {"class_name": "Alternatives", "sc1": "Real Estate/REITs","ticker": "CACCZ",   "mv_weight": 0.0150, "var_contrib": 0.0158},
    {"class_name": "Alternatives", "sc1": "Real Estate/REITs","ticker": "LODEZ",   "mv_weight": 0.0150, "var_contrib": 0.0087},
    {"class_name": "Alternatives", "sc1": "Private Credit",   "ticker": "PGLAZ",   "mv_weight": 0.0200, "var_contrib": 0.0142},
    {"class_name": "Alternatives", "sc1": "Private Credit",   "ticker": "LODUT",   "mv_weight": 0.0200, "var_contrib": 0.0139},
    {"class_name": "Alternatives", "sc1": "Private Credit",   "ticker": "ISHVF",   "mv_weight": 0.0200, "var_contrib": 0.0142},
]

# ── Summary page — Asset Allocation vs VaR Contribution drill-down ────────────
# Source: trg_web/prototype/summary.4.12.2026.html — allocDrillData
# Each level has: subtitle (str), rows (list of {label, mv, var, child|None})
# "child" is the key of the next level, or null for leaf nodes.

ALLOC_DRILLDOWN = {
    "all": {
        "subtitle": "Asset class · click a bar to drill down",
        "rows": [
            {"label": "Fixed Income", "mv": 45.3, "var": 19.7, "child": "fi-sub"},
            {"label": "Equity",       "mv": 34.8, "var": 70.8, "child": "eq-sub"},
            {"label": "Money Market", "mv": 11.1, "var":  1.0, "child": "mm-sub"},
            {"label": "Alternatives", "mv":  8.8, "var":  7.0, "child": "alt-sub"},
            {"label": "Commodities",  "mv":  1.8, "var":  2.0, "child": "comm-sub"},
        ],
    },

    "comm-sub": {
        "subtitle": "Commodities › subclass · click to drill down",
        "parent": "all",
        "rows": [
            {"label": "Oil", "mv": 42, "var": 38, "child": "comm1"},
            {"label": "Gold",    "mv": 30, "var": 34, "child": "comm2"},
            {"label": "Agriculture",       "mv": 18, "var": 20, "child": "comm3"},
            {"label": "Industrial Metals", "mv": 10, "var":  8, "child": "comm4"},],
    },

    "comm1": {
        "subtitle": "Oil · individual securities",
        "parent": "comm-sub",
        "parentLabel": "Commodities ",
        "rows": [
            {"label": "TLT",  "mv": 38, "var": 32, "child": None},
            {"label": "IEF",  "mv": 28, "var": 26, "child": None},
            {"label": "SHY",  "mv": 22, "var": 18, "child": None},
            {"label": "GOVT", "mv": 12, "var": 14, "child": None},
        ],
    },

    "comm2": {
        "subtitle": "Gas · individual securities",
        "parent": "comm-sub",
        "parentLabel": "Commodities ",
        "rows": [
            {"label": "TLT",  "mv": 38, "var": 32, "child": None},
            {"label": "IEF",  "mv": 28, "var": 26, "child": None},
            {"label": "SHY",  "mv": 22, "var": 18, "child": None},
            {"label": "GOVT", "mv": 12, "var": 14, "child": None},
        ],
    },

    "comm3": {
        "subtitle": "Agriculture · individual securities",
        "parent": "comm-sub",
        "parentLabel": "Commodities ",
        "rows": [
            {"label": "TLT",  "mv": 38, "var": 32, "child": None},
            {"label": "IEF",  "mv": 28, "var": 26, "child": None},
            {"label": "SHY",  "mv": 22, "var": 18, "child": None},
            {"label": "GOVT", "mv": 12, "var": 14, "child": None},
        ],
    },

    "comm4": {
        "subtitle": "Industrial Metals · individual securities",
        "parent": "comm-sub",
        "parentLabel": "Commodities ",
        "rows": [
            {"label": "TLT",  "mv": 38, "var": 32, "child": None},
            {"label": "IEF",  "mv": 28, "var": 26, "child": None},
            {"label": "SHY",  "mv": 22, "var": 18, "child": None},
            {"label": "GOVT", "mv": 12, "var": 14, "child": None},
        ],
    },


    "fi-sub": {
        "subtitle": "Fixed Income › subclass · click to drill down",
        "parent": "all",
        "rows": [
            {"label": "US Treasuries", "mv": 42, "var": 38, "child": "fi-treasuries"},
            {"label": "Corp Bonds",    "mv": 30, "var": 34, "child": "fi-corp"},
            {"label": "EM Debt",       "mv": 18, "var": 20, "child": "fi-em"},
            {"label": "MBS",           "mv": 10, "var":  8, "child": "fi-mbs"},
        ],
    },
    "fi-treasuries": {
        "subtitle": "US Treasuries · individual securities",
        "parent": "fi-sub",
        "parentLabel": "Fixed Income",
        "rows": [
            {"label": "TLT",  "mv": 38, "var": 32, "child": None},
            {"label": "IEF",  "mv": 28, "var": 26, "child": None},
            {"label": "SHY",  "mv": 22, "var": 18, "child": None},
            {"label": "GOVT", "mv": 12, "var": 14, "child": None},
        ],
    },
    "fi-corp": {
        "subtitle": "Corporate Bonds · individual securities",
        "parent": "fi-sub",
        "parentLabel": "Fixed Income",
        "rows": [
            {"label": "LQD",  "mv": 45, "var": 50, "child": None},
            {"label": "HYG",  "mv": 30, "var": 35, "child": None},
            {"label": "VCIT", "mv": 25, "var": 15, "child": None},
        ],
    },
    "fi-em": {
        "subtitle": "EM Debt · individual securities",
        "parent": "fi-sub",
        "parentLabel": "Fixed Income",
        "rows": [
            {"label": "EMB",  "mv": 55, "var": 60, "child": None},
            {"label": "VWOB", "mv": 45, "var": 40, "child": None},
        ],
    },
    "fi-mbs": {
        "subtitle": "MBS · individual securities",
        "parent": "fi-sub",
        "parentLabel": "Fixed Income",
        "rows": [
            {"label": "MBB",  "mv": 60, "var": 52, "child": None},
            {"label": "VMBS", "mv": 40, "var": 48, "child": None},
        ],
    },
    "eq-sub": {
        "subtitle": "Equity › subclass · click to drill down",
        "parent": "all",
        "rows": [
            {"label": "Large Cap", "mv": 52, "var": 65, "child": "eq-large"},
            {"label": "Mid Cap",   "mv": 22, "var": 18, "child": "eq-mid"},
            {"label": "Small Cap", "mv": 14, "var": 10, "child": "eq-small"},
            {"label": "Intl",      "mv": 12, "var":  7, "child": "eq-intl"},
        ],
    },
    "eq-large": {
        "subtitle": "Large Cap · individual securities",
        "parent": "eq-sub",
        "parentLabel": "Equity",
        "rows": [
            {"label": "AAPL",  "mv": 14.0, "var": 13.5, "child": None},
            {"label": "MSFT",  "mv": 11.0, "var":  6.8, "child": None},
            {"label": "AMZN",  "mv":  7.0, "var":  7.2, "child": None},
            {"label": "GOOGL", "mv":  7.5, "var":  3.8, "child": None},
            {"label": "NVDA",  "mv":  9.2, "var": 14.1, "child": None},
        ],
    },
    "eq-mid": {
        "subtitle": "Mid Cap · individual securities",
        "parent": "eq-sub",
        "parentLabel": "Equity",
        "rows": [
            {"label": "GNRC",  "mv": 35, "var": 40, "child": None},
            {"label": "FND",   "mv": 28, "var": 22, "child": None},
            {"label": "TREX",  "mv": 20, "var": 25, "child": None},
            {"label": "Other", "mv": 17, "var": 13, "child": None},
        ],
    },
    "eq-small": {
        "subtitle": "Small Cap · individual securities",
        "parent": "eq-sub",
        "parentLabel": "Equity",
        "rows": [
            {"label": "IWM",  "mv": 50, "var": 55, "child": None},
            {"label": "VBR",  "mv": 30, "var": 28, "child": None},
            {"label": "SCHA", "mv": 20, "var": 17, "child": None},
        ],
    },
    "eq-intl": {
        "subtitle": "International · individual securities",
        "parent": "eq-sub",
        "parentLabel": "Equity",
        "rows": [
            {"label": "EFA", "mv": 45, "var": 38, "child": None},
            {"label": "VEA", "mv": 33, "var": 40, "child": None},
            {"label": "EEM", "mv": 22, "var": 22, "child": None},
        ],
    },
    "mm-sub": {
        "subtitle": "Money Market › subclass · click to drill down",
        "parent": "all",
        "rows": [
            {"label": "T-Bills", "mv": 55, "var": 30, "child": "mm-tbills"},
            {"label": "Repo",    "mv": 30, "var": 50, "child": "mm-repo"},
            {"label": "MMF",     "mv": 15, "var": 20, "child": "mm-mmf"},
        ],
    },
    "mm-tbills": {
        "subtitle": "T-Bills · individual securities",
        "parent": "mm-sub",
        "parentLabel": "Money Market",
        "rows": [
            {"label": "BIL",  "mv": 60, "var": 50, "child": None},
            {"label": "SGOV", "mv": 40, "var": 50, "child": None},
        ],
    },
    "mm-repo": {
        "subtitle": "Repo · individual securities",
        "parent": "mm-sub",
        "parentLabel": "Money Market",
        "rows": [
            {"label": "REPO-A", "mv": 55, "var": 60, "child": None},
            {"label": "REPO-B", "mv": 45, "var": 40, "child": None},
        ],
    },
    "mm-mmf": {
        "subtitle": "Money Market Funds · individual securities",
        "parent": "mm-sub",
        "parentLabel": "Money Market",
        "rows": [
            {"label": "VMFXX", "mv": 65, "var": 55, "child": None},
            {"label": "SPAXX", "mv": 35, "var": 45, "child": None},
        ],
    },
    "alt-sub": {
        "subtitle": "Alternatives › subclass · click to drill down",
        "parent": "all",
        "rows": [
            {"label": "Private Eq.",  "mv": 45, "var": 50, "child": "alt-pe"},
            {"label": "Hedge Funds",  "mv": 35, "var": 32, "child": "alt-hf"},
            {"label": "Real Estate",  "mv": 20, "var": 18, "child": "alt-re"},
        ],
    },
    "alt-pe": {
        "subtitle": "Private Equity · individual funds",
        "parent": "alt-sub",
        "parentLabel": "Alternatives",
        "rows": [
            {"label": "KKR XII",    "mv": 40, "var": 45, "child": None},
            {"label": "BX IX",      "mv": 35, "var": 38, "child": None},
            {"label": "Apollo XI",  "mv": 25, "var": 17, "child": None},
        ],
    },
    "alt-hf": {
        "subtitle": "Hedge Funds · individual funds",
        "parent": "alt-sub",
        "parentLabel": "Alternatives",
        "rows": [
            {"label": "Bridgewater", "mv": 42, "var": 38, "child": None},
            {"label": "Citadel",     "mv": 33, "var": 36, "child": None},
            {"label": "Two Sigma",   "mv": 25, "var": 26, "child": None},
        ],
    },
    "alt-re": {
        "subtitle": "Real Estate · individual funds",
        "parent": "alt-sub",
        "parentLabel": "Alternatives",
        "rows": [
            {"label": "VNQ",    "mv": 50, "var": 45, "child": None},
            {"label": "REIT-A", "mv": 30, "var": 35, "child": None},
            {"label": "REIT-B", "mv": 20, "var": 20, "child": None},
        ],
    },
}

RISK_METRICS = [
    {"type": "Volatility",  "portfolio": 0.1230, "benchmark": 0.0980},
    {"type": "VaR 99%",     "portfolio": 0.0124, "benchmark": 0.0098},
]

RISK_ADJUSTED_RETURN = [
    {"type": "Sharpe (Vol)", "portfolio":  1.42, "benchmark":  1.15},
    {"type": "Sharpe (VaR)", "portfolio":  1.19, "benchmark":  0.96},
]

TOP_RISKS = [
    {"Ticker": "AEUUU",  "Security_Name": "Fixed Income ETF A",        "VaR_Contrib":  0.3079, "MV_Weight": 0.19},
    {"Ticker": "NUHGZ",  "Security_Name": "Commodity Gold Fund",        "VaR_Contrib":  0.1184, "MV_Weight": 0.19},
    {"Ticker": "CSTNL",  "Security_Name": "Equity ETF Core",            "VaR_Contrib":  0.1003, "MV_Weight": 0.025},
    {"Ticker": "SSDRF",  "Security_Name": "Equity Fund Select",         "VaR_Contrib":  0.0701, "MV_Weight": 0.02},
    {"Ticker": "JPMMZ",  "Security_Name": "Equity Fund Global",         "VaR_Contrib":  0.0649, "MV_Weight": 0.02},
    {"Ticker": "AGACZ",  "Security_Name": "Commodity Gas Trust",        "VaR_Contrib":  0.0560, "MV_Weight": 0.10},
    {"Ticker": "MFMER",  "Security_Name": "Equity Stock Mid",           "VaR_Contrib":  0.0541, "MV_Weight": 0.02},
    {"Ticker": "TIESI",  "Security_Name": "Equity Fund Income",         "VaR_Contrib":  0.0479, "MV_Weight": 0.02},
    {"Ticker": "EUEIC",  "Security_Name": "Fixed Income Bond EUR",      "VaR_Contrib": -0.0124, "MV_Weight": 0.09},
    {"Ticker": "USD.CCY","Security_Name": "Cash Money Market",          "VaR_Contrib": -0.0051, "MV_Weight": 0.05},
]

# ── Risk page — Historical VaR & Volatility ───────────────────────────────────

# ── Risk page — Parameters (portfolio settings) ───────────────────────────────

RISK_PARAMETERS = [
    {"label": "Size",         "value": "$178mm",    "badge": False},
    {"label": "As of Date",   "value": "4/10/2026", "badge": False},
    {"label": "Report Date",  "value": "4/10/2026", "badge": False},
    {"label": "Frequency",    "value": "Daily",     "badge": True},
    {"label": "Tail Measure", "value": "95% ES",    "badge": True},
    {"label": "Benchmark",    "value": "BM_50_50",  "badge": True},
    {"label": "Exp. Returns", "value": "Upload",    "badge": True},
]

# ── Risk page — Summary (measures + gauges) ───────────────────────────────────

RISK_SUMMARY_MOCK = {
    "measures": [
        {"label": "VaR 95% 1D", "value": "16.9", "unit": "M",  "sub": None},
        {"label": "ES 95% 1D",  "value": "24.1", "unit": "M",  "sub": None},
        {"label": "Volatility", "value": "5.9",  "unit": "%",  "sub": "annualised"},
        {"label": "Beta",       "value": "1.2",  "unit": None, "sub": "vs S&P 500"},
    ],
    "gaugeSharpe": {"value": 0.21, "target": 0.15, "band": 0.05, "max": 0.65},
    "gaugeRisk":   {"value": 16.9, "limit": 25.0,  "band": 1.25},
}

# ── Risk page — Concentrations ────────────────────────────────────────────────

RISK_CONCENTRATIONS = [
    {"name": "Asset Class", "ratio": 3.6},
    {"name": "Region",      "ratio": 2.8},
    {"name": "Currency",    "ratio": 2.2},
    {"name": "Industry",    "ratio": 1.5},
    {"name": "Single Name", "ratio": 0.4},
]

# ── Risk page — Top Contributors / Hedges ────────────────────────────────────

RISK_CONTRIB_MOCK = [
    {"name": "SPY",                   "pct":  7.6},
    {"name": "iShares MSCI Emerging", "pct":  5.6},
    {"name": "RSP",                   "pct":  4.2},
    {"name": "EWJ",                   "pct":  3.4},
    {"name": "FIAE LP65145998",       "pct":  2.8},
    {"name": "USMV",                  "pct":  2.6},
    {"name": "REZ",                   "pct":  2.2},
    {"name": "iShares Russell 1000",  "pct":  2.2},
    {"name": "EMCA",                  "pct":  2.1},
    {"name": "SPGP",                  "pct":  2.1},
    {"name": "VTV",                   "pct":  1.9},
    {"name": "IVW",                   "pct":  1.7},
    {"name": "QQQ",                   "pct":  1.5},
    {"name": "BIL",                   "pct": -0.1},
]

# ── Risk page — Asset Allocation drill-down (AllocBarCanvas levels format) ────

RISK_ASSET_LEVELS = {
    "all": {
        "subtitle": "Click to drill down", "parent": None, "parentLabel": None,
        "rows": [
            {"label": "Equity",       "mv": 46.3, "var": 55.6, "child": "eq-sub"},
            {"label": "Fixed Income", "mv": 18.9, "var": 12.4, "child": "fi-sub"},
            {"label": "Money Market", "mv": 14.7, "var":  6.2, "child": "mm-sub"},
            {"label": "Alternatives", "mv": 12.1, "var": 16.8, "child": "alt-sub"},
            {"label": "Commodities",  "mv":  5.0, "var":  7.2, "child": "com-sub"},
            {"label": "Cash",         "mv":  3.0, "var":  1.8, "child": None},
        ],
    },
    "eq-sub": {
        "subtitle": "Equity", "parent": "all", "parentLabel": "All Assets",
        "rows": [
            {"label": "Large Cap", "mv": 52, "var": 65, "child": "eq-large"},
            {"label": "Mid Cap",   "mv": 22, "var": 18, "child": "eq-mid"},
            {"label": "Small Cap", "mv": 14, "var": 10, "child": "eq-small"},
            {"label": "Intl",      "mv": 12, "var":  7, "child": "eq-intl"},
        ],
    },
    "fi-sub": {
        "subtitle": "Fixed Income", "parent": "all", "parentLabel": "All Assets",
        "rows": [
            {"label": "Govt Bonds", "mv": 38, "var": 28, "child": "fi-govt"},
            {"label": "Corp IG",    "mv": 32, "var": 30, "child": "fi-ig"},
            {"label": "Corp HY",    "mv": 18, "var": 26, "child": "fi-hy"},
            {"label": "MBS",        "mv": 12, "var": 16, "child": None},
        ],
    },
    "mm-sub": {
        "subtitle": "Money Market", "parent": "all", "parentLabel": "All Assets",
        "rows": [
            {"label": "T-Bills", "mv": 55, "var": 30, "child": "mm-tbills"},
            {"label": "Repo",    "mv": 30, "var": 50, "child": None},
            {"label": "MMF",     "mv": 15, "var": 20, "child": None},
        ],
    },
    "alt-sub": {
        "subtitle": "Alternatives", "parent": "all", "parentLabel": "All Assets",
        "rows": [
            {"label": "Private Eq.",  "mv": 45, "var": 50, "child": "alt-pe"},
            {"label": "Hedge Funds",  "mv": 35, "var": 32, "child": "alt-hf"},
            {"label": "Real Estate",  "mv": 20, "var": 18, "child": None},
        ],
    },
    "com-sub": {
        "subtitle": "Commodities", "parent": "all", "parentLabel": "All Assets",
        "rows": [
            {"label": "Energy",      "mv": 42, "var": 52, "child": "com-energy"},
            {"label": "Metals",      "mv": 35, "var": 30, "child": None},
            {"label": "Agriculture", "mv": 23, "var": 18, "child": None},
        ],
    },
    "eq-large": {
        "subtitle": "Equity - Large Cap", "parent": "eq-sub", "parentLabel": "Equity",
        "rows": [
            {"label": "AAPL",  "mv": 14.0, "var": 13.5, "child": None},
            {"label": "MSFT",  "mv": 11.0, "var":  6.8, "child": None},
            {"label": "NVDA",  "mv":  9.2, "var": 14.1, "child": None},
            {"label": "AMZN",  "mv":  7.0, "var":  7.2, "child": None},
            {"label": "GOOGL", "mv":  7.5, "var":  3.8, "child": None},
            {"label": "Other", "mv": 51.3, "var": 54.6, "child": None},
        ],
    },
    "eq-mid": {
        "subtitle": "Equity - Mid Cap", "parent": "eq-sub", "parentLabel": "Equity",
        "rows": [
            {"label": "GNRC",  "mv": 35, "var": 40, "child": None},
            {"label": "FND",   "mv": 28, "var": 22, "child": None},
            {"label": "TREX",  "mv": 20, "var": 25, "child": None},
            {"label": "Other", "mv": 17, "var": 13, "child": None},
        ],
    },
    "eq-small": {
        "subtitle": "Equity - Small Cap", "parent": "eq-sub", "parentLabel": "Equity",
        "rows": [
            {"label": "IWM",  "mv": 50, "var": 55, "child": None},
            {"label": "VBR",  "mv": 30, "var": 28, "child": None},
            {"label": "SCHA", "mv": 20, "var": 17, "child": None},
        ],
    },
    "eq-intl": {
        "subtitle": "Equity - International", "parent": "eq-sub", "parentLabel": "Equity",
        "rows": [
            {"label": "EFA", "mv": 45, "var": 38, "child": None},
            {"label": "VEA", "mv": 33, "var": 40, "child": None},
            {"label": "EEM", "mv": 22, "var": 22, "child": None},
        ],
    },
    "fi-govt": {
        "subtitle": "Fixed Income - Govt Bonds", "parent": "fi-sub", "parentLabel": "Fixed Income",
        "rows": [
            {"label": "2Y UST",  "mv": 28, "var": 18, "child": None},
            {"label": "5Y UST",  "mv": 32, "var": 24, "child": None},
            {"label": "10Y UST", "mv": 25, "var": 30, "child": None},
            {"label": "30Y UST", "mv": 15, "var": 28, "child": None},
        ],
    },
    "fi-ig": {
        "subtitle": "Fixed Income - Corp IG", "parent": "fi-sub", "parentLabel": "Fixed Income",
        "rows": [
            {"label": "AAPL 3.0%", "mv": 22, "var": 20, "child": None},
            {"label": "MSFT 2.4%", "mv": 18, "var": 16, "child": None},
            {"label": "JPM 3.2%",  "mv": 16, "var": 18, "child": None},
            {"label": "Other",     "mv": 44, "var": 46, "child": None},
        ],
    },
    "fi-hy": {
        "subtitle": "Fixed Income - High Yield", "parent": "fi-sub", "parentLabel": "Fixed Income",
        "rows": [
            {"label": "HYG ETF", "mv": 40, "var": 44, "child": None},
            {"label": "JNK ETF", "mv": 35, "var": 38, "child": None},
            {"label": "Other",   "mv": 25, "var": 18, "child": None},
        ],
    },
    "mm-tbills": {
        "subtitle": "Money Market - T-Bills", "parent": "mm-sub", "parentLabel": "Money Market",
        "rows": [
            {"label": "BIL",  "mv": 60, "var": 50, "child": None},
            {"label": "SGOV", "mv": 40, "var": 50, "child": None},
        ],
    },
    "alt-pe": {
        "subtitle": "Alternatives - Private Equity", "parent": "alt-sub", "parentLabel": "Alternatives",
        "rows": [
            {"label": "KKR XII",   "mv": 40, "var": 45, "child": None},
            {"label": "BX IX",     "mv": 35, "var": 38, "child": None},
            {"label": "Apollo XI", "mv": 25, "var": 17, "child": None},
        ],
    },
    "alt-hf": {
        "subtitle": "Alternatives - Hedge Funds", "parent": "alt-sub", "parentLabel": "Alternatives",
        "rows": [
            {"label": "Bridgewater", "mv": 42, "var": 38, "child": None},
            {"label": "Citadel",     "mv": 33, "var": 36, "child": None},
            {"label": "Two Sigma",   "mv": 25, "var": 26, "child": None},
        ],
    },
    "com-energy": {
        "subtitle": "Commodities - Energy", "parent": "com-sub", "parentLabel": "Commodities",
        "rows": [
            {"label": "USO (Crude)", "mv": 48, "var": 58, "child": None},
            {"label": "UNG (Gas)",   "mv": 32, "var": 28, "child": None},
            {"label": "Other",       "mv": 20, "var": 14, "child": None},
        ],
    },
}

# ── Risk page — Region drill-down ─────────────────────────────────────────────

RISK_REGION_LEVELS = {
    "all": {
        "subtitle": "Click to drill down", "parent": None, "parentLabel": None,
        "rows": [
            {"label": "North America", "mv": 52.1, "var": 58.4, "child": "reg-na"},
            {"label": "Europe",        "mv": 18.3, "var": 14.2, "child": "reg-eu"},
            {"label": "Asia Pacific",  "mv": 14.6, "var": 16.8, "child": "reg-ap"},
            {"label": "Emerging Mkt",  "mv":  9.4, "var":  8.1, "child": "reg-em"},
            {"label": "Latin America", "mv":  3.8, "var":  2.1, "child": "reg-la"},
            {"label": "Other",         "mv":  1.8, "var":  0.4, "child": None},
        ],
    },
    "reg-na": {
        "subtitle": "North America", "parent": "all", "parentLabel": "All Regions",
        "rows": [
            {"label": "United States", "mv": 44.2, "var": 51.3, "child": "reg-us"},
            {"label": "Canada",        "mv":  6.8, "var":  5.9, "child": "reg-ca"},
            {"label": "Mexico",        "mv":  1.1, "var":  1.2, "child": None},
        ],
    },
    "reg-eu": {
        "subtitle": "Europe", "parent": "all", "parentLabel": "All Regions",
        "rows": [
            {"label": "Germany",        "mv": 4.8, "var": 3.9, "child": "reg-de"},
            {"label": "France",         "mv": 4.1, "var": 3.2, "child": "reg-fr"},
            {"label": "United Kingdom", "mv": 3.9, "var": 3.1, "child": "reg-uk"},
            {"label": "Switzerland",    "mv": 2.8, "var": 2.0, "child": None},
            {"label": "Other Europe",   "mv": 2.7, "var": 2.0, "child": None},
        ],
    },
    "reg-ap": {
        "subtitle": "Asia Pacific", "parent": "all", "parentLabel": "All Regions",
        "rows": [
            {"label": "Japan",       "mv": 5.4, "var": 6.8, "child": "reg-jp"},
            {"label": "Australia",   "mv": 3.2, "var": 3.4, "child": None},
            {"label": "South Korea", "mv": 3.1, "var": 3.8, "child": "reg-kr"},
            {"label": "Taiwan",      "mv": 2.9, "var": 2.8, "child": None},
        ],
    },
    "reg-em": {
        "subtitle": "Emerging Markets", "parent": "all", "parentLabel": "All Regions",
        "rows": [
            {"label": "China",  "mv": 3.8, "var": 3.5, "child": "reg-cn"},
            {"label": "India",  "mv": 2.6, "var": 2.2, "child": "reg-in"},
            {"label": "Brazil", "mv": 1.8, "var": 1.4, "child": None},
            {"label": "Other",  "mv": 1.2, "var": 1.0, "child": None},
        ],
    },
    "reg-la": {
        "subtitle": "Latin America", "parent": "all", "parentLabel": "All Regions",
        "rows": [
            {"label": "Brazil",   "mv": 1.9, "var": 1.1, "child": None},
            {"label": "Chile",    "mv": 1.0, "var": 0.6, "child": None},
            {"label": "Colombia", "mv": 0.9, "var": 0.4, "child": None},
        ],
    },
    "reg-us": {
        "subtitle": "North America - United States", "parent": "reg-na", "parentLabel": "North America",
        "rows": [
            {"label": "AAPL",  "mv": 14.2, "var": 13.8, "child": None},
            {"label": "MSFT",  "mv": 12.6, "var": 11.4, "child": None},
            {"label": "NVDA",  "mv": 10.4, "var": 14.6, "child": None},
            {"label": "AMZN",  "mv":  8.8, "var":  8.2, "child": None},
            {"label": "GOOGL", "mv":  7.6, "var":  6.4, "child": None},
            {"label": "Other", "mv": 46.4, "var": 45.6, "child": None},
        ],
    },
    "reg-ca": {
        "subtitle": "North America - Canada", "parent": "reg-na", "parentLabel": "North America",
        "rows": [
            {"label": "CNQ",   "mv": 28.4, "var": 32.6, "child": None},
            {"label": "RY",    "mv": 24.6, "var": 22.8, "child": None},
            {"label": "TD",    "mv": 20.2, "var": 18.4, "child": None},
            {"label": "Other", "mv": 26.8, "var": 26.2, "child": None},
        ],
    },
    "reg-de": {
        "subtitle": "Europe - Germany", "parent": "reg-eu", "parentLabel": "Europe",
        "rows": [
            {"label": "SAP",     "mv": 28.4, "var": 26.8, "child": None},
            {"label": "Siemens", "mv": 24.6, "var": 22.4, "child": None},
            {"label": "BASF",    "mv": 18.2, "var": 20.6, "child": None},
            {"label": "Allianz", "mv": 14.8, "var": 13.6, "child": None},
            {"label": "Other",   "mv": 14.0, "var": 16.6, "child": None},
        ],
    },
    "reg-fr": {
        "subtitle": "Europe - France", "parent": "reg-eu", "parentLabel": "Europe",
        "rows": [
            {"label": "LVMH",          "mv": 32.4, "var": 30.8, "child": None},
            {"label": "TotalEnergies", "mv": 22.6, "var": 26.4, "child": None},
            {"label": "Airbus",        "mv": 20.8, "var": 18.6, "child": None},
            {"label": "Other",         "mv": 24.2, "var": 24.2, "child": None},
        ],
    },
    "reg-uk": {
        "subtitle": "Europe - United Kingdom", "parent": "reg-eu", "parentLabel": "Europe",
        "rows": [
            {"label": "SHEL",  "mv": 28.4, "var": 34.6, "child": None},
            {"label": "AZN",   "mv": 24.6, "var": 22.8, "child": None},
            {"label": "HSBA",  "mv": 20.4, "var": 22.6, "child": None},
            {"label": "ULVR",  "mv": 14.8, "var": 12.4, "child": None},
            {"label": "Other", "mv": 11.8, "var":  7.6, "child": None},
        ],
    },
    "reg-jp": {
        "subtitle": "Asia Pacific - Japan", "parent": "reg-ap", "parentLabel": "Asia Pacific",
        "rows": [
            {"label": "Toyota",   "mv": 26.4, "var": 30.8, "child": None},
            {"label": "Sony",     "mv": 22.8, "var": 24.6, "child": None},
            {"label": "Keyence",  "mv": 18.6, "var": 20.4, "child": None},
            {"label": "SoftBank", "mv": 16.4, "var": 14.2, "child": None},
            {"label": "Other",    "mv": 15.8, "var": 10.0, "child": None},
        ],
    },
    "reg-kr": {
        "subtitle": "Asia Pacific - South Korea", "parent": "reg-ap", "parentLabel": "Asia Pacific",
        "rows": [
            {"label": "Samsung",  "mv": 48.4, "var": 56.2, "child": None},
            {"label": "SK Hynix", "mv": 28.6, "var": 24.8, "child": None},
            {"label": "Hyundai",  "mv": 23.0, "var": 19.0, "child": None},
        ],
    },
    "reg-cn": {
        "subtitle": "Emerging Markets - China", "parent": "reg-em", "parentLabel": "Emerging Markets",
        "rows": [
            {"label": "Tencent", "mv": 28.4, "var": 26.8, "child": None},
            {"label": "Alibaba", "mv": 24.6, "var": 28.4, "child": None},
            {"label": "CATL",    "mv": 18.2, "var": 20.6, "child": None},
            {"label": "Meituan", "mv": 14.8, "var": 12.4, "child": None},
            {"label": "Other",   "mv": 14.0, "var": 11.8, "child": None},
        ],
    },
    "reg-in": {
        "subtitle": "Emerging Markets - India", "parent": "reg-em", "parentLabel": "Emerging Markets",
        "rows": [
            {"label": "Reliance",  "mv": 28.6, "var": 26.4, "child": None},
            {"label": "Infosys",   "mv": 24.4, "var": 22.8, "child": None},
            {"label": "HDFC Bank", "mv": 20.8, "var": 24.6, "child": None},
            {"label": "TCS",       "mv": 16.4, "var": 14.8, "child": None},
            {"label": "Other",     "mv":  9.8, "var": 11.4, "child": None},
        ],
    },
}

# ── Risk page — Industry drill-down ──────────────────────────────────────────

RISK_INDUSTRY_LEVELS = {
    "all": {
        "subtitle": "Click to drill down", "parent": None, "parentLabel": None,
        "rows": [
            {"label": "Technology",  "mv": 28.4, "var": 34.2, "child": "ind-tech"},
            {"label": "Financials",  "mv": 16.2, "var": 14.8, "child": "ind-fin"},
            {"label": "Healthcare",  "mv": 12.1, "var": 10.3, "child": "ind-hc"},
            {"label": "Industrials", "mv":  9.8, "var":  8.7, "child": "ind-ind"},
            {"label": "Consumer",    "mv":  9.1, "var":  7.4, "child": "ind-cons"},
            {"label": "Energy",      "mv":  7.3, "var":  9.6, "child": "ind-energy"},
            {"label": "Real Estate", "mv":  6.4, "var":  7.1, "child": "ind-re"},
            {"label": "Other",       "mv": 10.7, "var":  7.9, "child": None},
        ],
    },
    "ind-tech": {
        "subtitle": "Technology", "parent": "all", "parentLabel": "All Sectors",
        "rows": [
            {"label": "Semiconductors", "mv": 9.2, "var": 13.4, "child": "ind-semicon"},
            {"label": "Software",       "mv": 8.7, "var": 10.1, "child": "ind-software"},
            {"label": "Hardware",       "mv": 5.9, "var":  5.8, "child": "ind-hardware"},
            {"label": "IT Services",    "mv": 4.6, "var":  4.9, "child": None},
        ],
    },
    "ind-fin": {
        "subtitle": "Financials", "parent": "all", "parentLabel": "All Sectors",
        "rows": [
            {"label": "Banks",      "mv": 6.8, "var": 6.4, "child": "ind-banks"},
            {"label": "Insurance",  "mv": 4.2, "var": 3.8, "child": "ind-insur"},
            {"label": "Asset Mgmt", "mv": 3.1, "var": 2.9, "child": None},
            {"label": "Fintech",    "mv": 2.1, "var": 1.7, "child": None},
        ],
    },
    "ind-hc": {
        "subtitle": "Healthcare", "parent": "all", "parentLabel": "All Sectors",
        "rows": [
            {"label": "Pharma",      "mv": 5.4, "var": 4.8, "child": "ind-pharma"},
            {"label": "Biotech",     "mv": 3.8, "var": 3.2, "child": "ind-biotech"},
            {"label": "Med Devices", "mv": 2.9, "var": 2.3, "child": None},
        ],
    },
    "ind-ind": {
        "subtitle": "Industrials", "parent": "all", "parentLabel": "All Sectors",
        "rows": [
            {"label": "Aerospace", "mv": 3.4, "var": 3.1, "child": "ind-aero"},
            {"label": "Defence",   "mv": 2.8, "var": 2.6, "child": None},
            {"label": "Machinery", "mv": 2.2, "var": 1.9, "child": None},
            {"label": "Transport", "mv": 1.4, "var": 1.1, "child": None},
        ],
    },
    "ind-cons": {
        "subtitle": "Consumer", "parent": "all", "parentLabel": "All Sectors",
        "rows": [
            {"label": "Discretionary", "mv": 4.8, "var": 3.9, "child": "ind-disc"},
            {"label": "Staples",       "mv": 2.9, "var": 2.2, "child": "ind-stap"},
            {"label": "E-commerce",    "mv": 1.4, "var": 1.3, "child": None},
        ],
    },
    "ind-energy": {
        "subtitle": "Energy", "parent": "all", "parentLabel": "All Sectors",
        "rows": [
            {"label": "Oil & Gas",  "mv": 4.1, "var": 5.8, "child": "ind-oilgas"},
            {"label": "Renewables", "mv": 2.2, "var": 2.6, "child": None},
            {"label": "Utilities",  "mv": 1.0, "var": 1.2, "child": None},
        ],
    },
    "ind-re": {
        "subtitle": "Real Estate", "parent": "all", "parentLabel": "All Sectors",
        "rows": [
            {"label": "Office",      "mv": 3.2, "var": 3.8, "child": None},
            {"label": "Residential", "mv": 2.0, "var": 2.1, "child": None},
            {"label": "Industrial",  "mv": 1.2, "var": 1.2, "child": None},
        ],
    },
    "ind-semicon": {
        "subtitle": "Technology - Semiconductors", "parent": "ind-tech", "parentLabel": "Technology",
        "rows": [
            {"label": "NVDA",  "mv": 32.4, "var": 44.8, "child": None},
            {"label": "AVGO",  "mv": 18.6, "var": 20.2, "child": None},
            {"label": "AMD",   "mv": 14.2, "var": 16.8, "child": None},
            {"label": "TSM",   "mv": 12.8, "var": 10.4, "child": None},
            {"label": "INTC",  "mv": 10.4, "var":  5.2, "child": None},
            {"label": "Other", "mv": 11.6, "var":  2.6, "child": None},
        ],
    },
    "ind-software": {
        "subtitle": "Technology - Software", "parent": "ind-tech", "parentLabel": "Technology",
        "rows": [
            {"label": "MSFT",  "mv": 38.4, "var": 36.8, "child": None},
            {"label": "ADBE",  "mv": 18.2, "var": 20.4, "child": None},
            {"label": "CRM",   "mv": 16.8, "var": 18.6, "child": None},
            {"label": "Other", "mv": 26.6, "var": 24.2, "child": None},
        ],
    },
    "ind-hardware": {
        "subtitle": "Technology - Hardware", "parent": "ind-tech", "parentLabel": "Technology",
        "rows": [
            {"label": "AAPL",  "mv": 52.4, "var": 48.6, "child": None},
            {"label": "DELL",  "mv": 18.6, "var": 22.4, "child": None},
            {"label": "Other", "mv": 29.0, "var": 29.0, "child": None},
        ],
    },
    "ind-banks": {
        "subtitle": "Financials - Banks", "parent": "ind-fin", "parentLabel": "Financials",
        "rows": [
            {"label": "JPM",   "mv": 28.4, "var": 26.8, "child": None},
            {"label": "BAC",   "mv": 22.6, "var": 24.4, "child": None},
            {"label": "WFC",   "mv": 16.8, "var": 18.2, "child": None},
            {"label": "Other", "mv": 32.2, "var": 30.6, "child": None},
        ],
    },
    "ind-insur": {
        "subtitle": "Financials - Insurance", "parent": "ind-fin", "parentLabel": "Financials",
        "rows": [
            {"label": "BRK-B", "mv": 38.4, "var": 34.6, "child": None},
            {"label": "MET",   "mv": 24.2, "var": 26.8, "child": None},
            {"label": "AIG",   "mv": 20.6, "var": 22.4, "child": None},
            {"label": "Other", "mv": 16.8, "var": 16.2, "child": None},
        ],
    },
    "ind-pharma": {
        "subtitle": "Healthcare - Pharma", "parent": "ind-hc", "parentLabel": "Healthcare",
        "rows": [
            {"label": "LLY",   "mv": 32.6, "var": 30.4, "child": None},
            {"label": "JNJ",   "mv": 24.8, "var": 22.6, "child": None},
            {"label": "PFE",   "mv": 20.4, "var": 24.8, "child": None},
            {"label": "Other", "mv": 22.2, "var": 22.2, "child": None},
        ],
    },
    "ind-biotech": {
        "subtitle": "Healthcare - Biotech", "parent": "ind-hc", "parentLabel": "Healthcare",
        "rows": [
            {"label": "GILD",  "mv": 34.2, "var": 32.6, "child": None},
            {"label": "BIIB",  "mv": 28.6, "var": 30.4, "child": None},
            {"label": "MRNA",  "mv": 22.4, "var": 24.8, "child": None},
            {"label": "Other", "mv": 14.8, "var": 12.2, "child": None},
        ],
    },
    "ind-aero": {
        "subtitle": "Industrials - Aerospace", "parent": "ind-ind", "parentLabel": "Industrials",
        "rows": [
            {"label": "BA",  "mv": 38.4, "var": 42.6, "child": None},
            {"label": "RTX", "mv": 34.2, "var": 32.8, "child": None},
            {"label": "LMT", "mv": 27.4, "var": 24.6, "child": None},
        ],
    },
    "ind-disc": {
        "subtitle": "Consumer - Discretionary", "parent": "ind-cons", "parentLabel": "Consumer",
        "rows": [
            {"label": "AMZN",  "mv": 36.4, "var": 38.2, "child": None},
            {"label": "HD",    "mv": 24.8, "var": 22.6, "child": None},
            {"label": "NKE",   "mv": 20.6, "var": 22.4, "child": None},
            {"label": "Other", "mv": 18.2, "var": 16.8, "child": None},
        ],
    },
    "ind-stap": {
        "subtitle": "Consumer - Staples", "parent": "ind-cons", "parentLabel": "Consumer",
        "rows": [
            {"label": "KO",    "mv": 32.6, "var": 28.4, "child": None},
            {"label": "PG",    "mv": 28.4, "var": 26.8, "child": None},
            {"label": "WMT",   "mv": 24.8, "var": 28.6, "child": None},
            {"label": "Other", "mv": 14.2, "var": 16.2, "child": None},
        ],
    },
    "ind-oilgas": {
        "subtitle": "Energy - Oil & Gas", "parent": "ind-energy", "parentLabel": "Energy",
        "rows": [
            {"label": "XOM",   "mv": 36.4, "var": 42.8, "child": None},
            {"label": "CVX",   "mv": 28.6, "var": 32.4, "child": None},
            {"label": "COP",   "mv": 22.4, "var": 14.8, "child": None},
            {"label": "Other", "mv": 12.6, "var": 10.0, "child": None},
        ],
    },
}

# ── Risk page — Currency drill-down ──────────────────────────────────────────

RISK_CURRENCY_LEVELS = {
    "all": {
        "subtitle": "Click to drill down", "parent": None, "parentLabel": None,
        "rows": [
            {"label": "USD",   "mv": 58.3, "var": 52.1, "child": "cur-usd"},
            {"label": "EUR",   "mv": 14.2, "var": 16.8, "child": "cur-eur"},
            {"label": "JPY",   "mv":  8.1, "var": 10.4, "child": "cur-jpy"},
            {"label": "GBP",   "mv":  7.3, "var":  8.2, "child": "cur-gbp"},
            {"label": "CNY",   "mv":  5.9, "var":  7.1, "child": "cur-cny"},
            {"label": "Other", "mv":  6.2, "var":  5.4, "child": None},
        ],
    },
    "cur-usd": {
        "subtitle": "USD", "parent": "all", "parentLabel": "All Currencies",
        "rows": [
            {"label": "AAPL",  "mv": 14.2, "var": 13.8, "child": None},
            {"label": "MSFT",  "mv": 12.6, "var": 11.4, "child": None},
            {"label": "NVDA",  "mv": 10.4, "var": 14.6, "child": None},
            {"label": "AMZN",  "mv":  8.8, "var":  8.2, "child": None},
            {"label": "JPM",   "mv":  6.4, "var":  5.8, "child": None},
            {"label": "Other", "mv": 47.6, "var": 46.2, "child": None},
        ],
    },
    "cur-eur": {
        "subtitle": "EUR", "parent": "all", "parentLabel": "All Currencies",
        "rows": [
            {"label": "ASML",    "mv": 22.4, "var": 26.8, "child": None},
            {"label": "SAP",     "mv": 18.6, "var": 20.4, "child": None},
            {"label": "LVMH",    "mv": 16.4, "var": 14.8, "child": None},
            {"label": "Siemens", "mv": 14.2, "var": 12.6, "child": None},
            {"label": "Other",   "mv": 28.4, "var": 25.4, "child": None},
        ],
    },
    "cur-jpy": {
        "subtitle": "JPY", "parent": "all", "parentLabel": "All Currencies",
        "rows": [
            {"label": "Toyota",  "mv": 28.4, "var": 32.6, "child": None},
            {"label": "Sony",    "mv": 22.6, "var": 24.8, "child": None},
            {"label": "Keyence", "mv": 20.4, "var": 22.6, "child": None},
            {"label": "Other",   "mv": 28.6, "var": 20.0, "child": None},
        ],
    },
    "cur-gbp": {
        "subtitle": "GBP", "parent": "all", "parentLabel": "All Currencies",
        "rows": [
            {"label": "SHEL",  "mv": 28.4, "var": 34.6, "child": None},
            {"label": "AZN",   "mv": 24.6, "var": 22.8, "child": None},
            {"label": "HSBA",  "mv": 22.4, "var": 24.2, "child": None},
            {"label": "Other", "mv": 24.6, "var": 18.4, "child": None},
        ],
    },
    "cur-cny": {
        "subtitle": "CNY", "parent": "all", "parentLabel": "All Currencies",
        "rows": [
            {"label": "Tencent", "mv": 32.4, "var": 36.8, "child": None},
            {"label": "Alibaba", "mv": 28.6, "var": 30.4, "child": None},
            {"label": "CATL",    "mv": 22.4, "var": 20.6, "child": None},
            {"label": "Other",   "mv": 16.6, "var": 12.2, "child": None},
        ],
    },
}

# ── Stress Scenarios page ─────────────────────────────────────────────────────

STRESS_SCENARIOS_V2 = [
    {"name": "Global Financial Crisis",      "period": "Sep\u2013Dec 2008", "impactUSD": -6.72,  "impactPct": -27.2, "type": "Historical"},
    {"name": "COVID-19 Crash",               "period": "Feb\u2013Mar 2020", "impactUSD": -3.98,  "impactPct": -16.1, "type": "Historical"},
    {"name": "2022 Rate Shock",              "period": "Jan\u2013Oct 2022", "impactUSD": -2.85,  "impactPct": -11.5, "type": "Historical"},
    {"name": "2026 Iran War Oil Spike (+52%)","period": "Mar\u2013Apr 2026", "impactUSD": -1.94,  "impactPct":  -7.8, "type": "Historical"},
    {"name": "Tech Selloff \u221220%",       "period": "Hypothetical",      "impactUSD": -2.21,  "impactPct":  -8.9, "type": "Hypothetical"},
    {"name": "China Hard Landing",           "period": "Hypothetical",      "impactUSD": -1.83,  "impactPct":  -7.4, "type": "Hypothetical"},
    {"name": "EM Currency Crisis",           "period": "Hypothetical",      "impactUSD": -1.17,  "impactPct":  -4.7, "type": "Hypothetical"},
    {"name": "USD +10% (FX Shock)",          "period": "Hypothetical",      "impactUSD": -0.478, "impactPct":  -1.9, "type": "Hypothetical"},
]

# ── Portfolio page — Summary ──────────────────────────────────────────────────

PORTFOLIO_SUMMARY = {
    "aum":            177_700_000,
    "unrealizedGain":  12_400_000,
    "fundName":       "Consolidated Portfolio",
    "asOfDate":       "2026-04-10",
    "returns": [
        {"label": "SI",    "value":  7.5},
        {"label": "3Y",    "value": 13.3},
        {"label": "12M",   "value":  5.7},
        {"label": "YTD",   "value":  1.1},
        {"label": "Month", "value": -0.4},
        {"label": "Today", "value":  0.3},
    ],
}

# ── Portfolio page — Positions ────────────────────────────────────────────────

PORTFOLIO_POSITIONS = [
    {"id":  1, "ticker": "TLT",  "name": "iShares 20+ Yr Treasury Bond ETF",  "assetClass": "Bond",   "currency": "USD", "marketValue": 12_900_000, "weight":  7.3, "dayPnL":   7_740, "dayReturn":  0.06, "mtdReturn": -0.40, "ytdReturn":  1.70, "oneYearReturn":  3.2, "varContrib": 1.8},
    {"id":  2, "ticker": "SPY",  "name": "SPDR S&P 500 ETF Trust",             "assetClass": "Equity", "currency": "USD", "marketValue": 11_200_000, "weight":  6.3, "dayPnL":  60_480, "dayReturn":  0.54, "mtdReturn":  1.20, "ytdReturn":  6.50, "oneYearReturn":  9.8, "varContrib": 7.6},
    {"id":  3, "ticker": "QQQ",  "name": "Invesco QQQ Trust",                  "assetClass": "Equity", "currency": "USD", "marketValue":  9_800_000, "weight":  5.5, "dayPnL":  88_200, "dayReturn":  0.90, "mtdReturn":  2.10, "ytdReturn":  8.40, "oneYearReturn": 12.3, "varContrib": 5.2},
    {"id":  4, "ticker": "LQD",  "name": "iShares iBoxx $ Inv Grade Corp",     "assetClass": "Bond",   "currency": "USD", "marketValue": 10_800_000, "weight":  6.1, "dayPnL":  11_880, "dayReturn":  0.11, "mtdReturn":  0.30, "ytdReturn":  2.20, "oneYearReturn":  2.8, "varContrib": 2.1},
    {"id":  5, "ticker": "EWJ",  "name": "iShares MSCI Japan ETF",             "assetClass": "Equity", "currency": "JPY", "marketValue":  4_800_000, "weight":  2.7, "dayPnL":   6_240, "dayReturn":  0.13, "mtdReturn":  0.40, "ytdReturn":  1.70, "oneYearReturn":  4.1, "varContrib": 2.8},
    {"id":  6, "ticker": "GLD",  "name": "SPDR Gold Shares",                   "assetClass": "Alt",    "currency": "USD", "marketValue":  8_200_000, "weight":  4.6, "dayPnL": -16_400, "dayReturn": -0.20, "mtdReturn": -0.80, "ytdReturn":  3.10, "oneYearReturn":  8.5, "varContrib": 1.4},
    {"id":  7, "ticker": "UVXY", "name": "ProShares Ultra VIX Short-Term",     "assetClass": "Deriv",  "currency": "USD", "marketValue":  2_100_000, "weight":  1.2, "dayPnL":  -8_400, "dayReturn": -0.40, "mtdReturn": -2.10, "ytdReturn": -5.40, "oneYearReturn": -9.2, "varContrib": 0.8},
    {"id":  8, "ticker": "BIL",  "name": "SPDR Bloomberg 1-3 Month T-Bill",    "assetClass": "Cash",   "currency": "USD", "marketValue":  6_300_000, "weight":  3.5, "dayPnL":     630, "dayReturn":  0.01, "mtdReturn":  0.05, "ytdReturn":  0.60, "oneYearReturn":  0.9, "varContrib": 0.1},
    {"id":  9, "ticker": "VEA",  "name": "Vanguard FTSE Developed Mkts ETF",   "assetClass": "Equity", "currency": "USD", "marketValue":  7_400_000, "weight":  4.2, "dayPnL":  19_980, "dayReturn":  0.27, "mtdReturn":  0.80, "ytdReturn":  3.10, "oneYearReturn":  5.9, "varContrib": 1.7},
    {"id": 10, "ticker": "HYG",  "name": "iShares iBoxx $ High Yield Corp",    "assetClass": "Bond",   "currency": "USD", "marketValue":  7_200_000, "weight":  4.1, "dayPnL":  12_960, "dayReturn":  0.18, "mtdReturn":  0.60, "ytdReturn":  2.80, "oneYearReturn":  4.2, "varContrib": 3.1},
    {"id": 11, "ticker": "AAPL", "name": "Apple Inc.",                          "assetClass": "Equity", "currency": "USD", "marketValue":  5_800_000, "weight":  3.3, "dayPnL":  27_840, "dayReturn":  0.48, "mtdReturn":  1.80, "ytdReturn":  7.10, "oneYearReturn": 11.4, "varContrib": 4.2},
    {"id": 12, "ticker": "MSFT", "name": "Microsoft Corporation",               "assetClass": "Equity", "currency": "USD", "marketValue":  4_800_000, "weight":  2.7, "dayPnL":  28_800, "dayReturn":  0.60, "mtdReturn":  1.50, "ytdReturn":  6.80, "oneYearReturn": 10.1, "varContrib": 3.8},
    {"id": 13, "ticker": "EMB",  "name": "iShares J.P. Morgan USD EM Bond",    "assetClass": "Bond",   "currency": "USD", "marketValue":  8_000_000, "weight":  4.5, "dayPnL":  17_600, "dayReturn":  0.22, "mtdReturn":  0.90, "ytdReturn":  3.20, "oneYearReturn":  5.1, "varContrib": 2.4},
    {"id": 14, "ticker": "VNQ",  "name": "Vanguard Real Estate ETF",            "assetClass": "Alt",    "currency": "USD", "marketValue":  4_100_000, "weight":  2.3, "dayPnL":   8_200, "dayReturn":  0.20, "mtdReturn":  0.50, "ytdReturn":  2.30, "oneYearReturn":  3.9, "varContrib": 1.1},
    {"id": 15, "ticker": "IWM",  "name": "iShares Russell 2000 ETF",            "assetClass": "Equity", "currency": "USD", "marketValue":  4_300_000, "weight":  2.4, "dayPnL":  30_960, "dayReturn":  0.72, "mtdReturn":  1.90, "ytdReturn":  6.30, "oneYearReturn":  9.7, "varContrib": 2.1},
]

# ── Portfolio page — Chart (AUM history + 3 benchmarks) ──────────────────────
# Hardcoded series: portfolio (AUM in $M), sp500, blend6040, msci (index-relative)

PORTFOLIO_CHART = {
    "1M": {
        "labels":    ["Mar 18","Mar 19","Mar 20","Mar 21","Mar 24","Mar 25","Mar 26","Mar 27","Mar 28","Mar 31","Apr 1","Apr 2","Apr 3","Apr 4","Apr 7","Apr 8","Apr 9","Apr 10","Apr 11","Apr 14","Apr 15","Apr 16"],
        "portfolio": [169.1,169.8,170.4,171.0,171.5,172.2,172.8,173.5,173.9,174.6,175.2,175.8,176.3,176.9,175.4,176.0,176.8,177.2,177.5,177.0,177.5,177.7],
        "sp500":     [167.8,168.4,169.0,169.6,170.1,170.7,171.3,172.0,172.3,173.0,173.6,174.1,174.6,175.1,173.5,174.1,174.9,175.3,175.6,175.1,175.5,175.8],
        "blend6040": [168.2,168.7,169.2,169.7,170.2,170.7,171.2,171.8,172.1,172.7,173.2,173.6,174.0,174.5,173.2,173.7,174.3,174.6,174.8,174.4,174.8,175.0],
        "msci":      [166.9,167.5,168.1,168.7,169.2,169.8,170.4,171.1,171.4,172.1,172.7,173.2,173.7,174.2,172.6,173.2,174.0,174.4,174.7,174.2,174.6,174.9],
    },
    "3M": {
        "labels":    ["Jan 13","Jan 20","Jan 27","Feb 3","Feb 10","Feb 17","Feb 24","Mar 3","Mar 10","Mar 17","Mar 24","Mar 31","Apr 7","Apr 10"],
        "portfolio": [157.2,158.4,159.6,161.0,162.3,163.8,165.1,166.5,167.8,169.3,171.0,174.6,175.4,177.7],
        "sp500":     [155.8,157.0,158.1,159.5,160.7,162.1,163.3,164.7,165.9,167.3,169.0,172.5,173.3,175.5],
        "blend6040": [156.4,157.5,158.5,159.8,161.0,162.3,163.4,164.7,165.8,167.1,168.7,172.1,172.8,175.0],
        "msci":      [154.6,155.8,156.9,158.3,159.5,160.9,162.1,163.5,164.7,166.1,167.8,171.3,172.1,174.2],
    },
    "1Y": {
        "labels":    ["Apr '25","May '25","Jun '25","Jul '25","Aug '25","Sep '25","Oct '25","Nov '25","Dec '25","Jan '26","Feb '26","Mar '26","Apr '26"],
        "portfolio": [139.2,141.8,140.3,143.5,146.2,149.0,151.8,154.3,157.1,161.0,165.4,170.8,177.7],
        "sp500":     [137.5,140.0,138.4,141.5,144.1,146.8,149.5,151.9,154.6,158.3,162.6,167.8,174.5],
        "blend6040": [138.1,140.5,139.1,141.9,144.4,147.0,149.5,151.8,154.3,157.8,161.9,166.9,173.4],
        "msci":      [136.2,138.7,137.1,140.0,142.5,145.1,147.6,149.9,152.4,156.0,160.1,165.0,171.7],
    },
    "3Y": {
        "labels":    ["Apr '23","Jul '23","Oct '23","Jan '24","Apr '24","Jul '24","Oct '24","Jan '25","Apr '25","Jul '25","Oct '25","Jan '26","Apr '26"],
        "portfolio": [ 98.4,102.6,105.8,110.3,116.0,122.8,129.5,135.2,139.2,146.2,155.8,165.4,177.7],
        "sp500":     [ 97.1,101.2,104.3,108.7,114.3,121.0,127.5,133.1,137.5,144.1,153.5,163.0,175.0],
        "blend6040": [ 97.6,101.5,104.5,108.8,114.2,120.7,127.1,132.6,138.1,144.4,153.7,163.1,175.3],
        "msci":      [ 95.8, 99.9,103.0,107.4,112.9,119.6,126.1,131.5,136.2,142.5,151.8,161.2,173.4],
    },
    "ALL": {
        "labels":    ["2019","2020","2021","2022","2023","2024","H1 '25","H2 '25","Apr '26"],
        "portfolio": [68.2, 74.1, 90.3, 83.6, 100.8, 128.4, 145.2, 162.8, 177.7],
        "sp500":     [67.1, 73.0, 88.9, 82.0,  99.2, 126.5, 143.1, 160.5, 175.0],
        "blend6040": [67.5, 73.3, 89.4, 82.6,  99.7, 127.2, 143.8, 161.3, 175.6],
        "msci":      [65.8, 71.6, 87.2, 80.4,  97.5, 124.8, 141.4, 159.0, 173.4],
    },
}

# ── Portfolio page — Allocation drill-down (AllocBarCanvas levels + holdings tables)
# Five slices: asset, broker, region, industry, currency
# Each slice: levels map (for AllocBarCanvas) + holdings map (for DrillTable)

PORTFOLIO_ALLOC = {
    "asset": {
        "levels": {
            "all": {"subtitle": "Asset type \u00b7 click to drill down", "parent": None, "parentLabel": None,
                    "rows": [{"label": "Fixed Income", "mv": 45.3, "var": 0, "child": "fi"}, {"label": "Equity", "mv": 34.8, "var": 0, "child": "eq"}, {"label": "Money Market", "mv": 11.1, "var": 0, "child": "mm"}, {"label": "Alternatives", "mv": 8.8, "var": 0, "child": "alt"}]},
            "fi":  {"subtitle": "Fixed Income \u2014 sub-classes", "parent": "all", "parentLabel": "Asset Class",
                    "rows": [{"label": "US Treasuries", "mv": 42, "var": 0, "child": "fi-tsy"}, {"label": "Corp Bonds", "mv": 30, "var": 0, "child": "fi-corp"}, {"label": "EM Debt", "mv": 18, "var": 0, "child": "fi-em"}, {"label": "MBS", "mv": 10, "var": 0, "child": "fi-mbs"}]},
            "fi-tsy":  {"subtitle": "US Treasuries \u2014 securities", "parent": "fi", "parentLabel": "Fixed Income", "rows": [{"label": "TLT", "mv": 38, "var": 0, "child": None}, {"label": "IEF", "mv": 28, "var": 0, "child": None}, {"label": "SHY", "mv": 22, "var": 0, "child": None}, {"label": "GOVT", "mv": 12, "var": 0, "child": None}]},
            "fi-corp": {"subtitle": "Corp Bonds \u2014 securities",    "parent": "fi", "parentLabel": "Fixed Income", "rows": [{"label": "LQD",  "mv": 45, "var": 0, "child": None}, {"label": "HYG",  "mv": 30, "var": 0, "child": None}, {"label": "VCIT", "mv": 25, "var": 0, "child": None}]},
            "fi-em":   {"subtitle": "EM Debt \u2014 securities",       "parent": "fi", "parentLabel": "Fixed Income", "rows": [{"label": "EMB",  "mv": 55, "var": 0, "child": None}, {"label": "VWOB", "mv": 45, "var": 0, "child": None}]},
            "fi-mbs":  {"subtitle": "MBS \u2014 securities",           "parent": "fi", "parentLabel": "Fixed Income", "rows": [{"label": "MBB",  "mv": 60, "var": 0, "child": None}, {"label": "VMBS", "mv": 40, "var": 0, "child": None}]},
            "eq":  {"subtitle": "Equity \u2014 sub-classes", "parent": "all", "parentLabel": "Asset Class",
                    "rows": [{"label": "Large Cap", "mv": 52, "var": 0, "child": "eq-lc"}, {"label": "Mid Cap", "mv": 22, "var": 0, "child": "eq-mc"}, {"label": "Small Cap", "mv": 14, "var": 0, "child": "eq-sc"}, {"label": "Intl", "mv": 12, "var": 0, "child": "eq-intl"}]},
            "eq-lc":   {"subtitle": "Large Cap \u2014 securities",     "parent": "eq", "parentLabel": "Equity", "rows": [{"label": "AAPL", "mv": 18, "var": 0, "child": None}, {"label": "MSFT", "mv": 15, "var": 0, "child": None}, {"label": "NVDA", "mv": 14, "var": 0, "child": None}, {"label": "AMZN", "mv": 12, "var": 0, "child": None}, {"label": "GOOGL", "mv": 11, "var": 0, "child": None}, {"label": "Other", "mv": 30, "var": 0, "child": None}]},
            "eq-mc":   {"subtitle": "Mid Cap \u2014 securities",       "parent": "eq", "parentLabel": "Equity", "rows": [{"label": "GNRC", "mv": 32, "var": 0, "child": None}, {"label": "FND", "mv": 26, "var": 0, "child": None}, {"label": "TREX", "mv": 22, "var": 0, "child": None}, {"label": "Other", "mv": 20, "var": 0, "child": None}]},
            "eq-sc":   {"subtitle": "Small Cap \u2014 securities",     "parent": "eq", "parentLabel": "Equity", "rows": [{"label": "IWM", "mv": 50, "var": 0, "child": None}, {"label": "VBR", "mv": 30, "var": 0, "child": None}, {"label": "SCHA", "mv": 20, "var": 0, "child": None}]},
            "eq-intl": {"subtitle": "International \u2014 securities", "parent": "eq", "parentLabel": "Equity", "rows": [{"label": "EFA", "mv": 45, "var": 0, "child": None}, {"label": "VEA", "mv": 33, "var": 0, "child": None}, {"label": "EEM", "mv": 22, "var": 0, "child": None}]},
            "mm":  {"subtitle": "Money Market \u2014 sub-classes", "parent": "all", "parentLabel": "Asset Class",
                    "rows": [{"label": "T-Bills", "mv": 55, "var": 0, "child": "mm-tbills"}, {"label": "Repo", "mv": 30, "var": 0, "child": "mm-repo"}, {"label": "MMF", "mv": 15, "var": 0, "child": "mm-mmf"}]},
            "mm-tbills": {"subtitle": "T-Bills \u2014 securities",     "parent": "mm", "parentLabel": "Money Market", "rows": [{"label": "BIL",    "mv": 58, "var": 0, "child": None}, {"label": "SGOV",   "mv": 42, "var": 0, "child": None}]},
            "mm-repo":   {"subtitle": "Repo \u2014 instruments",       "parent": "mm", "parentLabel": "Money Market", "rows": [{"label": "REPO-A", "mv": 55, "var": 0, "child": None}, {"label": "REPO-B", "mv": 45, "var": 0, "child": None}]},
            "mm-mmf":    {"subtitle": "Money Market Funds",            "parent": "mm", "parentLabel": "Money Market", "rows": [{"label": "VMFXX", "mv": 65, "var": 0, "child": None}, {"label": "SPAXX",  "mv": 35, "var": 0, "child": None}]},
            "alt": {"subtitle": "Alternatives \u2014 sub-classes", "parent": "all", "parentLabel": "Asset Class",
                    "rows": [{"label": "Private Eq.", "mv": 45, "var": 0, "child": "alt-pe"}, {"label": "Hedge Funds", "mv": 35, "var": 0, "child": "alt-hf"}, {"label": "Real Estate", "mv": 20, "var": 0, "child": "alt-re"}]},
            "alt-pe": {"subtitle": "Private Equity \u2014 funds",      "parent": "alt", "parentLabel": "Alternatives", "rows": [{"label": "KKR XII",     "mv": 40, "var": 0, "child": None}, {"label": "BX IX",       "mv": 35, "var": 0, "child": None}, {"label": "Apollo XI",   "mv": 25, "var": 0, "child": None}]},
            "alt-hf": {"subtitle": "Hedge Funds \u2014 funds",         "parent": "alt", "parentLabel": "Alternatives", "rows": [{"label": "Bridgewater", "mv": 42, "var": 0, "child": None}, {"label": "Citadel",     "mv": 33, "var": 0, "child": None}, {"label": "Two Sigma",   "mv": 25, "var": 0, "child": None}]},
            "alt-re": {"subtitle": "Real Estate \u2014 funds",         "parent": "alt", "parentLabel": "Alternatives", "rows": [{"label": "VNQ",        "mv": 50, "var": 0, "child": None}, {"label": "REIT-A",      "mv": 30, "var": 0, "child": None}, {"label": "REIT-B",      "mv": 20, "var": 0, "child": None}]},
        },
        "holdings": {
            "all_asset":   {"cols": ["Asset type","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Fixed Income","80.4M","45.3%","+0.12%","+2.1%"],["Equity","61.7M","34.8%","+0.54%","+5.8%"],["Money Market","19.7M","11.1%","+0.01%","+0.5%"],["Alternatives","15.6M","8.8%","+0.08%","+1.2%"]], "foot": ["Total","177.4M","100%","+0.30%","+3.4%"]},
            "fi":          {"cols": ["Sub-class","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["US Treasuries","33.8M","42%","+0.08%","+1.9%"],["Corp Bonds","24.1M","30%","+0.14%","+2.4%"],["EM Debt","14.5M","18%","+0.21%","+3.1%"],["MBS","8.0M","10%","+0.06%","+1.2%"]], "foot": ["Total","80.4M","100%","+0.12%","+2.1%"]},
            "fi-tsy":      {"cols": ["Ticker","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["TLT","12.9M","38%","+0.06%","+1.7%"],["IEF","9.5M","28%","+0.07%","+1.5%"],["SHY","7.4M","22%","+0.04%","+0.9%"],["GOVT","4.1M","12%","+0.05%","+1.2%"]], "foot": ["Total","33.8M","100%","+0.08%","+1.9%"]},
            "fi-corp":     {"cols": ["Ticker","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["LQD","10.8M","45%","+0.11%","+2.2%"],["HYG","7.2M","30%","+0.18%","+2.8%"],["VCIT","6.0M","25%","+0.09%","+2.0%"]], "foot": ["Total","24.1M","100%","+0.14%","+2.4%"]},
            "fi-em":       {"cols": ["Ticker","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["EMB","8.0M","55%","+0.22%","+3.2%"],["VWOB","6.5M","45%","+0.19%","+3.0%"]], "foot": ["Total","14.5M","100%","+0.21%","+3.1%"]},
            "fi-mbs":      {"cols": ["Ticker","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["MBB","4.8M","60%","+0.06%","+1.1%"],["VMBS","3.2M","40%","+0.05%","+1.2%"]], "foot": ["Total","8.0M","100%","+0.06%","+1.2%"]},
            "eq":          {"cols": ["Sub-class","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Large Cap","32.1M","52%","+0.64%","+7.2%"],["Mid Cap","13.6M","22%","+0.52%","+5.1%"],["Small Cap","8.6M","14%","+0.71%","+6.3%"],["Intl","7.4M","12%","+0.28%","+3.2%"]], "foot": ["Total","61.7M","100%","+0.54%","+5.8%"]},
            "eq-lc":       {"cols": ["Ticker","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["AAPL","5.8M","18%","+0.48%","+7.1%"],["MSFT","4.8M","15%","+0.60%","+6.8%"],["NVDA","4.5M","14%","+1.20%","+12.3%"],["AMZN","3.9M","12%","+0.71%","+7.5%"],["GOOGL","3.5M","11%","+0.55%","+5.8%"],["Other","9.6M","30%","+0.52%","+5.4%"]], "foot": ["Total","32.1M","100%","+0.64%","+7.2%"]},
            "eq-mc":       {"cols": ["Ticker","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["GNRC","4.4M","32%","+0.55%","+5.3%"],["FND","3.5M","26%","+0.48%","+4.9%"],["TREX","3.0M","22%","+0.61%","+5.6%"],["Other","2.7M","20%","+0.45%","+4.7%"]], "foot": ["Total","13.6M","100%","+0.52%","+5.1%"]},
            "eq-sc":       {"cols": ["Ticker","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["IWM","4.3M","50%","+0.72%","+6.4%"],["VBR","2.6M","30%","+0.69%","+6.1%"],["SCHA","1.7M","20%","+0.73%","+6.5%"]], "foot": ["Total","8.6M","100%","+0.71%","+6.3%"]},
            "eq-intl":     {"cols": ["Ticker","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["EFA","3.3M","45%","+0.29%","+3.3%"],["VEA","2.4M","33%","+0.27%","+3.1%"],["EEM","1.6M","22%","+0.27%","+3.1%"]], "foot": ["Total","7.4M","100%","+0.28%","+3.2%"]},
            "mm":          {"cols": ["Sub-class","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["T-Bills","10.8M","55%","+0.01%","+0.6%"],["Repo","5.9M","30%","+0.01%","+0.5%"],["MMF","3.0M","15%","+0.01%","+0.4%"]], "foot": ["Total","19.7M","100%","+0.01%","+0.5%"]},
            "mm-tbills":   {"cols": ["Ticker","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["BIL","6.3M","58%","+0.01%","+0.6%"],["SGOV","4.5M","42%","+0.01%","+0.6%"]], "foot": ["Total","10.8M","100%","+0.01%","+0.6%"]},
            "mm-repo":     {"cols": ["Instrument","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["REPO-A","3.2M","55%","+0.01%","+0.5%"],["REPO-B","2.6M","45%","+0.01%","+0.5%"]], "foot": ["Total","5.9M","100%","+0.01%","+0.5%"]},
            "mm-mmf":      {"cols": ["Ticker","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["VMFXX","2.0M","65%","+0.01%","+0.4%"],["SPAXX","1.1M","35%","+0.01%","+0.4%"]], "foot": ["Total","3.0M","100%","+0.01%","+0.4%"]},
            "alt":         {"cols": ["Sub-class","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Private Eq.","7.0M","45%","+0.10%","+1.8%"],["Hedge Funds","5.5M","35%","+0.07%","+1.1%"],["Real Estate","3.1M","20%","+0.05%","+0.7%"]], "foot": ["Total","15.6M","100%","+0.08%","+1.2%"]},
            "alt-pe":      {"cols": ["Fund","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["KKR XII","2.8M","40%","+0.11%","+1.9%"],["BX IX","2.5M","35%","+0.09%","+1.7%"],["Apollo XI","1.7M","25%","+0.10%","+1.8%"]], "foot": ["Total","7.0M","100%","+0.10%","+1.8%"]},
            "alt-hf":      {"cols": ["Fund","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Bridgewater","2.3M","42%","+0.07%","+1.1%"],["Citadel","1.8M","33%","+0.08%","+1.2%"],["Two Sigma","1.4M","25%","+0.06%","+1.0%"]], "foot": ["Total","5.5M","100%","+0.07%","+1.1%"]},
            "alt-re":      {"cols": ["Fund","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["VNQ","1.6M","50%","+0.05%","+0.8%"],["REIT-A","0.9M","30%","+0.05%","+0.7%"],["REIT-B","0.6M","20%","+0.04%","+0.6%"]], "foot": ["Total","3.1M","100%","+0.05%","+0.7%"]},
        },
    },
    "broker": {
        "levels": {
            "all": {"subtitle": "Broker \u00b7 click to drill down", "parent": None, "parentLabel": None,
                    "rows": [{"label": "Interactive Brokers", "mv": 28.9, "var": 0, "child": "ibkr"}, {"label": "Fidelity", "mv": 25.5, "var": 0, "child": "fidel"}, {"label": "Charles Schwab", "mv": 21.8, "var": 0, "child": "schwab"}, {"label": "Goldman Sachs", "mv": 15.8, "var": 0, "child": "gs"}, {"label": "Morgan Stanley", "mv": 8.1, "var": 0, "child": "ms"}]},
            "ibkr":   {"subtitle": "Interactive Brokers \u2014 asset classes", "parent": "all", "parentLabel": "Broker", "rows": [{"label": "Equity", "mv": 55, "var": 0, "child": None}, {"label": "Fixed Income", "mv": 30, "var": 0, "child": None}, {"label": "Cash/MM", "mv": 15, "var": 0, "child": None}]},
            "fidel":  {"subtitle": "Fidelity \u2014 asset classes",            "parent": "all", "parentLabel": "Broker", "rows": [{"label": "Equity", "mv": 48, "var": 0, "child": None}, {"label": "Fixed Income", "mv": 38, "var": 0, "child": None}, {"label": "Cash/MM", "mv": 14, "var": 0, "child": None}]},
            "schwab": {"subtitle": "Charles Schwab \u2014 asset classes",      "parent": "all", "parentLabel": "Broker", "rows": [{"label": "Fixed Income", "mv": 52, "var": 0, "child": None}, {"label": "Equity", "mv": 32, "var": 0, "child": None}, {"label": "Cash/MM", "mv": 16, "var": 0, "child": None}]},
            "gs":     {"subtitle": "Goldman Sachs \u2014 asset classes",       "parent": "all", "parentLabel": "Broker", "rows": [{"label": "Fixed Income", "mv": 60, "var": 0, "child": None}, {"label": "Alternatives", "mv": 25, "var": 0, "child": None}, {"label": "Equity", "mv": 15, "var": 0, "child": None}]},
            "ms":     {"subtitle": "Morgan Stanley \u2014 asset classes",      "parent": "all", "parentLabel": "Broker", "rows": [{"label": "Equity", "mv": 50, "var": 0, "child": None}, {"label": "Fixed Income", "mv": 35, "var": 0, "child": None}, {"label": "Alternatives", "mv": 15, "var": 0, "child": None}]},
        },
        "holdings": {
            "all_broker": {"cols": ["Broker","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Interactive Brokers","51.3M","28.9%","+0.87%","+4.1%"],["Fidelity","45.2M","25.5%","+0.42%","+5.2%"],["Charles Schwab","38.7M","21.8%","-0.18%","+3.8%"],["Goldman Sachs","28.1M","15.8%","+0.11%","+1.9%"],["Morgan Stanley","14.4M","8.1%","-0.05%","+2.3%"]], "foot": ["Total","177.7M","100%","+0.30%","+3.4%"]},
            "ibkr":   {"cols": ["Asset class","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Equity","28.2M","55%","+1.20%","+5.8%"],["Fixed Income","15.4M","30%","+0.38%","+2.9%"],["Cash/MM","7.7M","15%","+0.01%","+0.6%"]], "foot": ["Total","51.3M","100%","+0.87%","+4.1%"]},
            "fidel":  {"cols": ["Asset class","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Equity","21.7M","48%","+0.55%","+6.1%"],["Fixed Income","17.2M","38%","+0.28%","+4.2%"],["Cash/MM","6.3M","14%","+0.01%","+0.5%"]], "foot": ["Total","45.2M","100%","+0.42%","+5.2%"]},
            "schwab": {"cols": ["Asset class","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Fixed Income","20.1M","52%","-0.12%","+3.1%"],["Equity","12.4M","32%","-0.28%","+4.5%"],["Cash/MM","6.2M","16%","+0.01%","+0.5%"]], "foot": ["Total","38.7M","100%","-0.18%","+3.8%"]},
            "gs":     {"cols": ["Asset class","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Fixed Income","16.9M","60%","+0.14%","+2.1%"],["Alternatives","7.0M","25%","+0.08%","+1.3%"],["Equity","4.2M","15%","+0.06%","+1.2%"]], "foot": ["Total","28.1M","100%","+0.11%","+1.9%"]},
            "ms":     {"cols": ["Asset class","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Equity","7.2M","50%","-0.04%","+2.8%"],["Fixed Income","5.0M","35%","-0.06%","+1.9%"],["Alternatives","2.2M","15%","-0.05%","+1.1%"]], "foot": ["Total","14.4M","100%","-0.05%","+2.3%"]},
        },
    },
    "region": {
        "levels": {
            "all": {"subtitle": "Region \u00b7 click to drill down", "parent": None, "parentLabel": None,
                    "rows": [{"label": "North America", "mv": 52, "var": 0, "child": "na"}, {"label": "Europe", "mv": 20, "var": 0, "child": "eu"}, {"label": "Asia Pacific", "mv": 16, "var": 0, "child": "ap"}, {"label": "Emerging Mkts", "mv": 8, "var": 0, "child": "em"}, {"label": "Other", "mv": 4, "var": 0, "child": "roth"}]},
            "na":   {"subtitle": "North America \u2014 by asset class", "parent": "all", "parentLabel": "Region", "rows": [{"label": "Equity", "mv": 58, "var": 0, "child": None}, {"label": "Fixed Income", "mv": 32, "var": 0, "child": None}, {"label": "Cash/MM", "mv": 10, "var": 0, "child": None}]},
            "eu":   {"subtitle": "Europe \u2014 by asset class",        "parent": "all", "parentLabel": "Region", "rows": [{"label": "Equity", "mv": 50, "var": 0, "child": None}, {"label": "Fixed Income", "mv": 38, "var": 0, "child": None}, {"label": "Cash/MM", "mv": 12, "var": 0, "child": None}]},
            "ap":   {"subtitle": "Asia Pacific \u2014 by asset class",  "parent": "all", "parentLabel": "Region", "rows": [{"label": "Equity", "mv": 62, "var": 0, "child": None}, {"label": "Fixed Income", "mv": 28, "var": 0, "child": None}, {"label": "Cash/MM", "mv": 10, "var": 0, "child": None}]},
            "em":   {"subtitle": "Emerging Markets \u2014 by asset class","parent": "all","parentLabel": "Region", "rows": [{"label": "Equity", "mv": 55, "var": 0, "child": None}, {"label": "Fixed Income", "mv": 40, "var": 0, "child": None}, {"label": "Cash/MM", "mv": 5, "var": 0, "child": None}]},
            "roth": {"subtitle": "Other Regions \u2014 by asset class",  "parent": "all", "parentLabel": "Region", "rows": [{"label": "Equity", "mv": 45, "var": 0, "child": None}, {"label": "Fixed Income", "mv": 40, "var": 0, "child": None}, {"label": "Cash/MM", "mv": 15, "var": 0, "child": None}]},
        },
        "holdings": {
            "all_region": {"cols": ["Region","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["North America","92.4M","52%","+0.41%","+4.9%"],["Europe","35.5M","20%","+0.18%","+2.3%"],["Asia Pacific","28.4M","16%","+0.22%","+1.8%"],["Emerging Mkts","14.2M","8%","-0.12%","+0.9%"],["Other","7.1M","4%","+0.05%","+1.1%"]], "foot": ["Total","177.6M","100%","+0.30%","+3.4%"]},
            "na":   {"cols": ["Asset class","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Equity","53.6M","58%","+0.54%","+5.8%"],["Fixed Income","29.6M","32%","+0.20%","+3.1%"],["Cash/MM","9.2M","10%","+0.01%","+0.5%"]], "foot": ["Total","92.4M","100%","+0.41%","+4.9%"]},
            "eu":   {"cols": ["Asset class","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Equity","17.8M","50%","+0.21%","+2.6%"],["Fixed Income","13.5M","38%","+0.14%","+2.0%"],["Cash/MM","4.3M","12%","+0.01%","+0.3%"]], "foot": ["Total","35.5M","100%","+0.18%","+2.3%"]},
            "ap":   {"cols": ["Asset class","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Equity","17.6M","62%","+0.27%","+2.1%"],["Fixed Income","7.9M","28%","+0.14%","+1.3%"],["Cash/MM","2.8M","10%","+0.01%","+0.2%"]], "foot": ["Total","28.4M","100%","+0.22%","+1.8%"]},
            "em":   {"cols": ["Asset class","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Equity","7.8M","55%","-0.14%","+1.1%"],["Fixed Income","5.7M","40%","-0.09%","+0.7%"],["Cash/MM","0.7M","5%","+0.01%","+0.2%"]], "foot": ["Total","14.2M","100%","-0.12%","+0.9%"]},
            "roth": {"cols": ["Asset class","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Equity","3.2M","45%","+0.06%","+1.3%"],["Fixed Income","2.8M","40%","+0.04%","+0.9%"],["Cash/MM","1.1M","15%","+0.01%","+0.2%"]], "foot": ["Total","7.1M","100%","+0.05%","+1.1%"]},
        },
    },
    "industry": {
        "levels": {
            "all": {"subtitle": "Industry \u00b7 click to drill down", "parent": None, "parentLabel": None,
                    "rows": [{"label": "Fixed Income", "mv": 45, "var": 0, "child": "ind-fi"}, {"label": "Technology", "mv": 18, "var": 0, "child": "ind-tech"}, {"label": "Healthcare", "mv": 12, "var": 0, "child": "ind-hc"}, {"label": "Financials", "mv": 10, "var": 0, "child": "ind-fin"}, {"label": "Consumer", "mv": 8, "var": 0, "child": "ind-cons"}, {"label": "Other", "mv": 7, "var": 0, "child": "ind-oth"}]},
            "ind-fi":   {"subtitle": "Fixed Income \u2014 sub-classes",  "parent": "all", "parentLabel": "Industry", "rows": [{"label": "US Treasuries", "mv": 42, "var": 0, "child": None}, {"label": "Corp Bonds", "mv": 30, "var": 0, "child": None}, {"label": "EM Debt", "mv": 18, "var": 0, "child": None}, {"label": "MBS", "mv": 10, "var": 0, "child": None}]},
            "ind-tech": {"subtitle": "Technology \u2014 securities",      "parent": "all", "parentLabel": "Industry", "rows": [{"label": "AAPL", "mv": 18, "var": 0, "child": None}, {"label": "MSFT", "mv": 15, "var": 0, "child": None}, {"label": "NVDA", "mv": 14, "var": 0, "child": None}, {"label": "AMZN", "mv": 12, "var": 0, "child": None}, {"label": "Other", "mv": 41, "var": 0, "child": None}]},
            "ind-hc":   {"subtitle": "Healthcare \u2014 securities",      "parent": "all", "parentLabel": "Industry", "rows": [{"label": "JNJ", "mv": 35, "var": 0, "child": None}, {"label": "UNH", "mv": 30, "var": 0, "child": None}, {"label": "PFE", "mv": 20, "var": 0, "child": None}, {"label": "Other", "mv": 15, "var": 0, "child": None}]},
            "ind-fin":  {"subtitle": "Financials \u2014 securities",      "parent": "all", "parentLabel": "Industry", "rows": [{"label": "JPM", "mv": 40, "var": 0, "child": None}, {"label": "BAC", "mv": 30, "var": 0, "child": None}, {"label": "GS", "mv": 30, "var": 0, "child": None}]},
            "ind-cons": {"subtitle": "Consumer \u2014 securities",        "parent": "all", "parentLabel": "Industry", "rows": [{"label": "AMZN", "mv": 45, "var": 0, "child": None}, {"label": "WMT", "mv": 30, "var": 0, "child": None}, {"label": "Other", "mv": 25, "var": 0, "child": None}]},
            "ind-oth":  {"subtitle": "Other Industries",                 "parent": "all", "parentLabel": "Industry", "rows": [{"label": "Energy", "mv": 40, "var": 0, "child": None}, {"label": "Utilities", "mv": 35, "var": 0, "child": None}, {"label": "Materials", "mv": 25, "var": 0, "child": None}]},
        },
        "holdings": {
            "all_industry": {"cols": ["Industry","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Fixed Income","79.9M","45%","+0.12%","+2.1%"],["Technology","32.0M","18%","+0.88%","+9.4%"],["Healthcare","21.3M","12%","+0.33%","+4.2%"],["Financials","17.8M","10%","+0.21%","+3.1%"],["Consumer","14.2M","8%","+0.15%","+2.8%"],["Other","12.4M","7%","+0.08%","+1.5%"]], "foot": ["Total","177.6M","100%","+0.30%","+3.9%"]},
            "ind-fi":   {"cols": ["Sub-class","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["US Treasuries","33.8M","42%","+0.08%","+1.9%"],["Corp Bonds","24.1M","30%","+0.14%","+2.4%"],["EM Debt","14.5M","18%","+0.21%","+3.1%"],["MBS","8.0M","10%","+0.06%","+1.2%"]], "foot": ["Total","79.9M","100%","+0.12%","+2.1%"]},
            "ind-tech": {"cols": ["Ticker","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["AAPL","5.8M","18%","+0.48%","+7.1%"],["MSFT","4.8M","15%","+0.60%","+6.8%"],["NVDA","4.5M","14%","+1.20%","+12.3%"],["AMZN","3.9M","12%","+0.71%","+7.5%"],["Other","13.0M","41%","+0.82%","+9.8%"]], "foot": ["Total","32.0M","100%","+0.88%","+9.4%"]},
            "ind-hc":   {"cols": ["Ticker","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["JNJ","7.5M","35%","+0.30%","+3.9%"],["UNH","6.4M","30%","+0.37%","+4.5%"],["PFE","4.3M","20%","+0.28%","+3.8%"],["Other","3.2M","15%","+0.32%","+4.4%"]], "foot": ["Total","21.3M","100%","+0.33%","+4.2%"]},
            "ind-fin":  {"cols": ["Ticker","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["JPM","7.1M","40%","+0.24%","+3.5%"],["BAC","5.3M","30%","+0.18%","+2.8%"],["GS","5.3M","30%","+0.21%","+3.1%"]], "foot": ["Total","17.8M","100%","+0.21%","+3.1%"]},
            "ind-cons": {"cols": ["Ticker","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["AMZN","6.4M","45%","+0.17%","+3.1%"],["WMT","4.3M","30%","+0.12%","+2.5%"],["Other","3.5M","25%","+0.14%","+2.7%"]], "foot": ["Total","14.2M","100%","+0.15%","+2.8%"]},
            "ind-oth":  {"cols": ["Sector","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Energy","5.0M","40%","+0.09%","+1.8%"],["Utilities","4.3M","35%","+0.07%","+1.4%"],["Materials","3.1M","25%","+0.07%","+1.3%"]], "foot": ["Total","12.4M","100%","+0.08%","+1.5%"]},
        },
    },
    "currency": {
        "levels": {
            "all": {"subtitle": "Currency \u00b7 click to drill down", "parent": None, "parentLabel": None,
                    "rows": [{"label": "USD", "mv": 54, "var": 0, "child": "usd"}, {"label": "EUR", "mv": 18, "var": 0, "child": "eur"}, {"label": "GBP", "mv": 10, "var": 0, "child": "gbp"}, {"label": "JPY", "mv": 8, "var": 0, "child": "jpy"}, {"label": "Other", "mv": 10, "var": 0, "child": "fxoth"}]},
            "usd":   {"subtitle": "USD \u2014 by asset class",  "parent": "all", "parentLabel": "Currency", "rows": [{"label": "Equity", "mv": 52, "var": 0, "child": None}, {"label": "Fixed Income", "mv": 38, "var": 0, "child": None}, {"label": "Cash/MM", "mv": 10, "var": 0, "child": None}]},
            "eur":   {"subtitle": "EUR \u2014 by asset class",  "parent": "all", "parentLabel": "Currency", "rows": [{"label": "Equity", "mv": 48, "var": 0, "child": None}, {"label": "Fixed Income", "mv": 40, "var": 0, "child": None}, {"label": "Cash/MM", "mv": 12, "var": 0, "child": None}]},
            "gbp":   {"subtitle": "GBP \u2014 by asset class",  "parent": "all", "parentLabel": "Currency", "rows": [{"label": "Equity", "mv": 50, "var": 0, "child": None}, {"label": "Fixed Income", "mv": 38, "var": 0, "child": None}, {"label": "Cash/MM", "mv": 12, "var": 0, "child": None}]},
            "jpy":   {"subtitle": "JPY \u2014 by asset class",  "parent": "all", "parentLabel": "Currency", "rows": [{"label": "Equity", "mv": 55, "var": 0, "child": None}, {"label": "Fixed Income", "mv": 35, "var": 0, "child": None}, {"label": "Cash/MM", "mv": 10, "var": 0, "child": None}]},
            "fxoth": {"subtitle": "Other FX \u2014 by currency","parent": "all", "parentLabel": "Currency", "rows": [{"label": "CHF", "mv": 35, "var": 0, "child": None}, {"label": "AUD", "mv": 30, "var": 0, "child": None}, {"label": "CAD", "mv": 20, "var": 0, "child": None}, {"label": "Other", "mv": 15, "var": 0, "child": None}]},
        },
        "holdings": {
            "all_currency": {"cols": ["Currency","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["USD","95.9M","54%","+0.35%","+4.1%"],["EUR","32.0M","18%","+0.22%","+2.8%"],["GBP","17.8M","10%","+0.18%","+2.2%"],["JPY","14.2M","8%","+0.11%","+1.4%"],["Other","17.8M","10%","+0.09%","+1.9%"]], "foot": ["Total","177.7M","100%","+0.30%","+3.4%"]},
            "usd":   {"cols": ["Asset class","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Equity","49.9M","52%","+0.42%","+4.8%"],["Fixed Income","36.4M","38%","+0.26%","+3.2%"],["Cash/MM","9.6M","10%","+0.01%","+0.5%"]], "foot": ["Total","95.9M","100%","+0.35%","+4.1%"]},
            "eur":   {"cols": ["Asset class","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Equity","15.4M","48%","+0.24%","+2.9%"],["Fixed Income","12.8M","40%","+0.09%","+1.8%"],["Cash/MM","3.8M","12%","+0.01%","+0.3%"]], "foot": ["Total","32.0M","100%","+0.22%","+2.8%"]},
            "gbp":   {"cols": ["Asset class","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Equity","8.9M","50%","+0.19%","+2.3%"],["Fixed Income","6.8M","38%","+0.09%","+1.6%"],["Cash/MM","2.1M","12%","+0.01%","+0.2%"]], "foot": ["Total","17.8M","100%","+0.18%","+2.2%"]},
            "jpy":   {"cols": ["Asset class","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["Equity","7.8M","55%","+0.12%","+1.6%"],["Fixed Income","5.0M","35%","+0.06%","+1.1%"],["Cash/MM","1.4M","10%","+0.00%","+0.1%"]], "foot": ["Total","14.2M","100%","+0.11%","+1.4%"]},
            "fxoth": {"cols": ["Currency","Mkt Val","Alloc %","1D Ret","YTD"], "rows": [["CHF","6.2M","35%","+0.10%","+2.0%"],["AUD","5.3M","30%","+0.09%","+1.8%"],["CAD","3.6M","20%","+0.09%","+1.8%"],["Other","2.7M","15%","+0.08%","+1.6%"]], "foot": ["Total","17.8M","100%","+0.09%","+1.9%"]},
        },
    },
}

VAR_HISTORY = {
    "1Y": [
        {"date": "Apr '25", "var1d95":  950000, "volatility":  9.8},
        {"date": "May '25", "var1d95": 1020000, "volatility": 10.5},
        {"date": "Jun '25", "var1d95": 1080000, "volatility": 11.0},
        {"date": "Jul '25", "var1d95":  990000, "volatility": 10.2},
        {"date": "Aug '25", "var1d95": 1150000, "volatility": 11.8},
        {"date": "Sep '25", "var1d95": 1200000, "volatility": 12.1},
        {"date": "Oct '25", "var1d95": 1180000, "volatility": 11.9},
        {"date": "Nov '25", "var1d95": 1100000, "volatility": 11.2},
        {"date": "Dec '25", "var1d95": 1050000, "volatility": 10.7},
        {"date": "Jan '26", "var1d95": 1120000, "volatility": 11.4},
        {"date": "Feb '26", "var1d95": 1190000, "volatility": 12.0},
        {"date": "Mar '26", "var1d95": 1240000, "volatility": 12.4},
    ],
    "3M": [
        {"date": "Jan 6",  "var1d95": 1080000, "volatility": 11.0},
        {"date": "Jan 13", "var1d95": 1100000, "volatility": 11.2},
        {"date": "Jan 20", "var1d95": 1120000, "volatility": 11.4},
        {"date": "Jan 27", "var1d95": 1110000, "volatility": 11.3},
        {"date": "Feb 3",  "var1d95": 1150000, "volatility": 11.7},
        {"date": "Feb 10", "var1d95": 1170000, "volatility": 11.9},
        {"date": "Feb 17", "var1d95": 1190000, "volatility": 12.0},
        {"date": "Feb 24", "var1d95": 1200000, "volatility": 12.1},
        {"date": "Mar 3",  "var1d95": 1210000, "volatility": 12.2},
        {"date": "Mar 10", "var1d95": 1220000, "volatility": 12.3},
        {"date": "Mar 17", "var1d95": 1230000, "volatility": 12.3},
        {"date": "Mar 24", "var1d95": 1240000, "volatility": 12.4},
    ],
    "1M": [
        {"date": "Mar 3",  "var1d95": 1210000, "volatility": 12.2},
        {"date": "Mar 4",  "var1d95": 1215000, "volatility": 12.2},
        {"date": "Mar 5",  "var1d95": 1220000, "volatility": 12.3},
        {"date": "Mar 6",  "var1d95": 1218000, "volatility": 12.3},
        {"date": "Mar 7",  "var1d95": 1222000, "volatility": 12.3},
        {"date": "Mar 10", "var1d95": 1225000, "volatility": 12.3},
        {"date": "Mar 11", "var1d95": 1228000, "volatility": 12.3},
        {"date": "Mar 12", "var1d95": 1230000, "volatility": 12.3},
        {"date": "Mar 13", "var1d95": 1235000, "volatility": 12.4},
        {"date": "Mar 14", "var1d95": 1232000, "volatility": 12.4},
        {"date": "Mar 17", "var1d95": 1230000, "volatility": 12.3},
        {"date": "Mar 18", "var1d95": 1228000, "volatility": 12.3},
        {"date": "Mar 19", "var1d95": 1233000, "volatility": 12.4},
        {"date": "Mar 20", "var1d95": 1238000, "volatility": 12.4},
        {"date": "Mar 21", "var1d95": 1240000, "volatility": 12.4},
        {"date": "Mar 24", "var1d95": 1242000, "volatility": 12.4},
        {"date": "Mar 25", "var1d95": 1238000, "volatility": 12.4},
        {"date": "Mar 26", "var1d95": 1235000, "volatility": 12.3},
        {"date": "Mar 27", "var1d95": 1240000, "volatility": 12.4},
        {"date": "Mar 28", "var1d95": 1240000, "volatility": 12.4},
    ],
}
