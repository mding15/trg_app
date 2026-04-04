import csv
import os

_CSV_PATH = os.path.join(os.path.dirname(__file__), "bar_chart.csv")


def _read_csv():
    rows = []
    with open(_CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                rows.append({
                    "ticker":         row["Ticker"].strip(),
                    "mv_weight":      float(row["MV_Weight"]),
                    "var_contrib":    float(row["VaR_Contribution"]),
                    "asset_class":    row["Asset Class"].strip(),
                    "asset_subclass": row["Asset Subclass"].strip(),
                    "industry":       row["Industry"].strip(),
                    "region":         row["Region"].strip(),
                    "country":        row["Country"].strip(),
                    "currency":       row["Currency"].strip(),
                })
            except (ValueError, KeyError):
                continue
    return rows


def read_asset_drilldown():
    return [
        {
            "asset_class":    r["asset_class"],
            "asset_subclass": r["asset_subclass"],
            "ticker":         r["ticker"],
            "mv_weight":      r["mv_weight"],
            "var_contrib":    r["var_contrib"],
        }
        for r in _read_csv()
    ]


def read_industry_drilldown():
    return [
        {
            "industry":    r["industry"],
            "ticker":      r["ticker"],
            "mv_weight":   r["mv_weight"],
            "var_contrib": r["var_contrib"],
        }
        for r in _read_csv()
    ]


def read_region_drilldown():
    return [
        {
            "region":      r["region"],
            "country":     r["country"],
            "ticker":      r["ticker"],
            "mv_weight":   r["mv_weight"],
            "var_contrib": r["var_contrib"],
        }
        for r in _read_csv()
    ]


def read_currency_drilldown():
    return [
        {
            "currency":    r["currency"],
            "ticker":      r["ticker"],
            "mv_weight":   r["mv_weight"],
            "var_contrib": r["var_contrib"],
        }
        for r in _read_csv()
    ]
