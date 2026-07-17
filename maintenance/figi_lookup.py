"""
Look up a security via OpenFIGI and return linked identifiers.

Usage:
    python figi_lookup.py --cusip 037833100
    python figi_lookup.py --symbol AAPL
    python figi_lookup.py --symbol AAPL --exchange US
    python figi_lookup.py --isin US0378331005
    python figi_lookup.py --figi BBG000B9XRY4
    python figi_lookup.py          # prompts for lookup type and value

All raw rows returned from the API are saved to data/maintenance/CSV/figi_lookup.csv.
One representative instrument is printed to console (home exchange preferred).
"""

import argparse
import csv
import json
import sys
import urllib.request
import urllib.parse
from pathlib import Path

import pandas as pd

from figi_utils import FIGI_COLUMNS, pick_representative
from _paths import CSV_DIR

OPENFIGI_API_KEY  = '9a5b92a9-cae1-47ad-bb52-9d6293d18364'
OPENFIGI_BASE_URL = "https://api.openfigi.com"

RAW_CSV_PATH = CSV_DIR / "figi_lookup.csv"

RAW_CSV_FIELDS = [
    "figi", "compositeFIGI", "shareClassFIGI",
    "isin", "cusip", "ticker", "name", "exchCode", 
    "securityType", "securityType2",
    "marketSector", "securityDescription",
]


# ── OpenFIGI API ──────────────────────────────────────────────────────────────

def api_call(path: str, data: dict | list | None = None, method: str = "POST"):
    headers = {"Content-Type": "application/json"}
    if OPENFIGI_API_KEY:
        headers["X-OPENFIGI-APIKEY"] = OPENFIGI_API_KEY
    request = urllib.request.Request(
        url=urllib.parse.urljoin(OPENFIGI_BASE_URL, path),
        data=data and bytes(json.dumps(data), encoding="utf-8"),
        headers=headers,
        method=method,
    )
    with urllib.request.urlopen(request) as response:
        return json.loads(response.read().decode("utf-8"))


# ── ISIN / CUSIP helpers ──────────────────────────────────────────────────────

def compute_isin_check_digit(isin_base: str) -> str:
    """Compute ISIN check digit using Luhn mod-10 algorithm."""
    digits = ""
    for ch in isin_base.upper():
        if ch.isdigit():
            digits += ch
        else:
            digits += str(ord(ch) - ord('A') + 10)
    digits += "0"
    total = 0
    for i, d in enumerate(reversed(digits)):
        n = int(d)
        if i % 2 == 1:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return str((10 - (total % 10)) % 10)


def cusip_to_isin(cusip: str, country_code: str = "US") -> str:
    base = country_code + cusip
    return base + compute_isin_check_digit(base)


# ── Row building ──────────────────────────────────────────────────────────────

def build_row(item: dict, isin: str = "", cusip: str = "") -> dict:
    def s(key): return item.get(key) or ""
    return {
        "figi":               s("figi"),
        "compositeFIGI":      s("compositeFIGI"),
        "shareClassFIGI":     s("shareClassFIGI"),
        "name":               s("name"),
        "ISIN":               isin,
        "CUSIP":              cusip,
        "ticker":             s("ticker"),
        "exchCode":           s("exchCode"),
        "securityType":       s("securityType"),
        "securityType2":      s("securityType2"),
        "marketSector":       s("marketSector"),
        "securityDescription":s("securityDescription"),
    }


def _apply_representative(raw_items: list[dict], isin: str = "", cusip: str = "") -> list[dict]:
    """
    Convert raw OpenFIGI items to a DataFrame, apply pick_representative,
    and return a single built row (or empty list if no data).
    """
    if not raw_items:
        return []
    rows = [{"isin": isin, **item} for item in raw_items]
    df = pd.DataFrame(rows)
    for col in FIGI_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[FIGI_COLUMNS]
    rep = pick_representative(df)
    if rep.empty:
        return []
    row = rep.fillna("").iloc[0].to_dict()
    return [build_row(row, isin=isin, cusip=cusip)]


# ── Lookup functions (return raw API items) ───────────────────────────────────

def _raw_items(response: list) -> list[dict]:
    """Extract data items from a single-job /v3/mapping response."""
    if not response:
        return []
    job = response[0]
    if "error" in job:
        print(f"API error: {job['error']}")
        return []
    if "warning" in job:
        print(f"API warning: {job['warning']}")
        return []
    return job.get("data", [])


def lookup_cusip(cusip: str) -> list[dict]:
    return _raw_items(api_call("/v3/mapping", [{"idType": "ID_CUSIP", "idValue": cusip}]))


def lookup_isin(isin: str, exchange: str = "") -> list[dict]:
    job = {"idType": "ID_ISIN", "idValue": isin}
    if exchange:
        job["exchCode"] = exchange.upper()
    return _raw_items(api_call("/v3/mapping", [job]))


def lookup_figi(figi: str) -> list[dict]:
    return _raw_items(api_call("/v3/mapping", [{"idType": "ID_BB_GLOBAL", "idValue": figi}]))


def lookup_symbol(symbol: str, exchange: str = None, sectype2: str = None) -> list[dict]:
    """Common sectype2 values: Common Stock, ETF, Mutual Fund, Preferred, Corporate Bond, Government Bond."""
    job = {"idType": "TICKER", "idValue": symbol}
    if exchange:
        job["exchCode"] = exchange.upper()
    if sectype2:
        job["securityType2"] = sectype2
    return _raw_items(api_call("/v3/mapping", [job]))


# ── Output ────────────────────────────────────────────────────────────────────

def write_raw_csv(raw_items: list[dict], isin: str = "", cusip: str = "") -> None:
    """Append raw API rows to CSV/figi_lookup.csv, skipping rows whose figi already exists."""
    CSV_DIR.mkdir(exist_ok=True)
    rows = [
        {
            "figi":                item.get("figi", ""),
            "compositeFIGI":       item.get("compositeFIGI", ""),
            "shareClassFIGI":      item.get("shareClassFIGI", ""),
            "isin":                isin,
            "cusip":               cusip,
            "ticker":              item.get("ticker", ""),
            "name":                item.get("name", ""),
            "exchCode":            item.get("exchCode", ""),
            "securityType":        item.get("securityType", ""),
            "securityType2":       item.get("securityType2", ""),
            "marketSector":        item.get("marketSector", ""),
            "securityDescription": item.get("securityDescription", ""),
        }
        for item in raw_items
    ]

    existing_figis: set[str] = set()
    file_exists = RAW_CSV_PATH.exists()
    if file_exists:
        with open(RAW_CSV_PATH, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("figi"):
                    existing_figis.add(row["figi"])

    new_rows = [r for r in rows if not r["figi"] or r["figi"] not in existing_figis]
    if not new_rows:
        print("No new rows to append (all FIGIs already in file).")
        return

    with open(RAW_CSV_PATH, "a" if file_exists else "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=RAW_CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_rows)
    print(f"Appended {len(new_rows)} new row(s) ({len(rows) - len(new_rows)} duplicate(s) skipped).")


def print_results(label: str, results: list[dict]) -> None:
    if not results:
        print("No results found.")
        return
    print(f"\nFound {len(results)} instrument(s) for {label}:\n")
    for i, r in enumerate(results, 1):
        print(f"  --- Result {i} ---")
        print(f"  figi:               {r['figi']}")
        print(f"  compositeFIGI:      {r['compositeFIGI']}")
        print(f"  shareClassFIGI:     {r['shareClassFIGI']}")
        print(f"  name:               {r['name']}")
        print(f"  ISIN:               {r['ISIN']}")
        print(f"  CUSIP:              {r['CUSIP']}")
        print(f"  ticker:             {r['ticker']}")
        print(f"  exchCode:           {r['exchCode']}")
        print(f"  securityType:       {r['securityType']}")
        print(f"  securityType2:      {r['securityType2']}")
        print(f"  marketSector:       {r['marketSector']}")
        print(f"  securityDescription:{r['securityDescription']}")
        print()


# ── Interactive prompt ────────────────────────────────────────────────────────

def prompt_interactive() -> tuple[str, str, str, str]:
    """Returns (mode, value, exchange, sectype2)."""
    print("Lookup type:")
    print("  1. CUSIP")
    print("  2. Exchange symbol")
    print("  3. ISIN")
    print("  4. FIGI")
    choice = input("Select (1/2/3/4): ").strip()
    if choice == "1":
        return "cusip", input("Enter CUSIP: ").strip().upper(), "", ""
    elif choice == "3":
        value    = input("Enter ISIN: ").strip().upper()
        exchange = input("Exchange code (optional, e.g. US, LN): ").strip().upper()
        return "isin", value, exchange, ""
    elif choice == "4":
        return "figi", input("Enter FIGI: ").strip().upper(), "", ""
    else:
        value    = input("Enter symbol: ").strip().upper()
        exchange = input("Exchange code (optional, e.g. US, LN, GY): ").strip().upper() or None
        sectype2 = input("Security type (optional, e.g. Common Stock, ETF): ").strip() or None
        return "symbol", value, exchange, sectype2


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Look up a security via OpenFIGI.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--cusip",  metavar="CUSIP",  help="9-character CUSIP identifier")
    group.add_argument("--symbol", metavar="SYMBOL", help="Exchange ticker symbol")
    group.add_argument("--isin",   metavar="ISIN",   help="12-character ISIN identifier")
    group.add_argument("--figi",   metavar="FIGI",   help="12-character FIGI identifier")
    parser.add_argument("--exchange", metavar="CODE", default="",
                        help="Exchange code filter (e.g. US, LN, GY). Optional — omit to search all exchanges.")
    parser.add_argument("--sectype", metavar="TYPE", default=None,
                        help="securityType2 for symbol lookup (optional). "
                             "Values: Common Stock, ETF, Mutual Fund, Preferred, Corporate Bond, "
                             "Government Bond, Index, Option, Future")
    args = parser.parse_args()

    if args.cusip:
        mode, value, exchange, sectype2 = "cusip", args.cusip.strip().upper(), "", ""
    elif args.symbol:
        mode, value, exchange, sectype2 = "symbol", args.symbol.strip().upper(), args.exchange.strip().upper() or None, args.sectype
    elif args.isin:
        mode, value, exchange, sectype2 = "isin", args.isin.strip().upper(), args.exchange.strip().upper(), ""
    elif args.figi:
        mode, value, exchange, sectype2 = "figi", args.figi.strip().upper(), "", ""
    else:
        mode, value, exchange, sectype2 = prompt_interactive()

    if mode == "cusip":
        if len(value) != 9:
            print(f"Warning: CUSIP is normally 9 characters (got {len(value)})")
        isin  = cusip_to_isin(value)
        label = f"CUSIP_{value}"
        print(f"Looking up CUSIP: {value} ...")
        raw     = lookup_cusip(value)
        results = _apply_representative(raw, isin=isin, cusip=value)

    elif mode == "isin":
        if len(value) != 12:
            print(f"Warning: ISIN is normally 12 characters (got {len(value)})")
        cusip = value[2:11] if value.startswith("US") and len(value) == 12 else ""
        label = f"ISIN_{value}" + (f"_{exchange}" if exchange else "")
        print(f"Looking up ISIN: {value}" + (f" on exchange {exchange}" if exchange else "") + " ...")
        raw     = lookup_isin(value, exchange)
        results = _apply_representative(raw, isin=value, cusip=cusip)

    elif mode == "figi":
        label = f"FIGI_{value}"
        print(f"Looking up FIGI: {value} ...")
        raw     = lookup_figi(value)
        results = _apply_representative(raw, isin=value)

    else:  # symbol
        label = f"SYMBOL_{value}" + (f"_{exchange}" if exchange else "")
        print(f"Looking up symbol: {value}" + (f" on exchange {exchange}" if exchange else "") + " ...")
        raw     = lookup_symbol(value, exchange, sectype2)
        results = _apply_representative(raw)

    if raw:
        isin  = locals().get("isin", "")
        cusip = locals().get("cusip", "")
        write_raw_csv(raw, isin=isin, cusip=cusip)
        print(f"{len(raw)} row(s) returned from API, reduced to {len(results)} after representative selection.")
        print(f"Raw results saved to: {RAW_CSV_PATH}")

    print_results(label, results)


if __name__ == "__main__":
    main()
