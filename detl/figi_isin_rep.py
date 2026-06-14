"""
Map ISINs to FIGIs via the OpenFIGI API and reduce the result to one
representative row per security (one row per shareClassFIGI, with
sensible fallbacks when shareClassFIGI is null).

Usage:
    df = map_isins(["LU1217871059", "US4592001014"], api_key="...")
    rep = pick_representative(df)
"""

from __future__ import annotations

import time
from typing import Iterable, Optional

import pandas as pd
import requests

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"

# ISIN country prefix -> preferred "home" composite exchange codes,
# in order of preference. Offshore domiciles get pragmatic fallbacks;
# prefixes with no home market (XS, XD, SX, ...) are simply absent.
HOME_EXCH: dict[str, list[str]] = {
    "US": ["US"],
    "CA": ["CN"],
    "GB": ["LN"],
    "IE": ["ID"],
    "CH": ["SW"],
    "LU": ["LX"],
    "NL": ["NA"],
    "DE": ["GR"],
    "FR": ["FP"],
    "JP": ["JP"],
    "HK": ["HK"],
    "AU": ["AU"],
    # Offshore domiciles -> where such issuers usually actually list
    "JE": ["LN"],
    "GG": ["LN"],
    "IM": ["LN"],
    "BM": ["US", "HK", "LN"],
    "KY": ["HK", "US", "LN"],
    "VG": ["US", "LN"],
}

# Columns we expect from /v3/mapping; missing ones are added as NA so
# downstream code never has to guard against absent columns.
RESULT_COLUMNS = [
    "isin", "figi", "name", "ticker", "exchCode", "compositeFIGI",
    "securityType", "marketSector", "shareClassFIGI", "securityType2",
    "securityDescription",
]


def _chunks(seq: list, size: int) -> Iterable[list]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def map_isins(
    isins: Iterable[str],
    api_key: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> pd.DataFrame:
    """
    Map ISINs to FIGI records. Returns a long DataFrame with one row per
    (ISIN, venue-level FIGI). ISINs with no match are reported and skipped.

    Respects OpenFIGI batch limits: 100 jobs/request with an API key,
    10 without. Retries politely on HTTP 429 using the ratelimit-reset
    header when present.
    """
    isins = list(dict.fromkeys(str(i).strip().upper() for i in isins))  # dedupe, keep order
    sess = session or requests.Session()
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key
    batch_size = 100 if api_key else 10

    rows: list[dict] = []
    unmatched: list[str] = []

    for batch in _chunks(isins, batch_size):
        jobs = [{"idType": "ID_ISIN", "idValue": isin} for isin in batch]

        while True:
            resp = sess.post(OPENFIGI_URL, json=jobs, headers=headers, timeout=30)
            if resp.status_code == 429:
                wait = float(resp.headers.get("ratelimit-reset", 10)) + 0.5
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break

        results = resp.json()
        for isin, result in zip(batch, results):
            if "data" in result:
                for rec in result["data"]:
                    rows.append({"isin": isin, **rec})
            else:
                # v3 uses "warning" for "No identifier found"; "error" for real failures
                unmatched.append(f"{isin}: {result.get('warning') or result.get('error')}")

    if unmatched:
        print(f"[map_isins] {len(unmatched)} ISIN(s) without data:")
        for line in unmatched:
            print(f"  - {line}")

    df = pd.DataFrame(rows)
    for col in RESULT_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[RESULT_COLUMNS]


def pick_representative(df: pd.DataFrame, isin_col: str = "isin") -> pd.DataFrame:
    """
    Reduce a long mapping result to one representative row per security.

    Grouping key (most stable first): shareClassFIGI -> compositeFIGI -> figi.
    Within each group, prefer:
      1. composite-level records (figi == compositeFIGI),
      2. records on the ISIN's home exchange (via HOME_EXCH; earlier
         entries in the list rank higher),
      3. lowest FIGI alphabetically (pure tie-breaker for determinism).

    Returns the original columns plus 'dedupe_key', sorted by it.
    """
    if df.empty:
        return df.copy()

    work = df.copy()

    # --- grouping key with fallbacks for null shareClassFIGI -------------
    work["dedupe_key"] = (
        work["shareClassFIGI"]
        .fillna(work["compositeFIGI"])
        .fillna(work["figi"])
    )

    # --- preference 1: composite-level record -----------------------------
    work["_is_composite"] = (
        work["figi"].notna()
        & work["compositeFIGI"].notna()
        & (work["figi"] == work["compositeFIGI"])
    ).astype(int)

    # --- preference 2: home-exchange rank from the ISIN prefix ------------
    def home_rank(row) -> int:
        isin = row.get(isin_col)
        if not isinstance(isin, str) or len(isin) < 2:
            return 99
        prefs = HOME_EXCH.get(isin[:2].upper(), [])
        try:
            return prefs.index(row["exchCode"])  # 0 = best
        except (ValueError, TypeError):
            return 99

    work["_home_rank"] = work.apply(home_rank, axis=1)

    # --- deterministic pick ------------------------------------------------
    work = work.sort_values(
        by=["dedupe_key", "_is_composite", "_home_rank", "figi"],
        ascending=[True, False, True, True],
        kind="mergesort",  # stable
    )
    rep = (
        work.groupby("dedupe_key", as_index=False, sort=True)
        .first()
        .drop(columns=["_is_composite", "_home_rank"])
    )

    # Keep original column order, with dedupe_key first for convenience
    cols = ["dedupe_key"] + [c for c in df.columns if c in rep.columns]
    return rep[cols].reset_index(drop=True)


if __name__ == "__main__":
    # Example: one Luxembourg-domiciled fund + IBM
    sample = ["LU1217871059", "US4592001014"]
    long_df = map_isins(sample, api_key=None)  # pass your key for higher limits
    print(f"\nMapped {len(long_df)} venue-level records "
          f"for {long_df['isin'].nunique()} ISIN(s).")

    rep_df = pick_representative(long_df)
    print(f"Reduced to {len(rep_df)} representative row(s):\n")
    print(rep_df.to_string(index=False))