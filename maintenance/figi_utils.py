"""
Shared OpenFIGI utilities used by security_lookup.py and isin_lookup.py.

Exports:
    HOME_EXCH           — ISIN country prefix -> preferred exchange codes
    FIGI_COLUMNS        — canonical column order for OpenFIGI result DataFrames
    pick_representative — reduce a long FIGI DataFrame to one row per ISIN
"""
from __future__ import annotations

import pandas as pd

# ISIN country prefix -> preferred home composite exchange codes, in priority order.
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
    # Offshore domiciles -> where such issuers typically list
    "JE": ["LN"],
    "GG": ["LN"],
    "IM": ["LN"],
    "BM": ["US", "HK", "LN"],
    "KY": ["HK", "US", "LN"],
    "VG": ["US", "LN"],
}

FIGI_COLUMNS = [
    "isin", "figi", "name", "ticker", "exchCode", "compositeFIGI",
    "securityType", "marketSector", "shareClassFIGI", "securityType2",
    "securityDescription",
]


def pick_representative(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reduce a long OpenFIGI mapping DataFrame to one representative row per ISIN.

    Within each ISIN group, prefers:
      1. Composite-level records (figi == compositeFIGI)
      2. Records on the ISIN's home exchange (via HOME_EXCH; earlier entries rank higher)
      3. Lowest FIGI alphabetically (deterministic tiebreaker)
    """
    if df.empty:
        return df.copy()

    work = df.copy()

    # Grouping key with fallbacks for null shareClassFIGI
    work["dedupe_key"] = (
        work["shareClassFIGI"]
        .fillna(work["compositeFIGI"])
        .fillna(work["figi"])
    )

    # Preference 1: composite-level record (figi == compositeFIGI)
    work["_is_composite"] = (
        work["figi"].notna()
        & work["compositeFIGI"].notna()
        & (work["figi"] == work["compositeFIGI"])
    ).astype(int)

    # Preference 2: home-exchange rank from the ISIN prefix
    def home_rank(row) -> int:
        isin = row.get("isin")
        if not isinstance(isin, str) or len(isin) < 2:
            return 99
        prefs = HOME_EXCH.get(isin[:2].upper(), [])
        try:
            return prefs.index(row["exchCode"])
        except (ValueError, TypeError):
            return 99

    work["_home_rank"] = work.apply(home_rank, axis=1)

    # Sort so the best row per ISIN comes first, then take the first per ISIN
    work = work.sort_values(
        by=["isin", "_is_composite", "_home_rank", "figi"],
        ascending=[True, False, True, True],
        kind="mergesort",
    )
    rep = (
        work.groupby("isin", as_index=False, sort=True)
        .first()
        .drop(columns=["_is_composite", "_home_rank"])
    )

    cols = ["dedupe_key"] + [c for c in df.columns if c in rep.columns]
    return rep[cols].reset_index(drop=True)
