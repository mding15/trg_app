"""
security_reconcile.py — Reconcile ISIN or CUSIP identifiers via OpenFIGI;
upsert results into figi_lookup or save to CSV.

Finds securities in security_xref that don't yet have a matching isin/cusip row
in figi_lookup, looks each one up via OpenFIGI, and either:
  - upserts the result into figi_lookup                                  (default)
  - writes the resolved rows to a CSV instead of the DB                  (--csv)
  - compares the resolved rows against the existing figi_lookup row (by
    security_id) and writes a CSV categorized as identical / different / new,
    without writing anything to the DB                                   (--compare)

CUSIP mode first derives a synthetic US ISIN via a Luhn mod-10 check digit
(cusip_to_isin()), since OpenFIGI's mapping endpoint prefers ISIN lookups;
non-alphanumeric CUSIPs are skipped. Multi-venue OpenFIGI results are reduced
to one representative row per security via pick_representative() (home-exchange
priority, see figi_utils.HOME_EXCH).

Usage:
    python security_reconcile.py --mode isin
    python security_reconcile.py --mode cusip
    python security_reconcile.py --mode isin  --csv
    python security_reconcile.py --mode cusip --csv
    python security_reconcile.py --mode isin  --limit 50
    python security_reconcile.py --mode isin  --dry-run
    python security_reconcile.py --mode isin  --compare
    python security_reconcile.py --mode cusip --compare
    python security_reconcile.py --mode isin  --test US0378331005
    python security_reconcile.py --mode cusip --test 037833100

Requires a unique index on figi_lookup(figi):
    CREATE UNIQUE INDEX IF NOT EXISTS uix_figi_lookup_figi ON figi_lookup(figi);
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection
from figi_utils import FIGI_COLUMNS, OPENFIGI_BATCH, fetch, pick_representative, FIGI_LOOKUP_UPSERT_SQL as _UPSERT_SQL
from _paths import CSV_DIR

CSV_FIELDS = [
    "security_id", "ISIN", "CUSIP", "ticker", "figi", "compositeFIGI",
    "shareClassFIGI", "name", "exchCode", "securityType", "securityType2",
    "marketSector", "securityDescription",
]

# Fields compared against the DB (securityDescription is not stored in figi_lookup)
COMPARE_FIELDS = [
    "ISIN", "CUSIP", "ticker", "figi", "compositeFIGI", "shareClassFIGI",
    "name", "exchCode", "securityType", "securityType2", "marketSector",
]

# Maps figi_lookup column names back to the canonical keys used in _build_row
_DB_TO_CANONICAL: dict[str, str] = {
    "isin":            "ISIN",
    "cusip":           "CUSIP",
    "ticker":          "ticker",
    "figi":            "figi",
    "comp_figi":       "compositeFIGI",
    "shareclass_figi": "shareClassFIGI",
    "name":            "name",
    "exch":            "exchCode",
    "sectype":         "securityType",
    "sectype2":        "securityType2",
    "mkt_sector":      "marketSector",
}

COMPARE_CSV_FIELDS = (
    ["status", "changed_fields", "security_id"]
    + COMPARE_FIELDS
    + [f"db_{f}" for f in COMPARE_FIELDS]
)


# ── Logging ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("security_reconcile")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


# ── ISIN check digit (Luhn mod-10) ────────────────────────────────────────────

def _isin_check_digit(isin_base: str) -> str:
    digits = ""
    for ch in isin_base.upper():
        if ch.isdigit():
            digits += ch
        elif ch.isalpha():
            digits += str(ord(ch) - ord('A') + 10)
        # skip hyphens and other non-alphanumeric characters
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
    return base + _isin_check_digit(base)


# ── DB helpers ────────────────────────────────────────────────────────────────

def _fetch_records(ref_type: str, limit: int | None) -> list[tuple[str, str]]:
    """Return (security_id, ref_id) tuples from security_xref, excluding
    identifiers already reconciled in figi_lookup (matched independently
    by isin or cusip column, not by security_id)."""
    sl_col = "isin" if ref_type == "ISIN" else "cusip"
    sql = f"""
        SELECT "SecurityID", "REF_ID"
        FROM security_xref
        WHERE "REF_TYPE" = '{ref_type}'
        AND NOT EXISTS (
            SELECT 1 FROM figi_lookup
            WHERE {sl_col} = security_xref."REF_ID"
        )
    """
    if limit is not None:
        sql += f" LIMIT {limit}"
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [(str(row[0]), str(row[1])) for row in cur.fetchall()]


def _trunc(value, max_len: int) -> str:
    s = value if isinstance(value, str) else (str(value) if value is not None and str(value) != "nan" else "")
    return s[:max_len]


def _build_row(security_id: str, row: pd.Series, cusip: str = "") -> dict:
    """Build a canonical row dict using readable field names (used for both CSV and DB output)."""
    isin = _trunc(row.get("isin"), 12)
    if not cusip:
        cusip = isin[2:11] if isin.startswith("US") and len(isin) == 12 else ""
    return {
        "security_id":        _trunc(security_id, 20),
        "ISIN":               isin,
        "CUSIP":              cusip,
        "ticker":             _trunc(row.get("ticker"), 50),
        "figi":               _trunc(row.get("figi"), 12),
        "compositeFIGI":      _trunc(row.get("compositeFIGI"), 12),
        "shareClassFIGI":     _trunc(row.get("shareClassFIGI"), 12),
        "name":               _trunc(row.get("name"), 255),
        "exchCode":           _trunc(row.get("exchCode"), 10),
        "securityType":       _trunc(row.get("securityType"), 100),
        "securityType2":      _trunc(row.get("securityType2"), 100),
        "marketSector":       _trunc(row.get("marketSector"), 50),
        "securityDescription": _trunc(row.get("securityDescription"), 255),
    }


def _to_db_dict(r: dict) -> dict:
    """Remap canonical row keys to figi_lookup column names for upsert."""
    return {
        "security_id":     r["security_id"],
        "name":            r["name"],
        "ticker":          r["ticker"],
        "exch":            r["exchCode"],
        "isin":            r["ISIN"],
        "cusip":           r["CUSIP"],
        "sedol":           "",
        "figi":            r["figi"],
        "comp_figi":       r["compositeFIGI"],
        "shareclass_figi": r["shareClassFIGI"],
        "sectype":         r["securityType"],
        "sectype2":        r["securityType2"],
        "mkt_sector":      r["marketSector"],
    }


def _upsert_rows(rows: list[dict]) -> int:
    if not rows:
        return 0
    db_rows = [_to_db_dict(r) for r in rows]
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(_UPSERT_SQL, db_rows)
        conn.commit()
    return len(db_rows)


def _fetch_existing(security_ids: list[str]) -> dict[str, dict]:
    """Return {security_id: canonical_row} for rows already in figi_lookup."""
    if not security_ids:
        return {}
    placeholders = ",".join(["%s"] * len(security_ids))
    sql = f"""
        SELECT security_id, name, ticker, exch, isin, cusip,
               figi, comp_figi, shareclass_figi, sectype, sectype2, mkt_sector
        FROM figi_lookup
        WHERE security_id IN ({placeholders})
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, security_ids)
            col_names = [desc[0] for desc in cur.description]
            db_rows   = cur.fetchall()

    result: dict[str, dict] = {}
    for row in db_rows:
        raw    = dict(zip(col_names, row))
        sec_id = str(raw["security_id"])
        result[sec_id] = {
            _DB_TO_CANONICAL[k]: (v or "") for k, v in raw.items() if k in _DB_TO_CANONICAL
        }
    return result


def _compare_and_write(rows: list[dict], mode: str, log: logging.Logger) -> Path:
    """
    Compare new OpenFIGI rows against existing figi_lookup rows (by security_id).
    Writes a CSV sorted by: different → new → identical.
    db_* columns are populated only for 'different' rows.
    """
    security_ids = [r["security_id"] for r in rows if r["security_id"]]
    log.info(f"  Fetching existing rows from figi_lookup for {len(security_ids)} security_id(s) …")
    existing = _fetch_existing(security_ids)
    log.info(f"  {len(existing)} match(es) found in DB")

    compare_rows: list[dict] = []
    counts = {"identical": 0, "different": 0, "new": 0}

    for new_row in rows:
        sec_id = new_row["security_id"]
        db_row = existing.get(sec_id)

        out: dict = {"security_id": sec_id}
        for f in COMPARE_FIELDS:
            out[f] = new_row.get(f, "")

        if db_row is None:
            out["status"]         = "new"
            out["changed_fields"] = ""
            for f in COMPARE_FIELDS:
                out[f"db_{f}"] = ""
            counts["new"] += 1
        else:
            changed = [
                f for f in COMPARE_FIELDS
                if (new_row.get(f) or "") != (db_row.get(f) or "")
            ]
            if changed:
                out["status"]         = "different"
                out["changed_fields"] = ", ".join(changed)
                for f in COMPARE_FIELDS:
                    out[f"db_{f}"] = db_row.get(f, "")
                counts["different"] += 1
            else:
                out["status"]         = "identical"
                out["changed_fields"] = ""
                for f in COMPARE_FIELDS:
                    out[f"db_{f}"] = ""
                counts["identical"] += 1

        compare_rows.append(out)

    _STATUS_ORDER = {"different": 0, "new": 1, "identical": 2}
    compare_rows.sort(key=lambda r: _STATUS_ORDER[r["status"]])

    CSV_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = CSV_DIR / f"security_reconcile_{mode}_compare_{timestamp}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COMPARE_CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(compare_rows)

    log.info(
        f"  identical: {counts['identical']}  "
        f"different: {counts['different']}  "
        f"new: {counts['new']}"
    )
    return path


def _write_csv(rows: list[dict], mode: str) -> Path:
    CSV_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = CSV_DIR / f"security_reconcile_{mode}_{timestamp}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return path


# ── Core logic ────────────────────────────────────────────────────────────────

def run(mode: str, limit: int | None, dry_run: bool, use_csv: bool, compare: bool = False) -> None:
    log = _setup_logger()
    ref_type = "ISIN" if mode == "isin" else "CUSIP"

    log.info(f"Fetching {ref_type}s from security_xref …")
    records = _fetch_records(ref_type, limit)
    log.info(f"  {len(records)} {ref_type}(s) found")

    if not records:
        log.warning(f"No {ref_type}s to process.")
        return

    if mode == "isin":
        id_pairs      = [(isin, isin) for _, isin in records]
        id_to_sec_id  = {isin: sec_id for sec_id, isin in records}
        id_to_cusip: dict[str, str] = {}
        api_id_type   = "ID_ISIN"
    else:  # cusip
        valid, skipped = [], []
        for sec_id, cusip in records:
            if cusip.isalnum():
                valid.append((sec_id, cusip))
            else:
                skipped.append(cusip)
        if skipped:
            log.warning(f"  {len(skipped)} CUSIP(s) skipped (non-alphanumeric): {skipped[:10]}")
        records = valid
        if not records:
            log.warning("No valid CUSIPs to process after filtering.")
            return
        id_pairs      = [(cusip, cusip_to_isin(cusip)) for _, cusip in records]
        id_to_sec_id  = {cusip_to_isin(cusip): sec_id for sec_id, cusip in records}
        id_to_cusip   = {cusip_to_isin(cusip): cusip  for _, cusip in records}
        api_id_type   = "ID_CUSIP"

    if dry_run:
        log.info("─" * 60)
        log.info("DRY RUN — no data will be written")
        log.info(f"  {ref_type}s to look up : {len(records)}")
        log.info(f"  API batches          : {-(-len(records) // OPENFIGI_BATCH)}")
        log.info(f"  Sample values        : {[v for v, _ in id_pairs[:5]]}")
        output_label = "compare CSV" if compare else ("CSV" if use_csv else "DB")
        log.info(f"  Output               : {output_label}")
        log.info("─" * 60)
        return

    with requests.Session() as session:
        long_df = fetch(id_pairs, api_id_type, session, log)

    log.info(f"  {len(long_df)} venue-level rows returned across {long_df['isin'].nunique()} ISIN(s)")

    if long_df.empty:
        log.warning("No data returned from OpenFIGI.")
        return

    rep_df = pick_representative(long_df)
    log.info(f"  Reduced to {len(rep_df)} representative row(s)")

    rows = []
    for _, row in rep_df.iterrows():
        isin_tag    = row.get("isin", "")
        security_id = id_to_sec_id.get(isin_tag, "")
        cusip       = id_to_cusip.get(isin_tag, "")  # empty for ISIN mode (derived in _build_row)
        rows.append(_build_row(security_id, row, cusip=cusip))

    if compare:
        path = _compare_and_write(rows, mode, log)
        log.info("─" * 60)
        log.info(f"Done.  Comparison written to {path}")
    elif use_csv:
        path = _write_csv(rows, mode)
        log.info("─" * 60)
        log.info(f"Done.  {len(rows)} row(s) written to {path}")
    else:
        inserted = _upsert_rows(rows)
        log.info("─" * 60)
        log.info(f"Done.  {inserted} row(s) upserted.")


# ── Test mode ─────────────────────────────────────────────────────────────────

def run_test(mode: str, value: str) -> None:
    log = _setup_logger()
    value = value.strip().upper()

    if mode == "isin":
        id_pairs    = [(value, value)]
        api_id_type = "ID_ISIN"
        cusip       = value[2:11] if value.startswith("US") and len(value) == 12 else ""
        log.info(f"Test lookup for ISIN: {value}")
    else:  # cusip
        derived_isin = cusip_to_isin(value)
        id_pairs     = [(value, derived_isin)]
        api_id_type  = "ID_CUSIP"
        cusip        = value
        log.info(f"Test lookup for CUSIP: {value}  (derived ISIN: {derived_isin})")

    with requests.Session() as session:
        long_df = fetch(id_pairs, api_id_type, session, log)

    if long_df.empty:
        log.warning("No data returned from OpenFIGI.")
        return

    log.info(f"  {len(long_df)} venue-level row(s) returned\n")
    print(long_df.to_string(index=False))

    rep_df = pick_representative(long_df)
    log.info(f"\n  Representative row (1 of {len(rep_df)}):\n")
    print(rep_df.to_string(index=False))

    row = rep_df.iloc[0]
    built = _build_row("", row, cusip=cusip)
    log.info("\n  Row that would be written:")
    for k, v in built.items():
        print(f"    {k:<22} {v}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reconcile ISIN or CUSIP identifiers via OpenFIGI.",
    )
    parser.add_argument(
        "--mode", required=True, choices=["isin", "cusip"],
        help="Identifier type to reconcile: isin or cusip",
    )
    parser.add_argument(
        "--limit", type=int, default=None, metavar="N",
        help="Process only the first N records (default: all)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would run without calling the API or writing output",
    )
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "--csv", action="store_true",
        help="Write representative rows to CSV instead of upserting to DB",
    )
    output_group.add_argument(
        "--compare", action="store_true",
        help=(
            "Compare OpenFIGI results against existing figi_lookup rows (by security_id) "
            "and write a comparison CSV showing identical / different / new rows"
        ),
    )
    parser.add_argument(
        "--test", metavar="VALUE",
        help="Look up a single ISIN or CUSIP and print results without writing output",
    )
    args = parser.parse_args()

    if args.test:
        run_test(args.mode, args.test)
    else:
        run(args.mode, args.limit, args.dry_run, args.csv, args.compare)


if __name__ == "__main__":
    main()
