"""
new_security.py — Core logic for the new-security workflow (finding
securities in position_var that aren't fully set up yet, and creating
security_info/security_xref/option_info rows for the Option ones).

Lives in security/ (stable) rather than maintenance/ (volatile, changes
often) so that api/ops_routes.py can depend on it directly, without coupling
the API to files that get edited/rearranged frequently. The CLI entry points
— maintenance/process_new_security.py (Step 1: scan) and
maintenance/insert_new_security.py (Step 2: insert) — are thin wrappers
around the functions here (argparse, console logging, file I/O only).

── Step 1: scan ───────────────────────────────────────────────────────────
_fetch_new_securities() currently only flags rows where security_id is
missing (blank or NULL) — see QUERY_MISSING_SECURITY_ID. A second query,
QUERY_MISSING_VAR_95, flags rows missing var_95 (NULL) as of that security's
own latest as_of_date, excluding cash positions (asset_class='Cash') and
bonds already matured as of that as_of_date; it's kept here for future use
but is not currently run.

For each (security_name, security_id) combination, only the row with the
latest as_of_date is kept. The CSV includes a `reason` column so you can
tell which condition(s) flagged each row (a row can appear twice, once per
reason, if it matches both — see _keep_latest_per_security for the tie-break).

security_name is also parsed into candidate fields (see
security/security_name_parser.py). Fields that map directly onto a DB column
insert_new_security.py writes are named to match that column exactly:
underlying, option_type, strike, maturity (all option_info columns). Fields
with no downstream DB write yet keep a parsed_ prefix: parsed_security_type
(used to decide the row is an option in the first place), parsed_issuer,
parsed_coupon_rate (bond_info fields — the bond path isn't wired up yet),
parsed_ok (True if the name matched a known pattern; parsed_ok=False means
no pattern matched — nothing was guessed, so check that row manually).

The `ticker` column has all whitespace stripped (e.g. an OCC option symbol
like 'AAPL  270115C0035000' -> 'AAPL270115C0035000') so it's usable as-is
for a security_xref Ticker value by Step 2.

For rows where parsed_security_type='Option', asset_class/asset_type are
overwritten to 'Derivative'/'Option' (the security_info convention for
options) and data_source is set to 'MANUAL' — all three are plain CSV/JSON
values so they're reviewable/editable (by hand in the CSV, or in the
trg_ops New Securities page), not hardcoded in Step 2.

── Step 2: insert ──────────────────────────────────────────────────────────
process_rows() mirrors the security data layers:

    base layer  — security_info (one row per security) + security_xref (one
                  or more identifier rows per security). Same for every type.
    type layer  — type-specific data, via _TYPE_HANDLERS keyed by
                  parsed_security_type:
                      Option                     -> option_info
                      Equity, Cash, Alternative  -> base only (no type data)
                  Anything else (incl. Bond — bond_info isn't wired up yet)
                  is skipped. Adding a type = a check/create pair + one
                  registry entry.

For each row, all checks run before any write, so a skipped row never leaves
a partial security behind:
    1. Type not in _TYPE_HANDLERS                  -> skipped_unsupported_type
    2. No Ticker / ISIN / CUSIP on the row         -> skipped_no_identifier
       (every security must have at least one security_xref row)
    3. Identifiers already in security_xref:
         all map to one security                   -> skipped_exists
         map to different securities               -> skipped_conflict
    4. asset_type blank                            -> skipped_missing_asset_type
       (data_source blank -> defaults to 'MANUAL')
    5. Type check — Option: `underlying` must resolve via security_xref
       (REF_TYPE='Ticker'), since an option_info row with no resolvable
       underlying silently breaks the options P&L engine's join (see
       process2/calc_options_pnl.py)            -> skipped_no_underlying
    6. dry_run                                     -> would_create
    7. create_security(...), add_xref_if_missing(...) per identifier, then
       the type's create step (Option: create_option_info(...)) -> created

Identifiers of rows created (or would-create) in this run are added to the
in-memory lookup, so a repeated identifier later in the same file is
reported as skipped_exists rather than created twice.

Every value written — currency, asset_class, asset_type, data_source,
option_class, option_type, strike, maturity, underlying, isin, cusip — comes
from the row; the only default is data_source='MANUAL' when blank.

process_rows() is the shared core used by both the CLI
(maintenance/insert_new_security.py) and the trg_ops API
(api/ops_routes.py's /api/ops/new-security/insert). All DB writes for a run
are committed in one transaction by the caller.
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, NamedTuple

import pandas as pd

from database2 import pg_connection
from security.security_name_parser import parse_security
from security.ticker_utils import normalize_ticker
from process2.security_utils import create_security, add_xref_if_missing, create_option_info

# workspace_root/data/maintenance/CSV — same location maintenance/_paths.py
# points at, computed independently here so this module has no dependency
# on anything under maintenance/.
CSV_DIR = Path(__file__).resolve().parents[2] / "data" / "maintenance" / "CSV"


# ═══════════════════════════════════════════════════════════════════════════
# Step 1: scan
# ═══════════════════════════════════════════════════════════════════════════

QUERY_MISSING_SECURITY_ID = """
    SELECT pv.security_name, pv.ticker, pv.isin, pv.cusip, pv.broker,
           pv.security_id, pv.asset_class, pv.asset_type, pv."class",
           bool_or(pv.is_option) AS is_option,
           MAX(pv.as_of_date) AS as_of_date,
           'USD' AS currency,
           'Equity' AS option_class,
           'missing_security_id' AS reason
    FROM position_var pv
    WHERE pv.security_id = '' OR pv.security_id IS NULL
    GROUP BY pv.security_name, pv.ticker, pv.isin, pv.cusip, pv.broker, pv.security_id,
             pv.asset_class, pv.asset_type, pv."class"
    ORDER BY security_name
"""

QUERY_MISSING_VAR_95 = """
    SELECT pv.security_name, pv.ticker, pv.broker,
           pv.security_id, pv.asset_class, pv.asset_type, pv."class",
           bool_or(pv.is_option) AS is_option,
           MAX(pv.as_of_date) AS as_of_date,
           MAX(bi."MaturityDate") AS "MaturityDate",
           MAX(bi."IssuerTicker") AS "IssuerTicker",
           MAX(bi."CouponRate")   AS "CouponRate",
           'missing_var_95' AS reason
    FROM position_var pv
    LEFT JOIN bond_info bi ON bi."SecurityID" = pv.security_id AND pv.asset_class = 'Bond'
    WHERE pv.var_95 IS NULL
      AND pv.asset_class IS DISTINCT FROM 'Cash'
      -- exclude bonds that had already matured as of this row's as_of_date —
      -- a matured bond isn't expected to have var_95, so it's not a setup gap.
      AND (bi."MaturityDate" IS NULL OR bi."MaturityDate" >= pv.as_of_date)
      AND (
            pv.security_id IS NULL OR pv.security_id = ''
            -- security_id is known: only flag it if it's STILL missing var_95
            -- as of its own latest as_of_date (i.e. it hasn't since been set up
            -- correctly on a later date — exclude it if it has).
            OR pv.as_of_date = (
                SELECT MAX(pv2.as_of_date) FROM position_var pv2
                WHERE pv2.security_id = pv.security_id
            )
          )
    GROUP BY pv.security_name, pv.ticker, pv.broker, pv.security_id,
             pv.asset_class, pv.asset_type, pv."class"
    ORDER BY security_name
"""


def _add_parsed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Parse security_name into structured fields (see security_name_parser.py).
    Fields with an active downstream DB write (option_info's underlying/
    option_type/strike/maturity) get that exact column name; fields with no
    downstream write yet (bond_info's issuer/coupon_rate, and the internal
    parsed_security_type/parsed_ok flags) keep a parsed_ prefix."""
    if df.empty:
        for col in ('parsed_security_type', 'underlying', 'option_type',
                    'strike', 'parsed_issuer', 'parsed_coupon_rate',
                    'maturity', 'parsed_ok'):
            df[col] = None
        return df

    parsed = df.apply(
        lambda row: parse_security(row['security_name'], row['asset_class'],
                                   row['asset_type'], row['is_option']),
        axis=1, result_type='expand',
    )
    parsed = parsed.rename(columns={
        'security_type':     'parsed_security_type',
        'underlying_ticker': 'underlying',
        'issuer':            'parsed_issuer',
        'coupon_rate':       'parsed_coupon_rate',
        'maturity_date':     'maturity',
        'parsed':            'parsed_ok',
    })
    return pd.concat([df, parsed], axis=1)


def _apply_option_defaults(df: pd.DataFrame) -> pd.DataFrame:
    """For rows the parser identified as options, overwrite asset_class/
    asset_type to the security_info convention for options ('Derivative'/
    'Option') and set data_source — all as plain, reviewable values rather
    than hardcoded in Step 2."""
    df['data_source'] = None
    if df.empty:
        return df
    is_option = df['parsed_security_type'] == 'Option'
    df.loc[is_option, 'asset_class'] = 'Derivative'
    df.loc[is_option, 'asset_type']  = 'Option'
    df.loc[is_option, 'data_source'] = 'MANUAL'
    return df


def _keep_latest_per_security(df: pd.DataFrame) -> pd.DataFrame:
    """For rows sharing the same (security_name, security_id), keep only the
    one with the latest as_of_date. Ties (e.g. a row flagged by both reasons
    on the same date) are broken by reason, for deterministic output."""
    if df.empty:
        return df
    return (
        df.sort_values(['as_of_date', 'reason'], ascending=[False, True])
          .drop_duplicates(subset=['security_name', 'security_id'], keep='first')
          .reset_index(drop=True)
    )


def _normalize_ticker_col(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize the raw broker ticker (see security/ticker_utils.py) so it's
    usable as a clean security_xref Ticker value downstream."""
    df['ticker'] = df['ticker'].apply(normalize_ticker)
    return df


_COLUMN_GROUP = ['ticker', 'isin', 'cusip', 'asset_class', 'asset_type', 'currency',
                 'data_source', 'option_class', 'option_type', 'underlying', 'strike', 'maturity']


def _reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Group the columns Step 2 reads together (right after security_name),
    in a fixed, reviewable order."""
    front = ['security_name'] + _COLUMN_GROUP
    rest = [c for c in df.columns if c not in front]
    return df[front + rest]


def _fetch_new_securities() -> pd.DataFrame:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(QUERY_MISSING_SECURITY_ID)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=cols)
    df = _keep_latest_per_security(df)
    df = _normalize_ticker_col(df)
    df = _add_parsed_columns(df)
    df = _apply_option_defaults(df)
    return _reorder_columns(df)


def write_csv(df: pd.DataFrame) -> Path:
    """Write df to a fresh timestamped CSV in CSV_DIR and return its path."""
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = CSV_DIR / f"new_securities_{timestamp}.csv"
    df.to_csv(out_path, index=False)
    return out_path


def df_to_records(df: pd.DataFrame) -> list[dict]:
    """Convert df to JSON-serializable records: NaN/NaT -> None, date/Timestamp
    objects -> ISO strings. Used by the trg_ops API to return scan results."""
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(object)
    records = df.where(pd.notnull(df), None).to_dict(orient='records')
    for rec in records:
        for k, v in rec.items():
            if hasattr(v, 'isoformat'):
                rec[k] = v.isoformat()
            # df.where(..., None) on a float-dtype column silently reverts
            # None back to NaN (numpy float arrays can't hold None) — catch
            # any that slip through so the JSON response has real nulls.
            elif isinstance(v, float) and v != v:
                rec[k] = None
    return records


# ═══════════════════════════════════════════════════════════════════════════
# Step 2: insert
# ═══════════════════════════════════════════════════════════════════════════

REQUIRED_COLUMNS = (
    'security_name', 'ticker', 'isin', 'cusip', 'currency', 'asset_class', 'asset_type',
    'data_source', 'option_class', 'option_type', 'strike', 'underlying',
    'maturity', 'parsed_security_type',
)


def _none_if_nan(v):
    return None if pd.isna(v) else v


def validate_and_normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Check required columns are present and normalize ticker/underlying/
    maturity. Raises ValueError if a required column is missing. Used for
    both CSV-loaded (CLI) and JSON-loaded (API) input."""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required column(s): {missing}")

    df = df.copy()
    df['ticker'] = df['ticker'].apply(normalize_ticker)
    df['underlying'] = df['underlying'].apply(normalize_ticker)
    df['maturity'] = pd.to_datetime(df['maturity'], errors='coerce').dt.date
    return df


def write_results_csv(results: list[dict]) -> Path:
    """Write per-row insert results to a fresh timestamped CSV in CSV_DIR
    (audit trail for a real, non-dry-run insert) and return its path."""
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = CSV_DIR / f"insert_new_security_results_{timestamp}.csv"
    pd.DataFrame(results).to_csv(out_path, index=False)
    return out_path


# ── Lookup ──────────────────────────────────────────────────────────────────

# security_xref REF_TYPE -> row column holding that identifier
_REF_COLUMNS = {'Ticker': 'ticker', 'ISIN': 'isin', 'CUSIP': 'cusip'}

DEFAULT_DATA_SOURCE = 'MANUAL'


def _ref_value(v) -> str | None:
    """Clean identifier value, or None if blank. CSV-loaded ISIN/CUSIP can
    arrive as numbers (e.g. an all-digit CUSIP), so coerce to str."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    v = str(v).strip()
    return v or None


def _row_refs(r) -> dict[str, str]:
    """{REF_TYPE: REF_ID} for the row's non-blank identifiers."""
    refs = {ref_type: _ref_value(r[col]) for ref_type, col in _REF_COLUMNS.items()}
    return {k: v for k, v in refs.items() if v}


def _batch_lookup_refs(cur, keys: set[tuple[str, str]]) -> dict[tuple[str, str], set[str]]:
    """Return {(REF_TYPE, REF_ID): {SecurityID, ...}} for every given key
    already present in security_xref."""
    if not keys:
        return {}
    ref_types = sorted({t for t, _ in keys})
    ref_ids   = sorted({i for _, i in keys})
    cur.execute(
        'SELECT "REF_TYPE", "REF_ID", "SecurityID" FROM security_xref '
        'WHERE "REF_TYPE" = ANY(%s) AND "REF_ID" = ANY(%s)',
        (ref_types, ref_ids),
    )
    found: dict[tuple[str, str], set[str]] = {}
    for ref_type, ref_id, sec_id in cur.fetchall():
        if (ref_type, ref_id) in keys:
            found.setdefault((ref_type, ref_id), set()).add(str(sec_id))
    return found


# ── Type layer ──────────────────────────────────────────────────────────────
#
# check(row, known)                    -> (skip_status | None, ctx)   before any write
# create(cur, security_id, row, ctx)   -> None                        after base rows
#
# `known` is the {(REF_TYPE, REF_ID): {SecurityID}} lookup. `ctx` carries
# whatever check() resolved (e.g. underlying_sec_id) through to create().

def _check_option(r, known) -> tuple[str | None, dict]:
    underlying = _ref_value(r['underlying'])
    sec_ids = known.get(('Ticker', underlying), set()) if underlying else set()
    if len(sec_ids) != 1:
        return 'skipped_no_underlying', {}
    return None, {'underlying': underlying, 'underlying_sec_id': next(iter(sec_ids))}


def _create_option(cur, security_id: str, r, ctx: dict) -> None:
    create_option_info(
        cur, security_id,
        _none_if_nan(r['option_type']), _none_if_nan(r['option_class']),
        _none_if_nan(r['maturity']), _none_if_nan(r['strike']),
        ctx['underlying'], ctx['underlying_sec_id'],
    )


class _TypeHandler(NamedTuple):
    check:  Callable | None = None
    create: Callable | None = None


_BASE_ONLY = _TypeHandler()

_TYPE_HANDLERS: dict[str, _TypeHandler] = {
    'Option':      _TypeHandler(_check_option, _create_option),
    'Equity':      _BASE_ONLY,
    'Cash':        _BASE_ONLY,
    'Alternative': _BASE_ONLY,
}


# ── Base layer ──────────────────────────────────────────────────────────────

def _check_base(r, refs: dict[str, str], known) -> tuple[str | None, dict]:
    """Checks common to every type. Returns (skip_status | None, extra result fields)."""
    if not refs:
        return 'skipped_no_identifier', {}

    matched = set().union(*(known.get(k, set()) for k in refs.items()))
    if len(matched) == 1:
        return 'skipped_exists', {'security_id': next(iter(matched))}
    if len(matched) > 1:
        return 'skipped_conflict', {'security_id': ', '.join(sorted(matched))}

    if _none_if_nan(r['asset_type']) is None:
        return 'skipped_missing_asset_type', {}
    return None, {}


def _create_base(cur, r, refs: dict[str, str], data_source: str) -> str:
    """Write security_info + one security_xref row per identifier; return SecurityID."""
    security_id = create_security(
        cur, r['security_name'], _none_if_nan(r['currency']),
        _none_if_nan(r['asset_class']), _none_if_nan(r['asset_type']), data_source,
    )
    for ref_type, ref_id in refs.items():
        add_xref_if_missing(cur, security_id, ref_type, ref_id, data_source)
    return security_id


def process_rows(cur, df: pd.DataFrame, dry_run: bool, log: logging.Logger) -> list[dict]:
    """Process every row in df (already validated/normalized). Returns one
    result dict per row, in df's original order, each with a 'status':
    'created' | 'would_create' | 'skipped_exists' | 'skipped_conflict' |
    'skipped_no_identifier' | 'skipped_missing_asset_type' |
    'skipped_no_underlying' | 'skipped_unsupported_type'.
    See the module docstring (Step 2) for the order checks run in."""
    row_refs = [_row_refs(r) for _, r in df.iterrows()]
    keys = {k for refs in row_refs for k in refs.items()}
    keys |= {('Ticker', u) for u in map(_ref_value, df['underlying']) if u}
    known = _batch_lookup_refs(cur, keys)
    log.info(f"{len(known)} identifier(s) already in security_xref")

    results: list[dict] = []

    for (_, r), refs in zip(df.iterrows(), row_refs):
        name  = r['security_name']
        stype = r['parsed_security_type']
        result = {'security_name': name, 'security_type': stype}

        handler = _TYPE_HANDLERS.get(stype)
        if handler is None:
            results.append({**result, 'status': 'skipped_unsupported_type'})
            continue

        status, extra = _check_base(r, refs, known)
        ctx: dict = {}
        if status is None and handler.check:
            status, ctx = handler.check(r, known)
        result.update(extra, **ctx)
        if status:
            log.info(f"  SKIP ({status}) [{stype}] '{name}'  refs={refs}"
                     + (f" -> {extra['security_id']}" if 'security_id' in extra else ''))
            results.append({**result, 'status': status})
            continue

        data_source = _none_if_nan(r['data_source']) or DEFAULT_DATA_SOURCE
        desc = (f"[{stype}] '{name}'  refs={refs}  currency={_none_if_nan(r['currency'])} "
                f"asset_class={_none_if_nan(r['asset_class'])} asset_type={_none_if_nan(r['asset_type'])} "
                f"data_source={data_source}")
        if stype == 'Option':
            desc += (f"  underlying='{ctx['underlying']}' ({ctx['underlying_sec_id']}) "
                     f"type={_none_if_nan(r['option_type'])} strike={_none_if_nan(r['strike'])} "
                     f"maturity={_none_if_nan(r['maturity'])} option_class={_none_if_nan(r['option_class'])}")

        if dry_run:
            security_id = '(new)'
            log.info(f"  WOULD CREATE {desc}")
            results.append({**result, 'status': 'would_create'})
        else:
            security_id = _create_base(cur, r, refs, data_source)
            if handler.create:
                handler.create(cur, security_id, r, ctx)
            log.info(f"  CREATED {security_id} — {desc}")
            results.append({**result, 'status': 'created', 'security_id': security_id})

        # A repeat of these identifiers later in the same file is now a duplicate
        for k in refs.items():
            known.setdefault(k, set()).add(security_id)

    return results
