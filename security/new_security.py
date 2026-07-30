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
process_rows() only processes rows with parsed_security_type == 'Option' —
every other row is left alone and reported as skipped (bonds and other types
aren't handled yet). Every value written to the database — currency,
asset_class, asset_type, data_source, option_class, option_type, strike,
maturity, underlying, isin, cusip — comes directly from the row; nothing is
hardcoded or re-derived here.

For each Option row:
    1. Skip if the row's (already whitespace-normalized) `ticker` already
       exists in security_xref as REF_TYPE='Ticker' — it's already been
       created, by a previous run or otherwise.
    2. Look up `underlying` in security_xref (REF_TYPE='Ticker') to resolve
       underlying_sec_id. Skip if not found — an option_info row with no
       resolvable underlying would silently break the options P&L engine's
       join (see process2/calc_options_pnl.py), so the underlying must
       already be set up first.
    3. Otherwise: create_security(...), add_xref_if_missing(..., 'Ticker',
       ...), and — only when non-blank — add_xref_if_missing(..., 'ISIN',
       ...) / (..., 'CUSIP', ...), then create_option_info(...).

process_rows() is the shared core used by both the CLI
(maintenance/insert_new_security.py) and the trg_ops API
(api/ops_routes.py's /api/ops/new-security/insert). All DB writes for a run
are committed in one transaction by the caller.
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

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


def _batch_check_tickers(cur, tickers: list[str]) -> dict[str, str]:
    """Return {ticker: SecurityID} for every given ticker already present in
    security_xref as REF_TYPE='Ticker'."""
    tickers = sorted({t for t in tickers if t})
    if not tickers:
        return {}
    cur.execute(
        'SELECT "REF_ID", "SecurityID" FROM security_xref WHERE "REF_TYPE" = %s AND "REF_ID" = ANY(%s)',
        ('Ticker', tickers),
    )
    return {ref_id: str(sec_id) for ref_id, sec_id in cur.fetchall()}


def process_rows(cur, df: pd.DataFrame, dry_run: bool, log: logging.Logger) -> list[dict]:
    """Process every row in df (already validated/normalized). Returns one
    result dict per row, in df's original order, each with a 'status':
    'created' | 'would_create' | 'skipped_exists' | 'skipped_no_underlying' |
    'skipped_not_option'."""
    is_option = df['parsed_security_type'] == 'Option'
    option_rows = df[is_option]

    existing = _batch_check_tickers(
        cur, list(option_rows['ticker']) + list(option_rows['underlying']),
    ) if not option_rows.empty else {}
    log.info(f"{len(existing)} ticker(s) already in security_xref")

    results: list[dict] = []

    for _, r in df.iterrows():
        name = r['security_name']

        if r['parsed_security_type'] != 'Option':
            results.append({'security_name': name, 'status': 'skipped_not_option'})
            continue

        ticker     = r['ticker']
        underlying = r['underlying']

        if ticker and ticker in existing:
            log.info(f"  SKIP (already exists) '{name}'  ticker='{ticker}' -> {existing[ticker]}")
            results.append({'security_name': name, 'status': 'skipped_exists', 'security_id': existing[ticker]})
            continue

        underlying_sec_id = existing.get(underlying) if underlying else None
        if underlying_sec_id is None:
            log.warning(f"  SKIP (no underlying) '{name}'  underlying='{underlying}' not found in security_xref")
            results.append({'security_name': name, 'status': 'skipped_no_underlying'})
            continue

        isin         = _none_if_nan(r['isin'])
        cusip        = _none_if_nan(r['cusip'])
        currency     = _none_if_nan(r['currency'])
        asset_class  = _none_if_nan(r['asset_class'])
        asset_type   = _none_if_nan(r['asset_type'])
        data_source  = _none_if_nan(r['data_source'])
        option_type  = _none_if_nan(r['option_type'])
        strike       = _none_if_nan(r['strike'])
        maturity     = _none_if_nan(r['maturity'])
        option_class = _none_if_nan(r['option_class'])

        if dry_run:
            log.info(
                f"  WOULD CREATE '{name}'  ticker='{ticker}' isin='{isin}' cusip='{cusip}' "
                f"underlying='{underlying}' ({underlying_sec_id})  currency={currency} "
                f"asset_class={asset_class} asset_type={asset_type} data_source={data_source}  "
                f"type={option_type} strike={strike} maturity={maturity} option_class={option_class}"
            )
            results.append({
                'security_name': name, 'status': 'would_create',
                'underlying_sec_id': underlying_sec_id,
            })
            continue

        security_id = create_security(cur, name, currency, asset_class, asset_type, data_source)
        add_xref_if_missing(cur, security_id, 'Ticker', ticker, data_source)
        add_xref_if_missing(cur, security_id, 'ISIN', isin, data_source)
        add_xref_if_missing(cur, security_id, 'CUSIP', cusip, data_source)
        create_option_info(
            cur, security_id, option_type, option_class, maturity,
            strike, underlying, underlying_sec_id,
        )
        log.info(
            f"  CREATED {security_id} — '{name}'  ticker='{ticker}' isin='{isin}' cusip='{cusip}' "
            f"underlying='{underlying}' ({underlying_sec_id})  type={option_type} strike={strike} maturity={maturity}"
        )
        results.append({
            'security_name': name, 'status': 'created', 'security_id': security_id,
            'underlying_sec_id': underlying_sec_id,
        })

    return results
