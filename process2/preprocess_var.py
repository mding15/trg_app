"""
preprocess_var.py — Build enriched positions DataFrame ready for VaR calculation.

Entry point: preprocess_var(as_of_date, feed_source, account_ids) → pd.DataFrame

The output DataFrame is passed directly to the VaR engine. Every row represents one
position with PascalCase column names (VaR engine convention). Excluded positions are
retained in the output with excluded=True so the engine can log and skip them cleanly.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PIPELINE OVERVIEW
─────────────────
  Step 1  Fetch positions from proc_positions          (db_position_var.fetch_proc_positions)
  Step 2  Map column names to VaR engine conventions   (_map_columns)
  Step 3  Enrich with security data + apply exclusions (update_security_info.update_security_info)
            3a  Exclude unknown securities (SecurityID is NULL)
            3b  Exclude securities not in current risk model
            3c  Update AssetClass / AssetType / Currency from security_info
            3d  Update classification + risk attributes from security_attribute
            3e  Exclude matured securities (MaturityDate < as_of_date)
            3f  Flag options (is_option)
            3g  Set UnderlyingID
  Step 4  Fill missing or stale prices                 (update_position_price.update_position_price)
            4a  Overwrite with market DB price where available and newer
            4b  Force LastPrice = 1 for Cash
            4c  Imply price from MarketValue / Quantity where still missing
            4d  Fallback LastPrice = 1 for anything remaining
  Step 5  Fetch and merge position-level betas         (_fetch_betas)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

STEP 1 — FETCH FROM proc_positions
────────────────────────────────────
Source table: proc_positions (falls back to proc_positions_hist if the primary table
returns no rows for the given as_of_date — useful for historical re-runs).

Filters applied via parameters:
  as_of_date   required; selects the processing date
  feed_source  optional; restricts to a single feed (e.g. 'ADVENT', 'UPLOAD')
  account_ids  optional; restricts to a subset of accounts

Stripped columns: id, insert_time (internal DB-only columns, removed before returning).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

STEP 2 — COLUMN MAPPING
────────────────────────
proc_positions uses snake_case; the VaR engine expects PascalCase. _map_columns()
applies the _RENAME dict and drops columns not consumed downstream:

  Dropped: as_of_date, asset_class, feed_source
    - as_of_date is passed as a separate parameter throughout the pipeline
    - asset_class from proc_positions is the feed-provided value; the authoritative
      AssetClass comes from security_info (Step 3c) and overwrites it
    - feed_source is a routing label not needed by the engine

Key renames (snake_case → PascalCase):
  security_id → SecurityID       ticker      → Ticker
  security_name → SecurityName   currency    → Currency
  quantity    → Quantity         last_price  → LastPrice
  market_value → MarketValue     last_price_date → LastPriceDate
  isin        → ISIN             position_id → pos_id
  cusip       → CUSIP

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

STEP 3 — SECURITY ENRICHMENT (update_security_info.py)
────────────────────────────────────────────────────────
All enrichment is in-memory; no database writes are performed here.
A per-run log file is written to logs/update_security_info_<port_id>_<timestamp>.log.

Step 3a — Exclude: unknown security (SecurityID is NULL)
  Positions with no SecurityID cannot be mapped to a risk model. These are marked:
    excluded = True, exclude_reason = 'unknown security'
  They remain in the DataFrame so the engine can log them explicitly.

Step 3b — Exclude: not in current risk model
  Modeled security set = (
      SELECT SecurityID FROM risk_factor
      JOIN risk_model ON risk_factor.model_id = risk_model.model_id
      WHERE risk_model.is_current = 1
  ) UNION (
      SELECT SecurityID FROM security_info WHERE AssetType = 'Treasury'
  )
  Treasuries are always included regardless of the risk model because they are handled
  via interest-rate factors, not equity/credit factors.
  This result is cached with @functools.cache for the process lifetime — the current
  risk model does not change mid-run and the query is expensive.

  Positions whose SecurityID is not in this set are marked:
    excluded = True, exclude_reason = 'not modeled'

Step 3c — Update: AssetClass, AssetType, Currency from security_info
  Source: security_info (SecurityID, AssetClass, AssetType, Currency)
  Batch-fetched for all known SecurityIDs via ANY(%s).
  These three fields are the authoritative classification; the feed-provided
  asset_class column (dropped in Step 2) is not used.

Step 3d — Update: classification + risk attributes from security_attribute
  Source: security_attribute, batch-fetched for all known SecurityIDs.
  Fields written to positions:
    ExpectedReturn        — annual expected return used in risk-adjusted metrics
    Class / SC1 / SC2     — internal asset class hierarchy (e.g. Equity > EQ ETF)
    Country / Region      — geographic classification
    Sector / Industry     — GICS-style classification
    OptionType            — 'Call', 'Put', or NULL
    PaymentFrequency      — for fixed-income coupons
    MaturityDate          — used for maturity exclusion (Step 3e)
    OptionStrike          — option strike price
    UnderlyingSecurityID  — links option to its underlying
    CouponRate            — fixed-income coupon
  Additionally, Ticker is filled from security_attribute where the position's Ticker
  is NULL or blank — the feed-provided ticker takes precedence when present.

Step 3e — Exclude: matured securities (MaturityDate < as_of_date)
  Fixed-income positions past their maturity date are excluded:
    excluded = True, exclude_reason = 'matured'
  This step is skipped entirely if as_of_date was not supplied to the function.

Step 3f — Flag options
  is_option is set True where OptionType IN ('Call', 'Put').
  The VaR engine uses this flag to route the position through the options pricer
  (Black-Scholes delta/gamma) rather than the standard equity model.

Step 3g — Set UnderlyingID
  UnderlyingID = UnderlyingSecurityID (alias used internally by the engine).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

STEP 4 — PRICE UPDATE (update_position_price.py)
──────────────────────────────────────────────────
The goal is to ensure every position has a valid LastPrice. The feed-provided
LastPrice is the starting point; the following logic replaces or supplements it:

Step 4a — Market DB price (highest priority)
  mkt_timeseries.get_last_prices(security_ids, as_of_date) fetches the most recent
  price on or before as_of_date from the market data database.
  The market DB price overwrites the feed price when either:
    - the feed LastPrice is NULL, OR
    - the market price date is strictly newer than the feed LastPriceDate
  This ensures stale feed prices are corrected by market data where available.

Step 4b — Cash override
  Positions where AssetClass == 'Cash' have LastPrice forced to 1 and
  LastPriceDate set to as_of_date. This overrides any market DB price.

Step 4c — Implied price
  If LastPrice is still NULL after steps 4a/4b but Quantity is known:
    LastPrice = MarketValue / Quantity
  This handles positions where the feed provides market value but no price
  (common for certain alternative / OTC instruments).
  Note: MarketValue recalculation (Quantity × LastPrice) is intentionally
  disabled (commented out) to preserve the feed-provided market value.

Step 4d — Final fallback
  Any position still missing LastPrice receives LastPrice = 1. This is a last-resort
  placeholder and should be investigated if it appears in production.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

STEP 5 — BETA MERGE
─────────────────────
Beta measures systematic market sensitivity and is stored separately in sec_beta,
keyed by (security_id, beta_key). Different accounts can use different beta sets
(e.g. 1-year vs 3-year regression window) controlled by account_parameters.beta_key.

Query:
  SELECT DISTINCT ON (pp.account_id, pp.security_id)
         pp.account_id, pp.security_id, sb.beta
  FROM proc_positions pp
  LEFT JOIN account_parameters ap ON pp.account_id = ap.account_id
  LEFT JOIN sec_beta sb
         ON pp.security_id = sb.security_id
        AND sb.beta_key    = ap.beta_key
  WHERE pp.as_of_date = %s AND pp.account_id = ANY(%s)
  ORDER BY pp.account_id, pp.security_id, ap.updated_at DESC

DISTINCT ON with ORDER BY updated_at DESC means that if an account has multiple
account_parameters rows (e.g. from parameter updates), the most recently updated
beta_key wins. Positions with no matching sec_beta row get beta = NULL, which the
VaR engine interprets as "use default market beta".

Result is LEFT-JOINed onto positions by (account_id, SecurityID).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

OUTPUT SCHEMA (key columns)
────────────────────────────
All columns are PascalCase. Selected key columns:

  Identity:     SecurityID, SecurityName, ISIN, CUSIP, Ticker, pos_id, account_id
  Position:     Quantity, MarketValue, Currency, LastPrice, LastPriceDate
  Classification: AssetClass, AssetType, Class, SC1, SC2, Country, Region,
                  Sector, Industry
  Risk inputs:  ExpectedReturn, beta
  Fixed income: CouponRate, PaymentFrequency, MaturityDate
  Options:      OptionType, OptionStrike, UnderlyingSecurityID, UnderlyingID, is_option
  Exclusion:    excluded (bool), exclude_reason ('unknown security' | 'not modeled' | 'matured')

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

RELATED MODULES
────────────────
  process2/db_position_var.py       — fetch_proc_positions, insert_results
  process2/update_security_info.py  — security enrichment + exclusion logic
  process2/update_position_price.py — price fallback chain
  mkt_data/mkt_timeseries.py        — market price lookup
  database2/                        — pg_connection and schema definitions
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from database2 import pg_connection
from process2.db_position_var import fetch_proc_positions
from process2.update_security_info import update_security_info
from process2.update_position_price import update_position_price


# ── column mapping ─────────────────────────────────────────────────────────────

_RENAME = {
    'security_id':     'SecurityID',
    'security_name':   'SecurityName',
    'quantity':        'Quantity',
    'market_value':    'MarketValue',
    'isin':            'ISIN',
    'cusip':           'CUSIP',
    'ticker':          'Ticker',
    'currency':        'Currency',
    'last_price':      'LastPrice',
    'last_price_date': 'LastPriceDate',
    'position_id':     'pos_id',
}

_DROP = ['as_of_date', 'asset_class', 'feed_source']


def _map_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename proc_positions columns to VaR engine conventions and drop unused columns."""
    df = df.drop(columns=[c for c in _DROP if c in df.columns])
    df = df.rename(columns=_RENAME)
    return df


# ── beta fetch ─────────────────────────────────────────────────────────────────

def _fetch_betas(as_of_date, account_ids: list[int]) -> pd.DataFrame:
    """Return (account_id, security_id, beta) rows from sec_beta via account_parameters."""
    if not account_ids:
        return pd.DataFrame(columns=['account_id', 'security_id', 'beta'])
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (pp.account_id, pp.security_id)
                       pp.account_id, pp.security_id, sb.beta
                FROM proc_positions pp
                LEFT JOIN account_parameters ap ON pp.account_id = ap.account_id
                LEFT JOIN sec_beta sb
                       ON pp.security_id = sb.security_id
                      AND sb.beta_key    = ap.beta_key
                WHERE pp.as_of_date = %s
                  AND pp.account_id  = ANY(%s)
                ORDER BY pp.account_id, pp.security_id, ap.updated_at DESC
                """,
                (as_of_date, account_ids),
            )
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=['account_id', 'security_id', 'beta'])


# ── main ───────────────────────────────────────────────────────────────────────

def preprocess_var(
    as_of_date,
    feed_source: str | None = None,
    account_ids: list[int] | None = None,
) -> pd.DataFrame:
    """
    Build enriched positions DataFrame for VaR calculation from proc_positions.

    as_of_date:   the as_of_date value in proc_positions to process.
    feed_source:  only rows with this feed_source are fetched.
    account_ids:  if provided, only rows for those account_ids are fetched.
    """
    positions = fetch_proc_positions(as_of_date, feed_source, account_ids)
    if positions.empty:
        acct_msg = f', account_ids={account_ids}' if account_ids is not None else ''
        raise ValueError(
            f'No proc_positions rows found for as_of_date={as_of_date}, '
            f'feed_source={feed_source!r}{acct_msg}'
        )

    positions = _map_columns(positions)
    positions = update_security_info(positions, asof_date=as_of_date)
    positions = update_position_price(positions, as_of_date)
    positions = positions.reset_index(drop=True)

    acct_ids = [int(a) for a in positions['account_id'].dropna().unique()]
    betas_df = _fetch_betas(as_of_date, acct_ids)
    positions = positions.merge(
        betas_df,
        left_on=['account_id', 'SecurityID'],
        right_on=['account_id', 'security_id'],
        how='left',
    ).drop(columns='security_id')

    return positions
