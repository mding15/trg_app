"""
backfill_mv_history.py — Backfill utilities for db_mv_history.

Two entry points:
    backfill_mv_hist()               — backfill via position_var (all accounts, last 252 dates)
    backfill_mv_hist_price(account_id) — backfill via price history CSV (one account)

CLI:
    python backfill_mv_history.py --backfill
    python backfill_mv_history.py --backfill-price <account_id>
"""
from __future__ import annotations

import bisect
import logging
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from database2 import pg_connection
from dashboard.positions_calc import (
    get_latest_feed_dates,
    get_account_ids_on_date,
    get_positions_on_date,
)
from dashboard.positions_db import (
    get_mv_history_dates,
    write_mv_history,
)
from dashboard.dashboard_process import _build_mv_rows

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)

_PRICE_HISTORY_CSV = os.path.join(os.path.dirname(__file__), "test_data", "price_history.csv")


# ── Price-history backfill helpers ────────────────────────────────────────────

def _load_price_history() -> pd.DataFrame:
    """
    Load and preprocess price_history.csv.

    Returns a DataFrame with:
    - Index: datetime.date, sorted ascending
    - Columns: security_ids
    - Values: float prices, with missing values forward-filled
    """
    df = pd.read_csv(_PRICE_HISTORY_CSV, index_col=0)
    df.index = [d.date() for d in pd.to_datetime(df.index)]
    df = df.apply(pd.to_numeric, errors="coerce")
    df = df.sort_index()
    df = df.ffill()
    return df


def _detect_scale_factors(
    price_df: pd.DataFrame,
    account_id: int,
    position_var_dates: set,
) -> tuple[dict[str, float], list[dict]]:
    """
    Auto-detect per-security price scaling by comparing quantity * price vs market_value
    in position_var on the most recent date that appears in both price_history and position_var.

    Detected scale factors:
    - 1.0    if  0.8  <= ratio <= 1.2   (equity, price per share)
    - 100.0  if  90   <= ratio <= 110   (bond, price per 100 face value)
    - 1000.0 if  900  <= ratio <= 1100  (bond, price per 1000 face value)
    - 1.0    otherwise (ambiguous — log warning, default to equity)

    Only securities present in price_df are included.
    Securities with insufficient data are silently omitted (caller defaults to 1.0).

    Returns:
        scale_factors: {security_id: scale_factor}
        details:       list of dicts with one row per security, for CSV output
    """
    overlap = sorted(set(price_df.index) & position_var_dates, reverse=True)
    if not overlap:
        logger.warning(
            f"No overlapping dates between price_history and position_var for "
            f"account_id={account_id}. All scale factors default to 1."
        )
        return {}, []

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT as_of_date, security_id,
                       SUM(quantity)     AS quantity,
                       SUM(market_value) AS market_value
                FROM position_var
                WHERE account_id = %s AND as_of_date = ANY(%s)
                GROUP BY as_of_date, security_id
                ORDER BY as_of_date DESC
                """,
                (account_id, overlap),
            )
            rows = cur.fetchall()

    if not rows:
        return {}, []

    # rows are sorted DESC by date — keep first occurrence per security (most recent)
    seen: dict[str, tuple] = {}
    for as_of_date, security_id, quantity, market_value in rows:
        if security_id not in seen:
            seen[security_id] = (as_of_date, quantity, market_value)

    scale_factors: dict[str, float] = {}
    details: list[dict] = []

    for security_id, (as_of_date, quantity, market_value) in seen.items():
        if security_id not in price_df.columns:
            details.append({
                "security_id":        security_id,
                "ref_date":           as_of_date,
                "quantity":           quantity,
                "price":              None,
                "market_value_posvar": market_value,
                "ratio":              None,
                "scale_factor":       None,
                "note":               "not in price_history.csv",
            })
            continue
        if as_of_date not in price_df.index:
            continue

        price = price_df.loc[as_of_date, security_id]
        if pd.isna(price) or price == 0:
            details.append({
                "security_id":        security_id,
                "ref_date":           as_of_date,
                "quantity":           quantity,
                "price":              None,
                "market_value_posvar": market_value,
                "ratio":              None,
                "scale_factor":       None,
                "note":               "price missing or zero on ref_date",
            })
            continue
        if not quantity or not market_value:
            details.append({
                "security_id":        security_id,
                "ref_date":           as_of_date,
                "quantity":           quantity,
                "price":              float(price),
                "market_value_posvar": market_value,
                "ratio":              None,
                "scale_factor":       None,
                "note":               "quantity or market_value zero in position_var",
            })
            continue

        ratio = float(quantity) * float(price) / float(market_value)

        if 0.8 <= ratio <= 1.2:
            scale = 1.0
            note = "equity (ratio ≈ 1)"
        elif 90.0 <= ratio <= 110.0:
            scale = 100.0
            note = "bond, price per 100 face value"
        elif 900.0 <= ratio <= 1100.0:
            scale = 1000.0
            note = "bond, price per 1000 face value"
        else:
            scale = 1.0
            note = f"ambiguous ratio={ratio:.4f} — defaulted to scale=1"
            logger.warning(
                f"  Ambiguous price scale for security_id={security_id}: "
                f"ratio={ratio:.4f} (qty={quantity}, price={price}, mv={market_value}). "
                "Defaulting to scale=1."
            )

        scale_factors[security_id] = scale
        details.append({
            "security_id":        security_id,
            "ref_date":           as_of_date,
            "quantity":           float(quantity),
            "price":              float(price),
            "market_value_posvar": float(market_value),
            "ratio":              round(ratio, 6),
            "scale_factor":       scale,
            "note":               note,
        })

    return scale_factors, details


# ── Backfill functions ─────────────────────────────────────────────────────────

def backfill_mv_hist() -> None:
    """
    Write market values to db_mv_history for all (as_of_date, account_id) pairs
    in position_var (last 252 dates) that do not already have rows in db_mv_history.
    """
    logger.info("Backfill mv_history started")

    with pg_connection() as conn:
        feed_dates = get_latest_feed_dates(conn, n=252)  # last 252 trading days ~ 1 year

    if not feed_dates:
        logger.warning("No dates found in position_var. Aborting backfill.")
        return

    logger.info(f"Found {len(feed_dates)} date(s) in position_var.")

    for date in feed_dates:
        with pg_connection() as conn:
            account_ids = get_account_ids_on_date(conn, date)

        for account_id in account_ids:
            existing_dates = get_mv_history_dates(account_id)
            if date in existing_dates:
                continue

            with pg_connection() as conn:
                df = get_positions_on_date(conn, date, account_id)

            if df.empty:
                logger.warning(f"  {date}  account_id={account_id}  no positions found, skipping.")
                continue

            mv_rows = _build_mv_rows(df)
            write_mv_history(account_id, date, mv_rows)
            logger.info(f"  {date}  account_id={account_id}  wrote {len(mv_rows)} rows to db_mv_history.")

    logger.info("Backfill mv_history completed.")


def backfill_mv_hist_price(account_id: int) -> None:
    """
    Backfill db_mv_history using historical stock prices for the given account_id.

    Processes dates from price_history.csv that satisfy all of:
    - On or before the most recent as_of_date already in db_mv_history for this account
    - NOT already present in db_mv_history (skip existing)
    - NOT present in position_var (those dates are handled by backfill_mv_hist)
    - Within the 252 most-recent qualifying dates

    For each qualifying date:
    - Quantities and fallback market_values come from the closest position_var date
      on or before the backfill date.
    - market_value = quantity * price / scale_factor
    - Securities absent from price_history.csv use the flat market_value from the
      reference position_var date (no price scaling applied).
    - scale_factor (1, 100, or 1000) is auto-detected per security; see
      _detect_scale_factors() for logic.

    CLI: python backfill_mv_history.py --backfill-price <account_id>
    """
    logger.info(f"backfill_mv_hist_price started  account_id={account_id}")

    # 1. Load and preprocess price history
    price_df = _load_price_history()
    logger.info(
        f"Loaded price_history.csv: {len(price_df)} dates, {len(price_df.columns)} securities  "
        f"({price_df.index[0]} → {price_df.index[-1]})"
    )

    # 2. Existing db_mv_history dates — skip logic
    existing_mv_dates = get_mv_history_dates(account_id)

    # 3. All position_var dates for this account — skip logic
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT as_of_date FROM position_var WHERE account_id = %s",
                (account_id,),
            )
            position_var_dates = {row[0] for row in cur.fetchall()}
    logger.info(f"Found {len(position_var_dates)} position_var dates for account_id={account_id}")

    # 4. All price_history dates not already in db_mv_history and not in position_var
    candidate_dates = sorted(
        d for d in price_df.index
        if d not in existing_mv_dates
        and d not in position_var_dates
    )

    if not candidate_dates:
        logger.info(f"No dates to backfill for account_id={account_id}.")
        return

    logger.info(
        f"Dates to backfill: {len(candidate_dates)}  "
        f"({min(candidate_dates)} → {max(candidate_dates)})"
    )

    # 5. Detect bond/equity scale factors and write audit CSV
    scale_factors, scale_details = _detect_scale_factors(price_df, account_id, position_var_dates)
    n_bond100  = sum(1 for s in scale_factors.values() if s == 100.0)
    n_bond1000 = sum(1 for s in scale_factors.values() if s == 1000.0)
    logger.info(
        f"Scale factors detected: {len(scale_factors)} securities  "
        f"({n_bond100} bond@100, {n_bond1000} bond@1000)"
    )

    if scale_details:
        csv_path = os.path.join(
            os.path.dirname(_PRICE_HISTORY_CSV),
            f"scale_factors_{account_id}.csv",
        )
        pd.DataFrame(scale_details).to_csv(csv_path, index=False)
        logger.info(f"Scale factor audit written to {csv_path}")

    # 6. Prepare sorted position_var date list for O(log n) lookback
    sorted_pv_dates: list = sorted(position_var_dates)

    # Cache positions fetched from position_var to avoid repeated DB round-trips
    ref_cache: dict = {}

    def _get_ref_positions(backfill_date) -> tuple:
        """Return (ref_date, {security_id: {quantity, market_value}}) for the closest
        position_var date on or before backfill_date. Falls back to the earliest
        position_var date if backfill_date predates all position_var entries."""
        idx = bisect.bisect_right(sorted_pv_dates, backfill_date) - 1
        ref_date = sorted_pv_dates[idx] if idx >= 0 else sorted_pv_dates[0]
        if ref_date not in ref_cache:
            with pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT security_id,
                               SUM(quantity)     AS quantity,
                               SUM(market_value) AS market_value
                        FROM position_var
                        WHERE account_id = %s AND as_of_date = %s
                        GROUP BY security_id
                        """,
                        (account_id, ref_date),
                    )
                    rows = cur.fetchall()
            ref_cache[ref_date] = {
                row[0]: {"quantity": row[1], "market_value": row[2]}
                for row in rows
            }
        return ref_date, ref_cache[ref_date]

    # 7. Backfill in chronological order
    for backfill_date in sorted(candidate_dates):
        ref_date, ref_positions = _get_ref_positions(backfill_date)

        if not ref_positions:
            logger.warning(f"  {backfill_date}: position_var is empty for ref_date={ref_date}. Skipping.")
            continue

        mv_rows = []
        for security_id, pos in ref_positions.items():
            quantity   = pos["quantity"]
            mv_flat    = float(pos["market_value"]) if pos["market_value"] is not None else 0.0

            if (
                security_id in price_df.columns
                and quantity is not None
                and quantity != 0
            ):
                price = price_df.loc[backfill_date, security_id]
                if pd.notna(price):
                    scale = scale_factors.get(security_id, 1.0)
                    mv = float(quantity) * float(price) / scale
                else:
                    mv = mv_flat  # price missing even after ffill — use flat
            else:
                mv = mv_flat  # security not in price_history — use flat

            mv_rows.append({"security_id": security_id, "market_value": mv})

        write_mv_history(account_id, backfill_date, mv_rows)
        logger.info(
            f"  {backfill_date}  ref={ref_date}  wrote {len(mv_rows)} rows to db_mv_history."
        )

    logger.info(f"backfill_mv_hist_price completed  account_id={account_id}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--backfill":
        backfill_mv_hist()
    elif len(sys.argv) > 2 and sys.argv[1] == "--backfill-price":
        backfill_mv_hist_price(int(sys.argv[2]))
    else:
        print("Usage:")
        print("  python backfill_mv_history.py --backfill")
        print("  python backfill_mv_history.py --backfill-price <account_id>")
