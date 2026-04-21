"""
backfill_mv_history.py — Backfill db_mv_history using historical prices.

Reads positions (quantity, market_value) from position_var for a given pos_date,
then re-prices each security using historical prices over a backfill date range.
new_mv = quantity × price; falls back to original market_value if price is missing.
Existing db_mv_history rows are overwritten (upsert).

Usage
-----
    # prices from DB, date range
    python dashboard/backfill_mv_history.py --account-id 1003 --pos-date 2026-03-31 \
        --from-date 2025-01-01 --to-date 2025-12-31

    # prices from DB, single date (no --from-date)
    python dashboard/backfill_mv_history.py --account-id 1003 --pos-date 2026-03-31 \
        --to-date 2025-06-30

    # prices from CSV file
    python dashboard/backfill_mv_history.py --account-id 1003 --pos-date 2026-03-31 \
        --from-date 2025-01-01 --to-date 2025-12-31 --price-file /path/to/prices.csv
"""
from __future__ import annotations

import argparse
import datetime
import logging
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from database2 import pg_connection
from dashboard.positions_db import write_mv_history
from mkt_data import mkt_timeseries

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)

_PRICE_HISTORY_CSV = os.path.join(os.path.dirname(__file__), "test_data", "price_history.csv")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_positions_for_backfill(account_id: int, pos_date) -> list[dict]:
    """
    Fetch security_id, quantity, and market_value from position_var for
    (account_id, pos_date), aggregated by security_id.

    Returns a list of dicts: {security_id, quantity, market_value}
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT security_id,
                       SUM(quantity)     AS quantity,
                       SUM(market_value) AS market_value
                FROM position_var
                WHERE account_id = %s AND as_of_date = %s
                  AND security_id IS NOT NULL
                GROUP BY security_id
                ORDER BY security_id
                """,
                (account_id, pos_date),
            )
            rows = cur.fetchall()
    return [
        {"security_id": row[0], "quantity": row[1], "market_value": row[2]}
        for row in rows
    ]


def _load_price_file(price_file: str) -> pd.DataFrame:
    """
    Load a wide-format price CSV (date index, security_id columns).
    Parses dates, coerces values to numeric, sorts ascending, forward-fills gaps.
    """
    df = pd.read_csv(price_file, index_col=0)
    df.index = [d.date() for d in pd.to_datetime(df.index)]
    df = df.apply(pd.to_numeric, errors="coerce").sort_index().ffill()
    return df


# ── Main backfill function ────────────────────────────────────────────────────

def backfill_mv_hist_price_db(
    account_id: int,
    pos_date,
    to_date,
    from_date=None,
    price_file: str = None,
) -> None:
    """
    Backfill db_mv_history for one account using historical prices.

    Parameters
    ----------
    account_id  : int
    pos_date    : date — date to read positions (quantity, market_value) from position_var
    to_date     : date — last backfill date (required)
    from_date   : date or None — first backfill date; if None, backfills to_date only
    price_file  : str or None — path to wide-format price CSV;
                                if None, prices are fetched from DB via mkt_timeseries
    """
    effective_from = from_date if from_date is not None else to_date

    logger.info(
        f"backfill_mv_hist_price_db started  account_id={account_id}  "
        f"pos_date={pos_date}  range=[{effective_from}, {to_date}]  "
        f"price_source={'file: ' + price_file if price_file else 'DB'}"
    )

    # 1. Get positions from position_var
    positions = _get_positions_for_backfill(account_id, pos_date)
    if not positions:
        logger.warning(
            f"No positions found in position_var for account_id={account_id}, "
            f"pos_date={pos_date}. Aborting."
        )
        return
    logger.info(f"Loaded {len(positions)} positions from position_var for pos_date={pos_date}")

    sec_ids = [p["security_id"] for p in positions]
    qty_map = {p["security_id"]: p["quantity"]     for p in positions}
    mv_flat = {
        p["security_id"]: float(p["market_value"]) if p["market_value"] is not None else 0.0
        for p in positions
    }

    # 2. Load prices
    if price_file:
        logger.info(f"Loading prices from file: {price_file}")
        price_df = _load_price_file(price_file)
    else:
        logger.info("Fetching prices from DB via mkt_timeseries...")
        price_df = mkt_timeseries.get(
            sec_ids,
            from_date=datetime.datetime.combine(effective_from, datetime.time.min),
            to_date=datetime.datetime.combine(to_date, datetime.time.min),
        )
        if price_df is None or price_df.empty:
            logger.warning("No prices returned from mkt_timeseries. Aborting.")
            return
        price_df.index = [
            d.date() if hasattr(d, "date") else d for d in price_df.index
        ]

    logger.info(
        f"Price matrix: {price_df.shape[0]} dates x {price_df.shape[1]} securities  "
        f"({price_df.index[0]} → {price_df.index[-1]})"
    )

    # 3. Determine backfill dates within range
    backfill_dates = sorted(
        d for d in price_df.index
        if effective_from <= d <= to_date
    )
    if not backfill_dates:
        logger.warning(
            f"No price dates found in range [{effective_from}, {to_date}]. Aborting."
        )
        return
    logger.info(
        f"Backfill dates: {len(backfill_dates)}  "
        f"({min(backfill_dates)} → {max(backfill_dates)})"
    )

    # 4. Backfill each date
    audit_rows: list[dict] = []

    for backfill_date in backfill_dates:
        mv_rows  = []
        n_priced = 0
        n_flat   = 0

        for security_id in sec_ids:
            quantity = qty_map[security_id]
            fallback = mv_flat[security_id]
            backfill_price = None

            if (
                security_id in price_df.columns
                and quantity is not None
                and quantity != 0
                and backfill_date in price_df.index
            ):
                price = price_df.loc[backfill_date, security_id]
                if pd.notna(price):
                    backfill_price = float(price)
                    mv = float(quantity) * backfill_price
                    n_priced += 1
                else:
                    mv = fallback
                    n_flat += 1
            else:
                mv = fallback
                n_flat += 1

            mv_rows.append({"security_id": security_id, "market_value": mv})

            # Collect audit data for the final date only
            if backfill_date == to_date:
                audit_rows.append({
                    "account_id":    account_id,
                    "pos_date":      pos_date,
                    "security_id":   security_id,
                    "quantity":      float(quantity) if quantity is not None else None,
                    "mv":            fallback,
                    "to_date":       to_date,
                    "backfill_price": backfill_price,
                    "backfill_mv":   mv,
                })

        write_mv_history(account_id, backfill_date, mv_rows)
        logger.info(
            f"  {backfill_date}  wrote {len(mv_rows)} rows  "
            f"({n_priced} priced, {n_flat} flat fallback)"
        )

    # 5. Write audit CSV (one row per security, final date only)
    if audit_rows:
        test_data_dir = os.path.dirname(_PRICE_HISTORY_CSV)
        to_date_str   = to_date.strftime("%Y%m%d")
        csv_path      = os.path.join(test_data_dir, f"backfill_mv_{account_id}_{to_date_str}.csv")
        pd.DataFrame(audit_rows).to_csv(csv_path, index=False)
        logger.info(f"Audit CSV written: {csv_path}  ({len(audit_rows)} rows)")

    logger.info(f"backfill_mv_hist_price_db completed  account_id={account_id}")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    def _parse_date(s: str) -> datetime.date:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()

    parser = argparse.ArgumentParser(
        description="Backfill db_mv_history using historical prices from DB or CSV file."
    )
    parser.add_argument("--account-id",  dest="account_id",  type=int,         required=True,
                        help="Account ID to backfill")
    parser.add_argument("--pos-date",    dest="pos_date",    type=_parse_date,  required=True,
                        help="Position date to read quantities from position_var (YYYY-MM-DD)")
    parser.add_argument("--to-date",     dest="to_date",     type=_parse_date,  required=True,
                        help="Last backfill date (YYYY-MM-DD)")
    parser.add_argument("--from-date",   dest="from_date",   type=_parse_date,  default=None,
                        help="First backfill date (YYYY-MM-DD); if omitted, backfills to-date only")
    parser.add_argument("--price-file",  dest="price_file",  type=str,          default=None,
                        help="Path to wide-format price CSV; if omitted, prices are fetched from DB")
    args = parser.parse_args()

    backfill_mv_hist_price_db(
        account_id = args.account_id,
        pos_date   = args.pos_date,
        to_date    = args.to_date,
        from_date  = args.from_date,
        price_file = args.price_file,
    )
