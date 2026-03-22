"""
dashboard_process.py — Daily computation process for the dashboard.

Steps:
    1. Get latest as_of_date from position_var
    2. Get all account_ids for that date
    3. For each account_id:
       a. Read positions from position_var, aggregate by security_id
       b. Write per-security market values to db_mv_history
       c. Compute portfolio summary (returns from db_mv_history)
       d. Write to db_portfolio_summary
       e. Compute positions (returns from db_mv_history)
       f. Write to db_positions

Run via cron or manually:
    python dashboard_process.py
"""
from __future__ import annotations

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
    compute_portfolio_summary,
    compute_positions,
)
from dashboard.positions_db import (
    delete_mv_history,
    delete_portfolio_summary,
    delete_positions,
    get_mv_history_dates,
    write_mv_history,
    write_portfolio_summary,
    write_positions,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)


def _build_mv_rows(df: pd.DataFrame) -> list[dict]:
    """Aggregate market_value by security_id and return as list of {security_id, market_value}."""
    df = df.copy()
    df['market_value'] = pd.to_numeric(df['market_value'], errors='coerce').fillna(0.0)
    agg = df.groupby('security_id', as_index=False)['market_value'].sum()
    return [
        {"security_id": row["security_id"], "market_value": float(row["market_value"])}
        for _, row in agg.iterrows()
        if row["security_id"]
    ]


def run() -> None:
    logger.info("Dashboard process started")

    # 1. Get latest as_of_date
    with pg_connection() as conn:
        dates = get_latest_feed_dates(conn, n=1)

    if not dates:
        logger.warning("No dates found in position_var. Aborting.")
        return

    as_of_date = dates[0]
    logger.info(f"Latest as_of_date: {as_of_date}")

    # 2. Get all account_ids for this date
    with pg_connection() as conn:
        account_ids = get_account_ids_on_date(conn, as_of_date)

    if not account_ids:
        logger.warning("No accounts found for latest date. Aborting.")
        return

    logger.info(f"Processing {len(account_ids)} account(s): {account_ids}")

    # 3. Process each account
    for account_id in account_ids:
        logger.info(f"--- account_id={account_id} ---")

        with pg_connection() as conn:
            df = get_positions_on_date(conn, as_of_date, account_id)

        if df.empty:
            logger.warning(f"No positions found for account_id={account_id}. Skipping.")
            continue

        # a. Write market values to db_mv_history
        deleted = delete_mv_history(account_id, as_of_date)
        logger.info(f"Deleted {deleted} existing rows from db_mv_history for {as_of_date}.")
        mv_rows = _build_mv_rows(df)
        write_mv_history(account_id, as_of_date, mv_rows)
        logger.info(f"Wrote {len(mv_rows)} rows to db_mv_history.")

        # b. Compute and write portfolio summary
        delete_portfolio_summary(account_id, as_of_date)
        summary = compute_portfolio_summary(account_id)
        write_portfolio_summary(account_id, summary)
        logger.info(f"Portfolio summary written  aum={summary.get('aum')}  asOfDate={summary.get('asOfDate')}")

        # c. Compute and write positions
        delete_positions(account_id, as_of_date)
        positions = compute_positions(account_id)
        write_positions(account_id, as_of_date, positions)
        logger.info(f"Wrote {len(positions)} positions to db_positions.")

    logger.info("Dashboard process completed.")


def backfill_mv_hist() -> None:
    """
    Write market values to db_mv_history for all (as_of_date, account_id) pairs
    in position_var (last 252 dates) that do not already have rows in db_mv_history.
    """
    logger.info("Backfill mv_history started")

    with pg_connection() as conn:
        feed_dates = get_latest_feed_dates(conn, n=252)

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


# command:
#  python dashboard_process.py            # normal daily run
#  python dashboard_process.py --backfill # backfill missing mv_history dates
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--backfill":
        backfill_mv_hist()
    else:
        run()
