"""
backfill_breakdowns.py — Backfill db_portfolio_breakdown from position_var data.

Iterates over all (account_id, as_of_date) pairs in position_var that fall
within the given date range and writes breakdown rows to db_portfolio_breakdown.
Skips dates that already have rows unless --overwrite is specified.

Usage
-----
    # all accounts, all dates from 2026-04-21 to today
    python dashboard/backfill_breakdowns.py --from-date 2026-04-21

    # single account, date range
    python dashboard/backfill_breakdowns.py --account-id 1001 \
        --from-date 2026-01-01 --to-date 2026-04-21

    # overwrite existing rows
    python dashboard/backfill_breakdowns.py --from-date 2026-04-21 --overwrite
"""
from __future__ import annotations

import argparse
import datetime
import logging
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import trg_config  # noqa: F401 — loads DB credentials into environment

from database2 import pg_connection
from dashboard.breakdown_calc import compute_breakdowns
from dashboard.breakdown_db import delete_breakdowns, write_breakdowns
from dashboard.positions_calc import get_positions_on_date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)


def _get_dates_to_backfill(
    account_id: int | None,
    from_date: datetime.date,
    to_date: datetime.date,
) -> list[tuple[int, datetime.date]]:
    """Return [(account_id, as_of_date)] from position_var within the date range."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            if account_id:
                cur.execute(
                    """
                    SELECT DISTINCT account_id, as_of_date
                    FROM position_var
                    WHERE account_id = %s AND as_of_date BETWEEN %s AND %s
                    ORDER BY account_id, as_of_date
                    """,
                    (account_id, from_date, to_date),
                )
            else:
                cur.execute(
                    """
                    SELECT DISTINCT account_id, as_of_date
                    FROM position_var
                    WHERE as_of_date BETWEEN %s AND %s
                    ORDER BY account_id, as_of_date
                    """,
                    (from_date, to_date),
                )
            return [(row[0], row[1]) for row in cur.fetchall()]


def _already_exists(account_id: int, as_of_date: datetime.date) -> bool:
    """Return True if db_portfolio_breakdown already has rows for (account_id, as_of_date)."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM db_portfolio_breakdown WHERE account_id = %s AND as_of_date = %s LIMIT 1",
                (account_id, as_of_date),
            )
            return cur.fetchone() is not None


def backfill_breakdowns(
    from_date: datetime.date,
    to_date: datetime.date,
    account_id: int | None = None,
    overwrite: bool = False,
) -> None:
    pairs = _get_dates_to_backfill(account_id, from_date, to_date)
    logger.info(
        f"Found {len(pairs)} (account, date) pairs in position_var "
        f"between {from_date} and {to_date}."
    )

    skipped = 0
    processed = 0
    errors = 0

    for acct, as_of in pairs:
        if not overwrite and _already_exists(acct, as_of):
            logger.debug(f"Skipping account_id={acct} {as_of} — already exists.")
            skipped += 1
            continue

        try:
            with pg_connection() as conn:
                df = get_positions_on_date(conn, as_of, acct)

            if df.empty:
                logger.warning(f"No position rows for account_id={acct} {as_of} — skipping.")
                skipped += 1
                continue

            rows = compute_breakdowns(acct, as_of, df=df)
            delete_breakdowns(acct, as_of)
            write_breakdowns(acct, as_of, rows)
            logger.info(f"  account_id={acct}  {as_of}  wrote {len(rows)} rows")
            processed += 1

        except Exception as exc:
            logger.error(f"  account_id={acct}  {as_of}  ERROR: {exc}")
            errors += 1

    logger.info(
        f"Backfill complete — processed: {processed}, skipped: {skipped}, errors: {errors}"
    )


if __name__ == "__main__":
    def _parse_date(s: str) -> datetime.date:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()

    parser = argparse.ArgumentParser(
        description="Backfill db_portfolio_breakdown from position_var data."
    )
    parser.add_argument("--account-id", dest="account_id", type=int, default=None,
                        help="Limit to a single account (default: all accounts)")
    parser.add_argument("--from-date",  dest="from_date",  type=_parse_date, required=True,
                        help="First date to backfill (YYYY-MM-DD)")
    parser.add_argument("--to-date",    dest="to_date",    type=_parse_date,
                        default=datetime.date.today(),
                        help="Last date to backfill (YYYY-MM-DD, default: today)")
    parser.add_argument("--overwrite",  action="store_true", default=False,
                        help="Overwrite existing rows (default: skip dates already present)")
    args = parser.parse_args()

    backfill_breakdowns(
        from_date  = args.from_date,
        to_date    = args.to_date,
        account_id = args.account_id,
        overwrite  = args.overwrite,
    )
