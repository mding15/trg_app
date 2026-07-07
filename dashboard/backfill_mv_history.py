"""
backfill_mv_history.py — Backfill db_mv_history with a year-end snapshot.

For each target account: if no rows exist before the current year's start,
copy all rows from the earliest available date to (first_of_year - 1 day).

Usage:
    python backfill_mv_history.py [--account-id N] [--dry-run]
"""
from __future__ import annotations

import argparse
import sys
import os
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from database2 import pg_connection


def get_target_accounts(cur, account_id: int | None) -> list[int]:
    if account_id is not None:
        return [account_id]
    cur.execute("SELECT DISTINCT account_id FROM db_mv_history ORDER BY account_id")
    return [row[0] for row in cur.fetchall()]


def backfill_account(cur, account_id: int, first_of_year: date, year_end: date, dry_run: bool) -> int:
    """Returns number of rows inserted (or would-be inserted in dry-run)."""

    # Skip if pre-year data already exists
    cur.execute(
        "SELECT 1 FROM db_mv_history WHERE account_id = %s AND as_of_date < %s LIMIT 1",
        (account_id, first_of_year),
    )
    if cur.fetchone():
        print(f"  account {account_id}: already has data before {first_of_year} — skipped")
        return 0

    # Find earliest date
    cur.execute(
        "SELECT MIN(as_of_date) FROM db_mv_history WHERE account_id = %s",
        (account_id,),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        print(f"  account {account_id}: no rows found — skipped")
        return 0

    earliest_date = row[0]

    # Fetch all rows at earliest date
    cur.execute(
        """
        SELECT security_id, broker, broker_account, market_value
        FROM db_mv_history
        WHERE account_id = %s AND as_of_date = %s
        """,
        (account_id, earliest_date),
    )
    source_rows = cur.fetchall()

    if not source_rows:
        print(f"  account {account_id}: no rows at earliest date {earliest_date} — skipped")
        return 0

    print(f"  account {account_id}: earliest date={earliest_date}, "
          f"{len(source_rows)} row(s) -> copy to {year_end}"
          + (" [DRY RUN]" if dry_run else ""))

    if dry_run:
        for r in source_rows:
            print(f"    would insert: security_id={r[0]!r}, broker={r[1]!r}, "
                  f"broker_account={r[2]!r}, market_value={r[3]}")
        return len(source_rows)

    inserted = 0
    for security_id, broker, broker_account, market_value in source_rows:
        cur.execute(
            """
            INSERT INTO db_mv_history (account_id, as_of_date, security_id, broker, broker_account, market_value)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (account_id, year_end, security_id, broker, broker_account, market_value),
        )
        inserted += cur.rowcount

    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill db_mv_history with year-end snapshot.")
    parser.add_argument("--account-id", type=int, default=None, help="Single account to backfill")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without writing to DB")
    args = parser.parse_args()

    today         = date.today()
    first_of_year = date(today.year, 1, 1)
    year_end      = first_of_year - timedelta(days=1)

    print(f"Backfill target date: {year_end}  (first_of_year={first_of_year})")
    if args.dry_run:
        print("*** DRY RUN — no changes will be written ***")

    total_inserted = 0

    with pg_connection() as conn:
        with conn.cursor() as cur:
            accounts = get_target_accounts(cur, args.account_id)
            print(f"Accounts to process: {accounts}\n")

            for account_id in accounts:
                n = backfill_account(cur, account_id, first_of_year, year_end, args.dry_run)
                total_inserted += n

        if not args.dry_run:
            conn.commit()

    print(f"\nDone. Rows {'that would be ' if args.dry_run else ''}inserted: {total_inserted}")


if __name__ == "__main__":
    main()
