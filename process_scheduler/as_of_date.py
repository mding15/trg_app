# -*- coding: utf-8 -*-
"""
process_scheduler/as_of_date.py

Calculates and persists the current as_of_date into proc_asof_date.

Rules (America/New_York time):
  - Weekday (Mon–Fri) → as_of_date = today
  - Weekend (Sat–Sun) → as_of_date = most recent Friday

The table always holds exactly one row, updated in place each run.

Usage:
  python as_of_date.py              # run normally
  python as_of_date.py --register   # register/update job in scheduler
"""

import sys
import os
from datetime import date, timedelta

import pytz
from datetime import datetime

# Allow running from process_scheduler/ or from trg_app root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from database2 import pg_connection

NY_TZ = pytz.timezone('America/New_York')


def get_as_of_date() -> date:
    """Return today's date (NY time), rolling back to Friday on weekends."""
    today = datetime.now(NY_TZ).date()
    weekday = today.weekday()   # 0=Mon … 4=Fri, 5=Sat, 6=Sun
    if weekday == 5:            # Saturday → back 1 day
        return today - timedelta(days=1)
    if weekday == 6:            # Sunday → back 2 days
        return today - timedelta(days=2)
    return today


def update_as_of_date(as_of_date: date) -> None:
    """Upsert the single row in proc_asof_date."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE proc_asof_date
                SET as_of_date = %s, updated_at = NOW()
            """, (as_of_date,))
            if cur.rowcount == 0:
                # Table is empty on first run — insert the row
                cur.execute("""
                    INSERT INTO proc_asof_date (as_of_date, updated_at)
                    VALUES (%s, NOW())
                """, (as_of_date,))
        conn.commit()


def main():
    as_of_date = get_as_of_date()
    update_as_of_date(as_of_date)
    print(f"as_of_date set to {as_of_date}")


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--register':
        from register import register_by_id
        register_by_id('as_of_date')
    else:
        main()
