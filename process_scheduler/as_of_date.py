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
import json
import urllib.request
import urllib.error
from datetime import date, timedelta

import pytz
from datetime import datetime

# Allow running from process_scheduler/ or from trg_app root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from database2 import pg_connection

SCHEDULER_URL   = 'https://dev2.tailriskglobal.com/scheduler'
PROCESS_ID      = 'as_of_date'
SCRIPT_PATH     = '/home/ec2-user/api/trgapp/process_scheduler/as_of_date.py'

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


def register_job() -> None:
    """Register or update this process in the scheduler via PUT."""
    payload = json.dumps({
        'name':                 'As-Of Date',
        'description':          'Calculates and persists the current as_of_date.',
        'script_path':          SCRIPT_PATH,
        'script_type':          'python',
        'schedule_time':        None,
        'dependencies':         [],
        'max_retries':          2,
        'retry_delay_seconds':  60,
        'enabled':              True,
        'venv_path':            None,
    }).encode()

    url = f'{SCHEDULER_URL}/api/processes/{PROCESS_ID}'
    req = urllib.request.Request(
        url, data=payload, method='PUT',
        headers={'Content-Type': 'application/json'},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            body = json.loads(resp.read())
            print(f"Registered: {body.get('id')} — {body.get('name')}")
    except urllib.error.HTTPError as e:
        # 404 means it doesn't exist yet — fall back to POST
        if e.code == 404:
            url = f'{SCHEDULER_URL}/api/processes'
            payload_create = json.dumps({
                'id': PROCESS_ID,
                'name':                 'As-Of Date',
                'description':          'Calculates and persists the current as_of_date.',
                'script_path':          SCRIPT_PATH,
                'script_type':          'python',
                'schedule_time':        None,
                'dependencies':         [],
                'max_retries':          2,
                'retry_delay_seconds':  60,
                'enabled':              True,
                'venv_path':            None,
            }).encode()
            req2 = urllib.request.Request(
                url, data=payload_create, method='POST',
                headers={'Content-Type': 'application/json'},
            )
            with urllib.request.urlopen(req2) as resp:
                body = json.loads(resp.read())
                print(f"Created: {body.get('id')} — {body.get('name')}")
        else:
            print(f"Error {e.code}: {e.read().decode()}")
            raise


def main():
    as_of_date = get_as_of_date()
    update_as_of_date(as_of_date)
    print(f"as_of_date set to {as_of_date}")


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--register':
        register_job()
    else:
        main()
