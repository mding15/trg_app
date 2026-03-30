"""
process_scheduler/calculate_var.py

Scheduler entry point for VaR calculation.
Reads as_of_date from proc_asof_date table and calls calculate_var(feed_source='mssb', as_of_date).

Usage:
  python calculate_var.py              # run normally
  python calculate_var.py --register   # register/update job in scheduler
"""

import os
import sys

# Allow imports from trg_app root and process2/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from database2 import pg_connection
from process2.calculate_var import calculate_var


def _get_proc_asof_date() -> str:
    """Return as_of_date from proc_asof_date table as 'YYYY-MM-DD'. Raises if missing."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT as_of_date FROM proc_asof_date LIMIT 1')
            row = cur.fetchone()
            if not row or row[0] is None:
                raise RuntimeError('proc_asof_date table is empty — cannot determine as_of_date')
            return row[0].isoformat() if hasattr(row[0], 'isoformat') else str(row[0])


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--register':
        from register import register_by_id
        register_by_id('calculate_var')
    else:
        as_of_date = _get_proc_asof_date()
        calculate_var(feed_source='mssb', as_of_date=as_of_date)
