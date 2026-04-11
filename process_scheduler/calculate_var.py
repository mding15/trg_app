"""
process_scheduler/calculate_var.py

Scheduler entry point for VaR calculation.
Reads as_of_date from proc_asof_date table and calls calculate_var(feed_source=None, as_of_date=as_of_date).

Usage:
  python calculate_var.py              # run normally
  python calculate_var.py --register   # register/update job in scheduler
"""

import os
import sys

# Allow imports from trg_app root and process2/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from database2 import get_proc_asof_date
from process2.calculate_var import calculate_var


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--register':
        from register import register_by_id
        register_by_id('calculate_var')
    else:
        as_of_date = get_proc_asof_date()
        calculate_var(feed_source=None, as_of_date=as_of_date)
