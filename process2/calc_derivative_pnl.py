"""
calc_derivative_pnl.py — Run all derivative P&L calculations in sequence.

Calls the following scripts in order, stopping immediately if any fails:
    1. calc_options_pnl       — equity options (option_class = 'Equity')
    2. calc_vix_pnl           — VIX options    (option_class = 'VIX')
    3. calc_structured_note_pnl — structured notes

Usage:
    python calc_derivative_pnl.py                     # date from proc_asof_date
    python calc_derivative_pnl.py --date 2026-06-27   # specific date
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from process2.calc_options_pnl import calc_options_pnl
from process2.calc_vix_pnl import calc_vix_pnl
from process2.calc_structured_note_pnl import calc_structured_note_pnl


STEPS = [
    ('calc_options_pnl',         calc_options_pnl),
    ('calc_vix_pnl',             calc_vix_pnl),
    ('calc_structured_note_pnl', calc_structured_note_pnl),
]


def calc_derivative_pnl(as_of_date: date = None) -> None:
    for i, (name, fn) in enumerate(STEPS, 1):
        print(f'\n[{i}/{len(STEPS)}] {name}')
        fn(as_of_date)
    print('\nAll derivative P&L steps completed.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run all derivative P&L calculations.')
    parser.add_argument(
        '--date', '-d', metavar='YYYY-MM-DD',
        help='As-of date (default: read from proc_asof_date table)',
    )
    args = parser.parse_args()

    asof = datetime.strptime(args.date, '%Y-%m-%d').date() if args.date else None
    calc_derivative_pnl(asof)
