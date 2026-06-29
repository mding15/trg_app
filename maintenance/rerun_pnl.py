"""
rerun_pnl.py — Re-run all P&L calculation scripts for a given date.

Runs the following process2 scripts in order:
    1. calc_linear_product_pnl
    2. calc_treasury_pnl
    3. calc_bond_pnl
    4. calc_options_pnl
    5. calc_vix_pnl
    6. calc_structured_note_pnl
    7. calc_unadj_alt_pnl
    8. calc_stress_test_pnl

Each step runs independently — a failure is printed and execution continues.

Usage:
    python maintenance/rerun_pnl.py                  # date from proc_asof_date
    python maintenance/rerun_pnl.py --date 2026-06-27
"""
from __future__ import annotations

import argparse
import sys
import traceback
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from process2.calc_linear_product_pnl import calc_linear_product_pnl
from process2.calc_treasury_pnl import calc_treasury_pnl
from process2.calc_bond_pnl import calc_bond_pnl
from process2.calc_derivative_pnl import calc_derivative_pnl
from process2.calc_unadj_alt_pnl import calc_unadj_alt_pnl
from process2.calc_stress_test_pnl import run as calc_stress_test_pnl


STEPS = [
    ('calc_linear_product_pnl', lambda d: calc_linear_product_pnl(d)),
    ('calc_treasury_pnl',       lambda d: calc_treasury_pnl(d)),
    ('calc_bond_pnl',           lambda d: calc_bond_pnl(d)),
    ('calc_derivative_pnl',     lambda d: calc_derivative_pnl(d)),
    ('calc_unadj_alt_pnl',      lambda d: calc_unadj_alt_pnl()),
    ('calc_stress_test_pnl',    lambda d: calc_stress_test_pnl()),
]


def rerun_pnl(as_of_date: date = None) -> None:
    failed = []
    for i, (name, fn) in enumerate(STEPS, 1):
        print(f'\n[{i}/{len(STEPS)}] {name}')
        try:
            fn(as_of_date)
        except Exception:
            print(f'ERROR in {name}:')
            traceback.print_exc()
            failed.append(name)

    print(f'\n{"─" * 50}')
    if failed:
        print(f'Completed with errors in: {", ".join(failed)}')
    else:
        print('All steps completed successfully.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Re-run all P&L calculation scripts.')
    parser.add_argument(
        '--date', '-d', metavar='YYYY-MM-DD',
        help='As-of date (default: read from proc_asof_date table)',
    )
    args = parser.parse_args()

    asof = datetime.strptime(args.date, '%Y-%m-%d').date() if args.date else None
    rerun_pnl(asof)
