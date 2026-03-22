"""
run_mssb_process.py — Run the MSSB process2 pipeline for a given as_of_date.

Steps:
    1. process_mssb_positions: process raw mssb_posit feed into proc_positions.
    2. calculate_var: run VaR for feed_source='mssb' (auto-detects latest as_of_date).

Usage:
    python run_mssb_process.py 2026-03-04
"""
from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from process2.process_mssb_positions import process_mssb_positions
from process2.calculate_var import calculate_var


def main() -> None:
    if len(sys.argv) < 2:
        print('Usage: python run_mssb_process.py <as_of_date>')
        print('Example: python run_mssb_process.py 2026-03-04')
        sys.exit(1)

    as_of_date = sys.argv[1]

    print(f'=== Step 1: process_mssb_positions  as_of_date={as_of_date} ===')
    process_mssb_positions(as_of_date)

    print()
    print(f'=== Step 2: calculate_var  feed_source=mssb  as_of_date={as_of_date} ===')
    calculate_var('mssb', as_of_date)

    print()
    print(f'=== process2 pipeline completed for as_of_date={as_of_date} ===')


if __name__ == '__main__':
    main()
