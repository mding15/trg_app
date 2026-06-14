"""
maintenance/prefix_cusip.py

Add or remove the "CUSIP_" prefix on all values in the "CUSIP" column of a CSV file.
Skips blank/null values. Overwrites the file in place.

Usage:
  python prefix_cusip.py <path_to_csv> --add     # CUSIP_ prefix added
  python prefix_cusip.py <path_to_csv> --remove  # CUSIP_ prefix stripped
"""

import argparse
import sys
import pandas as pd

PREFIX = 'CUSIP_'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('file', help='Path to CSV file')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--add',    action='store_true', help='Add CUSIP_ prefix')
    group.add_argument('--remove', action='store_true', help='Remove CUSIP_ prefix')
    args = parser.parse_args()

    df = pd.read_csv(args.file, dtype=str)

    if 'CUSIP' not in df.columns:
        print(f'Error: no "CUSIP" column found in {args.file}')
        print(f'Columns: {list(df.columns)}')
        sys.exit(1)

    mask = df['CUSIP'].notna() & (df['CUSIP'].str.strip() != '')

    if args.add:
        eligible = mask & ~df['CUSIP'].str.startswith(PREFIX)
        df.loc[eligible, 'CUSIP'] = PREFIX + df.loc[eligible, 'CUSIP']
        print(f'Added prefix to {eligible.sum()} CUSIP values in {args.file}')
    else:
        eligible = mask & df['CUSIP'].str.startswith(PREFIX)
        df.loc[eligible, 'CUSIP'] = df.loc[eligible, 'CUSIP'].str.removeprefix(PREFIX)
        print(f'Removed prefix from {eligible.sum()} CUSIP values in {args.file}')

    df.to_csv(args.file, index=False)


if __name__ == '__main__':
    main()
