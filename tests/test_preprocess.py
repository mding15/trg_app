import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from trg_config import config
from preprocess.read_portfolio import read_input_file

_DEFAULT_FILE = config['TEST_DIR'] / 'clients' / 'Test1.xlsx'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Test read_input_file() in preprocess/read_portfolio.py.')
    parser.add_argument('--file', type=Path, default=_DEFAULT_FILE, metavar='FILE')
    args = parser.parse_args()

    if not args.file.exists():
        print(f'Error: file not found: {args.file}')
        sys.exit(1)

    print(f'Reading: {args.file}\n')
    try:
        params, positions, limit = read_input_file(args.file)
    except Exception as e:
        print(f'Error: {e}')
        sys.exit(1)

    print('--- params ---')
    for k, v in params.items():
        print(f'  {k}: {v}')

    print(f'\n--- positions ({len(positions)} rows x {len(positions.columns)} cols) ---')
    print(f'  columns: {list(positions.columns)}')
    print(positions.to_string(index=False, max_rows=10))

    print('\n--- limit ---')
    for k, v in limit.items():
        print(f'  {k}: {v}')
