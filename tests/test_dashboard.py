import os
import sys
import pprint
from pathlib import Path

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database2 import pg_connection, get_proc_asof_date
from dashboard.positions_calc import get_positions_on_date, compute_positions

ACCOUNT_ID = 1003
OUTPUT_DIR = Path(__file__).parent / 'output'

if __name__ == '__main__':
    as_of_date = get_proc_asof_date()
    print(f"Calling compute_positions(account_id={ACCOUNT_ID}, as_of_date={as_of_date})...\n")
    try:
        with pg_connection() as conn:
            df = get_positions_on_date(conn, as_of_date, ACCOUNT_ID)

        if df.empty:
            print(f"No positions found for account_id={ACCOUNT_ID}.")
            sys.exit(0)

        positions = compute_positions(account_id=ACCOUNT_ID, as_of_date=as_of_date, df=df)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

    if not positions:
        print(f"No positions returned for account_id={ACCOUNT_ID}.")
        sys.exit(0)

    print(f"Returned {len(positions)} position(s):\n")
    #pprint.pprint(positions)

    df_out = pd.DataFrame(positions)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUTPUT_DIR / f'positions_{ACCOUNT_ID}.csv'
    df_out.to_csv(csv_path, index=False)
    print(f"\nCSV written to: {csv_path}")
