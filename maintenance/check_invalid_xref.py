"""
maintenance/check_invalid_xref.py

Find all invalid ISIN and CUSIP values in security_xref and report
the corresponding security_info rows.

Output is printed to console and saved to data/maintenance/CSV/invalid_xref_<date>.csv.

Usage:
  python check_invalid_xref.py
"""

import os
import sys
from datetime import date

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database import db_utils
from utils.security_util import cusip_is_valid, isin_is_valid
from _paths import CSV_DIR

VALIDATORS = {
    'ISIN':  isin_is_valid,
    'CUSIP': cusip_is_valid,
}


def load_xref():
    types = ', '.join(f"'{t}'" for t in VALIDATORS)
    return db_utils.get_sql_df(
        f'SELECT "REF_TYPE", "REF_ID", "SecurityID" FROM security_xref '
        f'WHERE "REF_TYPE" IN ({types})'
    )


def load_security_info(security_ids):
    ids = ', '.join(f"'{sid}'" for sid in security_ids)
    return db_utils.get_sql_df(
        f'SELECT "SecurityID", "SecurityName", "AssetClass", "AssetType", "Currency", "DataSource" '
        f'FROM security_info WHERE "SecurityID" IN ({ids})'
    )


def find_invalid(xref_df):
    results = []
    for _, row in xref_df.iterrows():
        ref_type = row['REF_TYPE']
        ref_id   = str(row['REF_ID']).strip() if row['REF_ID'] else ''
        validate = VALIDATORS[ref_type]
        if not ref_id or not validate(ref_id):
            results.append({
                'REF_TYPE':   ref_type,
                'REF_ID':     ref_id,
                'SecurityID': row['SecurityID'],
            })
    return results


def main():

    print('Loading security_xref ...')
    xref_df = load_xref()
    print(f'  {len(xref_df)} ISIN/CUSIP rows found')

    invalid = find_invalid(xref_df)
    print(f'  {len(invalid)} invalid entries\n')

    if not invalid:
        print('No invalid ISIN or CUSIP values found.')
        return

    # Enrich with security_info
    security_ids = list({r['SecurityID'] for r in invalid})
    info_df = load_security_info(security_ids)
    info_map = info_df.set_index('SecurityID').to_dict('index')

    # Build report rows
    import pandas as pd
    rows = []
    for r in invalid:
        info = info_map.get(r['SecurityID'], {})
        rows.append({
            'REF_TYPE':    r['REF_TYPE'],
            'REF_ID':      r['REF_ID'],
            'SecurityID':  r['SecurityID'],
            'SecurityName': info.get('SecurityName', ''),
            'AssetClass':  info.get('AssetClass', ''),
            'AssetType':   info.get('AssetType', ''),
            'Currency':    info.get('Currency', ''),
            'DataSource':  info.get('DataSource', ''),
        })

    report = pd.DataFrame(rows)

    # Print summary by type
    for ref_type, group in report.groupby('REF_TYPE'):
        print(f'── Invalid {ref_type} ({len(group)}) ──────────────────────────')
        print(group[['REF_ID', 'SecurityID', 'SecurityName', 'AssetClass']].to_string(index=False))
        print()

    os.makedirs(CSV_DIR, exist_ok=True)
    out_file = os.path.join(CSV_DIR, f'invalid_xref_{date.today().strftime("%Y%m%d")}.csv')
    report.to_csv(out_file, index=False)
    print(f'Saved: {out_file}')


if __name__ == '__main__':
    main()
