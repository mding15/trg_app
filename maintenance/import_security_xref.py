"""
maintenance/import_security_xref.py

Insert or update rows in security_xref from maintenance/Excel/security_xref.xlsx.

- Sheet: security_xref
- Identifier columns (ISIN, CUSIP, Ticker, YH, YF_ID, BB_UNIQUE, BB_GLOBAL) are
  unpivoted into (REF_TYPE, REF_ID) rows.
- CUSIP_ prefix is stripped before inserting.
- Existing (REF_ID, REF_TYPE) rows are updated; only non-null Excel fields are written.
- New rows are inserted; DateAdded defaults to today.

Usage:
  python import_security_xref.py
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from database import pg_connection

EXCEL_PATH = os.path.join(os.path.dirname(__file__), 'Excel', 'security_xref.xlsx')
SHEET_NAME = 'security_xref'
ID_COLUMNS = ['ISIN', 'CUSIP', 'Ticker', 'YH', 'YF_ID', 'BB_UNIQUE', 'BB_GLOBAL']


def load_excel():
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME, dtype=str)
    keep = ['SecurityID', 'DataSource'] + [c for c in ID_COLUMNS if c in df.columns]
    return df[keep]


def unpivot(df):
    melted = df.melt(
        id_vars=['SecurityID', 'DataSource'],
        value_vars=[c for c in ID_COLUMNS if c in df.columns],
        var_name='REF_TYPE',
        value_name='REF_ID',
    )
    melted = melted[melted['REF_ID'].notna() & (melted['REF_ID'].str.strip() != '')]
    melted['REF_ID'] = melted['REF_ID'].str.strip()
    return melted.reset_index(drop=True)


def strip_cusip_prefix(df):
    mask = (df['REF_TYPE'] == 'CUSIP') & df['REF_ID'].str.startswith('CUSIP_')
    df.loc[mask, 'REF_ID'] = df.loc[mask, 'REF_ID'].str.removeprefix('CUSIP_')
    return df


def load_existing(pairs):
    if not pairs:
        return pd.DataFrame(columns=['id', 'REF_ID', 'REF_TYPE', 'SecurityID', 'DataSource'])
    placeholders = ', '.join(['(%s, %s)'] * len(pairs))
    flat = [v for pair in pairs for v in pair]
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT id, "REF_ID", "REF_TYPE", "SecurityID", "DataSource" '
                f'FROM security_xref WHERE ("REF_ID", "REF_TYPE") IN ({placeholders})',
                flat,
            )
            rows = cur.fetchall()
    existing = pd.DataFrame(rows, columns=['id', 'REF_ID', 'REF_TYPE', 'SecurityID', 'DataSource'])
    # keep first match per (REF_ID, REF_TYPE) in case of existing duplicates
    return existing.drop_duplicates(subset=['REF_ID', 'REF_TYPE'], keep='first')


def do_inserts(rows):
    if rows.empty:
        return 0
    records = rows[['REF_ID', 'REF_TYPE', 'SecurityID', 'DataSource']].to_dict('records')
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                'INSERT INTO security_xref ("REF_ID", "REF_TYPE", "SecurityID", "DataSource") '
                'VALUES (%s, %s, %s, %s)',
                [(r['REF_ID'], r['REF_TYPE'],
                  r['SecurityID'] if pd.notna(r['SecurityID']) else None,
                  r['DataSource'] if pd.notna(r['DataSource']) else None)
                 for r in records],
            )
        conn.commit()
    return len(records)


def do_updates(upd_rows):
    updated = 0
    with pg_connection() as conn:
        with conn.cursor() as cur:
            for _, row in upd_rows.iterrows():
                fields, values = [], []
                if pd.notna(row['SecurityID']) and str(row['SecurityID']).strip():
                    fields.append('"SecurityID" = %s')
                    values.append(row['SecurityID'])
                if pd.notna(row['DataSource']) and str(row['DataSource']).strip():
                    fields.append('"DataSource" = %s')
                    values.append(row['DataSource'])
                if not fields:
                    continue
                values.append(int(row['id']))
                cur.execute(
                    f'UPDATE security_xref SET {", ".join(fields)} WHERE id = %s',
                    values,
                )
                updated += cur.rowcount
        conn.commit()
    return updated


def main():
    print(f'Reading {EXCEL_PATH} ...')
    raw = load_excel()
    print(f'  {len(raw)} securities read')

    data = unpivot(raw)
    data = strip_cusip_prefix(data)
    print(f'  {len(data)} identifier rows after unpivot')

    pairs = list(zip(data['REF_ID'], data['REF_TYPE']))
    existing = load_existing(pairs)

    exist_keys = set(zip(existing['REF_ID'], existing['REF_TYPE']))
    is_new = ~data.apply(lambda r: (r['REF_ID'], r['REF_TYPE']) in exist_keys, axis=1)

    new_rows = data[is_new].copy()
    upd_rows = data[~is_new].copy()

    exist_id_map = existing.set_index(['REF_ID', 'REF_TYPE'])['id'].to_dict()
    upd_rows['id'] = upd_rows.apply(
        lambda r: exist_id_map[(r['REF_ID'], r['REF_TYPE'])], axis=1
    )

    inserted = do_inserts(new_rows)
    updated  = do_updates(upd_rows)

    print(f'\nDone: {inserted} inserted, {updated} updated.')


if __name__ == '__main__':
    main()
