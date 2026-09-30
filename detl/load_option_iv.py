"""
load_option_iv.py — Daily load of option implied vols into the option_iv table.

There is no market data source yet, so every point is filled forward:
  1. as_of_date = command-line arg, else proc_asof_date.
  2. Source date = the most recent as_of_date before it that has option_iv
     rows (bridges holidays and missed runs).
  3. Every (underlying_sec_id, tenor_years, moneyness) point on the source
     date that is NOT already on as_of_date is copied forward with the same
     iv, and source = 'fill_forward'. underlying_price is refreshed to the
     latest current_price close on/before as_of_date (previous value kept if
     there is none), so moneyness K/S stays tied to the current spot.

Points already on as_of_date (e.g. uploaded with maintenance/upload_option_iv.py)
are never overwritten, so re-runs are safe no-ops. When a real data source is
added, load its rows first; the fill-forward step then only fills whatever
points the source didn't provide.

Fails (exit 1) if there is no earlier date to copy from and as_of_date has no
rows either.

Table: see database2/tables.sql (option_iv)

Usage:
    python detl/load_option_iv.py                  # date from proc_asof_date
    python detl/load_option_iv.py 2026-09-30       # override as_of_date
    python detl/load_option_iv.py --dry-run        # show what would be copied
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection, get_proc_asof_date
from mkt_data.price_timeseries import get_current_price

SOURCE = 'fill_forward'

_KEY_COLS = ['underlying_sec_id', 'tenor_years', 'moneyness']
_COPY_COLS = ['underlying_sec_id', 'underlying', 'tenor_years', 'moneyness', 'iv', 'underlying_price']


def _prior_date(cur, as_of_date: date) -> date | None:
    cur.execute('SELECT max(as_of_date) FROM option_iv WHERE as_of_date < %s', (as_of_date,))
    return cur.fetchone()[0]


def _count_rows(cur, as_of_date: date) -> int:
    cur.execute('SELECT count(*) FROM option_iv WHERE as_of_date = %s', (as_of_date,))
    return cur.fetchone()[0]


def _missing_points(cur, src_date: date, as_of_date: date) -> pd.DataFrame:
    """Points on src_date with no row on as_of_date."""
    cur.execute(
        f"""
        SELECT {', '.join('s.' + c for c in _COPY_COLS)}
        FROM option_iv s
        WHERE s.as_of_date = %s
          AND NOT EXISTS (
              SELECT 1 FROM option_iv t
              WHERE t.as_of_date = %s
                AND {' AND '.join(f't.{c} = s.{c}' for c in _KEY_COLS)}
          )
        ORDER BY {', '.join('s.' + c for c in _KEY_COLS)}
        """,
        (src_date, as_of_date),
    )
    return pd.DataFrame(cur.fetchall(), columns=_COPY_COLS)


def _refresh_prices(df: pd.DataFrame, as_of_date: date) -> pd.DataFrame:
    """Set underlying_price to the latest close on/before as_of_date; keep the
    previous value where there is none."""
    prices = get_current_price(df['underlying_sec_id'].unique().tolist(), as_of_date)
    df = df.copy()
    df['underlying_price'] = [
        prices[s][0] if s in prices else p
        for s, p in zip(df['underlying_sec_id'], df['underlying_price'])
    ]

    no_price = sorted(set(df['underlying_sec_id']) - set(prices))
    stale = sorted(f'{s} ({d})' for s, (_, d) in prices.items() if d < as_of_date)
    if no_price:
        print(f'  No current_price for {", ".join(no_price)} - kept previous underlying_price')
    if stale:
        print(f'  Latest price is before {as_of_date} for: {", ".join(stale)}')
    return df


def _insert(cur, df: pd.DataFrame, as_of_date: date) -> int:
    cols = ['as_of_date'] + _COPY_COLS + ['source']
    rows = [(as_of_date, *r, SOURCE) for r in df[_COPY_COLS].itertuples(index=False)]
    inserted = execute_values(
        cur,
        f"""
        INSERT INTO option_iv ({', '.join(f'"{c}"' for c in cols)}) VALUES %s
        ON CONFLICT (as_of_date, {', '.join(_KEY_COLS)}) DO NOTHING
        RETURNING 1
        """,
        rows,
        fetch=True,
    )
    return len(inserted)


def run(as_of_date: date | None = None, dry_run: bool = False) -> int:
    """Fill forward option_iv for as_of_date. Returns the number of rows inserted."""
    if as_of_date is None:
        as_of_date = date.fromisoformat(get_proc_asof_date())
    print(f'as_of_date: {as_of_date}')

    with pg_connection() as conn:
        with conn.cursor() as cur:
            n_existing = _count_rows(cur, as_of_date)
            src_date = _prior_date(cur, as_of_date)
            print(f'  {n_existing} point(s) already on {as_of_date}')

            if src_date is None:
                if n_existing:
                    print('No earlier date in option_iv - nothing to fill forward.')
                    return 0
                raise SystemExit(f'ERROR: option_iv has no rows on or before {as_of_date} - nothing to fill forward from')

            missing = _missing_points(cur, src_date, as_of_date)
            print(f'  Fill forward from {src_date}: {len(missing)} missing point(s)')
            if missing.empty:
                return 0

            missing = _refresh_prices(missing, as_of_date)
            for sec_id, g in missing.groupby('underlying_sec_id'):
                name = g['underlying'].dropna().iloc[0] if g['underlying'].notna().any() else ''
                print(f'    {sec_id:<12} {name:<8} {len(g):>4} point(s)  underlying_price {g["underlying_price"].iloc[0]}')

            if dry_run:
                print('DRY RUN - nothing written')
                return 0
            n = _insert(cur, missing, as_of_date)
        conn.commit()

    print(f'Inserted {n} row(s) into option_iv for {as_of_date} (source={SOURCE})')
    return n


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load option implied vols into option_iv (fill forward).')
    parser.add_argument('asof_date', nargs='?', default=None, type=date.fromisoformat,
                        help='Override as-of date (YYYY-MM-DD); default: proc_asof_date')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be copied without writing')
    args = parser.parse_args()

    run(as_of_date=args.asof_date, dry_run=args.dry_run)
