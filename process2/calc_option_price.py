"""
calc_option_price.py — Daily prices for all option securities (equity and VIX)
held in proc_positions, saved to the option_price table.

Steps:
    1. Option securities: proc_positions joined to security_info with
       AssetClass='Derivative', AssetType='Option'.
    2. MSSB prices: for securities fed from MSSB (feed_source='mssb',
       asset_class='OP'), price = sum(|market_value|) / sum(|quantity|) * 0.01
       (MSSB market value is per contract; x0.01 gives per share; absolute
       values so short positions give a positive price).
       Only securities from step 1 are kept.
    3. Model prices for step-1 securities without an MSSB price — Black-Scholes
       (engine/eq_option_var.py::calc_price) with:
         - contract terms from option_info (latest row per security_id)
         - underlying price: latest current_price close on/before as_of_date
           (VIX options use VIX spot, as in calc_vix_pnl.py)
         - tenor = calendar days to maturity / 365; rate from models/ust_curve.py
         - iv from option_iv for the underlying, latest as_of_date on/before
           as_of_date, interpolated linearly in moneyness (K/S) within each
           tenor, then in tenor; flat beyond the grid edges
       Options that can't be priced (no option_info, no underlying price, no
       option_iv, no rate, matured) are skipped with a warning — the job still
       succeeds.
    4. Replace the as_of_date rows in option_price (delete + insert, one
       transaction), so re-runs are safe and dropped securities don't linger.

Prices are per share (no x100 contract multiplier — codebase convention).

Usage:
    python calc_option_price.py                        # date from proc_asof_date
    python calc_option_price.py --date 2026-09-28      # specific date
    python calc_option_price.py --date 2026-09-28 --dry-run
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values

from database2 import pg_connection, get_proc_asof_date
from mkt_data.price_timeseries import get_current_price
from engine import eq_option_var as opt
from models.ust_curve import get_rate

_TABLE_COLS = [
    'as_of_date', 'security_id', 'price', 'price_source',
    'option_type', 'strike', 'maturity', 'underlying_sec_id', 'underlying_price',
    'moneyness', 'tenor_years', 'iv', 'iv_date', 'risk_free_rate',
]


# ── Queries ───────────────────────────────────────────────────────────────────

def _get_option_securities(cur, as_of_date) -> pd.DataFrame:
    """Step 1: option securities in proc_positions."""
    cur.execute(
        """
        SELECT DISTINCT si."SecurityID", si."SecurityName"
        FROM proc_positions pp
        JOIN security_info si ON pp.security_id = si."SecurityID"
        WHERE pp.as_of_date = %s
          AND si."AssetClass" = 'Derivative'
          AND si."AssetType" = 'Option'
        """,
        (as_of_date,),
    )
    return pd.DataFrame(cur.fetchall(), columns=['security_id', 'security_name'])


def _get_mssb_prices(cur, as_of_date) -> pd.Series:
    """Step 2: per-share prices implied from MSSB market values, by security_id."""
    cur.execute(
        """
        SELECT pp.security_id,
               SUM(ABS(pp.market_value)) / NULLIF(SUM(ABS(pp.quantity)), 0) * 0.01 AS price
        FROM proc_positions pp
        WHERE pp.as_of_date = %s
          AND pp.feed_source = 'mssb'
          AND pp.asset_class = 'OP'
        GROUP BY pp.security_id
        """,
        (as_of_date,),
    )
    return pd.Series({sid: p for sid, p in cur.fetchall()}, dtype=float)


def _get_option_info(cur, security_ids: list[str]) -> pd.DataFrame:
    cur.execute(
        """
        SELECT DISTINCT ON (security_id)
               security_id, option_type, option_class, maturity, strike, underlying, underlying_sec_id
        FROM option_info
        WHERE security_id = ANY(%s)
        ORDER BY security_id, id DESC
        """,
        (security_ids,),
    )
    df = pd.DataFrame(cur.fetchall(), columns=['security_id', 'option_type', 'option_class', 'maturity',
                                               'strike', 'underlying', 'underlying_sec_id'])
    df['strike'] = df['strike'].astype(float)
    return df


def _get_iv_grids(cur, underlying_ids: list[str], as_of_date) -> dict[str, tuple[date, pd.DataFrame]]:
    """{underlying_sec_id: (iv_date, grid)} using each underlying's latest
    option_iv date on/before as_of_date. grid columns: tenor_years, moneyness, iv."""
    cur.execute(
        """
        SELECT o.underlying_sec_id, o.as_of_date, o.tenor_years, o.moneyness, o.iv
        FROM option_iv o
        JOIN (
            SELECT underlying_sec_id, MAX(as_of_date) AS as_of_date
            FROM option_iv
            WHERE as_of_date <= %s AND underlying_sec_id = ANY(%s)
            GROUP BY underlying_sec_id
        ) latest USING (underlying_sec_id, as_of_date)
        """,
        (as_of_date, underlying_ids),
    )
    df = pd.DataFrame(cur.fetchall(), columns=['underlying_sec_id', 'as_of_date', 'tenor_years', 'moneyness', 'iv'])
    for c in ['tenor_years', 'moneyness', 'iv']:
        df[c] = df[c].astype(float)
    return {sid: (g['as_of_date'].iloc[0], g[['tenor_years', 'moneyness', 'iv']])
            for sid, g in df.groupby('underlying_sec_id')}


# ── Pricing ───────────────────────────────────────────────────────────────────

def interp_iv(grid: pd.DataFrame, tenor: float, moneyness: float) -> float:
    """Linear in moneyness within each tenor, then linear in tenor; flat beyond
    the grid edges (np.interp clamps to the end values)."""
    tenors, ivs = [], []
    for t, g in grid.sort_values(['tenor_years', 'moneyness']).groupby('tenor_years'):
        tenors.append(t)
        ivs.append(np.interp(moneyness, g['moneyness'], g['iv']))
    return float(np.interp(tenor, tenors, ivs))


def _model_prices(cur, securities: pd.DataFrame, as_of_date) -> tuple[pd.DataFrame, list[str]]:
    """Step 3: Black-Scholes prices. Returns (priced rows, skip messages)."""
    skipped: list[str] = []
    info = _get_option_info(cur, securities['security_id'].tolist())
    df = securities.merge(info, on='security_id', how='left')

    und_ids = df['underlying_sec_id'].dropna().unique().tolist()
    und_prices = get_current_price(und_ids, as_of_date)
    iv_grids = _get_iv_grids(cur, und_ids, as_of_date)

    stale = sorted(f'{s} ({d})' for s, (_, d) in und_prices.items() if d < as_of_date)
    if stale:
        print(f'Warning: latest underlying price is before {as_of_date} for: {", ".join(stale)}')
    stale = sorted(f'{s} ({d})' for s, (d, _) in iv_grids.items() if d < as_of_date)
    if stale:
        print(f'Warning: latest option_iv is before {as_of_date} for: {", ".join(stale)}')

    rows = []
    for _, r in df.iterrows():
        def skip(reason):
            skipped.append(f"{r['security_id']}  {r['security_name']}: {reason}")

        if pd.isna(r['option_type']):
            skip('no option_info row'); continue
        if r['option_type'] not in ('Call', 'Put'):
            skip(f"unsupported option_type {r['option_type']!r}"); continue
        if pd.isna(r['strike']) or pd.isna(r['maturity']) or pd.isna(r['underlying_sec_id']):
            skip('option_info missing strike/maturity/underlying_sec_id'); continue

        T = (pd.Timestamp(r['maturity']) - pd.Timestamp(as_of_date)).days / 365
        if T <= 0:
            skip(f"matured ({r['maturity']})"); continue
        und = und_prices.get(r['underlying_sec_id'])
        if und is None:
            skip(f"no current_price for underlying {r['underlying_sec_id']}"); continue
        grid = iv_grids.get(r['underlying_sec_id'])
        if grid is None:
            skip(f"no option_iv for underlying {r['underlying_sec_id']} ({r['underlying']}) on/before {as_of_date}"); continue
        try:
            rate = get_rate(T, as_of_date)
        except ValueError as e:
            skip(f'no risk-free rate: {e}'); continue

        S, K = float(und[0]), float(r['strike'])
        iv_date, g = grid
        iv = interp_iv(g, T, K / S)
        price = float(opt.calc_price(r['option_type'], S, K, T, rate, iv))
        if not np.isfinite(price):
            skip(f'model price is {price}'); continue

        rows.append({
            'as_of_date': as_of_date, 'security_id': r['security_id'], 'price': price,
            'price_source': 'model', 'option_type': r['option_type'], 'strike': K,
            'maturity': r['maturity'], 'underlying_sec_id': r['underlying_sec_id'],
            'underlying_price': S, 'moneyness': K / S, 'tenor_years': T, 'iv': iv,
            'iv_date': iv_date, 'risk_free_rate': float(rate),
        })
    return pd.DataFrame(rows, columns=_TABLE_COLS), skipped


# ── Save ──────────────────────────────────────────────────────────────────────

def _save(cur, df: pd.DataFrame, as_of_date) -> int:
    cur.execute('DELETE FROM option_price WHERE as_of_date = %s', (as_of_date,))
    if df.empty:
        return 0
    records = [tuple(None if pd.isna(v) else v for v in r) for r in df[_TABLE_COLS].itertuples(index=False)]
    execute_values(cur, f"INSERT INTO option_price ({', '.join(_TABLE_COLS)}) VALUES %s", records)
    return len(records)


# ── Main ──────────────────────────────────────────────────────────────────────

def calc_option_price(as_of_date: date = None, dry_run: bool = False) -> pd.DataFrame:
    """Price all option securities for as_of_date and save to option_price.
    Returns the rows written (or that would be written, with dry_run)."""
    if as_of_date is None:
        as_of_date = date.fromisoformat(get_proc_asof_date())
    print(f'as_of_date: {as_of_date}')

    with pg_connection() as conn:
        with conn.cursor() as cur:
            # Step 1
            securities = _get_option_securities(cur, as_of_date)
            print(f'Option securities found: {len(securities)}')
            if securities.empty:
                print('No option securities found - option_price not changed.')
                return pd.DataFrame(columns=_TABLE_COLS)

            # Step 2
            mssb = _get_mssb_prices(cur, as_of_date)
            extra = sorted(set(mssb.index) - set(securities['security_id']))
            if extra:
                print(f'Warning: MSSB OP securities not in step 1 (ignored): {", ".join(extra)}')
            mssb = mssb[mssb.index.isin(securities['security_id'])]
            no_price = mssb.index[mssb.isna()].tolist()
            if no_price:
                print(f'Warning: MSSB price could not be computed (zero quantity?) for: {", ".join(no_price)}'
                      ' - falling back to model price')
            mssb = mssb.dropna()
            mssb_rows = pd.DataFrame({'as_of_date': as_of_date, 'security_id': mssb.index,
                                      'price': mssb.values, 'price_source': 'mssb'},
                                     columns=_TABLE_COLS)
            print(f'MSSB prices: {len(mssb_rows)}')

            # Step 3
            to_model = securities[~securities['security_id'].isin(mssb.index)]
            model_rows, skipped = _model_prices(cur, to_model, as_of_date)
            print(f'Model prices: {len(model_rows)} of {len(to_model)}')
            if skipped:
                print(f'Warning: {len(skipped)} option(s) not priced:')
                for s in skipped:
                    print(f'    {s}')

            parts = [df for df in (mssb_rows, model_rows) if not df.empty]
            result = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=_TABLE_COLS)
            if not result.empty:
                with pd.option_context('display.width', 250, 'display.max_columns', None,
                                       'display.float_format', '{:.4f}'.format):
                    print(result[['security_id', 'price_source', 'price', 'option_type', 'strike', 'maturity',
                                  'underlying_price', 'moneyness', 'tenor_years', 'iv']].to_string(index=False))

            if dry_run:
                print('DRY RUN - option_price not changed')
                return result

            # Step 4
            n = _save(cur, result, as_of_date)
        conn.commit()

    print(f'option_price: {n} row(s) written for {as_of_date}')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Calculate option prices and save to option_price')
    parser.add_argument('--date', metavar='YYYY-MM-DD', default=None,
                        help='As-of date (default: read from proc_asof_date table)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Compute and print prices without writing')
    args = parser.parse_args()

    calc_option_price(date.fromisoformat(args.date) if args.date else None, dry_run=args.dry_run)
