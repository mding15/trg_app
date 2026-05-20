"""
calc_treasury_pnl.py — Daily P&L for US Treasury securities assuming $1 market value.

P&L has one component only (no spread risk for Treasuries):
    IR: pnl_ir = (w1 * dist_ir[T1] + w2 * dist_ir[T2]) * ir_pv01

Where:
    Price          = br.bond_price(yield, coupon_rate, tenor, payment_frequency)
                     Yield interpolated from treasury_yield table via linear interpolation
                     at the bond's tenor; calculated prices saved to bond_price table.
    Yield          = br.bond_yield(coupon_rate, tenor, payment_frequency, price)
    Duration       = br.bond_duration(yield, coupon_rate, ir_tenor, payment_frequency)
    ir_pv01        = Duration / 10_000  (for $1 market value)
    IR_Tenor       = 0.5 for Variable (floating-rate) bonds, else years to maturity
    IR mapping     = UST tenor-point interpolation via bond_risk logic (USD bonds only)

Steps:
    1. Query bond_info WHERE "BondType"='Treasury' for SecurityID, MaturityDate,
       CouponRate, CouponType, PaymentFrequency, IssuedCurrency.
    2. Fetch treasury_yield curve for as_of_date; interpolate yield at each bond's tenor;
       compute par-relative price via br.bond_price(); save to bond_price table.
    3. Compute Tenor, IR_Tenor, Yield, Duration; derive ir_pv01.
    4. IR P&L: map each bond to two adjacent UST tenor points; get_dist(ust_ids, 'IR');
               compute (w1 * dist_ir[T1] + w2 * dist_ir[T2]) * ir_pv01.
    5. Save one Series per security under 'PNL/{SecurityID}' in security_pnl.h5.
    6. Compute distribution statistics and save to log/ as a timestamped CSV.

Usage:
    python process2/calc_treasury_pnl.py                     # date from proc_asof_date
    python process2/calc_treasury_pnl.py --date 2026-05-16   # specific date
    python process2/calc_treasury_pnl.py test                # test mode → Excel output
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import numpy as np
import pandas as pd
from psycopg2.extras import execute_batch

from trg_config import config
from database2 import pg_connection, get_proc_asof_date
from models import bond_risk as br
from utils import hdf_utils, var_utils, stat_utils

# Treasury yield curve column names and their corresponding tenors (years)
_YIELD_COLS   = ['bc_1month', 'bc_2month', 'bc_3month', 'bc_6month',
                 'bc_1year',  'bc_2year',  'bc_3year',  'bc_5year',
                 'bc_7year',  'bc_10year', 'bc_20year', 'bc_30year']
_YIELD_TENORS = [1/12, 2/12, 3/12, 0.5, 1, 2, 3, 5, 7, 10, 20, 30]


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _get_treasury_securities() -> pd.DataFrame:
    """Query bond_info for all Treasury securities with required analytics fields."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT "SecurityID", "MaturityDate", "CouponRate", "CouponType", '
                '       "PaymentFrequency", "IssuedCurrency" '
                'FROM bond_info '
                'WHERE "BondType" = %s AND "MaturityDate" IS NOT NULL',
                ('Treasury',),
            )
            rows = cur.fetchall()
    df = pd.DataFrame(
        rows,
        columns=['SecurityID', 'MaturityDate', 'CouponRate', 'CouponType',
                 'PaymentFrequency', 'Currency'],
    )
    df = df.drop_duplicates(subset='SecurityID', keep='first')
    df['PaymentFrequency'] = df['PaymentFrequency'].fillna(2).astype(int)
    df['CouponRate'] = df['CouponRate'] / 100   # DB stores %, functions expect decimal
    return df


def _get_yield_curve(as_of_date: date) -> tuple[list, list]:
    """Fetch the latest treasury yield curve on or before as_of_date.

    Returns (tenors, yields) where yields are in decimal form (e.g. 0.0432).
    Non-NULL points only; shorter lists if some maturities are missing.
    Returns ([], []) if no curve is available.
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT ' + ', '.join(_YIELD_COLS) + ' '
                'FROM treasury_yield WHERE date <= %s ORDER BY date DESC LIMIT 1',
                (as_of_date,),
            )
            row = cur.fetchone()
    if row is None:
        return [], []
    tenors, yields = [], []
    for t, v in zip(_YIELD_TENORS, row):
        if v is not None:
            tenors.append(t)
            yields.append(float(v) / 100)   # percentage → decimal
    return tenors, yields


def _calc_prices_from_yield_curve(securities: pd.DataFrame, as_of_date: date) -> pd.Series:
    """Calculate par-relative bond prices from the treasury yield curve.

    For each bond, the treasury yield is linearly interpolated at the bond's
    tenor (clamped to curve endpoints for tenors outside the published range).
    Price is computed via br.bond_price(yield, coupon_rate, tenor, freq).
    Prices outside [0.75, 1.5] are capped to 1.0 with a warning.
    Falls back to par (1.0) for all bonds if the yield curve is unavailable.
    """
    prices = pd.Series(1.0, index=securities['SecurityID'], name='Price')

    tenors_arr, yields_arr = _get_yield_curve(as_of_date)
    if not tenors_arr:
        print('Warning: no treasury yield curve available for price calculation, using par.')
        return prices

    for _, row in securities.iterrows():
        sec_id = row['SecurityID']
        tenor  = max((row['MaturityDate'] - as_of_date).days / 365.25, 0)
        if tenor <= 0:
            continue
        y = float(np.interp(tenor, tenors_arr, yields_arr))
        prices[sec_id] = br.bond_price(y, row['CouponRate'], tenor, int(row['PaymentFrequency']))

    bad = (prices < 0.75) | (prices > 1.5)
    if bad.any():
        for sec_id in prices[bad].index:
            print(f'Warning: calculated price out of range for {sec_id}: '
                  f'{prices[sec_id] * 100:.2f}, using par.')
        prices[bad] = 1.0

    return prices


def _save_prices_to_db(prices: pd.Series, as_of_date: date) -> None:
    """Overwrite bond_price rows for as_of_date with calculated prices.

    Prices are stored as market quotes (par-relative × 100, e.g. 98.50).
    Existing rows for the same securities and date are deleted first.
    """
    sec_ids = prices.index.tolist()
    records = [(sec_id, as_of_date, float(prices[sec_id]) * 100) for sec_id in sec_ids]
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'DELETE FROM bond_price WHERE security_id = ANY(%s) AND price_date = %s',
                (sec_ids, as_of_date),
            )
            execute_batch(
                cur,
                'INSERT INTO bond_price (security_id, price_date, price) VALUES (%s, %s, %s)',
                records,
            )
        conn.commit()
    print(f'Saved {len(records)} prices to bond_price for {as_of_date}.')


# ── Analytics ──────────────────────────────────────────────────────────────────

def _calc_treasury_analytics(securities: pd.DataFrame, as_of_date: date) -> pd.DataFrame:
    """Compute bond analytics and add result columns to the securities DataFrame.

    Required input columns:
        MaturityDate     — date; Tenor = (MaturityDate - as_of_date) / 365.25
        CouponRate       — annual coupon rate (decimal)
        CouponType       — 'Variable' sets IR_Tenor = 0.5
        PaymentFrequency — coupon payments per year
        Price            — par-relative price from _get_bond_prices()

    Columns added:
        Tenor      — years to maturity (floored at 0 for matured bonds)
        IR_Tenor   — 0.5 for Variable bonds, else = Tenor
        Yield      — solved from Price, CouponRate, Tenor, PaymentFrequency
        Duration   — modified duration using IR_Tenor
    """
    securities['Tenor'] = securities['MaturityDate'].apply(
        lambda d: max((d - as_of_date).days / 365.25, 0)
    )

    securities['IR_Tenor'] = securities['Tenor']
    float_idx = securities['CouponType'] == 'Variable'
    securities.loc[float_idx, 'IR_Tenor'] = 0.5

    securities['Yield'] = securities.apply(
        lambda r: br.bond_yield(r.CouponRate, r.Tenor, r.PaymentFrequency, r.Price),
        axis=1,
    )

    securities['Duration'] = securities.apply(
        lambda r: br.bond_duration(r.Yield, r.CouponRate, r.IR_Tenor, r.PaymentFrequency),
        axis=1,
    )

    return securities


# ── IR P&L ─────────────────────────────────────────────────────────────────────

def _calc_ir_pnl(securities: pd.DataFrame, ir_pv01: pd.Series) -> pd.DataFrame:
    """IR P&L = (w1*dist_ir[T1] + w2*dist_ir[T2]) * ir_pv01, USD bonds only."""
    usd = securities[
        (securities['Currency'] == 'USD') & (securities['IR_Tenor'] > 0)
    ].copy()
    if usd.empty:
        return pd.DataFrame()

    bins      = [0] + br.get_ust_tenors()['Tenor'].tolist()
    bins[-1]  = 100
    labels_t1 = ['UST0M'] + br.get_ust_tenors().iloc[:-1]['SecurityID'].tolist()
    labels_t2 = br.get_ust_tenors()['SecurityID'].tolist()

    usd['T1'] = pd.cut(usd['IR_Tenor'], bins=bins, labels=labels_t1)
    usd['T2'] = pd.cut(usd['IR_Tenor'], bins=bins, labels=labels_t2)
    usd['w1'] = usd.apply(lambda r: br.calc_w1(r.IR_Tenor, r.T1, r.T2), axis=1)
    usd['w2'] = 1 - usd['w1']

    usd = usd[usd['T1'] != 'UST0M']
    if usd.empty:
        return pd.DataFrame()

    ust_ids = list(set(usd['T1'].astype(str).tolist() + usd['T2'].astype(str).tolist()))
    dist_ir = var_utils.get_dist(ust_ids, 'IR') * 10000  # convert to bps
    if dist_ir.empty:
        return pd.DataFrame()

    cols = {}
    for _, row in usd.iterrows():
        sec_id = row['SecurityID']
        t1, t2 = str(row['T1']), str(row['T2'])
        w1, w2 = row['w1'], row['w2']
        pv01   = ir_pv01.get(sec_id)
        if pv01 is None:
            continue

        pnl = pd.Series(0.0, index=dist_ir.index)
        if t1 in dist_ir.columns:
            pnl = pnl + w1 * dist_ir[t1]
        if t2 in dist_ir.columns:
            pnl = pnl + w2 * dist_ir[t2]
        cols[sec_id] = pnl * pv01

    return pd.DataFrame(cols, index=dist_ir.index)


# ── Main ───────────────────────────────────────────────────────────────────────

def calc_treasury_pnl(as_of_date: date = None) -> pd.DataFrame:
    """Return IR P&L DataFrame (scenarios × securities) and save to HDF.

    Args:
        as_of_date: valuation date used to compute tenors; defaults to proc_asof_date.
    """
    if as_of_date is None:
        as_of_date = date.fromisoformat(get_proc_asof_date())

    # Step 1: Treasury securities from bond_info
    securities = _get_treasury_securities()
    print(f'Treasury securities found: {len(securities)}')

    if securities.empty:
        print('No treasury securities — output not written.')
        return pd.DataFrame()

    # Step 2: Bond prices from treasury yield curve; save to bond_price table
    prices = _calc_prices_from_yield_curve(securities, as_of_date)
    _save_prices_to_db(prices, as_of_date)
    securities['Price'] = securities['SecurityID'].map(prices)

    # Step 3: Analytics — Tenor, IR_Tenor, Yield, Duration
    securities = _calc_treasury_analytics(securities, as_of_date)

    matured = (securities['Tenor'] == 0).sum()
    if matured:
        print(f'Warning: {matured} matured bond(s) excluded (Tenor = 0).')
    securities = securities[securities['Tenor'] > 0]

    nan_yield = securities['Yield'].isna().sum()
    if nan_yield:
        print(f'Warning: {nan_yield} security(ies) excluded (Yield could not be solved).')
    securities = securities.dropna(subset=['Yield', 'Duration'])

    ir_pv01 = securities.set_index('SecurityID')['Duration'] / 10_000

    # Step 4: IR P&L
    pnl = _calc_ir_pnl(securities, ir_pv01)
    print(f'IR P&L: {pnl.shape[1] if not pnl.empty else 0} securities, '
          f'{pnl.shape[0] if not pnl.empty else 0} scenarios')

    if pnl.empty:
        print('No P&L computed — output not written.')
        return pd.DataFrame()

    # Step 5: Save one Series per security under 'PNL/{SecurityID}'
    output_file = config['VaR_DIR'] / 'security_pnl.h5'
    hdf_utils.save(pnl, 'PNL', output_file)
    print(f'Saved: {output_file}')

    # Step 6: Distribution statistics → timestamped CSV in log/
    stats     = stat_utils.dist_stat(pnl)
    ts        = datetime.now().strftime('%Y%m%d_%H%M%S')
    stat_file = config['LOG_DIR'] / f'treasury_pnl_stat_{ts}.csv'
    stats.to_csv(stat_file)
    print(f'Stats saved: {stat_file}')

    return pnl


# ── Test ───────────────────────────────────────────────────────────────────────

def test():
    """Step-by-step calculation with all intermediate results written to Excel."""
    today   = date.fromisoformat(get_proc_asof_date())
    results = {}

    # Step 1: Treasury securities from bond_info
    step1 = _get_treasury_securities()
    results['Step1_Securities'] = step1
    print(f'Step 1: {len(step1)} treasury securities')

    if step1.empty:
        print('No treasury securities found — stopping.')
        return

    # Step 2: Bond prices from treasury yield curve; save to bond_price table
    prices = _calc_prices_from_yield_curve(step1, today)
    _save_prices_to_db(prices, today)
    results['Step2_Prices'] = prices.rename('Price').to_frame()
    print(f'Step 2: {len(prices)} prices calculated from treasury yield curve')

    # Step 3: Analytics — Tenor, IR_Tenor, Yield, Duration, ir_pv01
    securities = step1.copy()
    securities['Price'] = securities['SecurityID'].map(prices)
    securities = _calc_treasury_analytics(securities, today)
    securities = securities[securities['Tenor'] > 0].dropna(subset=['Yield', 'Duration'])

    ir_pv01 = securities.set_index('SecurityID')['Duration'] / 10_000

    step3 = securities[['SecurityID', 'Currency', 'CouponType', 'MaturityDate',
                         'CouponRate', 'PaymentFrequency', 'Price',
                         'Tenor', 'IR_Tenor', 'Yield', 'Duration']].copy()
    step3['ir_pv01'] = step3['SecurityID'].map(ir_pv01)
    step3 = step3.set_index('SecurityID')
    results['Step3_Analytics'] = step3
    print(f'Step 3: analytics for {len(step3)} securities')

    # Step 4: IR P&L
    step4 = _calc_ir_pnl(securities, ir_pv01)
    results['Step4_IR_PnL'] = step4
    print(f'Step 4: IR P&L {step4.shape}')

    # Step 5: Save to HDF
    output_hdf = config['VaR_DIR'] / 'security_pnl.h5'
    if not step4.empty:
        hdf_utils.save(step4, 'PNL', output_hdf)
    results['Step5_HDF'] = pd.DataFrame([{
        'output_file': str(output_hdf),
        'scenarios':   step4.shape[0] if not step4.empty else 0,
        'securities':  step4.shape[1] if not step4.empty else 0,
    }])
    print(f'Step 5: saved to {output_hdf}')

    # Step 6: Distribution statistics
    step6 = stat_utils.dist_stat(step4) if not step4.empty else pd.DataFrame()
    results['Step6_Stats'] = step6
    print('Step 6: stats computed')

    # Write all results to Excel
    ts          = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir  = os.path.join(os.path.dirname(__file__), 'output')
    os.makedirs(output_dir, exist_ok=True)
    output_xlsx = os.path.join(output_dir, f'treasury_pnl_test_{ts}.xlsx')

    with pd.ExcelWriter(output_xlsx, engine='openpyxl') as writer:
        for sheet, df in results.items():
            if df is not None and not df.empty:
                df.to_excel(writer, sheet_name=sheet, index=True)
            else:
                pd.DataFrame(['(empty)']).to_excel(
                    writer, sheet_name=sheet, index=False, header=False,
                )

    print(f'Excel saved: {output_xlsx}')


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Calculate Treasury bond P&L')
    parser.add_argument(
        'mode', nargs='?', default='run',
        choices=['run', 'test'],
        help='run (default): calc_treasury_pnl();  test: test() → Excel output',
    )
    parser.add_argument(
        '--date', metavar='YYYY-MM-DD', default=None,
        help='As-of date (default: read from proc_asof_date table)',
    )
    args = parser.parse_args()

    as_of = date.fromisoformat(args.date) if args.date else None

    if args.mode == 'test':
        test()
    else:
        calc_treasury_pnl(as_of)
