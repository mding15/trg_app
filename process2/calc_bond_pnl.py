"""
calc_bond_pnl.py — Daily P&L for Bond/Bond securities assuming $1 market value.

P&L has two components:
    SPREAD: pnl_spread = dist_spread * spread_pv01
    IR:     pnl_ir     = (w1 * dist_ir[T1] + w2 * dist_ir[T2]) * ir_pv01

Where:
    Price          = latest market quote / 100 from bond_price table (fallback: 1.0)
    Yield          = br.bond_yield(coupon_rate, tenor, payment_frequency, price)
    Duration       = br.bond_duration(yield, coupon_rate, ir_tenor, payment_frequency)
    SpreadDuration = br.bond_duration(yield, coupon_rate, full_tenor, payment_frequency)
    ir_pv01        = Duration       / 10_000  (for $1 market value)
    spread_pv01    = SpreadDuration / 10_000  (for $1 market value)
    IR_Tenor       = 0.5 for Variable (floating-rate) bonds, else years to maturity
    IR mapping     = UST tenor-point interpolation via bond_risk logic (USD bonds only)

Steps:
    1. Query security_info for AssetClass='Bond', AssetType='Bond'.
    2. Query bond_info for MaturityDate, CouponRate, CouponType, PaymentFrequency.
    3. Query bond_price for latest market price on or before as_of_date; convert to par-relative.
    4. Compute Tenor, Yield, Duration, SpreadDuration; derive ir_pv01 and spread_pv01.
    5. SPREAD P&L: get_dist(sec_ids, 'SPREAD'), multiply by spread_pv01.
    6. IR P&L:     map each USD bond to two UST tenor points; get_dist(ust_ids, 'IR');
                   compute (w1 * dist_ir[T1] + w2 * dist_ir[T2]) * ir_pv01.
    7. Total P&L = pnl_spread + pnl_ir (aligned on scenario index).
    8. Save one Series per security under 'PNL/{SecurityID}' in security_pnl.h5.
    9. Compute distribution statistics and save to log/ as a timestamped CSV.

Usage:
    python calc_bond_pnl.py                        # calc_bond_pnl() with date from proc_asof_date
    python calc_bond_pnl.py --date 2026-05-16      # calc_bond_pnl() with specific date
    python calc_bond_pnl.py test                   # run test()
    python calc_bond_pnl.py test_ir                # run test_ir_pnl()
"""
from __future__ import annotations

import os
import sys
from datetime import date, datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from trg_config import config
from database2 import pg_connection, get_proc_asof_date
from models import bond_risk as br
from utils import hdf_utils, var_utils, stat_utils
from process2.db_pnl_stat import save_pnl_stat, save_security_sensitivity

def _get_bond_securities() -> pd.DataFrame:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT cs."SecurityID", si."AssetClass", si."AssetType", si."Currency"
                FROM current_security cs
                JOIN security_info si ON si."SecurityID" = cs."SecurityID"
                WHERE si."AssetClass" = %s AND si."AssetType" = %s
                """,
                ('Bond', 'Bond'),
            )
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=['SecurityID', 'AssetClass', 'AssetType', 'Currency'])


def _get_bond_info(sec_ids: list) -> pd.DataFrame:
    """Query bond_info and return analytics inputs for the given SecurityIDs.

    Columns used and their purpose:
        SecurityID       — join key; bonds missing from bond_info are excluded
        MaturityDate     — required (NULLs filtered in query); used to compute Tenor
                           and IR_Tenor (years to maturity from today)
        CouponRate       — passed to br.bond_yield() and br.bond_duration()
        CouponType       — 'Variable' triggers IR_Tenor override to 0.5
                           (floating-rate bonds have near-zero IR duration)
        PaymentFrequency — coupon payments per year; passed to br.bond_yield() and
                           br.bond_duration(); defaults to 2 (semi-annual) if NULL
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT "SecurityID", "MaturityDate", "CouponRate", "CouponType", "PaymentFrequency" '
                'FROM bond_info WHERE "SecurityID" = ANY(%s) AND "MaturityDate" IS NOT NULL',
                (sec_ids,),
            )
            rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=['SecurityID', 'MaturityDate', 'CouponRate', 'CouponType', 'PaymentFrequency'])
    df = df.drop_duplicates(subset='SecurityID', keep='first')
    df['PaymentFrequency'] = df['PaymentFrequency'].fillna(2).astype(int)
    df['CouponRate'] = df['CouponRate'] / 100   # DB stores %, functions expect decimal
    return df


def _get_bond_prices(sec_ids: list, as_of_date: date) -> pd.Series:
    """Return par-relative prices indexed by SecurityID.

    Uses the latest price on or before as_of_date from bond_price table.
    Market quote (e.g. 98.5) is converted to par-relative by dividing by 100.
    Prices outside [0.75, 1.5] are capped to 1.0 with a warning.
    Missing prices fall back to 1.0 (par).
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT DISTINCT ON (security_id) security_id, price '
                'FROM bond_price '
                'WHERE security_id = ANY(%s) AND price_date <= %s '
                'ORDER BY security_id, price_date DESC',
                (sec_ids, as_of_date),
            )
            rows = cur.fetchall()
    prices = pd.Series(1.0, index=sec_ids, name='Price')
    if rows:
        df = pd.DataFrame(rows, columns=['SecurityID', 'Price'])
        prices.update(df.set_index('SecurityID')['Price'] / 100)
    bad = (prices < 0.75) | (prices > 1.5)
    if bad.any():
        for sec_id in prices[bad].index:
            print(f'Warning: bad bond price for {sec_id}: {prices[sec_id] * 100:.2f}, using par.')
        prices[bad] = 1.0
    return prices


def _calc_bond_analytics(securities: pd.DataFrame, as_of_date: date) -> pd.DataFrame:
    """Compute bond analytics and add result columns to the securities DataFrame.

    Required input columns:
        MaturityDate     — date; used to compute Tenor = (MaturityDate - as_of_date) / 365.25
        CouponRate       — annual coupon rate (decimal); input to bond_yield / bond_duration
        CouponType       — str; 'Variable' sets IR_Tenor = 0.5, all others use full Tenor
        PaymentFrequency — int; coupon payments per year; input to bond_yield / bond_duration
        Price            — par-relative price (e.g. 0.985); sourced from _get_bond_prices()

    Columns added:
        Tenor          — years to maturity (floored at 0 for matured bonds)
        IR_Tenor       — effective IR tenor: 0.5 for Variable bonds, else = Tenor
        Yield          — solved from Price, CouponRate, Tenor, PaymentFrequency
        SpreadDuration — modified duration using full Tenor (credit risk sensitivity)
        Duration       — modified duration using IR_Tenor (rate risk sensitivity;
                         short for floaters, same as SpreadDuration for fixed-rate)
    """
    securities['Tenor'] = securities['MaturityDate'].apply(
        lambda d: max((d - as_of_date).days / 365.25, 0)
    )

    # IR_Tenor: floaters capped at 0.5 (near-zero rate sensitivity)
    securities['IR_Tenor'] = securities['Tenor']
    float_idx = securities['CouponType'] == 'Variable'
    securities.loc[float_idx, 'IR_Tenor'] = 0.5

    # Yield solved from actual price and full tenor
    securities['Yield'] = securities.apply(
        lambda r: br.bond_yield(r.CouponRate, r.Tenor, r.PaymentFrequency, r.Price),
        axis=1,
    )

    # SpreadDuration uses full tenor (before floater IR override)
    securities['SpreadDuration'] = securities.apply(
        lambda r: br.bond_duration(r.Yield, r.CouponRate, r.Tenor, r.PaymentFrequency),
        axis=1,
    )

    # IR Duration uses IR_Tenor (0.5 for floaters, full tenor for fixed)
    securities['Duration'] = securities.apply(
        lambda r: br.bond_duration(r.Yield, r.CouponRate, r.IR_Tenor, r.PaymentFrequency),
        axis=1,
    )

    return securities


def _calc_spread_pnl(sec_ids: list, spread_pv01: pd.Series) -> pd.DataFrame:
    """SPREAD P&L = dist_spread * spread_pv01, per security."""
    dist = var_utils.get_dist(sec_ids, 'SPREAD') * 100  # convert to bps
    if dist.empty:
        return dist
    return dist.multiply(spread_pv01, axis='columns')


def _calc_ir_pnl(securities: pd.DataFrame, ir_pv01: pd.Series) -> pd.DataFrame:
    """IR P&L = (w1*dist_ir[T1] + w2*dist_ir[T2]) * ir_pv01, USD bonds only."""
    usd = securities[
        (securities['Currency'] == 'USD') & (securities['IR_Tenor'] > 0)
    ].copy()
    if usd.empty:
        return pd.DataFrame()

    # Map each bond to two adjacent UST tenor points (same logic as ir_risk_factors)
    bins      = [0] + br.get_ust_tenors()['Tenor'].tolist()
    bins[-1]  = 100
    labels_t1 = ['UST0M'] + br.get_ust_tenors().iloc[:-1]['SecurityID'].tolist()
    labels_t2 = br.get_ust_tenors()['SecurityID'].tolist()

    usd['T1'] = pd.cut(usd['IR_Tenor'], bins=bins, labels=labels_t1)
    usd['T2'] = pd.cut(usd['IR_Tenor'], bins=bins, labels=labels_t2)
    usd['w1'] = usd.apply(lambda r: br.calc_w1(r.IR_Tenor, r.T1, r.T2), axis=1)
    usd['w2'] = 1 - usd['w1']

    # Exclude bonds below the shortest UST bucket
    usd = usd[usd['T1'] != 'UST0M']
    if usd.empty:
        return pd.DataFrame()

    # Fetch IR distributions for all required UST tenor-point SecurityIDs
    ust_ids = list(set(usd['T1'].astype(str).tolist() + usd['T2'].astype(str).tolist()))
    dist_ir = var_utils.get_dist(ust_ids, 'IR') * 10000  # convert to bps
    if dist_ir.empty:
        return pd.DataFrame()

    # Compute weighted IR P&L per bond
    cols = {}
    for _, row in usd.iterrows():
        sec_id  = row['SecurityID']
        t1, t2  = str(row['T1']), str(row['T2'])
        w1, w2  = row['w1'], row['w2']
        pv01    = ir_pv01.get(sec_id)
        if pv01 is None:
            continue

        pnl = pd.Series(0.0, index=dist_ir.index)
        if t1 in dist_ir.columns:
            pnl = pnl + w1 * dist_ir[t1]
        if t2 in dist_ir.columns:
            pnl = pnl + w2 * dist_ir[t2]
        cols[sec_id] = pnl * pv01

    return pd.DataFrame(cols, index=dist_ir.index)


def calc_bond_pnl(as_of_date: date = None) -> pd.DataFrame:
    """Return total P&L DataFrame (scenarios × securities) and save to HDF.

    Args:
        as_of_date: valuation date used to compute bond tenors; defaults to today.
    """
    if as_of_date is None:
        as_of_date = date.fromisoformat(get_proc_asof_date())

    # Step 1: Bond/Bond securities from security_info
    securities = _get_bond_securities()
    print(f'Bond securities found: {len(securities)}')

    # Step 2: Bond analytics from bond_info
    sec_ids  = securities['SecurityID'].tolist()
    bond_info = _get_bond_info(sec_ids)

    missing_info = len(securities) - len(bond_info)
    if missing_info:
        print(f'Warning: {missing_info} securities excluded (no bond_info record).')

    securities = securities.merge(bond_info, on='SecurityID', how='inner')

    # Step 2b: Bond prices from bond_price table
    prices = _get_bond_prices(securities['SecurityID'].tolist(), as_of_date)
    securities['Price'] = securities['SecurityID'].map(prices)

    # Step 3: Compute Tenor, Yield, Duration, SpreadDuration; filter matured bonds
    securities = _calc_bond_analytics(securities, as_of_date)
    matured = (securities['Tenor'] == 0).sum()
    if matured:
        print(f'Warning: {matured} matured bonds excluded (Tenor = 0).')
    securities = securities[securities['Tenor'] > 0]

    nan_yield = securities['Yield'].isna().sum()
    if nan_yield:
        print(f'Warning: {nan_yield} securities excluded (Yield could not be solved).')
    securities = securities.dropna(subset=['Yield', 'Duration', 'SpreadDuration'])

    # PV01 = Duration / 10_000  (MV = $1)
    idx          = securities.set_index('SecurityID')
    ir_pv01      = idx['Duration']       / 10_000
    spread_pv01  = idx['SpreadDuration'] / 10_000

    # Step 4: SPREAD P&L
    pnl_spread = _calc_spread_pnl(securities['SecurityID'].tolist(), spread_pv01)
    print(f'SPREAD P&L: {pnl_spread.shape[1] if not pnl_spread.empty else 0} securities')

    # Step 5: IR P&L
    pnl_ir = _calc_ir_pnl(securities, ir_pv01)
    print(f'IR P&L: {pnl_ir.shape[1] if not pnl_ir.empty else 0} securities')

    # Step 6: Total P&L = pnl_spread + pnl_ir
    if pnl_spread.empty and pnl_ir.empty:
        print('No P&L computed — output not written.')
        return pd.DataFrame()

    if pnl_spread.empty:
        pnl = pnl_ir
    elif pnl_ir.empty:
        pnl = pnl_spread
    else:
        pnl = pnl_spread.add(pnl_ir, fill_value=0)

    # Step 7: Save one Series per security under 'PNL/{SecurityID}'
    output_file = config['VaR_DIR'] / 'security_pnl.h5'
    hdf_utils.save(pnl, 'PNL', output_file)
    if not pnl_ir.empty:
        hdf_utils.save(pnl_ir, 'IR_PNL', output_file)
    if not pnl_spread.empty:
        hdf_utils.save(pnl_spread, 'SPREAD_PNL', output_file)
    print(f'Saved: {output_file}')

    # Save security-level sensitivities + skewness/kurtosis from P&L distribution
    pnl_stats = pd.DataFrame({'Skewness': pnl.skew(), 'Kurtosis': pnl.kurt()})
    pnl_stats.index.name = 'SecurityID'
    sens = securities.set_index('SecurityID').join(pnl_stats, how='left').reset_index()
    n = save_security_sensitivity(sens, as_of_date)
    print(f'Sensitivities written to DB: {n} rows')

    # Step 8: Distribution statistics → timestamped CSV in log/ and DB
    stats     = stat_utils.dist_stat(pnl)
    n = save_pnl_stat(stats, as_of_date, 'BOND')
    print(f'Stats written to DB: {n} rows (pnl_type=BOND)')

    return pnl


def test():
    sec_ids = [
        'T10001061',
    ]
    sec_ids = _get_bond_securities()['SecurityID'].tolist()  # override with all Bond/Bond securities for testing

    today   = date.fromisoformat(get_proc_asof_date())
    results = {}  # sheet_name → DataFrame

    # Step 1: securities from security_info filtered to sec_ids
    all_securities = _get_bond_securities()
    step1 = all_securities[all_securities['SecurityID'].isin(sec_ids)].copy()
    results['Step1_Securities'] = step1
    print(f'Step 1: {len(step1)} securities')

    # Step 2: bond_info
    step2 = _get_bond_info(sec_ids)
    results['Step2_BondInfo'] = step2
    print(f'Step 2: {len(step2)} bond_info records')

    # Step 3: bond prices from bond_price table
    prices = _get_bond_prices(sec_ids, today)
    step3 = prices.rename('Price').to_frame()
    results['Step3_BondPrices'] = step3
    print(f'Step 3: {len(step3)} bond prices')

    # Step 4: bond analytics — Tenor, Yield, Duration, SpreadDuration, PV01
    securities = step1.merge(step2, on='SecurityID', how='inner')
    securities['Price'] = securities['SecurityID'].map(prices)
    securities = _calc_bond_analytics(securities, today)
    securities = securities[securities['Tenor'] > 0].dropna(
        subset=['Yield', 'Duration', 'SpreadDuration']
    )

    idx         = securities.set_index('SecurityID')
    ir_pv01     = idx['Duration']       / 10_000
    spread_pv01 = idx['SpreadDuration'] / 10_000

    step4 = securities[['SecurityID', 'Currency', 'CouponType', 'MaturityDate',
                         'CouponRate', 'PaymentFrequency', 'Price', 'Tenor', 'IR_Tenor',
                         'Yield', 'SpreadDuration', 'Duration']].copy()
    step4['spread_pv01'] = step4['SecurityID'].map(spread_pv01)
    step4['ir_pv01']     = step4['SecurityID'].map(ir_pv01)
    step4 = step4.set_index('SecurityID')
    results['Step4_Analytics'] = step4
    print(f'Step 4: analytics for {len(step4)} securities')

    # Step 5: SPREAD P&L
    valid_ids  = securities['SecurityID'].tolist()
    step5      = _calc_spread_pnl(valid_ids, spread_pv01)
    results['Step5_SpreadPnL'] = step5
    print(f'Step 5: SPREAD P&L {step5.shape}')

    # Step 6: IR P&L
    step6 = _calc_ir_pnl(securities, ir_pv01)
    results['Step6_IR_PnL'] = step6
    print(f'Step 6: IR P&L {step6.shape}')

    # Step 7: Total P&L = SPREAD + IR
    if step5.empty and step6.empty:
        step7 = pd.DataFrame()
    elif step5.empty:
        step7 = step6.copy()
    elif step6.empty:
        step7 = step5.copy()
    else:
        step7 = step5.add(step6, fill_value=0)
    results['Step7_TotalPnL'] = step7
    print(f'Step 7: Total P&L {step7.shape}')

    # Step 8: Save to HDF
    output_hdf = config['VaR_DIR'] / 'security_pnl.h5'
    if not step7.empty:
        hdf_utils.save(step7, 'PNL', output_hdf)
    results['Step8_HDF'] = pd.DataFrame([{
        'output_file': str(output_hdf),
        'rows':        step7.shape[0] if not step7.empty else 0,
        'securities':  step7.shape[1] if not step7.empty else 0,
    }])
    print(f'Step 8: saved to {output_hdf}')

    # Step 9: distribution statistics
    step9 = stat_utils.dist_stat(step7) if not step7.empty else pd.DataFrame()
    results['Step9_Stats'] = step9
    print(f'Step 9: stats computed')

    # Write all results to Excel, one sheet per step
    ts          = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir  = os.path.join(os.path.dirname(__file__), 'output')
    output_xlsx = os.path.join(output_dir, f'bond_pnl_test_{ts}.xlsx')

    with pd.ExcelWriter(output_xlsx, engine='openpyxl') as writer:
        for sheet, df in results.items():
            if df is not None and not df.empty:
                df.to_excel(writer, sheet_name=sheet, index=True)
            else:
                pd.DataFrame(['(empty)']).to_excel(
                    writer, sheet_name=sheet, index=False, header=False
                )

    print(f'Excel saved: {output_xlsx}')


def test_ir_pnl():
    """Dump intermediate data from _calc_ir_pnl() into Excel for step-by-step verification.

    Sheets:
        1_USD_Bonds       — filtered USD bonds with IR_Tenor
        2_Tenor_Mapping   — T1, T2, w1, w2, ir_pv01 per bond
        3_IR_Distributions — raw UST IR distributions in bps (scenarios × UST tenor points)
        4_Weighted_IR     — w1*dist[T1] + w2*dist[T2] per bond, before PV01 (scenarios × bonds)
        5_IR_PnL          — final IR P&L = weighted_IR × ir_pv01 (scenarios × bonds)
    """
    sec_ids = ['T10001061']  # same as test()

    # Setup: replicate steps 1-3 to get securities and ir_pv01
    today      = date.fromisoformat(get_proc_asof_date())
    all_securities = _get_bond_securities()
    step1      = all_securities[all_securities['SecurityID'].isin(sec_ids)].copy()
    step2      = _get_bond_info(sec_ids)
    securities = step1.merge(step2, on='SecurityID', how='inner')
    prices     = _get_bond_prices(sec_ids, today)
    securities['Price'] = securities['SecurityID'].map(prices)
    securities = _calc_bond_analytics(securities, today)
    securities = securities[securities['Tenor'] > 0].dropna(
        subset=['Yield', 'Duration', 'SpreadDuration']
    )
    ir_pv01 = securities.set_index('SecurityID')['Duration'] / 10_000

    results = {}

    # Sheet 1: USD bonds after currency and tenor filter
    usd = securities[
        (securities['Currency'] == 'USD') & (securities['IR_Tenor'] > 0)
    ].copy()
    results['1_USD_Bonds'] = (
        usd[['SecurityID', 'Currency', 'IR_Tenor']].set_index('SecurityID')
    )

    # Sheet 2: UST tenor-point mapping (T1, T2, w1, w2) + ir_pv01
    bins      = [0] + br.get_ust_tenors()['Tenor'].tolist()
    bins[-1]  = 100
    labels_t1 = ['UST0M'] + br.get_ust_tenors().iloc[:-1]['SecurityID'].tolist()
    labels_t2 = br.get_ust_tenors()['SecurityID'].tolist()

    usd['T1'] = pd.cut(usd['IR_Tenor'], bins=bins, labels=labels_t1)
    usd['T2'] = pd.cut(usd['IR_Tenor'], bins=bins, labels=labels_t2)
    usd['w1'] = usd.apply(lambda r: br.calc_w1(r.IR_Tenor, r.T1, r.T2), axis=1)
    usd['w2'] = 1 - usd['w1']
    usd = usd[usd['T1'] != 'UST0M']

    tenor_map = usd[['SecurityID', 'IR_Tenor', 'T1', 'T2', 'w1', 'w2']].copy()
    tenor_map['ir_pv01'] = tenor_map['SecurityID'].map(ir_pv01)
    results['2_Tenor_Mapping'] = tenor_map.set_index('SecurityID')

    # Sheet 3: raw UST IR distributions (bps)
    ust_ids = list(set(usd['T1'].astype(str).tolist() + usd['T2'].astype(str).tolist()))
    dist_ir  = var_utils.get_dist(ust_ids, 'IR') * 10_000   # convert to bps
    results['3_IR_Distributions'] = dist_ir

    # Sheet 4: weighted IR scenarios per bond before PV01 scaling
    w_cols = {}
    for _, row in usd.iterrows():
        sec_id  = row['SecurityID']
        t1, t2  = str(row['T1']), str(row['T2'])
        w1, w2  = row['w1'], row['w2']
        pnl = pd.Series(0.0, index=dist_ir.index)
        if t1 in dist_ir.columns:
            pnl = pnl + w1 * dist_ir[t1]
        if t2 in dist_ir.columns:
            pnl = pnl + w2 * dist_ir[t2]
        w_cols[sec_id] = pnl
    weighted = pd.DataFrame(w_cols, index=dist_ir.index)
    results['4_Weighted_IR'] = weighted

    # Sheet 5: final IR P&L = weighted_IR × ir_pv01
    ir_cols = {sec_id: weighted[sec_id] * pv01
               for sec_id in weighted.columns
               if (pv01 := ir_pv01.get(sec_id)) is not None}
    results['5_IR_PnL'] = pd.DataFrame(ir_cols, index=dist_ir.index)

    # Write to Excel
    ts          = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir  = os.path.join(os.path.dirname(__file__), 'output')
    output_xlsx = os.path.join(output_dir, f'ir_pnl_test_{ts}.xlsx')

    with pd.ExcelWriter(output_xlsx, engine='openpyxl') as writer:
        for sheet, df in results.items():
            if df is not None and not df.empty:
                df.to_excel(writer, sheet_name=sheet, index=True)
            else:
                pd.DataFrame(['(empty)']).to_excel(
                    writer, sheet_name=sheet, index=False, header=False
                )

    print(f'Excel saved: {output_xlsx}')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Calculate bond P&L')
    parser.add_argument(
        'mode', nargs='?', default='run',
        choices=['run', 'test', 'test_ir'],
        help='run (default): calc_bond_pnl();  test: test();  test_ir: test_ir_pnl()',
    )
    parser.add_argument(
        '--date', metavar='YYYY-MM-DD', default=None,
        help='As-of date (default: read from proc_asof_date table)',
    )
    args = parser.parse_args()

    as_of = date.fromisoformat(args.date) if args.date else None

    if args.mode == 'test':
        test()
    elif args.mode == 'test_ir':
        test_ir_pnl()
    else:
        calc_bond_pnl(as_of)
