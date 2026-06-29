# -*- coding: utf-8 -*-
"""
Created on Thu Oct 16 16:01:13 2025

@author: mgdin
"""

import pandas as pd
import numpy as np
from datetime import datetime
import xlwings as xw
from scipy import optimize
from pathlib import Path


from security import security_info
from utils import xl_utils, date_utils, stat_utils, tools, var_utils, data_utils
from models import model_utils
from models import risk_factors
from models import binary_option, equity_options, bond_risk

from psycopg2.extras import execute_batch

from database2 import pg_connection
from models.ust_curve import get_rate as _ust_get_rate

SECURITIES_DTYPE = {
    'quantity': float,
    'quantity': float,
    'Strike': float,
    'strike': float,
    'RiskfreeRate': float,
    'risk_free_rate': float,
    'UnderlyingPrice': float,
    'underlying_price': float,
    'Price': float,
    'Maturity': 'datetime64[ns]',
    'maturity': 'datetime64[ns]',
}
    


##################################################################################################
# analytic
def calc_st_value(positions, sigma=0.2):
    values = []
    for i, row in positions.iterrows():
        ty, quantity, S, K, T, r = row[['security_type', 'quantity', 'underlying_price', 'strike', 'tenor', 'risk_free_rate']]
        if ty == 'Binary Call':
            values.append( binary_option.CALL(S, K, T, r, sigma) * quantity)
        elif ty == 'Binary Put':
            values.append (binary_option.PUT(S, K, T, r, sigma) * quantity )
        elif ty == 'Call':
            values.append(equity_options.BS_CALL(S, K, T, r, sigma) * quantity )
        elif ty == 'Put':
            values.append(equity_options.BS_PUT(S, K, T, r, sigma) * quantity)
        elif ty == 'Zero Bond':
            values.append(bond_risk.zero_bond_price(r, T) * quantity)
        else:
            values.append(None)
    return values

def calc_value(positions, sigma=0.2):
    value = calc_st_value(positions, sigma)
    return sum(value)
    
def calc_iv(price, positions, x0=0.2):
    def f(sigma):
        return calc_value(positions, sigma) - price

    try:
        sigma = optimize.newton(f, x0, maxiter=500, tol=0.001)
    except RuntimeError:
        sigma = np.nan
    
    return sigma

def calc_greeks(positions):
    
    deltas = []
    gammas = []
    for i, row in positions.iterrows():
        ty, quantity, S, K, T, sigma, r = row[['security_type', 'quantity', 'underlying_price', 'strike', 'tenor', 'iv', 'risk_free_rate']]

        delta, gamma = 0.0, 0.0
        if ty == 'Binary Call':
            delta, gamma = binary_option.call_delta_gamma(S, K, T, r, sigma)
        elif ty == 'Binary Put':
            delta, gamma = binary_option.put_delta_gamma(S, K, T, r, sigma)
        elif ty == 'Call':
            delta, gamma = equity_options.call_delta_gamma(S, K, T, r, sigma)
        elif ty == 'Put':
            delta, gamma = equity_options.put_delta_gamma(S, K, T, r, sigma)
            
        deltas.append(delta * quantity)
        gammas.append(gamma * quantity)
    return deltas, gammas

def calc_slides(positions, shocks):
    
    # shocks = [-0.99, -0.3, -0.2, -0.15, -0.1, -0.05, 0, 0.05, 0.1, 0.15, 0.2, 0.3, 1]
    
    pos = positions.copy()
    S0 = positions.iloc[0]['underlying_price']
    sigma = positions.iloc[0]['iv']
    V0 = calc_value(positions, sigma)

    values = []
    for s in shocks:
        pos['underlying_price'] = S0 * (1+s)
        value = calc_value(pos, sigma)        
        values.append(value / V0 - 1) # percentage return

    df = pd.DataFrame({'shock': shocks, 'value': values})
    return df
    
#####################################################################################
# auxilary
def read_replica_db(security_ids: list) -> pd.DataFrame:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM sn_replica WHERE security_id = ANY(%s)', (security_ids,))
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    replica = pd.DataFrame(rows, columns=cols)
    for col, typ in SECURITIES_DTYPE.items():
        if col in replica.columns:
            replica[col] = replica[col].astype(typ)
    return replica

def _read_securities_xl(xl_file, tab='Securities'):
    securities = pd.read_excel(xl_file, sheet_name=tab, engine='openpyxl')
    securities.columns = [x.replace(' ', '') for x in securities.columns]
    for col, typ in SECURITIES_DTYPE.items():
        if col in securities.columns:
            securities[col] = securities[col].astype(typ)
    return securities



def _get_underlying_prices(sec_ids: list, as_of_date) -> dict:
    """Batch-fetch most recent close price on or before as_of_date from current_price."""
    if not sec_ids:
        return {}
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON ("SecurityID") "SecurityID", "Close"
                FROM current_price
                WHERE "Date" <= %s AND "SecurityID" = ANY(%s)
                ORDER BY "SecurityID", "Date" DESC
                """,
                (as_of_date, sec_ids),
            )
            return {row[0]: float(row[1]) for row in cur.fetchall()}


def _get_slide_shocks(slide_name: str) -> list:
    """Return sorted list of shocks for slide_name from slide_shocks table."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT shock FROM slide_shocks WHERE slide_name = %s ORDER BY shock',
                (slide_name,),
            )
            return [float(row[0]) for row in cur.fetchall()]


def _save_replica_calc(records: list, security_ids: list, as_of_date) -> None:
    """Delete existing rows for all security_ids and batch-insert all records."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'DELETE FROM sn_replica_calc WHERE security_id = ANY(%s) AND as_of_date = %s',
                (security_ids, as_of_date),
            )
            execute_batch(
                cur,
                """INSERT INTO sn_replica_calc
                       (security_id, as_of_date, position_id,
                        underlying_price, tenor, risk_free_rate, iv, value, delta, gamma)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                records,
            )
        conn.commit()


def calculate(security_ids: list, as_of_date, sn_prices: dict) -> tuple:
    """Return (pnl, sens) for a list of structured notes.

    Batches all DB and distribution lookups, then loops per security to calibrate
    and interpolate. Saves all intermediate results in one DB write at the end.

    Args:
        security_ids: list of structured note security IDs
        as_of_date:   valuation date
        sn_prices:    {security_id: market_price} note prices for $1 notional
    Returns:
        pnl:  DataFrame (scenarios × security_id) of P&L returns
        sens: DataFrame (security_id, delta, gamma, iv) of aggregated sensitivities
    """
    # 1. Batch read all replicas
    all_replicas = read_replica_db(security_ids)

    missing = [sid for sid in security_ids if sid not in all_replicas['security_id'].values]
    if missing:
        print(f'Warning: skipping security_ids not found in sn_replica: {missing}')
        security_ids = [sid for sid in security_ids if sid not in missing]
    if not security_ids:
        return pd.DataFrame(), pd.DataFrame()

    # 2. Batch fetch underlying prices and apply to all rows
    und_sec_ids = all_replicas['underlying_sec_id'].unique().tolist()
    price_map = _get_underlying_prices(und_sec_ids, as_of_date)
    all_replicas['underlying_price'] = all_replicas['underlying_sec_id'].map(price_map)

    # 3. Tenor & date for all rows, filter expired legs
    all_replicas['price_date'] = as_of_date
    all_replicas['tenor'] = (
        pd.to_datetime(all_replicas['maturity']) - pd.to_datetime(all_replicas['price_date'])
    ).dt.days / 365
    all_replicas = all_replicas[all_replicas['tenor'] > 0].copy()
    all_replicas['risk_free_rate'] = all_replicas['tenor'].apply(
        lambda t: _ust_get_rate(t, as_of_date)
    )

    # 4. Shocks and underlying distributions (same for all securities)
    shocks = _get_slide_shocks('structured note')
    und_dist_df = var_utils.get_dist(und_sec_ids)

    # 5. Per-security loop: calibration, slides, distribution
    all_records = []
    results = {}
    notional_map = {}
    for security_id in security_ids:
        replica = all_replicas[all_replicas['security_id'] == security_id].copy()

        note_notional = float(replica['note_notional'].iloc[0])
        notional_map[security_id] = note_notional

        iv = calc_iv(sn_prices[security_id] * note_notional, replica)
        replica['iv'] = iv
        replica['value'] = calc_st_value(replica, sigma=iv)
        deltas, gammas = calc_greeks(replica)
        replica['delta'] = deltas
        replica['gamma'] = gammas

        for _, row in replica.iterrows():
            all_records.append((
                security_id, as_of_date, row['position_id'],
                float(row['underlying_price']), float(row['tenor']),
                float(row['risk_free_rate']), float(row['iv']),
                float(row['value']), float(row['delta']), float(row['gamma']),
            ))

        slides = calc_slides(replica, shocks)
        und_sec_id = replica['underlying_sec_id'].iloc[0]
        dist_values = np.interp(und_dist_df[und_sec_id], xp=slides['shock'], fp=slides['value'])
        results[security_id] = dist_values

    # 6. Batch save all intermediate results
    _save_replica_calc(all_records, security_ids, as_of_date)

    # 7. Aggregate per-leg results into per-security sensitivities, normalised by note_notional
    calc_df = pd.DataFrame(all_records, columns=[
        'security_id', 'as_of_date', 'position_id', 'underlying_price',
        'tenor', 'risk_free_rate', 'iv', 'value', 'delta', 'gamma',
    ])
    sens = calc_df.groupby('security_id').agg(
        delta=('delta', 'sum'),
        gamma=('gamma', 'sum'),
        iv=('iv', 'mean'),
    ).reset_index()
    note_notionals = sens['security_id'].map(notional_map)
    sens['delta'] = sens['delta'] / note_notionals
    sens['gamma'] = sens['gamma'] / note_notionals

    return pd.DataFrame(results), sens


def calculate_debug(security_ids: list, as_of_date, sn_prices: dict) -> Path:
    """Run calculate() and write all intermediate results to an Excel file.

    Each step gets its own sheet. Returns the path to the output file.
    """
    sheets = {}

    # Step 1: raw replica from DB
    all_replicas = read_replica_db(security_ids)
    sheets['1_replica'] = all_replicas.copy()

    missing = [sid for sid in security_ids if sid not in all_replicas['security_id'].values]
    if missing:
        print(f'Warning: skipping security_ids not found in sn_replica: {missing}')
        security_ids = [sid for sid in security_ids if sid not in missing]
    if not security_ids:
        return None

    # Step 2: underlying prices
    und_sec_ids = all_replicas['underlying_sec_id'].unique().tolist()
    price_map = _get_underlying_prices(und_sec_ids, as_of_date)
    all_replicas['underlying_price'] = all_replicas['underlying_sec_id'].map(price_map)
    sheets['2_underlying_price'] = all_replicas.copy()

    # Step 3: tenor, date, filter, risk-free rate
    all_replicas['price_date'] = as_of_date
    all_replicas['tenor'] = (
        pd.to_datetime(all_replicas['maturity']) - pd.to_datetime(all_replicas['price_date'])
    ).dt.days / 365
    all_replicas = all_replicas[all_replicas['tenor'] > 0].copy()
    all_replicas['risk_free_rate'] = all_replicas['tenor'].apply(
        lambda t: _ust_get_rate(t, as_of_date)
    )
    sheets['3_tenor_rfr'] = all_replicas.copy()

    # Step 4: shocks and underlying distributions
    shocks = _get_slide_shocks('structured note')
    sheets['4_shocks'] = pd.DataFrame({'shock': shocks})
    und_dist_df = var_utils.get_dist(und_sec_ids)
    sheets['4_und_dist'] = und_dist_df.copy()

    # Step 5: per-security calibration, slides, distribution
    all_records = []
    results = {}
    notional_map = {}
    for security_id in security_ids:
        replica = all_replicas[all_replicas['security_id'] == security_id].copy()

        note_notional = float(replica['note_notional'].iloc[0])
        notional_map[security_id] = note_notional

        iv = calc_iv(sn_prices[security_id] * note_notional, replica)
        replica['iv'] = iv
        replica['value'] = calc_st_value(replica, sigma=iv)
        deltas, gammas = calc_greeks(replica)
        replica['delta'] = deltas
        replica['gamma'] = gammas
        sheets[f'5_{security_id}_calib'] = replica.copy()

        for _, row in replica.iterrows():
            all_records.append((
                security_id, as_of_date, row['position_id'],
                float(row['underlying_price']), float(row['tenor']),
                float(row['risk_free_rate']), float(row['iv']),
                float(row['value']), float(row['delta']), float(row['gamma']),
            ))

        slides = calc_slides(replica, shocks)
        sheets[f'5_{security_id}_slides'] = slides.copy()

        und_sec_id = replica['underlying_sec_id'].iloc[0]
        dist_values = np.interp(und_dist_df[und_sec_id], xp=slides['shock'], fp=slides['value'])
        results[security_id] = dist_values

    # Step 6: sensitivities and final distribution (no DB write in debug mode)
    calc_df = pd.DataFrame(all_records, columns=[
        'security_id', 'as_of_date', 'position_id', 'underlying_price',
        'tenor', 'risk_free_rate', 'iv', 'value', 'delta', 'gamma',
    ])
    sheets['6_calc'] = calc_df

    sens = calc_df.groupby('security_id').agg(
        delta=('delta', 'sum'),
        gamma=('gamma', 'sum'),
        iv=('iv', 'mean'),
    ).reset_index()
    note_notionals = sens['security_id'].map(notional_map)
    sens['delta'] = sens['delta'] / note_notionals
    sens['gamma'] = sens['gamma'] / note_notionals
    sheets['6_sens'] = sens

    dist = pd.DataFrame(results)
    sheets['6_dist'] = dist

    # Write all sheets to Excel
    output_dir = Path(__file__).parent / 'test_output'
    output_dir.mkdir(parents=True, exist_ok=True)
    xl_file = output_dir / f'debug.{as_of_date.strftime("%Y%m%d")}.xlsx'
    with pd.ExcelWriter(xl_file, engine='openpyxl') as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f'Debug output written to {xl_file}')
    return xl_file


#####################################################################################
# Test function

def test():
    as_of_date = datetime.strptime('2026-06-23', '%Y-%m-%d')
    security_ids = ['T10001619', 'T10001618']
    sn_prices = {'T10001619': 0.9922, 'T10001618': 0.9850}
    calculate_debug(security_ids, as_of_date, sn_prices)

if __name__ == '__main__':
    test()

