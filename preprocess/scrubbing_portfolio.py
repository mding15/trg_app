# -*- coding: utf-8 -*-
"""
Created on Sat May 31 14:47:37 2025

@author: mgdin
"""
import pandas as pd
from psycopg2.extras import execute_batch

from database import db_utils
from database import pg_connection
from database import model_aux
from mkt_data import mkt_timeseries
from engine import validate_positions

def scrubbing_portfolio(params, positions, limit):

    port_id = params['port_id']    
    market_value = positions['MarketValue'].sum()
    as_of_date = params['AsofDate']

    # insert params, positions into db 
    save_portfolio_to_db(params, positions, limit)
    
    # add security info 
    positions = add_security_info(port_id)

    # filter out unknown positions    
    unknown_positions = positions[positions['unknown_security']]
    positions = positions[positions['unknown_security']==False]
    
    # check positions if it is empty. If empty, raise error
    if positions.empty:
        raise Exception(f"All positions are unknown. No available positions found.")
    
    # update position prices
    positions = update_position_price(port_id, positions, as_of_date)
    
    # check duplicated securities
    check_duplicates(positions)

    # add option data
    positions = add_option_data(params, positions)
    
    # update db        
    print('update database')
    model_aux.update_portfolio_status(port_id, status='running', 
                                      as_of_date=params['AsofDate'], 
                                      market_value=float(market_value), 
                                      tail_measure=params['TailMeasure'], 
                                      risk_horizon=params['RiskHorizon'], 
                                      benchmark=params['Benchmark'])
    print('scrubbing data is successful')
    return params, positions, unknown_positions


# save position and parameters into database
def save_portfolio_to_db(params, positions, limit):
    
    # save params
    df = pd.DataFrame([params])
    db_utils.insert_df('port_parameters', df, 'port_id')

    # save positions
    df = positions
    db_utils.insert_df('port_positions', df, 'port_id')
    
    # save limit
    limit_data = []
    for limit_category, limit_value in limit.items():
        if limit_category != 'port_id':
            limit_data.append({
                'port_id': limit['port_id'],
                'limit_category': limit_category,
                'limit_value': limit_value
            })
    
    df = pd.DataFrame(limit_data)
    db_utils.insert_df('port_limit', df, 'port_id')

    
def add_security_info(port_id):
    db_utils.call_procedure('UpdateSecurityInfo', (port_id,))

    df = db_utils.get_sql_df('select * from port_positions where port_id=%(port_id)s', {'port_id': port_id})

    return df

def update_position_price(port_id, positions, as_of_date):
    positions = positions[~positions['SecurityID'].isna()]
    sec_ids = positions['SecurityID'].to_list()
    prices = mkt_timeseries.get_last_prices(sec_ids, as_of_date)
    
    # update position price
    prices = prices.rename(columns={'Price': 'xPrice', 'PriceDate': 'xPriceDate'})
    pos = positions.merge(prices, on='SecurityID', how='left')
    mask = ~pos['xPrice'].isna() & (pos['LastPriceDate'] < pos['xPriceDate'])
    
    pos.loc[mask, 'LastPrice'] = pos.loc[mask, 'xPrice']
    pos.loc[mask, 'LastPriceDate'] = pos.loc[mask, 'xPriceDate']
    positions = pos.drop(['xPrice', 'xPriceDate'], axis=1)
    
    # Set cash price to 1
    mask = positions['asset_class'] == 'Cash'
    positions.loc[mask, 'LastPrice'] = 1
    positions.loc[mask, 'LastPriceDate'] = as_of_date
    
    # implied price
    mask = positions['LastPrice'].isna() & ~positions['Quantity'].isna()
    positions.loc[mask, 'LastPrice'] = positions.loc[mask, 'MarketValue'] / positions.loc[mask, 'Quantity']
    positions.loc[mask, 'LastPriceDate'] = as_of_date
    
    # fallback price to 1
    mask = positions['LastPrice'].isna()
    positions.loc[mask, 'LastPrice'] = 1 
    positions.loc[mask, 'LastPriceDate'] = as_of_date
    
    # update market value    
    positions['MarketValue'] =  positions['Quantity'] * positions['LastPrice']

    
    # update database
    update_sql = f"""
        UPDATE port_positions
        SET "LastPrice" = %s, "LastPriceDate" = %s, "MarketValue" = %s
        WHERE "ID" = %s and port_id = {port_id}
    """

    update_data = [(row.LastPrice, row.LastPriceDate, row.MarketValue, row.ID) for _, row in positions.iterrows()]
    with pg_connection() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, update_sql, update_data)
            conn.commit()

    return positions

# df = positions
def check_duplicates(df):
    df = df[~df['SecurityID'].isna()]
    dups = df.duplicated(subset=['SecurityID'])
    dup_secs = df[dups]['SecurityID'].to_list()
    if dup_secs:
        error = 'Found duplicated securities:\n' + df[df['SecurityID'].isin(dup_secs)].to_csv(index=False)
        raise Exception(error)


def add_option_data(params, positions):
    errors = []
    positions = validate_positions.check_options(positions, errors)
    if errors:
        raise Exception('\n'.join(errors))
        
    return positions

def test():
    port_id = 5063
    df = add_security_info(port_id)
