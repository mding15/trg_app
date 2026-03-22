# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 21:18:17 2024

@author: mgdin
"""
import pandas as pd
from pathlib import Path
import datetime
from database import pg_connection
from psycopg2.extras import execute_batch

from trg_config import config
from api import portfolios
from database import model_aux
from database import db
from database import db_utils
from preprocess import read_portfolio, portfolio_utils
from engine import validate_positions
from security import security_info
from models import risk_factors
from security import security_info as sc
from utils import tools, xl_utils
from mkt_data import mkt_timeseries


def scrubbing_portfolio(port_id):
    
    # port_id = 5003
    print(f'scrubbing portfolio: {port_id}')

    # portfolio_info
    port = model_aux.get_portfolio_by_id(port_id)
    if port is None:
        raise Exception(f'can not find portfolio {port_id}')
    
    # get file path
    file_path = portfolio_utils.get_port_file_path(port)
    if file_path.exists():
        print('scrubbing file: ' + str(file_path))
    else:
        raise Exception(f'file not found: {str(file_path)}')

    try:
        # read input file
        params, positions, limit = read_portfolio.read_input_file(file_path)
        
        # add port_id and other port_name
        positions['port_id'] = port_id
        params['port_id'] = port_id        
        params['PortfolioName'] = port.port_name
        limit['port_id'] = port_id
        market_value = positions['MarketValue'].sum()
        as_of_date = params['AsofDate']
        
        # insert params, positions, limit into db 
        portfolios.save_to_db(port_id, params, positions, limit)
        
        # add security info 
        positions = add_security_info(port_id)
        
        # update position prices
        positions = update_position_price(port_id, positions, as_of_date)
        
        # check duplicated securities
        check_duplicates(positions)
        
        # unknown security
        unknown_positions = positions[positions['unknown_security']]
        
        positions = positions[positions['unknown_security']==False]
        
        # add security prices
        # positions = add_security_prices(params, positions)

        # add option data
        positions = add_option_data(params, positions)
        
        # save params and positions to csv files
        portfolios.save_portfolio_by_port_id(port_id, params, positions, unknown_positions)

        # write unknown_position to uploaded portfolio file
        write_unknown_positions(file_path, unknown_positions)
        
        # update db        
        print('update database')
        model_aux.update_portfolio_status(port_id, status='running', 
                                          as_of_date=params['AsofDate'], 
                                          market_value=float(market_value), 
                                          tail_measure=params['TailMeasure'], 
                                          risk_horizon=params['RiskHorizon'], 
                                          benchmark=params['Benchmark'])
        print('scrubbing data is successful')
        
        
    except Exception as e:
        # save error to file
        error_file = get_error_filename(file_path)
        write_error_to_file(str(e), error_file)
        
        # update db
        relative_path = error_file.relative_to(config['CLIENT_DIR'])
        relative_path = Path(*relative_path.parts[1:])
        model_aux.update_portfolio_status(port_id, status='error', message=str(relative_path))
        
        raise Exception('Input file error')

def add_security_info(port_id):
    db_utils.call_procedure('UpdateSecurityInfo', (port_id,))

    df = db_utils.get_sql_df('select * from port_positions where port_id=%(port_id)s', {'port_id': port_id})

    return df

# update price and price_date, then market_value, then update the port_positions table
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

    
def check_data(params, positions):
    error_message = []

    # parameters
    params, errors = validate_positions.check_parameters(params)
    if errors:
        error_message.append('\nThe Parameter tab has the following errors:')
        error_message.extend(errors)
        
    # positions
    positions, errors = validate_positions.check_positions(positions)
    if errors:
        error_message.append('\nThe Position has the following errors:')
        error_message.extend(errors)
        
    if error_message:
        error_message.insert(0, 'Your input file has errors!')
        raise Exception('\n'.join(error_message))

    return params, positions
    
# *** retired function ***
def check_security(positions):
    # check security
    positions['SecurityID'] = sc.get_SecurityID_by_ref(positions)
    
    # check if there are duplicated securities
    check_duplicates(positions)
    
    # check if security is modeled
    modeled_secs = risk_factors.get_modeled_securities()

    # split positions
    known_positions    = positions[positions['SecurityID'].isin(modeled_secs)]
    unknown_positions  = positions.loc[positions.index.difference(known_positions.index)]
     
    return known_positions, unknown_positions

# df = positions
def check_duplicates(df):
    df = df[~df['SecurityID'].isna()]
    dups = df.duplicated(subset=['SecurityID'])
    dup_secs = df[dups]['SecurityID'].to_list()
    if dup_secs:
        error = 'Found duplicated securities:\n' + df[df['SecurityID'].isin(dup_secs)].to_csv(index=False)
        raise Exception(error)
    
    
# write unknown_position to uploaded portfolio file   
def write_unknown_positions(file_path, unknown_positions):
    book = {'Unknown Positions': unknown_positions}
    xl_utils.write_book_to_xl(book, file_path)

def get_error_filename(file_path):
        error_file = file_path.parent / f'{file_path.stem}.errors.csv'
        return error_file

def write_error_to_file(error, error_file):
        with open(error_file, 'w', newline='') as f:
            f.write(str(error))
        print(f'write errors to file: {str(error_file)}')
        

# **** retired function ***
# def add_security_attributes(positions):
#     positions = positions[POSITION_COLUMNS]
    
#     sec_ids = positions['SecurityID'].to_list()
#     df = get_security_attributes(sec_ids)
    
#     positions = positions.merge(df, on='SecurityID', how='left')
    
#     return positions

def add_security_prices(params, positions):
    
    df = get_last_prices(params, positions)
    
    positions['LastPrice'] = tools.df_series_merge(positions, df['LastPrice'], key='SecurityID')
    positions['LastPriceDate'] = tools.df_series_merge(positions, df['LastPriceDate'], key='SecurityID')
    
    return positions

def add_option_data(params, positions):
    errors = []
    positions = validate_positions.check_options(positions, errors)
    if errors:
        raise Exception('\n'.join(errors))
        
    return positions


from utils import date_utils, mkt_data

# get last available prices upto rpt_date
def get_last_prices(params, positions):
    sec_ids = positions['SecurityID'].unique()
    cob = params['AsofDate']

    from_date = date_utils.add_years(cob, -1)
    prices = mkt_data.get_market_data(sec_ids, from_date, cob)
    prices = prices.dropna(axis=1, how='all') # drop columns that do not have any value
    
    last_prices = pd.DataFrame(columns=['LastPrice', 'LastPriceDate'])
    for sec_id, data in prices.items():
        date = data.last_valid_index()
        price = data[date]
        last_prices.loc[sec_id] = [price, date]

    last_prices.index.name = 'SecurityID'
    
    # missing prices
    missing = set(sec_ids).difference(last_prices.index)
    if missing:
        # implied prices = mtm/quantity    
        imp = positions.set_index('SecurityID').loc[list(missing),['MarketValue', 'Quantity']].copy()
        imp['LastPrice'] = imp['MarketValue'] / imp['Quantity']
        imp['LastPriceDate'] = cob
        imp = imp[['LastPrice', 'LastPriceDate']]
        imp = imp[~imp['LastPrice'].isna()]
        last_prices = pd.concat([last_prices, imp[['LastPrice', 'LastPriceDate']]])
    missing = set(sec_ids).difference(last_prices.index)

    # assign cash price to 1    
    df = security_info.get_security_by_ID(missing)
    df = df[df['AssetClass']=='Cash']        
    df['LastPrice']=1
    df['LastPriceDate'] = cob
    df = df.set_index('SecurityID')
    last_prices = pd.concat([last_prices, df[['LastPrice', 'LastPriceDate']]])

    missing = set(sec_ids).difference(last_prices.index)
    if missing:
        msg = ', '.join(list(missing))
        raise Exception(f'Missing Price: {msg}')
    
    return last_prices

# from utils import xl_utils
# import xlwings as xw
# wb = xw.Book('Book5')
# xl_utils.add_df_to_excel(df2, wb, 'Pos2')

def get_security_attributes(sec_ids):
    
    # sec_ids = ['T10000006', 'T10000008', 'T10001091', 'T10001550', 'T10000108', 'T10000011', 'T10000880']

    sql = """
    select  securityid as "SecurityID", 
            expectedreturn as "ExpectedReturn",
            currency as "Currency",
            class as "Class",
            sc1 as "SC1",
            sc2 as "SC2",
            country as "Country",
            region as "Region",
            sector as "Sector",
            industry as "Industry",
            optiontype as "OptionType",
            paymentfrequency as "PaymentFrequency",
            maturitydate as "MaturityDate",
            optionstrike as "OptionStrike",
            underlyingsecurityid as "UnderlyingSecurityID",
            couponrate as "CouponRate"
            
    from security_attribute 
    where securityid = ANY(%s)
    """
    with db.engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=(sec_ids,))

    df = df.drop_duplicates(subset=['SecurityID'], keep='first')

    return df