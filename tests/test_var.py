# -*- coding: utf-8 -*-
"""
Created on Thu Oct  2 10:34:00 2025

@author: mgdin
"""

import os
import pandas as pd
import xlwings as xw
from pathlib import Path
from trg_config import config
from api import app
app.app_context().push()

from api import request_handler
from api import portfolios, scrubbing_portfolio
from api import create_account
from api import run_calculation 
from utils import xl_utils, tools
from utils import var_utils
from engine import VaR_engine
from report import powerbi as pbi
from security import security_info
from preprocess import upload_security, portfolio_utils
from preprocess import read_portfolio
from preprocess import scrubbing_portfolio as scrub
from process import process
from database import db_utils, model_aux, ms_sql_server
from database import sync_report_mapping
from database.models import User as User
from mkt_data import mkt_timeseries
from report import preprocess, performance, sharpe_ratio, back_test

PROJ_DIR=Path(r'C:\Users\mgdin\OneDrive - tailriskglobal.com\Documents - TRG Project\Research and Development\Prototype\VaR')
test_wb = xw.Book(PROJ_DIR / 'test_var.xlsx')
test_wb = xw.Book('Book1')
wb = test_wb

def test_var():
    file_path = Path.home() / 'Downloads' / 'MS_9.30.2025.xlsx'

    params, positions, unknown_positions = pre_process(file_path)
        
    DATA = cal_var(positions, params)

    xl_utils.add_df_to_excel(params, test_wb, 'params', index=False)
    xl_utils.add_df_to_excel(positions, test_wb, 'positions', index=False)

    # delete portfolio
    port_id_list = [5307, 5308, 5309]
    portfolios.delete_portfolios(port_id_list)

def pre_process(file_path):
    
    username = os.environ['test_username']
    
    # save portfolio
    user, file_path, port_name = portfolios.test_save_portfolio_file(file_path, username)
    
    # create a new portfolio in table portfolio_info
    port = process.create_portfolio(user, file_path.name, port_name)

    # read input file
    params, positions, limit = process.read_input_file(port)
    
    # scrube data        
    params, positions, unknown_positions = scrub.scrubbing_portfolio(params, positions, limit)       

    print(f'port_id: {port.port_id}')
    
    return params, positions, unknown_positions
    

def cal_var(positions, params, wb):
    DATA = VaR_engine.calc_VaR(positions, params)
    if 'Error' in DATA:
        print(DATA['Error'])
    
    # write VaR results
    write_results(DATA, wb)
    
    return DATA
    
def write_results(results, wb):
    for tab, df in results.items():
        if tab in ['RF_PnL']:
            continue
        if isinstance(df, dict):
            df = pd.DataFrame([df])
        
        xl_utils.add_df_to_excel(df, wb, tab)
    
    
def get_security_attributes(sec_ids):

    sec_id_list = ','.join([f"'{x}'" for x in sec_ids if x is not None])
    
    sql = f"""
        select security_id, expected_return, currency, "class", sc1, sc2, country, region,sector, 
            industry, option_type, payment_frequency,maturity_date,
            option_strike,underlying_security_id,coupon_rate
        from security_attribute
        where security_id in ({sec_id_list})
    """
    sa = db_utils.get_sql_df(sql)
    
    return sa
    

    

    
    
    
def get_modeled_securities(positions):
    sec_ids = positions['SecurityID'].to_list()
    
    sec_id_list = ','.join([f"'{x}'" for x in sec_ids if x is not None])
    
    sql = f"""
    select rf."SecurityID" from risk_factor rf, risk_model rm 
    where rf.model_id = rm.model_id and rm.is_current=1
    and rf."SecurityID" in ({sec_id_list})
    union 
    select si."SecurityID" from security_info si where si."AssetType" = 'Treasury'
    and si."SecurityID" in ({sec_id_list})
    """
    rf = db_utils.get_sql_df(sql)
    
    return rf['SecurityID'].unique()
    
    
    

###############################################################################    
def ad_hoc():
    sql = """
    select * from port_positions pp where pp.port_id = 5269
    """
    
    df = db_utils.get_sql_df(sql)

    xl_utils.add_df_to_excel(df, test_wb, 'db', index=False)
    
    
###############################################################################    
def read_portfolio_file(file_path):
    # read input file
    
    params, positions, limit = read_portfolio.read_input_file(file_path)
    
    port_id = 0
    positions['port_id'] = port_id
    params['port_id'] = port_id
    params['PortfolioName'] = 'Test'
    limit['port_id'] = port_id

    xl_utils.add_df_to_excel(params, test_wb, tab='params', index=False)
    xl_utils.add_df_to_excel(positions, test_wb, tab='positions', index=False)
    xl_utils.add_df_to_excel(limit, test_wb, tab='limit', index=False)
    
    return params, positions, limit

def drop_columns(df, columns):
    columns = set(df.columns) & set(columns)
    return df.drop(columns=columns)

def scrub_data(params, positions):

    port_id = params['port_id']    
    as_of_date = params['AsofDate']

    # scrube data   
    new_columns = ['AssetClass', 'AssetType', 
                    'ExpectedReturn', 'Currency', 'Class','SC1','SC2','Country','Region','Sector','Industry','OptionType',
                    'PaymentFrequency', 'MaturityDate', 'OptionStrike','UnderlyingSecurityID','CouponRate']
    positions = drop_columns(positions, new_columns)
    
    # SecurityID
    positions['SecurityID'] = security_info.get_SecurityID_by_ref(positions)
    sec_ids = positions[~positions['SecurityID'].isna()]['SecurityID'].to_list()

    # unknown security_id
    positions['unknown_security'] = positions['SecurityID'].isna()
    
    # security is not modeled
    modeled_secs = get_modeled_securities(positions)
    positions.loc[~positions['SecurityID'].isin(modeled_secs),'unknown_security'] = True
    
    # AssetClass, AssetType    
    sec_info = security_info.get_security_by_ID(positions['SecurityID'].unique())
    sec_info = sec_info.set_index('SecurityID')[['AssetClass', 'AssetType']]
    positions = positions.merge(sec_info, on="SecurityID", how="left")

    # security attributes
    sa = get_security_attributes(sec_ids)
    sa = sa.rename(columns={ 'security_id':'SecurityID', 'expected_return': 'ExpectedReturn', 'currency': 'Currency',
                             'class': 'Class', 'sc1': 'SC1', 'sc2': 'SC2', 
                             'country': 'Country', 'region': 'Region', 
                             'sector' : 'Sector', 'industry': 'Industry',
                             'option_type': 'OptionType', 
                             'payment_frequency': 'PaymentFrequency',
                             'maturity_date': 'MaturityDate',
                             'option_strike':'OptionStrike',
                             'underlying_security_id':'UnderlyingSecurityID',
                             'coupon_rate': 'CouponRate'
                             })
    positions = positions.merge(sa, on="SecurityID", how="left")

    # exclude maturity < asOfDate        
    positions.loc[positions['MaturityDate'] < as_of_date, 'unknown_security'] = True
    
    # update options
    positions.loc[positions['OptionType'].isin(['Call', 'Put']), 'is_option'] = True
    positions['UnderlyingID'] = None

    # filter out unknown positions    
    unknown_positions = positions[positions['unknown_security']]
    positions = positions[positions['unknown_security']==False]
    
    # check positions if it is empty. If empty, raise error
    if positions.empty:
        raise Exception(f"All positions are unknown. No available positions found.")
    
    # update position prices
    positions = update_position_price(positions, as_of_date)
    
    # check duplicated securities
    scrubbing_portfolio.check_duplicates(positions)
    
    # add option data
    positions = scrubbing_portfolio.add_option_data(params, positions)

    return params, positions, unknown_positions

def update_position_price(positions, as_of_date):
    positions = positions[~positions['SecurityID'].isna()]
    sec_ids = positions['SecurityID'].to_list()
    prices = mkt_timeseries.get_last_prices(sec_ids, as_of_date)
    
    # update position price
    prices = prices.rename(columns={'Price': 'LastPrice', 'PriceDate': 'LastPriceDate'})
    positions = positions.merge(prices, on='SecurityID', how='left')
    
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
    
    return positions