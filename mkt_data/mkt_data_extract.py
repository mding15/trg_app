# -*- coding: utf-8 -*-
"""
Created on Sat May 17 21:16:37 2025

@author: mgdin
"""
import pandas as pd

from database import db_utils
from utils import mkt_data
from detl import yh_extract
from mkt_data import mkt_data_info, mkt_timeseries


    
# pull historical prices from YH, save to db and hdf, update mkt_data_info table
def extract_yh_price(ticker_filter=None):

    # get SourceID and SecurityID
    df = get_yh_source_id(ticker_filter)
    
    # pull YH historical prices and save to db
    yh_extract.update_hist_price(df['SourceID'].to_list() )
                      
    # copy data from db to hdf
    for _, row in df.iterrows():
        ticker, sec_id = row[['SourceID', 'SecurityID']]
        # print(ticker, sec_id)
        copy_yh_from_db(ticker, sec_id)
        
    # update table mkt_data_info
    mkt_data_info.update_stat_by_sec_id(df['SecurityID'].to_list(), 'YH', 'PRICE')

# return df['SecurityID', 'SourceID']
# in param: ticker_filter: filter for the tickers
def get_yh_source_id(ticker_filter):
    # YH tickers
    query = """
    select * from mkt_data_source where "Source" ='YH'
    """
    df =db_utils.get_sql_df(query)

    if ticker_filter:
        df = df[df['SourceID'].isin(ticker_filter)]
    
    return df

# copy yh from  db to hdf
def yh_db_2_hdf():
    # YH tickers
    query = """
    select * from mkt_data_source where "Source" ='YH'
    """
    df =db_utils.get_sql_df(query)
    for _, row in df.iterrows():
        ticker, sec_id = row[['SourceID', 'SecurityID']]
        # print(ticker, sec_id)
        copy_yh_from_db(ticker, sec_id)
    
def yh_stat():
    sec_list = get_yh_sec_list()
    prices = mkt_timeseries.get(sec_list)
    stat = mkt_data_info.calc_stat(prices)

    file_path = yh_extract.get_stat_file() 
    stat.to_csv(file_path, index=False)
    print(f'saved file: {file_path}')

def curr_sec_stat():
    sec_list = get_current_sec_list()
    prices = mkt_timeseries.get(sec_list)
    stat = mkt_data_info.calc_stat(prices)

    file_path = yh_extract.get_stat_file() 
    stat.to_csv(file_path, index=False)
    print(f'saved file: {file_path}')

########################################################################################    
def copy_yh_from_db(ticker, sec_id):
    
    # ticker = 'SPY'
    # sec_id = 'T10000108'
    print(f'copy yh stock price: ticker={ticker}, security_id={sec_id}')
    
    # get hdf data
    hdf_ts = mkt_data.get_market_data([sec_id]) 
    end_date = hdf_ts.index.max()
    
    # get data from db
    df = db_utils.get_sql_df(f"select * from yh_stock_price where ticker= '{ticker}'")
    if len(df) == 0:
        return
    
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    df = df[['close']].rename(columns={'close': sec_id})
    if len(hdf_ts) > 0:
        df = df[df.index > end_date]

    if len(df) > 0:    
        ts = pd.concat([hdf_ts, df])

        # save to hdf
        mkt_data.save_market_data(ts, source='YH', category='PRICE')    
    
def test_copy_yh_from_db_hdf():
    ticker, sec_id = 'COIN', 'T10001583'
    copy_yh_from_db(ticker, sec_id)
    
#######################################
# auxilary
def get_current_sec_list():
    df =db_utils.get_sql_df('select * from current_security')
    sec_list = df['SecurityID'].to_list()
    return sec_list    
    
def get_yh_sec_list():
    query = """
    select * from security_xref where "REF_TYPE" ='YH'
    """
    df =db_utils.get_sql_df(query)
    return df['SecurityID'].to_list()
