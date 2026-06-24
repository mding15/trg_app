# -*- coding: utf-8 -*-
"""
Created on Sat May 17 21:16:37 2025

@author: mgdin
"""
import sys
from pathlib import Path
# sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from database2 import pg_connection
from utils import mkt_data
from detl import yh_extract
from mkt_data import mkt_data_info, mkt_timeseries


def _pg_df(sql, params=None):
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)


    
# pull historical prices from YH, save to db and hdf, update mkt_data_info table
def extract_yh_price(security_ids=None, tickers=None):

    # get SourceID and SecurityID
    df = get_yh_source_id(security_ids, tickers)
    
    # pull YH historical prices and save to db
    yh_extract.update_hist_price(df['SourceID'].to_list() )
                      
    # copy data from db to hdf
    for _, row in df.iterrows():
        ticker, sec_id = row[['SourceID', 'SecurityID']]
        # print(ticker, sec_id)
        copy_yh_from_db(ticker, sec_id)
        
    # update table mkt_data_info
    mkt_data_info.update_stat_by_sec_id(df['SecurityID'].to_list(), 'YH', 'PRICE')

def get_yh_source_id(security_ids=None, tickers=None):
    from database2 import pg_connection
    if security_ids and tickers:
        sql = """
            SELECT * FROM mkt_data_source
            WHERE "Source" = 'YH'
              AND ("SecurityID" = ANY(%s) OR "SourceID" = ANY(%s))
        """
        params = (security_ids, tickers)
    elif security_ids:
        sql = 'SELECT * FROM mkt_data_source WHERE "Source" = \'YH\' AND "SecurityID" = ANY(%s)'
        params = (security_ids,)
    elif tickers:
        sql = 'SELECT * FROM mkt_data_source WHERE "Source" = \'YH\' AND "SourceID" = ANY(%s)'
        params = (tickers,)
    else:
        sql = 'SELECT * FROM mkt_data_source WHERE "Source" = \'YH\''
        params = None

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)

# copy yh from  db to hdf
def yh_db_2_hdf():
    df = _pg_df('SELECT * FROM mkt_data_source WHERE "Source" = \'YH\'')
    for _, row in df.iterrows():
        ticker, sec_id = row[['SourceID', 'SecurityID']]
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
    df = _pg_df('SELECT * FROM yh_stock_price WHERE ticker = %s', (ticker,))
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
    df = _pg_df('SELECT "SecurityID" FROM current_security')
    return df['SecurityID'].to_list()

def get_yh_sec_list():
    df = _pg_df('SELECT "SecurityID" FROM security_xref WHERE "REF_TYPE" = \'YH\'')
    return df['SecurityID'].to_list()


def test():
    extract_yh_price(tickers=['AAPL'])


if __name__ == '__main__':
    test()
