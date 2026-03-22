# -*- coding: utf-8 -*-
import xlwings as xw
import pandas as pd

from trg_config import config
from mkt_data import mkt_timeseries    
from utils import api_utils, mkt_data
from security import security_info
from api import data_pack
from utils import xl_utils

# copy market data from server to local
# tickers=tics
# sec_list=[]
def sync_mkt_data(sec_list, tickers=[]):
    # tickers=['SPY', 'AAPL']
    if tickers:
        sec_ids = security_info.get_ID_by_Ticker(tickers)['SecurityID'].to_list()
        sec_list.extend(sec_ids)
    
    if len(sec_list) == 0:
        return
        
    # get data from remote server
    df = api_get_market_data(sec_list)
        
    # update local hdf
    for sec_id in df.columns:
        print(f'sync security: {sec_id} ...')
        ts = df[[sec_id]].dropna()
        update_hdf(ts)

def test_sync_mkt_data():
    tickers=['SPY', 'AAPL']
    sec_list = ['T10001565', 'T10001566']
    sync_mkt_data(sec_list, tickers)

# df=ts    
def update_hdf(df, category='PRICE'):
    sec_id = df.columns[0]
    
    # get hdf data
    hdf_ts = mkt_data.get_market_data([sec_id], category=category) 
    end_date = hdf_ts.index.max()
    
    if len(hdf_ts) > 0:
        df = df[df.index > end_date]

    if len(df) > 0:
        ts = pd.concat([hdf_ts, df])
        ts = ts[~ts.index.duplicated(keep='last')]

        # save to hdf
        sec_id = ts.columns[0]
        print(f'update mkt_data for {sec_id}, length: {len(ts)}')
        mkt_data.save_market_data(ts, source='server', category=category)    

# from_date, to_date = '2018-01-01', '2023-12-31'
def api_get_market_data(sec_list, from_date=None, to_date=None, category='PRICE'):
    token = api_utils.login()

    # get timeseries
    payload ={
        "Request":   'MarketData',
        "Type":      'GetHistory',
        "Data Category":  category,
        "From Date":      from_date,
        "To Date":        to_date,
        "SecurityID":     sec_list
        }

    response = api_utils.request(token, 'data_request', payload)
    
    df = data_pack.extract_df(response, 'DATA')
    if not df.empty:
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.set_index('Date')
    return df
    # wb = xw.Book('Book1')
    # xl_utils.add_df_to_excel(df, wb, 'prices', index=False)    

