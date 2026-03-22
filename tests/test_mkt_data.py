# -*- coding: utf-8 -*-
"""
Created on Sun Mar  2 11:25:00 2025

@author: mgdin
"""
import pandas as pd
import xlwings as xw
from mkt_data import mkt_timeseries, sync_mkt_data
from security import security_info
from utils import xl_utils

def test():
    wb = xw.Book('Book1')

    # get all market data IDs    
    df = mkt_timeseries.get_mkt_data_sec_list()
    xl_utils.add_df_to_excel(df, wb, 'mkt_list')

    # get securityID, then get timeseries
    
    tickers = ['SPY', 'QQQ']    
    tickers = ['BM_0_100','BM_100_0','BM_10_90','BM_20_80','BM_30_70','BM_40_60','BM_50_50','BM_60_40','BM_70_30','BM_80_20','BM_90_10']
    df = security_info.get_ID_by_Ticker(tickers)
    sec_ids = df['SecurityID'].to_list()
    id_map = df.set_index('SecurityID')['Ticker'].to_dict()
    
    # get timeseries
    df = mkt_timeseries.get(sec_ids)
    # from_date='2020-01-01'
    # to_date='2025-02-28'
    # category='PRICE'
    # df = mkt_timeseries.get(sec_ids, from_date, to_date, category)
    
    # Get timeseries from server
    df = sync_mkt_data.api_get_market_data(sec_ids)
    
    # write to excel
    df = df.rename(columns=id_map)
    xl_utils.add_df_to_excel(df, wb, 'timeseries')

def ad_hoc():
    wb = xw.Book('Book1')
    
    # get sec_list from positions
    positions = xl_utils.read_df_from_excel(wb, 'Positions')
    positions['SecurityID'] = security_info.get_SecurityID_by_ref(positions)
    df = positions 
    
    # get market data
    sec_ids = positions['SecurityID'].tolist()
    
    df = mkt_timeseries.get(sec_ids) # from local
    df = sync_mkt_data.api_get_market_data(sec_ids) # from remote

    id_map = positions.set_index('SecurityID')['Ticker'].to_dict()
    df = df.rename(columns=id_map)

    xl_utils.add_df_to_excel(df, wb, 'price_hist')
    xl_utils.add_df_to_excel(positions, wb, 'pos')


