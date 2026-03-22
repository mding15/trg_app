# -*- coding: utf-8 -*-
"""
Created on Mon Nov 17 11:34:21 2025

@author: mgdin

source market data

"""
import pandas as pd
import xlwings as xw
from database import db_utils
from utils import xl_utils, stat_utils
from mkt_data import mkt_data_extract
from mkt_data import sync_mkt_data, mkt_timeseries
from detl import YH_API


file_path=r'C:\Users\mgdin\OneDrive\Documents\dev\TRG_App\Workbooks\mkt_data_source_wb.xlsx'

def main():
    wb = xw.Book(file_path)

    # market data source 
    sql = """
    select * from mkt_data_source where is_active=1
    """

    df = db_utils.get_sql_df(sql)    
    xl_utils.add_df_to_excel(df, wb, 'mkt_data_source', index=False)    

    # pull YH hist data 
    # run from prod2
    # bin/yh_pull.sh
    
    sql = """
        select  mds."SecurityID", y.ticker, max(y.date) as max_date, count(*) as price_count
        from yh_stock_price y, mkt_data_source mds 
        where y.ticker = mds."SourceID" and mds."Source" = 'YH'
        group by mds."SecurityID", y.ticker 
    """
    df = db_utils.get_sql_df(sql)    
    xl_utils.add_df_to_excel(df, wb, 'yh_stock_price', index=False)    

    # copy mkt_data from aws to local
    sec_list = df['SecurityID'].to_list()
    len(sec_list )

    BATCH_SIZE = 20
    for i in range(0, len(sec_list), BATCH_SIZE):
        batch = sec_list[i: i+BATCH_SIZE]
        sync_mkt_data.sync_mkt_data(batch)
        
    # local hist price stat
    data = []
    BATCH_SIZE = 50
    for i in range(0, len(sec_list), BATCH_SIZE):
        batch = sec_list[i: i+BATCH_SIZE]
        prices = mkt_timeseries.get(batch, category='PRICE')
        df = stat_utils.hist_stat(prices)    
        data.append(df)
    
    hist_stat = pd.concat(data)
    xl_utils.add_df_to_excel(hist_stat, wb, 'hist_stat')    


    
def test_yh(wb):
    ticker = '^GSPC'
    df_price, df_div = YH_API.GET_HISTORY(ticker)    
    xl_utils.add_df_to_excel(df_price, wb, 'df_price', index=False)    
    xl_utils.add_df_to_excel(df_div, wb, 'df_div', index=False)    
    
    tickers = ['SPY', 'QQQ']
    df = YH_API.GET_QUOTES(tickers)
    xl_utils.add_df_to_excel(df, wb, 'yh_quote', index=False)    

    wb1 = xw.Book('Postgres.xlsx')
    df = xl_utils.read_df_from_excel(wb1, 'mkt_data_source')
    tickers = df['SourceID'].to_list()
