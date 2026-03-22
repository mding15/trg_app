# -*- coding: utf-8 -*-
"""
Created on Sun May  4 16:25:01 2025

@author: mgdin
"""
import pandas as pd
import xlwings as xw
from utils import xl_utils, date_utils, mkt_data
from database import db_utils
from security import security_info
from mkt_data import mkt_data_info, mkt_timeseries, mkt_data_extract
from detl import yh_extract
from mkt_data import sync_mkt_data


def console():
    wb = xw.Book('Book1')

    # mkt_data_info
    df = mkt_data_info.get_mkt_data_info_df()
    xl_utils.add_df_to_excel(df, wb, 'md_info', index=False)    

    # sec_list
    df = mkt_timeseries.get_mkt_data_sec_list() 
    
    # df = sec_list
    xl_utils.add_df_to_excel(df, wb, 'sec_list')    
    
    # copy mkt_data from aws to local
    tickers = ['SPY', 'JEPI']
    sync_mkt_data.sync_mkt_data(tickers)


def get_timeseries(wb):
    tickers = ['SPY', 'AGG']
    id_to_ticker = get_IDs_to_tickers(tickers)
    sec_ids = id_to_ticker.keys()
    
    sec_ids = ['T10000001', 'T10001565', 'T10001566']
    df = mkt_timeseries.get(sec_ids)
    xl_utils.add_df_to_excel(df, wb, 'ts')    

def get_timeseries_by_tickers(wb):
    tickers = ['JEPI', 'SPY']
    id_to_ticker = get_IDs_to_tickers(tickers)
    
    df = mkt_timeseries.get(id_to_ticker.keys())
    df = df.rename(columns=id_to_ticker)    
    
    xl_utils.add_df_to_excel(df, wb, 'ts')    
    
# upload timeseries from workbook
def upload_timeseries_wb():
    wb = xw.Book('Book2')
    df = xl_utils.read_df_from_excel(wb, 'Prices', index=True)    

    mkt_timeseries.save(df, source='Spreadsheet', category='PRICE')
    
# get historical prices from YH for tickers
def get_yh_hist_prices(wb):
    tickers = ['JEPI', 'SPY']
    price_df, div_df = yh_extract.api_hist_price(tickers)
    xl_utils.add_df_to_excel(price_df, wb, 'price')    
    xl_utils.add_df_to_excel(div_df, wb, 'dividend')

    
def yh_stat(wb):
    sql = """
    select y.ticker, max(y.date) as max_date, count(*) as price_count
    from yh_stock_price y,  security_xref x
    where y.ticker = x."REF_ID" 
    and x."REF_TYPE" = 'YH' 
    group by y.ticker 
    order by max_date
    """

    df = db_utils.get_sql_df(sql)    
    xl_utils.add_df_to_excel(df, wb, 'yh_stat', index=False)
    
def mkt_data_stat(wb=None):
    
    # df = mkt_timeseries.get_mkt_data_sec_list()
    # sec_list = df[df['Category']=='PRICE']['SecurityID'].to_list()
    sec_list = mkt_data_extract.get_current_sec_list()
    sec_list = mkt_data_extract.get_yh_sec_list()

    prices = mkt_timeseries.get(sec_list)
    stat = mkt_data_info.calc_stat(prices)
    xl_utils.add_df_to_excel(stat, wb, 'stat4', index=False)    

# extract data from YH and insert into yh_stock_price table
def extract_yh_hist():
    
    # extract data from YH and insert into yh_stock_price table
    yh_extract.update_hist_price()

    # copy price data from db to hdf
    mkt_data_extract.extract_yh_price()
    
###############################################################################    
from sqlalchemy import text
from sqlalchemy import bindparam
import numpy as np
def get_yh_stock_prices(tickers):
    query = text("SELECT * FROM  yh_stock_price WHERE ticker IN :ids").bindparams(bindparam("ids", expanding=True))
    df = db_utils.get_sql_df(query, params={'ids': tickers})  
    
    pivot = pd.pivot_table(
        df,
        values='close',
        index='date',
        columns='ticker',
        aggfunc='mean',
        fill_value=np.nan  # replace NaN with 0
    )
    return pivot
        
def get_IDs_to_tickers(tickers):
    df = security_info.get_ID_by_Ticker(tickers)
    id_to_ticker = df.set_index('SecurityID')['Ticker'].to_dict()
    return id_to_ticker