# -*- coding: utf-8 -*-
"""
Created on Sat Mar 29 11:16:15 2025

@author: mgdin
"""
import pandas as pd
import numpy as np
import xlwings as xw
import datetime
from utils import xl_utils
from utils import date_utils
from mkt_data import mkt_timeseries

from database import db_utils
from database import ms_sql_server as msss

def test(DATA):
    wb = xw.Book('Fact_Consolidated.xlsx')    
    df = xl_utils.read_df_from_excel(wb, 'Consolidated')
    df['report_id']=123    
    msss.insert_df('dm_port_consolidated', df, 'report_id')    

    # get_port_returns(DATA)
    
    wb = xw.Book('Book1')
    positions = DATA['Positions']
    xl_utils.add_df_to_excel(positions, wb, 'positions', index=False)
    
# port_consolidated
def get_port_consolidated(DATA):
    
    df = DATA['Positions']
    df = df[['SecurityName', 'SecurityID', 'ISIN','CUSIP', 'Ticker',            
             'Currency', 'Class', 'SC1', 'SC2', 'Country', 'Region', 'Sector', 'Industry', 
             'LastPrice', 'LastPriceDate', 'Quantity', 'MarketValue']]    

    df = df.rename(columns = {
        'SecurityName': 'security_name',
        'SecurityID': 'security_id',
        'ISIN': 'isin',
        'CUSIP': 'cusip',
        'Ticker': 'ticker',
        'Currency': 'currency',
        'Class': 'class',
        'SC1': 'sc1',
        'SC2': 'sc2',
        'Country': 'country',
        'Region': 'region',
        'Sector': 'sector',
        'Industry': 'industry',
        'LastPrice': 'last_price',
        'LastPriceDate': 'as_of_date',
        'Quantity': 'quantity',
        'MarketValue': 'market_value',
        })

    df['report_id'] = DATA['port_id']
    
    return df
    
    
# port_hist_value
def get_port_hist_value(DATA):

    report_id = DATA['port_id']
    mv_hist = DATA['MV_Hist']
    df = mv_hist  
    df.index.name = 'as_of_date'
    df = df.reset_index()
    df = df.rename(columns={'price_date': 'as_of_date', 'MarketValue': 'portfolio_value'})
    df['daily_return'] = df['portfolio_value'].pct_change(1).fillna(0)
    df['report_id'] = report_id
    # msss.insert_df('dm_port_hist_value', df, 'report_id')    
    
    # take only 100 days, there is bug
    df = df.tail(100)
    
    return df

# portfolio returns
def get_port_returns(DATA):    
    
    mv_hist = DATA['MV_Hist']
    today = mv_hist.index[-1]
    
    df = pd.DataFrame(columns=['port_return'])
    df.index.name = 'period'
    
    df.loc['Today', 'port_return'] = (mv_hist.iloc[-1] / mv_hist.iloc[-2] - 1).iloc[0]
    if len(mv_hist) > 21:
        df.loc['Month', 'port_return'] = (mv_hist.iloc[-1] / mv_hist.iloc[-22] - 1).iloc[0]

    # YTD
    e_year = datetime.datetime(today.year - 1, 12, 31)
    last_year_mv = mv_hist[mv_hist.index<=e_year]
    if len(last_year_mv) > 0:
        df.loc['YTD', 'port_return'] = (mv_hist.iloc[-1] / last_year_mv.iloc[-1] - 1).iloc[0]
    
    # Last 12 month
    if len(mv_hist) > 251:
        df.loc['Last 12 Months', 'port_return'] = (mv_hist.iloc[-1] / mv_hist.iloc[-252] - 1).iloc[0]

    # Last 2 years
    if len(mv_hist) > 503:
        df.loc['Last 2 Years', 'port_return'] = (mv_hist.iloc[-1] / mv_hist.iloc[-504] - 1).iloc[0]

    # Last 3 years
    if len(mv_hist) > (250*3-1):
        df.loc['Last 3 Years', 'port_return'] = (mv_hist.iloc[-1] / mv_hist.iloc[-250*3] - 1).iloc[0]

    # Last 5 years
    if len(mv_hist) > (250*5-1):
        df.loc['Last 5 Years', 'port_return'] = (mv_hist.iloc[-1] / mv_hist.iloc[-250*5] - 1).iloc[0]

    df['benchmark_return']  =  get_benchmark_returns(DATA)
    df = df.reset_index()
    
    df['report_id'] = DATA['port_id']
    return df


def get_benchmark_returns(DATA):    
    
    params = DATA['Parameters']
    bm_ticker = params['Benchmark']
    to_date = params['AsofDate']
    from_date = date_utils.add_years(to_date, -5)
    
    bm_hist = mkt_timeseries.get_by_tickers([bm_ticker], from_date, to_date, category='PRICE')
    
    df = pd.DataFrame(columns=['benchmark_return'])
    df.index.name = 'period'
    
    df.loc['Today', 'benchmark_return'] = (bm_hist.iloc[-1] / bm_hist.iloc[-2] - 1).iloc[0]
    if len(bm_hist) > 21:
        df.loc['Month', 'benchmark_return'] = (bm_hist.iloc[-1] / bm_hist.iloc[-22] - 1).iloc[0]

    # YTD
    e_year = datetime.datetime(to_date.year - 1, 12, 31)
    last_year_mv = bm_hist[bm_hist.index<=e_year]
    if len(last_year_mv) > 0:
        df.loc['YTD', 'benchmark_return'] = (bm_hist.iloc[-1] / last_year_mv.iloc[-1] - 1).iloc[0]

    # Last 12 month
    if len(bm_hist) > 251:
        df.loc['Last 12 Months', 'benchmark_return'] = (bm_hist.iloc[-1] / bm_hist.iloc[-252] - 1).iloc[0]

    # Last 2 years
    if len(bm_hist) > 503:
        df.loc['Last 2 Years', 'benchmark_return'] = (bm_hist.iloc[-1] / bm_hist.iloc[-504] - 1).iloc[0]

    # Last 3 years
    if len(bm_hist) > (250*3-1):
        df.loc['Last 3 Years', 'benchmark_return'] = (bm_hist.iloc[-1] / bm_hist.iloc[-250*3] - 1).iloc[0]

    # Last 5 years
    if len(bm_hist) > (250*5-1):
        df.loc['Last 5 Years', 'benchmark_return'] = (bm_hist.iloc[-1] / bm_hist.iloc[-250*5] - 1).iloc[0]
        
    return df['benchmark_return']
