# -*- coding: utf-8 -*-
"""
Created on Sun Nov 16 17:56:24 2025

@author: mgdin
"""

import pandas as pd
import xlwings as xw

from mkt_data import mkt_timeseries
from utils import xl_utils

from security import security_info


def proxy_workbook():
    file_path = r'C:\Users\mgdin\dev\TRG_App\Workbooks\Proxy_WB.xlsx'
    wb = xw.Book(file_path)

    proxy_securities = read_data(file_path)

    prices = get_hist_prices(proxy_securities)    
    xl_utils.add_df_to_excel(prices, wb, 'Prices')
    
    

def read_data(file_path):
    
    securities = pd.read_excel(file_path, sheet_name='Security', dtype={'ISIN': str, 'CUSIP': str, 'SEDOL': str} )
    
    securities['SecurityID'] = security_info.get_SecurityID_by_ref(securities)
    
    # normalize column names
    securities.columns = [x.replace(' ', '') for x in securities.columns]
    
    proxy_securities = pd.read_excel(file_path, sheet_name='Proxy', dtype={'Correlation': float, 'Vol Multiple': float})
    proxy_securities = proxy_securities.merge(securities[['ID', 'SecurityID']], on='ID', how='left')
    proxy_securities.columns = [x.replace(' ', '') for x in proxy_securities.columns]
    
    return proxy_securities
    
def get_hist_prices(proxy_securities):
    
    # security IDs
    sec_ids = proxy_securities['SecurityID'].to_list()
    
    # proxy
    proxy_tickers = proxy_securities['ProxyTicker'].to_list()
    df = security_info.get_ID_by_Ticker(proxy_tickers)
    sec_ids2 = df['SecurityID'].to_list()

    # security and proxy
    sec_list = sec_ids + sec_ids2 
    
    # get hist prices
    prices = mkt_timeseries.get(sec_list)

    return prices
    
