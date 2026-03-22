# -*- coding: utf-8 -*-
"""
Created on Tue Mar 25 15:15:33 2025

@author: mgdin
"""
import xlwings as xw
from utils import xl_utils, date_utils
from detl import YH_API, yh_extract

def test():
    wb = xw.Book('Book1')
    
    today = date_utils.get_cob()
    
    tickers = ['SPY', 'FEZ','XLF','ILF','CRCL','HDV','GHYG','JAAA']
    tickers = ['CIBR', 'MLPX', 'IAI', 'DBJP', 'APO', 'CRWD', 'GEV', 'BRK-B']

    # profile
    df = yh_extract.api_stock_profiles(tickers)
    xl_utils.add_df_to_excel(df, wb, 'profile')
    
    # today's price
    df = YH_API.GET_QUOTES(tickers)
    xl_utils.add_df_to_excel(df, wb, 'Quotes')
    
    prices = yh_extract.extract_eod_price(df)
    xl_utils.add_df_to_excel(prices, wb, 'price', index=False)

    # profile
    df = yh_extract.api_stock_profiles(tickers)
    xl_utils.add_df_to_excel(df, wb, 'profile')

    
# pip install requests pandas python-dateutil
import os
import requests
import pandas as pd
from dateutil import tz
from trg_config import config
import http.client
import json


API_KEY  = config['YH_API_KEY']
API_HOST = 'yahoo-finance15.p.rapidapi.com'

def API_GET(url):
    ticker = 'BILS'
    url = '/api/v1/markets/stock/history?symbol={ticker}&interval=1d&diffandsplits=true'
    
    headers = {
        'x-rapidapi-key': API_KEY,
        'x-rapidapi-host': API_HOST
    }
    conn = http.client.HTTPSConnection(API_HOST)
    conn.request("GET", url, headers=headers)
    
    res = conn.getresponse()
    data = res.read()
    conn.close()
    # print(data.decode("utf-8"))
    
    data = json.loads(data.decode("utf-8"))

    if 'body' in data:
        body = data['body']
        meta = data['meta']
        print('status:', meta['status'])    
        return body


    elif 'message' in data:
        raise Exception(data['message'])

    else:
        raise Exception('Unknown error')

    if 'events' in body:
        events = body.pop('events')
        if 'dividends' in events:
            dividends = events.pop('dividends')
    


    df = pd.DataFrame(body)
    df = df.T
    df['ticker'] = ticker 
    
    if dividends:
        df_div = pd.DataFrame(dividends)
        df_div = df_div.T
        df_div['div_date'] = df_div['date'].apply(date_utils.timestamp_to_datetime)
    
    df.to_csv('df.csv')

    my_dict  = {'a': 1, 'b':2}
    a = my_dict.pop('a')    
    print(a)    
    print(my_dict)

def test_get_hist():
    ticker = 'MARA'
    YH_API.test_GET_HISTORY(ticker)
