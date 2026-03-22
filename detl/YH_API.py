# -*- coding: utf-8 -*-
"""
Created on Thu Feb 27 20:33:26 2025

@author: mgdin
"""
import pandas as pd
import urllib.parse
import http.client
import json
from datetime import datetime, timezone

from trg_config import config
from utils import date_utils

API_KEY  = config['YH_API_KEY']
API_HOST = 'yahoo-finance15.p.rapidapi.com'


# API GET for a gien url, return the body of the data
def API_GET(url):
    # url = f'/api/v1/markets/stock/modules?ticker=AAPL&module=asset-profile'
    
    headers = {
        'x-rapidapi-key': API_KEY,
        'x-rapidapi-host': API_HOST
    }
    conn = http.client.HTTPSConnection("yahoo-finance15.p.rapidapi.com")
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


#
# /markets/tickers?page=2&type=STOCKS
# type: STOCKS or ETF or MUTUALFUNDS or FUTURES or INDEX
#
# sec_type='INDEX'
def GET_TICKERS(sec_type='STOCKS', page=141):
    url = f"/api/v2/markets/tickers?page={page}&type={sec_type}"
    print(f'GET {url}')
    body = API_GET(url)
    df = pd.DataFrame(body)
    return df

def test_API_GET_TICKERS():
    df = GET_TICKERS('INDEX', page=1)
    print(df)
    
#
# /api/v1/markets/stock/quotes
#
def GET_QUOTES(tickers):
    # tickers = ['SPY', 'QQQ']
    encoded_tickers = urllib.parse.quote(",".join(tickers))
    url = f'/api/v1/markets/stock/quotes?ticker={encoded_tickers}'
    
    body = API_GET(url)
    
    for x in body:
        x['corporateActions'] = json.dumps(x['corporateActions'])
    df = pd.DataFrame(body)
    
    return df

def test_API_GET_QUOTES():
    tickers = ['SPY', 'QQQ']
    df = GET_QUOTES(tickers)
    df.to_csv('df.csv', index=False)
    print(df)

#
# 
#
def STOCK_PROFILE(ticker):
    
    url = f'/api/v1/markets/stock/modules?ticker={ticker}&module=asset-profile'
    body = API_GET(url)
    
    del body['companyOfficers']
    del body['executiveTeam']
    
    body["ticker"] = ticker
    df = pd.DataFrame([body])
    
    return df


def GET_HISTORY(ticker):
    url = f"/api/v1/markets/stock/history?ticker={ticker}&interval=1d&diffandsplits=true"
    print(f'GET {url}')
    try:
        body = API_GET(url)
    except Exception as e:
        print(f'Ticker: {ticker}: {e}')
        body = None        

    dividends = None
    if body:
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
            df_div['ticker'] = ticker
            df_div['ex_date'] = df_div['date'].apply(date_utils.timestamp_to_datetime)
        else:
            df_div = pd.DataFrame()

        return df, df_div
    else:
        return pd.DataFrame(), pd.DataFrame()

def test_GET_HISTORY(ticker=None):
    if not ticker:
        ticker = 'SPY'
        
    df, df_div = GET_HISTORY(ticker)
    df.to_csv('df.csv', index=False)
    
    if not df_div.empty:
        df_div.to_csv('df_div.csv', index=False)
    print(df.head())

# cob = '2023-11-30'
def STOCK_DIVIDEND(cob):
    # url = "/api/v1/markets/calendar/dividends?date=2023-11-30"
    url = f"/api/v1/markets/calendar/dividends?date={cob}"
    body = API_GET(url)
    df = pd.DataFrame(body)
    
    return df


###########################################################################################
# test
def test():
    url = "/api/v1/markets/calendar/dividends?date=2023-11-30"
    print(f'GET {url}')
    body = API_GET(url)
    df = pd.DataFrame(body)
    