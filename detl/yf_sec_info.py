# -*- coding: utf-8 -*-
"""
Created on Sun Sep  8 11:27:19 2024

@author: mgdin
"""
import pandas as pd
import xlwings as xw
import yfinance as yf

from trg_config import config
from detl import yf_extract
from utils import xl_utils, date_utils, tools

YF_SEC_INFO_FILE = config['YF_DIR'] / 'sec_info.csv'
def save_sec_info(df):
    df.to_csv(YF_SEC_INFO_FILE, index=False)

def read_sec_info():
    if YF_SEC_INFO_FILE.exists():
        df = pd.read_csv(YF_SEC_INFO_FILE)
    else:
        df = pd.DataFrame()
    return df

def get(tickers):
    df = read_sec_info()
    if tickers is None:
        return df
    else:
        return df[df['symbol'].isin(tickers)]

def add_sec_info(new_data):
    df = read_sec_info()
    
    # update date
    new_data['UpdateDate'] = date_utils.today()

    # securityID for the new_data
    sec_ids = new_data['symbol'].to_list()
    
    # drop existing data
    if len(df) > 0:
        df = df[~df['symbol'].isin(sec_ids)]
    
    # Concat the new sectors
    df = pd.concat([df, new_data], ignore_index=True)
    
    # save to file
    save_sec_info(df)
    
    print(f'created new data to file {YF_SEC_INFO_FILE}', len(new_data))
    
    
def download_yf_sec_info(tickers):

    sec_info = {}
    for yf_id in tickers:
        print(yf_id)
        df = download_data(yf_id)
        sec_info[yf_id] = df
    
    res = {}
    for sec_id in tickers:
        print(sec_id)
        df = sec_info[sec_id]
        res[sec_id] = df
        
    df = pd.concat(res.values())
    
    # Save to csv file
    # df = tools.df_move_columns(df, ['symbol', 'shortName', 'longName', 'currency', 'sector', 'industry', 'country'])
    add_sec_info(df)
    
def download_data(ticker):
    #ticker = 'AAPL'
    stock = yf.Ticker(ticker)
    info = stock.info
    
    if 'companyOfficers' in info:
        del info['companyOfficers'] # officers is not a value
    
    return pd.DataFrame([info])
