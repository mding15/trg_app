# -*- coding: utf-8 -*-
"""
Created on Wed Feb  5 20:26:11 2025

@author: mgdin
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

from trg_config import config
from security   import security_info
from api import app
app.app_context().push()


import xlwings as xw
from utils import xl_utils as xl

# turn off yfinance logging
import logging
logging.basicConfig(level=logging.WARNING)
logging.getLogger("yfinance").setLevel(logging.WARNING)
logging.getLogger("peewee").setLevel(logging.WARNING)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)

MAX_NUM_TICKERS = 500

# output: df = ['YF_ID', 'SecurityID', 'SecurityName']
def get_yf_sec_list():
    
    # select all YF_ID xrefs
    xrefs = security_info.xref_by_ref_ids('YF_ID')
    sec_ids = [x.SecurityID for x in xrefs]
    
    # select securities
    securities = security_info.get_securities_with_xref(sec_ids, ref_types=['YF_ID'])
    
    # select 3 columns
    yf_list = securities[['YF_ID', 'SecurityID', 'SecurityName']]

    return yf_list

def get_1wk_data_from_today(tickers):
    data = yf.download(tickers, period="1wk", group_by='ticker')
    return data

def get_1wk_data(tickers, today):

    # Calculate the start of the week (Monday)
    start_of_week = get_monday(today)
    end_of_week = start_of_week + timedelta(days=5)
    
    data = yf.download(tickers, start=start_of_week.strftime('%Y-%m-%d'), end=end_of_week.strftime('%Y-%m-%d'), group_by='ticker', threads=False)
    return data

def get_data_folder(today):
    data_folder = config['YF_DIR'] / f'{today.year}'    
    if not data_folder.exists():
        data_folder.mkdir(parents=True, exist_ok=True)
        print('making directory:', data_folder)
    return data_folder

def get_monday(today):
    monday = today - timedelta(days=today.weekday())
    return monday    
    
def get_file_path(today):
    data_folder = get_data_folder(today)
    monday = get_monday(today).strftime('%Y%m%d')
    file_path = data_folder / f'yf.{monday}.parquet'
    return file_path
    
def read_downloaded(today):
    file_path = get_file_path(today)
    if not file_path.exists():
        return pd.DataFrame(columns=['Date', 'Ticker'])
    else:
        return pd.read_parquet(file_path, engine="pyarrow")

def read_exclude_tickers():
    file_path = config['YF_DIR'] / 'exclude_tickers.csv'
    df = pd.read_csv(file_path)
    return df['Ticker'].to_list()
    
def read_failed_tickers(today):
    exclude_tickers = read_exclude_tickers()
    
    file_path = get_file_path(today)
    file_failed = file_path.parent / f'{file_path.stem}.failed.csv'

    if file_failed.exists():
        df = pd.read_csv(file_failed)
        exclude_tickers.extend(df['Ticker'].to_list())

    return exclude_tickers

def save_failed_tickers(today, tickers):

    file_path = get_file_path(today)
    file_failed = file_path.parent / f'{file_path.stem}.failed.csv'
    
    df = pd.DataFrame({'Ticker': tickers})
    df.to_csv(file_failed, index=False)
    
# today = datetime(2025,2,6)
def download_yf(today=None):
    if not today:
        today = datetime.today()
    
    monday = get_monday(today).strftime('%Y%m%d')
    print(f"Monday: {monday}")
   
    # all yf tickers
    yf_sec_list = get_yf_sec_list()

    # security tickers
    tickers = yf_sec_list['YF_ID'].to_list()
    
    # read downloaded data
    downloade_data = read_downloaded(today)
    
    # read failed tickers
    failed_tickers = read_failed_tickers(today)
    
    # exclude failed tickers
    tickers = list(set(tickers).difference(failed_tickers))
    
    # exclude downloaded tickers
    tickers = list(set(tickers).difference(downloade_data['Ticker']))

    # MAX SIZE = 500
    tickers = tickers[:MAX_NUM_TICKERS]

    if len(tickers) == 0:
        print('All tickers have been downloaded!')
        return
    
    # number of tickers
    print(f"number of tickers: {len(tickers)}")
    
    
    # Split tickers into batches of 50
    batch_size = 50
    batches = np.array_split(tickers, len(tickers) // batch_size + 1)

    
    all_data = []
    for batch in batches:
        df = get_1wk_data(batch.tolist(), today)
        df = df.stack(level=0).reset_index()
        all_data.append(df)
        time.sleep(1)

    combined_data = pd.concat(all_data, ignore_index=True)
    failed_tks = list(set(tickers).difference(combined_data['Ticker']))
    failed_tickers.extend(failed_tks)
    save_failed_tickers(today, failed_tickers)

    combined_data = pd.concat([downloade_data, combined_data], ignore_index=True)
    file_path = get_file_path(today)
    combined_data.to_parquet(file_path, engine="pyarrow")
    print(f'saved data to: {file_path}')
    n_combined = len(combined_data['Ticker'].unique())
    print(f"number of data: {n_combined}")
    # df1 = combined_data.xs('Close', axis=1, level=1)
    # wb  = xw.Book('Book2')
    # xl.add_df_to_excel(df1, wb, 'price2')

def read_data(today):

    monday = get_monday(today).strftime('%Y%m%d')
    data_folder = get_data_folder(today)

    succeed_tickers  = set()   
    all_data = {}
    for file_path in list(data_folder.glob(f'yf.{monday}.*.parquet')):
        print(f'reading file: {file_path}')
        df = pd.read_parquet(file_path, engine="pyarrow")
        df = df.xs('Close', axis=1, level=1)
        all_data.update(df)
        succeed_tickers.update(df)
        
    files = list(data_folder.glob(f'yf.{monday}.*.parquet'))  
    all_data = []
    for file in files:
        df = pd.read_parquet(file, engine="pyarrow")
        df = df.stack(level=0).reset_index()
        all_data.append(df)

    df = pd.concat(all_data, ignore_index=True)            
        
    
    # add data to mkt_data file
    for tk in all_data:
        df = all_data[tk]
        print(tk)
        print(df)
        
    file_path = data_folder / 'yf.20250203.parquet'
    df = pd.read_parquet(file_path, engine="pyarrow")
    df.stack(level=0).reset_index()
    
    
    for tk in df.xs('Close', axis=1, level=1).columns:
        print(tk)
        ts = df.xs(tk, axis=1, level=0)
        
    
    # wb = xw.Book()
    # xl.add_df_to_excel(df, wb, 'prices', index=False)
    # df = pd.concat(all_data.values(), axis=1)
    # xl.add_df_to_excel(df, wb, 'prices')
    # df.info()
    
    
def test():
    
    # download 1 week data
    today = datetime(2025,2, 8)
    download_yf(today)

    # read parquet
    df = read_downloaded(today)
    wb = xw.Book('Book2')
    xl.add_df_to_excel(df, wb, 'prices', index=False)


    ###############################################
    # debug
    
    # test tickers
    tickers = ['SPY', 'QQQ', 'AAPL', 'MSFT', 'NVDA']
    tickers = ['EWA', 'POOL']
    get_1wk_data(tickers, today)

    tickers = ['SPY']
    yf.download(tickers, period="1wk", group_by='ticker')

    