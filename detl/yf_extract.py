# -*- coding: utf-8 -*-
"""
Created on Tue May  7 22:01:40 2024

@author: mgdin

pip install yfinance

"""

import yfinance as yf
import pandas as pd
import numpy as np
import uuid
import json
from pathlib import Path

from trg_config import config
from security   import security_info
from utils      import date_utils, tools
from mkt_data   import mkt_timeseries, mkt_data_info

# turn off yfinance logging
import logging
logging.basicConfig(level=logging.WARNING)
logging.getLogger("yfinance").setLevel(logging.WARNING)
logging.getLogger("peewee").setLevel(logging.WARNING)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)

from api import app
app.app_context().push()

# YF Config    
filename = config['CNFG_DIR'] / 'yahoofinance.json'
with open(filename) as f:
    yf_config = json.load(f)

# parameters
YF_START_DATE        = pd.to_datetime(yf_config['start_date'])
YF_SECURITY_FILE     = config['YF_DIR'] / yf_config['security_file']
YF_DIVIDEND_FILE     = config['YF_DIR'] / 'dividend_sec_list.csv'
TEST_MODE            = yf_config['test_mode']
CHUNK_SIZE           = 500

log_file = config['YF_DIR'] / 'log.csv'
log_failed_file = config['YF_DIR'] / 'log_failed.csv'
last_cob_file = config['YF_DIR'] / 'last_cob.csv'
exclude_ticker_file = config['YF_DIR'] / 'exclude_tickers.csv'
run_id = str(uuid.uuid4())[:6]

##############################################################################
# main function
#
# cob = '2024-01-06'
# cob = pd.to_datetime(cob)

def download(cob):
    #cob = date_utils.get_cob()

    print('***** download data for COB: ', cob, ' ******')
       
    # read security list
    df = read_security_list(cob, redo_failed=False)
        
    # split into new and existing
    new_tickers, existing_tickers =  split_new_existing_tickers(df)       
    tic_to_sec = df.set_index('YF_ID')['SecurityID'].to_dict()
    
    # download prices for new tickers
    new_id_list, failed_ids1 = download_new(cob, new_tickers, tic_to_sec)

    # download prices for existing tickers
    updated_id_list, failed_ids2 = download_existing(cob, existing_tickers, tic_to_sec)

    # failed ID list
    failed_ids = failed_ids1 + failed_ids2
    
    # log
    save_log(cob, len(new_id_list), len(updated_id_list), len(failed_ids))
    
    # save the cob to a file
    update_last_cob(cob)


# reaf security list from file
def read_security_list(cob, redo_failed=False):
    # read security list
    df = pd.read_csv(YF_SECURITY_FILE)

    failed_df = pd.read_csv(log_failed_file)
    failed_df['COB'] = pd.to_datetime(failed_df['COB'])
    failed_today = failed_df[failed_df['COB'] == cob]
    df = df[df['YF_ID'].isin(failed_today['Ticker'])]
    
    # exclude tickers
    xlist = get_exclude_tickers()
    df = df[~df['YF_ID'].isin(xlist)]
    if TEST_MODE:
        df = df.iloc[:10]
        
    return df


# download dividend
def download_dividend(cob):
    print('***** download dividends for COB: ', cob, ' ******')
    
    # read security list
    df = pd.read_csv(YF_DIVIDEND_FILE)
    tickers = df['YF_ID'].to_list()
    
    # download prices from YF
    dividends = download_hist_dividend(tickers)
    
    # save to csv file
    save_to_csv(dividends, cob, 'dividends')

    # convert ticker to sec_id        
    dividends = dividends.rename(columns=df.set_index('YF_ID')['SecurityID'].to_dict())

    # save to HDF
    mkt_timeseries.save(dividends, 'YF', 'DIVIDEND')
    
    
# existing tickers are those that exists in the curret mkt_data
#tickers = existing_tickers
def download_existing(cob, tickers, tic_to_sec=None):
    if len(tickers) == 0:
        return [], []
    
    # last download date
    last_date = last_download_cob()
    if last_date >= cob:
        cob_str, last_date_str = cob.strftime('%Y-%m-%d'), last_date.strftime('%Y-%m-%d')
        print(f'[download_existing] skipped because cob: {cob_str} is before or equal to last downloading date: {last_date_str}')
        return [], []

    # get map from ticker to sec_id
    if tic_to_sec is None:
        tic_to_sec =  get_tic_to_sec(tickers)


    # split into trunks
    ticker_chunks = tools.split_list(tickers, CHUNK_SIZE)
    
    # download data and save to csv files
    start_date = date_utils.previous_bus_date(last_date)
    end_date   = cob
    failed_id_list = []
    updated_id_list = []
    for i in range(len(ticker_chunks)):
        # i=0
        tickers = ticker_chunks[i]
        
        # download prices from YF
        prices, failed_ids = download_hist_price(tickers, start_date, end_date)

        # save to csv file
        save_to_csv(prices, cob, f'update.{i}')
        
        # convert ticker to sec_id        
        prices.columns = [tic_to_sec[x] for x in prices.columns]

        # save to HDF
        mkt_timeseries.update_existing(prices, 'YF', 'PRICE')

        # failed security ids and updated id list
        failed_id_list.extend(failed_ids)
        updated_id_list.extend(prices.columns)

    save_failed(cob, failed_id_list)
    return updated_id_list, failed_id_list

    
# new tickers are those are not in the curret mkt_data
# tickers = new_tickers
def download_new(cob, tickers, tic_to_sec=None):
    if len(tickers) == 0:
        return [], []

    # get map from ticker to sec_id
    if tic_to_sec is None:
        tic_to_sec =  get_tic_to_sec(tickers)
      
    # split into trunks
    ticker_chunks = tools.split_list(tickers, CHUNK_SIZE)

    # download data and save to csv files
    start_date = YF_START_DATE
    end_date   = cob
    failed_id_list = []
    new_id_list =[]
    for i in range(len(ticker_chunks)):
        # i=0
        tickers = ticker_chunks[i]

        # download prices from YF
        prices, failed_ids = download_hist_price(tickers, start_date, end_date)
        
        # save to csv file
        save_to_csv(prices, cob, f'new.{i}')

        # convert ticker to sec_id        
        prices.columns = [tic_to_sec[x] for x in prices.columns]

        # save to HDF
        mkt_timeseries.save_new(prices, 'YF', 'PRICE')


        # failed security ids
        failed_id_list.extend(failed_ids)
        new_id_list.extend(prices.columns)
    
    save_failed(cob, failed_id_list)
    return new_id_list, failed_id_list


# new tickers that not in the curret mkt_data_info
# df = ['YF_ID', 'SecurityID', ...]
def split_new_existing_tickers(df):
    
    # get mkt_data sec_ids
    mkt_sec_ids = mkt_data_info.get_sec_ids('YF')
    
    existing_tickers = df[df['SecurityID'].isin(mkt_sec_ids)]['YF_ID'].to_list()
    new_tickers = df[~df['SecurityID'].isin(mkt_sec_ids)]['YF_ID'].to_list()

    return new_tickers, existing_tickers    
    
def save_log(cob, nc_new, nc_update, nc_failed):
    if log_file.exists():
        log = pd.read_csv(log_file)
    else:
        log = pd.DataFrame(columns=['COB','New', 'Update', 'Failed', 'Timestamp'])

    cob_str = cob.strftime('%Y-%m-%d')
    log.loc[len(log)] = [cob_str, nc_new, nc_update, nc_failed, date_utils.timestamp()]
    log.to_csv(log_file, index=False)
    
    print(f'download job is completed successfully. new: {nc_new}, update: {nc_update}, failed: {nc_failed}')    
    
    
def save_failed(cob, failed_ids):
    if log_failed_file.exists():
        log = pd.read_csv(log_failed_file)
    else:
        log = pd.DataFrame(columns=['COB','Ticker', 'Timestamp'])

    df = pd.DataFrame({'Ticker': failed_ids})
    df['COB'] = cob.strftime('%Y-%m-%d')
    df['Timestamp'] = date_utils.timestamp()
    
    log = pd.concat([log, df], ignore_index=True)
    
    log.to_csv(log_failed_file, index=False)


def get_exclude_tickers():
    if exclude_ticker_file.exists():
        df = pd.read_csv(exclude_ticker_file)
    else:
        df = pd.DataFrame(columns=['Ticker'])
        
    return df['Ticker'].to_list()
  
# collect all failed tickers in failed file and append to exclusion file
def gen_exclude_tickers():
    
    # read failed files
    df = pd.read_csv(log_failed_file)
    df = df[['Ticker']].drop_duplicates()
    
    # append to existing exclusion list
    if exclude_ticker_file.exists():
        df1 = pd.read_csv(exclude_ticker_file)
    else:
        df1 = pd.DataFrame()
    df1 = pd.concat([df1, df], ignore_index=True)
    df1 = df1[['Ticker']].drop_duplicates()    
    
    # save to file
    df1.to_csv(exclude_ticker_file, index=False)
    

# save last cob to a file
def update_last_cob(cob):
    cob_str = cob.strftime('%Y-%m-%d')
    df = pd.DataFrame({'COB': [cob_str]})
    df.to_csv(last_cob_file, index=False)

def last_download_cob():

    if last_cob_file.exists() == False:
        last_date = YF_START_DATE
    else:
        df = pd.read_csv(last_cob_file)
        last_date = df['COB'].max()
    
    return pd.to_datetime(last_date)

##################################################################################
# return dict that maps ticker to sec_id
def get_tic_to_sec(tickers):
    df = security_info.get_xref_by_ref_ids('YF_ID', tickers)
    missing = set(tickers).difference(df['YF_ID'])
    if len(missing) > 0:
        missing_str = ', '.join(list(missing))
        raise Exception(f'missing tickers in SecurityInfo table: {missing_str}')
    tic_to_sec = df.set_index('YF_ID')['SecurityID'].to_dict()
    return tic_to_sec    


##############################################################################
# get hist prices from yf 
def download_hist_price(tickers, start_date, end_date):
    print(f'YF downloading... from: {start_date}, to: {end_date}, tickers: {len(tickers)}')
    
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    end_date = date_utils.previous_bus_date(end_date,-1) # add one day to include end_date
    prices = yf.download(tickers, start_date, end_date, threads=False)[['Close']]
    
    if len(tickers)==1:
        prices.columns = tickers
    else:
        prices.columns = [x[1] for x in prices.columns]

    # find sec_ids that have 0 rows data
    nc = prices.count()
    failed_ids = nc[nc==0].index.to_list()
    success_ids = nc[nc>0].index.to_list()
    
    prices = prices[success_ids]
    return prices, failed_ids

# download dividends
# tickers = ['AGG', 'SPY']
def download_hist_dividend(tickers):

    data = {}    
    for ticker in tickers:
        print(ticker)

        dividends = yf_dividends(ticker)
        if len(dividends)>0:
            dividends.index = [d.date() for d in dividends.index]
            dividends.index = pd.to_datetime(dividends.index)
            data[ticker] = dividends
            
    return pd.concat(data, axis=1)
    
def yf_dividends(ticker):
    data = yf.Ticker(ticker)
    return data.dividends
##############################################################################

# convert yf_id to SecurityID
# df = hist_data
def yf_id_to_security_id(df):

    # get the map that maps Ticker to SecurityID
    id_list = df.columns.tolist()
    sec_id_list = [['YF_ID', x] for x in id_list]
    securities = security_info.get_security_by_sec_id_list(sec_id_list)
    missing = set(df.columns).difference(securities['YF_ID'])
    if len(missing) > 0:
        missing_ids = ", ".join(missing)
        raise Exception(f'Can not find SecurityID for: [{missing_ids}]')
    
    theMap = securities.set_index('YF_ID')['SecurityID'].to_dict()
    
    # convert BB_Global to SecurityID
    df.columns = [theMap[x] for x in df.columns]

    return df
    
def save_to_csv(prices, cob, tag):
    filename = get_out_filename(cob, tag)
    print('save to:', filename)
    prices.to_csv(filename)
    
def get_out_filename(end_date, tag):

    year, month = end_date.year, end_date.month
    out_dir = config['YF_DIR'] / f'{year:04}' / f'{month:02}'
    if not out_dir.exists():
        print('\nmaking dir:', out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
    
    #identifier =str(uuid.uuid4())[0:6]
    date_str = end_date.strftime("%Y%m%d")
    filename = out_dir / f'{date_str}.{run_id}.{tag}.csv'
    
    return filename

####################################################
# select all SecurityInfo where YF_ID != None
def gen_yf_sec_list_file():
    
    # select all YF_ID xrefs
    xrefs = security_info.xref_by_ref_ids('YF_ID')
    sec_ids = [x.SecurityID for x in xrefs]
    
    # select securities
    securities = security_info.get_securities_with_xref(sec_ids, ref_types=['YF_ID'])
    
    # select 3 columns
    yf_list = securities[['YF_ID', 'SecurityID', 'SecurityName']]

    # save to file
    yf_list.to_csv(YF_SECURITY_FILE, index=False)
    print('saved file:', YF_SECURITY_FILE)
    
####################################################
import xlwings as xw
from utils import xl_utils
def test():
    
    cob = pd.to_datetime('2024-03-31')        
    download(cob)        
    
    tickers = ['AAPL', 'TSLA']    
    start_date, end_date = '2024-01-01', '2024-06-09'
    prices, failed_id = download_hist_price(tickers, start_date, end_date)
    
    wb = xw.Book()
    xl_utils.add_df_to_excel(prices, wb, 'prices')    

    # generate yf_sec_list file
    gen_yf_sec_list_file()
    
def test2():
    cob = date_utils.get_cob()
    
    # test failed ticker
    tickers = ['ABC', 'AAPL']
    df = security_info.get_ID_by_YF(tickers)
    tic_to_sec = df.set_index('YF_ID')['SecurityID'].to_dict()
    
    # download prices for new tickers
    failed_ids = download_new(cob, tickers, tic_to_sec)

    
from yahooquery import Ticker
def get_sector_industry(symbol):
    stock = Ticker(symbol)
    summary = stock.summary_profile
    if symbol in summary:
        sector = summary[symbol].get('sector', 'N/A')
        industry = summary[symbol].get('industry', 'N/A')
        return sector, industry
    return None, None

# Example usage
def example_industry():
    symbols = ['AAPL', 'MSFT', 'GOOGL']  # Replace with your stock symbols
    for symbol in symbols:
        sector, industry = get_sector_industry(symbol)
        print(f"{symbol} - Sector: {sector}, Industry: {industry}")
    
    
####################################################

# a basic example of using YF to download data
def example():
    tickers = ['ZTO']
    start_date, end_date = '2025-01-01', '2025-01-23'

    prices = yf.download(tickers, start_date, end_date, threads=False)
    prices[['Close']]
    print(prices)
    
    market_data_info = mkt_data_info.get_mkt_data_info_df()
    df = market_data_info
    cob = date_utils.get_cob()
    df['EndDate'] = pd.to_datetime(df['EndDate'])
    df = df[df['EndDate'] < cob]
    wb = xw.Book()
    xl_utils.add_df_to_excel(df, wb, 'mkt_data_info')
    
    from datetime import datetime, timedelta
    import time
    
    today = datetime.today()
    today = datetime(2025,1,27)
    # Calculate the start of the week (Monday)
    start_of_week = today - timedelta(days=today.weekday())

    tickers = ['SPY', 'QQQ']
    data = yf.download(tickers, start=start_of_week.strftime('%Y-%m-%d'), end=today.strftime('%Y-%m-%d'), group_by='ticker')
    
    # Save the data to a CSV file
    data.to_csv('weekly_stock_data.csv')

    # Read the data from the CSV file
    data = pd.read_csv('weekly_stock_data.csv', header=[0, 1], index_col=0)

    # Display the data
    print(data)
    
    # Extract Close prices
    df = data.xs('Close', axis=1, level=1)
    
    # Extract 'SPY' prices
    df = data.xs('SPY', axis=1, level=0)

    
    # split tickers into smaller batches
    tickers = ['SPY', 'QQQ', 'AAPL', 'MSFT', 'NVDA']

    # Split tickers into batches of 50
    batch_size = 3
    batches = np.array_split(tickers, len(tickers) // batch_size + 1)

    # Fetch data for each batch
    all_data = {}
    for batch in batches:
        print(batch)
        data = yf.download(batch.tolist(), period="1wk", group_by='ticker')
        all_data.update(data)
        time.sleep(2)

    # Combine data if needed
    combined_data = pd.concat(all_data.values(), axis=1)
    
    
    
    
    
    
####################################################
def run():
    cob = date_utils.get_cob()
    download(cob)
