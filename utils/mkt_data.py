# -*- coding: utf-8 -*-
"""
Created on Sun Apr  7 16:40:26 2024

@author: mgdin
"""
import pandas as pd
import numpy as np
import xlwings as xw

from trg_config import config
from utils import hdf_utils as hdf
from utils import xl_utils as xl
from utils import tools, date_utils
# from security import security_info


# app.app_context().push()


mkt_file = config['mkt_file']
log_file = config['MKTD_DIR'] / 'log.csv'

if not mkt_file.exists():
    df = pd.DataFrame(columns=['T00000000'])
    df.loc['2000-01-01'] = 100
    df.index.name = 'Date'
    df.index = pd.to_datetime(df.index)
    hdf.save(df, 'PRICE', mkt_file)
    
# return df['Category', 'SecurityID']
def get_mkt_data_sec_list():
    df = hdf.list(mkt_file)
    df = df.rename(columns={'Key': 'SecurityID'})
    return df


# IDs = sec_list
def get_market_data(IDs, from_date=None, to_date=None, category='PRICE'):
    df = hdf.read(IDs, category, mkt_file)
    if len(df) == 0:
        return df
    
    if from_date:
        df = df[df.index >= from_date]
    if to_date:
        df = df[df.index <= to_date]
    
    return df


# df = prices
# source='YF'
def save_market_data(df, source, category='PRICE'):
    # assine index name to 'Date'
    df.index.name = 'Date'
    
    # make sure index is datetime
    df.index = pd.to_datetime(df.index)

    # deduplicate index — keep last occurrence
    df = df[~df.index.duplicated(keep='last')]

    # make sure value type is float
    df= df.map(lambda x: x if isinstance(x, float) else np.NaN)

    hdf.save(df, category, mkt_file)
    save_log(df, source, category)

# df = prices
def append_market_data(df, source, category='PRICE'):
    # assine index name to 'Date'
    df.index.name = 'Date'
    
    # make sure index is datetime 
    df.index = pd.to_datetime(df.index)
    
    # make sure value type is float
    df= df.map(lambda x: x if isinstance(x, float) else np.NaN)
    
    sec_ids = df.columns.to_list()
    start_date = df.index[0]
    
    # get existing prices
    prices = get_market_data(sec_ids, category=category)
    prices = prices[prices.index < start_date]
    
    # concat
    prices = pd.concat([prices, df])
    
    hdf.save(prices, category, mkt_file)
    save_log(df, source, category)


def save_log(df, source, category):
    if len(df) == 0:
        return
    
    if log_file.exists():
        log = pd.read_csv(log_file)
    else:
        log = pd.DataFrame(columns=['UpdateDate','SecurityID', 'Category','Source', 'StartDate', 'EndDate', 'Length'])
        
    log_df = pd.DataFrame({'SecurityID': df.columns.to_list()})        
    log_df['UpdateDate'] = tools.timestamp()
    log_df['Category'] = category
    log_df['Source'] = source
    
    log_df = log_df.set_index('SecurityID')
    log_df['StartDate'] = date_utils.get_first_date(df)
    log_df['EndDate'] = date_utils.get_last_date(df)
    log_df['Length'] = df.count()
    log_df = log_df.reset_index()
    
    
    log = pd.concat([log, log_df])
    log.to_csv(log_file, index=False)


# get last update date from log file
# source='YF'
def get_last_dn_date(source):
    
    df = pd.read_csv(log_file)
    last_date = df[df['Source']==source]['EndDate'].iloc[-20:].min()
    last_date = pd.to_datetime(last_date)
    return last_date



##################################################################################
def test():
    IDs = ['T10000022', 'T10000026']
    from_date = '2018-01-01'
    prices = get_market_data(IDs, from_date=from_date)

    # wb = xw.Book()
    # xl.add_df_to_excel(prices, wb, 'prices')
    return prices
