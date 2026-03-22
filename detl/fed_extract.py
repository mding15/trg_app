# -*- coding: utf-8 -*-
"""
Created on Sun Jun 16 19:37:30 2024

@author: mgdin
"""
import xlwings as xw
import pandas as pd

from trg_config import config
from mkt_data import mkt_timeseries
from utils import xl_utils as xl


fed_url = 'https://fred.stlouisfed.org/series'


FED_DIR = config['DATA_DIR'] / 'FED'
if not FED_DIR.exists():
    FED_DIR.mkdir(parents=True, exist_ok=True)



#
# Load Fed Corporate data
#     date = '20231106'
def load_FRED_Corp_new():

    date = '20260109'
    folder = FED_DIR / date
    if not folder.exists():
        raise Exception(f"Can not find folder: {folder}")

    # read securities    
    securities = pd.read_csv(FED_DIR / 'securities.csv')
    
    # read data    
    data = pd.DataFrame()
    for ticker in securities['Ticker']:
        print(ticker)
        df = pd.read_csv( folder / f'{ticker}.csv', index_col= 0)
        data = pd.concat([data, df], axis=1)
        
    ticker_to_secid = securities.set_index('Ticker')['SecurityID'].to_dict()
    data = data.rename(columns=ticker_to_secid)
    data.index = pd.to_datetime(data.index)
    
    for col in data.columns:
        print(col)
        data = data[data[col] != '.']
        data[col] = pd.to_numeric(data[col])
    
    # save to market file
    mkt_timeseries.save(data, 'FED', 'MACRO')

