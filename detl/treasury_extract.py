# -*- coding: utf-8 -*-
"""
Created on Sat Jun 15 14:51:45 2024

@author: mgdin
"""
import pandas as pd
from trg_config import config
from mkt_data import mkt_timeseries

#
# import US Treasury yield to market_data from trgdata/treasury.gov folder
#
# download IR data, see workbook: Models\Data\FED\USTreasury.xlsx

def load_UST():
    source_folder = config['DATA_DIR']  / 'treasury.gov'
    source_folder.exists()    
    data = pd.DataFrame()

    # csv files in the folder
    files = list(source_folder.glob('daily-treasury-rates.*'))

    # read last two files - the two files just downloaded
    for file in files[-1:]:
        rates = pd.read_csv(file)
        print('loading:', len(rates), 'file:', file.name)
        rates['Date'] = pd.to_datetime(rates['Date'])
        data = pd.concat([data, rates.sort_values(by='Date')], ignore_index=True)
    
    # drop duplicates
    data = data.drop_duplicates(subset=['Date'])
    data = data.set_index('Date')
    
    # rename columns
    df = pd.read_csv(source_folder / 'UST.symbols.csv', index_col='header')
    data = data[df.index].rename(columns = df['SecurityID'].to_dict())
    
    # the original value is in percentage points, convert it to decimal
    data = data * 0.01
    
    # save the data to market file
    mkt_timeseries.update_existing(data, 'treasury.gov', 'YIELD')
    
    