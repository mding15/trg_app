# -*- coding: utf-8 -*-
"""
Created on Wed Feb 14 12:26:47 2024

@author: mgding

Description:
    HDF utilities
"""

import pandas as pd
from trg_config import config
from utils import date_utils

# save DataFrame to hdf_file    
def save(df, category, hdf_file):
    if len(df) == 0:
        return 
    
    # go through columns of df
    nc = 0
    with pd.HDFStore(hdf_file) as store:
        for col in df:
            path = category + '/' + col
            store[path] = df[col]
            nc = nc + 1
            
    print(date_utils.timestamp(), 'save data:', nc)    

# read data for symbols
def read(symbols, category, hdf_file):

    data = []
    with pd.HDFStore(hdf_file) as store:
        for symbol in symbols:
            path = category + '/' + symbol
            if path in store:
                ts = store[path]
                data.append(ts)

    if len(data) > 0:
        results = pd.concat(data, axis=1)
    else:
        results = pd.DataFrame()
    
    return results

# list all symbols in hdf_file
def list(hdf_file):
    
    with pd.HDFStore(hdf_file) as store:
        keys = store.keys()
        
    keys = [x.split('/')[-2:] for x in keys]
    keys = pd.DataFrame(keys, columns=['Category', 'Key'])

    return keys

def remove(keys, category, hdf_file):
    with pd.HDFStore(hdf_file, mode='a') as store:
        for key in keys:
            path = category + '/' + key
            store.remove(path)
    
###################################################################################
def test():
    # test data
    df = pd.DataFrame({'AAPL': [10,20,30,40,50]})
    print(df)
    
    # save data to HDF file
    mkt_file = 'mkt_file.h5'
    with pd.HDFStore(mkt_file) as store:
        path = 'TEST' + '/' + 'AAPL'
        store[path] = df['AAPL']
    print('saved to:', mkt_file)

    # retrieve data from DHF
    with pd.HDFStore(mkt_file) as store:
        path = 'TEST' + '/' + 'AAPL'
        ts = store[path]
    print(ts)    
    
    
        
