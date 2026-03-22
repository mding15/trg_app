# -*- coding: utf-8 -*-
"""
Created on Fri Dec 27 11:01:54 2024

@author: mgdin
"""
import pandas as pd

def test():
    # test data
    df = pd.DataFrame({'AAPL': [10,20,30,40,50]})
    print(df)

    # hdf file name
    mkt_file = 'mkt_file.h5'
    
    # save data to HDF file
    print('save hdf')
    with pd.HDFStore(mkt_file) as store:
        path = 'TEST' + '/' + 'AAPL'
        store[path] = df['AAPL']
    print('saved to:', mkt_file)

    # retrieve data from DHF
    print('retrieve hdf')
    with pd.HDFStore(mkt_file) as store:
        path = 'TEST' + '/' + 'AAPL'
        ts = store[path]
    print(ts)    