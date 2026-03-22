# -*- coding: utf-8 -*-
"""
Created on Sun Jan  7 20:36:59 2024

@author: mgdin
"""
import pandas as pd
import numpy as np
import xlwings as xw

from trg_config import config
from models import model_utils as mu

version = '20240109'
    
def get_core_factors():
    return mu.read_model_data('core_factors.csv', version)

def save_core_factors_dist(core_factors):
    mu.save_model_data(core_factors, 'core_factors_dist.csv', version, index=False)

# Returns: core factor log return time series
def read_core_factors_dist():
    return mu.read_model_data('core_factors_dist.csv', version)

def read_core_index():
    filename = config['MODEL_DIR'] / version / 'core_index.csv'
    df = pd.read_csv(filename)
    return df['Date']

# return core factor historical price time series
def get_core_factors_prices():

    # core factor security IDs
    df = mu.get_core_factors()
    sec_ids = df['SecurityID'].to_list()
    
    # historical time series
    prices = mu.get_market_data(sec_ids)

    # use ticke as column names
    prices.columns = df['Ticker'].to_list()

    return prices    

# generate log return timeseries for core factors
def create_core_factors():
    # use residual index for now    
    index = read_core_index()
    
    core_prices = get_core_factors_prices()
    core_prices = core_prices.fillna(method='ffill')
    pct_return = core_prices.pct_change(1)
    log_return = np.log( 1 + pct_return )
    core_factors = log_return.loc[index]
    bad = mu.check_count(core_factors)
    if  len(bad) > 0 :
        print('Error: found null values in core factors. Could be bad indexing')
    else:
        save_core_factors_dist(core_factors)    

def test():
    wb = xw.Book('CoreFactors.xlsx')
    mu.save_core_factors(wb)
    
    create_core_factors()
    
    
    