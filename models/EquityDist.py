# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 12:14:15 2024

@author: mgdin
"""
import pandas as pd
from io import StringIO
import requests

from trgapp.api import client 
from trgapp import config


def get_market_data(sec_list):
    token = client.get_token()

    params = {
        'token': token
    }
    
    payload = {
        'Client ID': '123',
        'Request': 'MarketData',
        'Data Category': 'PRICE',
        'From Date': '2020-01-01',
        'To Date': '2023-12-31',
        'SecurityID': sec_list
    }
    
    response = requests.post(f'{client.host}/api/data_request', params=params, json=payload)
    
    if response.status_code == 200: # success
        data = response.json()['DATA']
        df = pd.read_csv(StringIO(data), index_col=0)    
        return df
    else:
        print('Request failed', response.status_code)
        

#####
# yidong's code here

#
# Input data
#
equity_model_config    = config.MODEL_DIR / 'equity_model_configuration.csv'
equity_securities_file = config.MODEL_DIR / 'equity_securities.csv'
core_factor_file       = config.MODEL_DIR / 'core_factors.csv'

#
# Output data
#
equity_residual_dates = config.MODEL_DIR / 'equity_residual_dates.csv'
equity_residual       = config.MODEL_DIR / 'equity_residual.csv'
equity_regression     = config.MODEL_DIR / 'equity_regression.csv'
equity_exclusion      = config.MODEL_DIR / 'equity_exclusion.csv'
equity_distribution   = config.MODEL_DIR / 'equity_distribution.csv'

def generate_equity_distribution():
    
    
    




######
def test():
    sec_list = ['T10000021', 'T10000022']
    df = get_market_data(sec_list)
    print(df.head())

