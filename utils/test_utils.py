# -*- coding: utf-8 -*-
"""
Created on Fri May 24 21:46:07 2024

@author: mgdin
"""
import xlwings as xw
import pandas as pd
import json

from trg_config import config
from utils import xl_utils, tools
from api import data_pack

def test_portfolio_filename():
    return config['SRC_DIR'] / 'test_data' / 'test_portfolio.json'

def save_test_portfolio(params, positions):
    payload = data_pack.pack_payload('CalculateVaR', positions, params)
    filename = test_portfolio_filename()
    with open(filename, 'w') as f:
        json.dump(payload, f)    
    print('saved to:', filename)

# save_test_portfolio(params, positions)

def get_test_portfolio():
    filename = test_portfolio_filename()
    payload = read_json(filename)
    params = payload['Parameters']
    positions = data_pack.extract_df(payload, 'Positions')
    
    return params, positions

def template_test_portfolio():
    wb = xw.Book()
    
    params, positions = get_test_portfolio()
    parameters = tools.dict_to_df(params)
    xl_utils.add_df_to_excel(positions, wb, 'Positions', index=False)    
    xl_utils.add_df_to_excel(parameters, wb, 'Parameters', index=False)    

    return wb


def write_DATA_to_xl(DATA, wb):
    # wb = xw.Book('Book2')
    
    add_data_to_xl(DATA, 'Log', wb, tab='Log')
    add_data_to_xl(DATA, 'Positions', wb, tab='CalcPositions')
    add_data_to_xl(DATA, 'ErrorPositions', wb, tab='ErrorPositions')
    add_data_to_xl(DATA, 'RiskFactor', wb, tab='RiskFactor')
    add_data_to_xl(DATA, 'VaR', wb, tab='VaR')
    add_data_to_xl(DATA, 'RF_VaR', wb, tab='RF_VaR', index=True)
    add_data_to_xl(DATA, 'Static', wb, tab='Static', index=True)
    add_data_to_xl(DATA, 'Security_info', wb, tab='Security_info', index=True)
    add_data_to_xl(DATA, 'LastPrices', wb, tab='LastPrices', index=True)
    add_data_to_xl(DATA, 'Bonds', wb, tab='Bonds', index=True)

    # df = DATA['RF_PnL']
    # add_data_to_xl(DATA['RF_PnL'], wb, tab='RF_PnL', index=True)
    # add_data_to_xl(DATA['PnL'], wb, tab='PnL', index=True)

def add_data_to_xl(DATA, name, wb, tab, index=False):
    if name in DATA:
        xl_utils.add_df_to_excel(DATA[name], wb, tab, index=index)
        
    
    
def read_json(filename):
    with open(filename, 'r') as f:
        payload = json.load(f)    
    return payload
