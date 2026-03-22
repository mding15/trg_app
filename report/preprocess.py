# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 16:43:01 2024

@author: mgdin
"""
from report import performance, sharpe_ratio, back_test
from security import security_info
from api import portfolios

def preprocess(DATA):
    # check required data
    check_input(DATA)
    
    # position exception
    # read_position_exception(DATA)
    
    # amend data for report
    amend_data(DATA)
    
    #  Performance, calculate past 1Y returns
    print('calculate Performance....')
    performance.calc_performance(DATA)

    # calculate Sharp Ratio, add to pos_var dataframe
    print('calculate Sharpe Ratio')
    sharpe_ratio.calc_sharpe_ratio(DATA)

    # calculate back_test
    print('calculate back test...')
    back_test.back_test(DATA)

def check_input(DATA):
    # benchmark
    params = DATA['Parameters']
    bm_ticker = params['Benchmark']
    bm_id = security_info.get_ID_by_Ticker([bm_ticker])
    if len(bm_id) == 0:
        raise Exception(f'can not find benchamrk for {bm_ticker}')

def read_position_exception(DATA):
    port_id = DATA['port_id']
    position_exception = portfolios.load_position_exception_by_port_id(port_id)
    DATA['position_exception'] = position_exception
    
def amend_data(DATA):
    positions = DATA['Positions']
    
    # add ticker
    positions['Ticker'] = positions['Ticker'].fillna(positions['SecurityName'].str[:20])
    
    DATA['Positions'] = positions


    
    
