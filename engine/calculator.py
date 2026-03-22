# -*- coding: utf-8 -*-
"""
Created on Sat Mar  2 17:55:37 2024

@author: mgdin
"""

import pandas as pd
from trgapp import config
import trgapp.utils.hdf_utils as hdf


# Files used in the test
TEST_POSITION_FILE       = config.TEST_DIR / 'data' /'test_positions.csv'
TEST_RISKMAP_FILE        = config.TEST_DIR / 'data' /'RiskMap.csv'
TEST_DISTRIBUTION_FILE   = config.TEST_DIR / 'data' /'dist.h5'

def calculate_pnl_VaR(pnl, CL=0.95):
    return pd.DataFrame(pnl.quantile(1-CL).rename('VaR')) * (-1)

def test():
    # read test position
    positions = pd.read_csv(TEST_POSITION_FILE)
    
    # get risk factors
    RiskMap = pd.read_csv(TEST_RISKMAP_FILE)
    risk_factors = RiskMap[RiskMap['SecurityID'].isin(positions.SecurityID)]  
    
    # Delta
    delta_positions =  risk_factors [risk_factors['RiskCategory'] == 'DELTA'][['SecurityID', 'RiskFactor']]
    delta_positions = positions.merge(delta_positions, on='SecurityID', how='left')
    
    # get risk factor distribution
    dist = hdf.read(delta_positions['RiskFactor'], 'DELTA', TEST_DISTRIBUTION_FILE)
    dist.columns = delta_positions['PositionID'].to_list()
    
    # calculate P/L
    delta = delta_positions.set_index('PositionID')['MarketValue'] 
    pnl  = delta * dist
    
    # calculate VaR
    pos_var = calculate_pnl_VaR(pnl, 0.95)
    print(pos_var)    
    
    # total VaR
    total_pnl = pd.DataFrame(pnl.sum(axis=1))
    total_var = calculate_pnl_VaR(total_pnl, 0.95)
    print(total_var)
    
    # pack results
    results = {}
    results['Position VaR'] = pos_var
    results['Total VaR'] = total_var

    return results


