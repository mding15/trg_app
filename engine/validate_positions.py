# -*- coding: utf-8 -*-
"""
Created on Fri Jan 17 12:17:47 2025

@author: mgdin

"""
import pandas as pd
import numpy as np
import datetime
from utils import tools, date_utils

from security import security_info as sc

#################################################################################
TAIL_MEASURES = ['95% TailVaR', '99% TailVaR', '95% VaR', '99% VaR']
RETURN_FREQUENCIES = ['Daily', 'Weekly', 'Monthly', 'Quarterly']
BENCHMARKS = ['BM_0_100', 'BM_10_90', 'BM_20_80', 'BM_30_70', 'BM_40_60', 'BM_50_50', 'BM_60_40', 'BM_70_30', 'BM_80_20', 'BM_90_10', 'BM_100_0']
EXPECTED_RETURN = ['Upload']
BASE_CURRENCY_LIST= ['USD', 'EUR','GDP', 'JPY', 'CLP']  # add more      
RISK_HORIZON_DAYS = {'1 Day':1, 'Day':1, '1 Month':21, 'Month':21, '1 Quarter': 63, 'Quarter': 63,'1 Year':252, 'Year':252}

def check_parameters(params):
    errors = []
    # as of date
    try:
        if isinstance(params['AsofDate'], str):
            params['AsofDate'] = date_utils.parse_date(params['AsofDate'])
    except Exception as e:
        errors.append(f'Parameters: As of Date: {str(e)}')
    
    # Report Date
    try:
        if isinstance(params['AsofDate'], str):
            params['AsofDate'] = date_utils.parse_date(params['AsofDate'])
    except Exception as e:
        errors.append(f'Parameters: As of Date: {str(e)}')
    
    # Risk Horizon
    risk_horizon = params['RiskHorizon']
    if risk_horizon not in RISK_HORIZON_DAYS.keys():
        errors.append(f'Parameters: Risk Horizon: "{risk_horizon}" invalid value')

    # Tail Measure
    tail_measure = params['TailMeasure']
    if tail_measure not in TAIL_MEASURES:
        errors.append(f'Parameters: Tail Measure: "{tail_measure}" invalid value')

    # Return Frequency
    return_frequency = params['ReturnFrequency']
    if return_frequency not in RETURN_FREQUENCIES:
        errors.append(f'Parameters: Return Frequency: "{return_frequency}" invalid value')
    
    # Benchmark
    benchmark = params['Benchmark']
    if benchmark not in BENCHMARKS:
        errors.append(f'Parameters: Benchmark: "{benchmark}" invalid value')

    # Expected Return
    Expected_return = params['ExpectedReturn']
    if Expected_return not in EXPECTED_RETURN:
        errors.append(f'Parameters: Expected Return: "{Expected_return}" invalid value')

    # Base Currency
    base_currency = params['BaseCurrency']
    if base_currency not in BASE_CURRENCY_LIST:
        errors.append(f'Parameters: Base Currency: "{base_currency}" invalid value')

    return params, errors


import re
def clean_market_value(value):
    if re.match(r'^-?\d*\.?\d+$', str(value)):
        return value
    else:
        return np.nan

def check_positions(positions):
    errors = []
    
    # market value
    # check empty value
    df = positions[positions['MarketValue'].isna()]
    if not df.empty:
        sec_ids = ', '.join(df['ID'])
        errors.append(f'missing MarketValue for ID: {sec_ids}')

    try:
        positions['MarketValue'] = pd.to_numeric(positions['MarketValue'])
    except Exception as e:
        errors.append(f'Positions: Market Value: {str(e)}')

    # check zero market value
    zero_mv = positions[positions['MarketValue'].isna() | (positions['MarketValue']==0)]
    if len(zero_mv):
        error = zero_mv.to_csv(index=False)
        errors.append(f'The following position(s) have Zero Market Value!\n{error}')
        
    # Quantity
    # check empty value
    df = positions[positions['Quantity'].isna()]
    if not df.empty:
        sec_ids = ', '.join(df['ID'])
        errors.append(f'missing Quantity for ID: {sec_ids}')
        
    try:
        # check if value  is numerical
        positions['Quantity'] = pd.to_numeric(positions['Quantity'])
    except Exception as e:
        errors.append(f'Positions: Quantity: {str(e)}')
    
    # # LastPrice
    # if 'LastPrice' in positions:
    #     try:
    #         positions['LastPrice'] = pd.to_numeric(positions['LastPrice'])
    #     except Exception as e:
    #         errors.append(f'Positions: LastPrice: {str(e)}')
    
    # # LastPriceDate
    # if 'LastPriceDate' in positions:
    #     try:
    #         positions['LastPriceDate'] = pd.to_datetime(positions['LastPriceDate'])
    #     except Exception as e:
    #         errors.append(f'Positions: LastPriceDate: {str(e)}')

    # # Maturity Date
    # if 'MaturityDate' in positions:
    #     try:
    #         positions['MaturityDate'] = pd.to_datetime(positions['MaturityDate'])
    #     except Exception as e:
    #         errors.append(f'Positions: MaturityDate: {str(e)}')
    
    # check security
    # positions['SecurityID'] = sc.get_SecurityID_by_ref(positions)

    # check underlying security
    # positions = check_options(positions, errors)
    
    # if missing securities
    # missing = positions[positions['SecurityID'].isna()]
    # if len(missing):
    #     miss_pos = missing.to_csv(index=False)
    #     errors.append(f'Unknown Securities:\n{miss_pos}')

    return positions, errors

        
def check_options(positions, errors):
    
    # errors = []

    positions['is_option'] = positions['OptionType'].isin(['Call', 'Put'])
    
    # options, assign temp IDs ['X1', 'X2', ...]
    options = positions[positions['is_option']]
    positions.loc[positions['is_option'], 'SecurityID'] = ['X' + str(i) for i in options.index]

    missing = options[options['UnderlyingSecurityID'].isna()]
    if len(missing) > 0:
        names = missing['SecurityName'].to_list()
        errors.append(f'missing underlying security for {names}')

    # assume user input is ticker for underlying, get TRG SecurityID
    trg_ids = sc.get_ID_by_Ticker(options['UnderlyingSecurityID'].unique())
    positions['UnderlyingID'] = (positions[['UnderlyingSecurityID']]
                                 .reset_index()
                                 .merge(trg_ids, left_on='UnderlyingSecurityID', right_on='Ticker')
                                 .set_index('index')['SecurityID'])
    
    options = positions[positions['is_option']]
    missing = options[options['UnderlyingID'].isna()]
    if len(missing) > 0:
        tickers = missing['UnderlyingSecurityID'].to_list()
        errors.append(f'unknown underlying: {tickers}')

    return positions



# In[] test
def test_option():
    import xlwings as xw
    from utils import xl_utils as xl
    from utils import tools
    wb = xw.Book('Book1')
    positions = tools.read_positions(wb, 'Positions')
    positions['SecurityID'] = sc.get_SecurityID_by_ref(positions)

    positions, errors = check_options(positions)
    

    # xl.add_df_to_excel(positions, wb, 'pos1')


