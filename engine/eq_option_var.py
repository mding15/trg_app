# -*- coding: utf-8 -*-
"""
Created on Mon Jan 20 14:31:33 2025

@author: mgdin
"""
import pandas as pd
import numpy as np
import xlwings as xw
from pathlib import Path
import datetime
import re
import xlwings as xw
from utils import xl_utils as xl
from utils import tools, var_utils
from security import security_info as sc
from models import equity_options
from mkt_data import mkt_timeseries
from models import risk_factors as rfactors



# In[10]:

def calc_perf_prices_options(Positions, from_date='2022-01-01', end_date='2023-03-31'):
    # get option securities
    securities = get_eq_option_securities(Positions)
    if len(securities) == 0:
        return None
    
    # get underelying price history
    price_hist = mkt_timeseries.get_market_data(['SPX'], from_date, end_date)
    price_hist = price_hist.fillna(method='ffill')
    
    # get VOL historical data
    vol_hist = mkt_timeseries.get_market_data(['VIX'], from_date, end_date)
    vol_hist = vol_hist.fillna(method='ffill')
    if len(vol_hist.columns) == 0:
        print('error: can not find VIX timeseries')
        vol_hist['VIX']=np.nan
    
    # calculate IV and Greeks
    greeks = calc_option_greeks(securities)
    
    # attach greeks to securities
    securities = securities.merge(greeks, on='SecurityID', how='left')
    
    # calc option price history
    end_date = price_hist.index[-1]

    op_price_hist = pd.DataFrame(index=price_hist.index)
    for i in range(len(securities)):
        ID, op_type, und_sec, K, TT, r, iv = securities.iloc[i][
                ['SecurityID', 'OptionType', 'UnderlyingSecurityID', 'OptionStrike', 'Tenor', 'RiskFree', 'IV']]
        # scaled by implied vol
        sigma = vol_hist / vol_hist.iloc[-1] * iv
        ts = pd.concat([price_hist, sigma], axis=1)
        ts.columns = ['S', 'sigma']
        ts['TT'] = [(end_date - x).days/365+TT for x in ts.index]
        op_price_hist[ID] = ts.apply(lambda x: calc_price(op_type, x.S, K, x.TT, r, x.sigma), axis=1)    
        
    return op_price_hist


# In[20]:
def get_eq_option_securities(Positions):
    i_options = (Positions['AssetClass'] == 'Derivative') & (Positions['AssetType'] == 'Option')
    securities = Positions[i_options].drop_duplicates(subset=['SecurityID'])
    return securities[['SecurityID','Price', 'PriceDate','OptionType','UnderlyingSecurityID','OptionStrike', 'Maturity']]



# In[30]:
# xl_utils.add_df_to_excel()
# calc greeks of options
def calc_option_greeks(positions):
    
    columns = ['SecurityID', 'IV', 'DELTA', 'GAMMA', 'VEGA']
    options = positions.loc[positions.is_option]
    
    if len(options) == 0:
        return pd.DataFrame(columns=columns)

    
    option_greeks = options[['SecurityID', 'LastPrice', 'UnderlyingPrice', 'OptionType', 'UnderlyingSecurityID', 
                             'OptionStrike', 'RiskFreeRate', 'Tenor']].copy()
    
    i_valid = option_greeks['Tenor'] > 0
    
    # option IV
    option_greeks['IV'] = option_greeks[i_valid].apply(lambda x: calc_iv(x.OptionType, x.LastPrice, 
                                        x.UnderlyingPrice, x.OptionStrike, x.Tenor, x.RiskFreeRate), axis=1)

    # Greeks
    greeks = option_greeks[i_valid].apply(lambda x: 
                    calc_greeks(x.OptionType, x.UnderlyingPrice, x.OptionStrike, x.Tenor, x.RiskFreeRate, x.IV), axis=1)

    option_greeks.loc[i_valid, ['DELTA', 'GAMMA', 'VEGA']] = [[*x] for x in greeks]
    
    
    greeks = option_greeks[columns].set_index('SecurityID')
    return greeks



# In[127]:

# expected columns = ['SecurityID', 'UnderlyingID', 'DELTA']
def risk_factors(positions):
    
    risk_factors = rfactors.empty_risk_factors()

    options = positions[positions.is_option]
    if len(options) == 0:
        return risk_factors
    
    options = options.drop_duplicates(subset=['SecurityID']).set_index('SecurityID')
    
    # Options DELTA
    rf = pd.DataFrame(index=options.index)
    rf['Category'] = 'DELTA'
    rf['RF_ID'] = options['UnderlyingID']
    # dP = delta * dS = delta * S * (dS/S) = exposure * (dS/S)
    # exposure = delta * S * quantity * 100
    # rf['Sensitivity'] = options.DELTA * options.UnderlyingPrice * options['Quantity'] * 100
    rf['Sensitivity'] = 1
    rf = rf.reset_index()
    risk_factors = pd.concat([risk_factors, rf], ignore_index=True)
    
    # Options GAMMA
    rf = pd.DataFrame(index=options.index)
    rf['Category'] = 'GAMMA'
    rf['RF_ID'] = options['UnderlyingID']

    # dP = 0.5 * gamma * (dS)^2
    # dP = 0.5 * gamma * S^2 * (dS/S)^2 = 0.5 * exposure * (dS/S)^2
    # exposure = gamma * S^2 * quantity * 100
    # rf['Sensitivity'] = options.GAMMA * (options.UnderlyingPrice**2) * options['Quantity'] * 100
    rf['Sensitivity'] = 1
    rf = rf.reset_index()
    risk_factors = pd.concat([risk_factors, rf], ignore_index=True)
    
    # Options VEGA
    rf_id = sc.get_ID_by_Ticker(['VIX']).set_index('Ticker') # Hard coded, fix this later
    rf = pd.DataFrame(index=options.index)
    rf['Category'] = 'VEGA'
    rf['RF_ID'] = rf_id.loc['VIX', 'SecurityID']
    # dP = vega * ds  (ds: change in sigma)
    # PnL = vega * q * 100 * ds
    # exposure = vega *  q * 100
    rf['Sensitivity'] = 1
    rf = rf.reset_index()
    risk_factors = pd.concat([risk_factors, rf], ignore_index=True)
    
    
    return risk_factors


# In[11]:


def calc_iv(option_type, price, S, K, T, r):
    
    #print(option_type, price, S, K, T, r)

    if option_type == 'Call':
        iv = equity_options.iv_call(price, S, K, T, r)
    elif option_type == 'Put':
        iv = equity_options.iv_put(price, S, K, T, r)
    else:
        iv = None
    return iv
    

# S, K, T, r, sigma
# delta = dP/dS, gamma = ddP / (dS)^2, 
# vega = dP / d(vega), 1 percentage point change
def calc_greeks(op_type, S, K, T, r, sigma):
    if op_type == 'Call':
        delta,gamma = equity_options.call_delta_gamma(S, K, T, r, sigma)
        vega = equity_options.call_vega(S, K, T, r, sigma)
    elif op_type == 'Put':
        delta,gamma = equity_options.put_delta_gamma(S, K, T, r, sigma)
        vega = equity_options.put_vega(S, K, T, r, sigma)
    else:
        delta,gamma, vega = None, None, None
        
    return delta, gamma, vega
    
def calc_price(op_type, S, K, T, r, sigma):
    if op_type == 'Call':
        price = equity_options.BS_CALL(S, K, T, r, sigma)
    elif op_type == 'Put':
        price = equity_options.BS_PUT(S, K, T, r, sigma)
    else:
        price = np.nan
    return price


# In[ ]:

# underlying security price
def get_underlying_price(options, price_date):
    # price_date = datetime.datetime(2024, 9, 10)
    from_date = price_date + datetime.timedelta(-10)
    
    # sec_ids = ['T10000108', 'T10000368']
    sec_ids = options['UnderlyingID'].unique()
    prices = mkt_timeseries.get(sec_ids, from_date, price_date)
    return prices.ffill().iloc[-1]
    
    # und_prices['Und_Price'] = und_prices.apply(lambda x: mkt_timeseries.get_market_data([x['UnderlyingSecurityID']], x['PriceDate'], x['PriceDate']).iloc[0, 0], axis=1)
    # und_prices = und_prices.set_index('UnderlyingSecurityID')['Und_Price']
    # return tools.df_series_merge(options, und_prices, 'UnderlyingSecurityID') 
    
# wb = xw.Book('EquityOption.xlsx')

def wb_calc_greeks(wb):
    options = xl.read_df_from_excel(wb, 'Options')

    # underlying security price
    options['UnderlyingSecurityID'] = sc.Ticker_to_ID(options['UnderlyingSecurityID'])['SecurityID']

    # underlying security price
    options['UnderlyingPrice'] = get_underlying_price(options)
    
    # implied vol
    options['IV'] = options.apply(lambda x: 
                    calc_iv(x.OptionType, x.Price, x.UnderlyingPrice, x.OptionStrike, x.Tenor, x.RiskFree), axis=1)

    # Greeks
    greeks = options.apply(lambda x: 
                    calc_greeks(x.OptionType, x.UnderlyingPrice, x.OptionStrike, x.Tenor, x.RiskFree, x.IV), axis=1)

    options['DELTA'] = [x[0] for x in greeks]
    options['GAMMA'] = [x[1] for x in greeks]
    options['VEGA']  = [x[2] for x in greeks]

    xl.add_df_to_excel(options, wb, 'Options', index=False)

# calc option price
def wb_calc_prices(wb):
    options = xl.read_df_from_excel(wb, 'Options')

    # underlying security price
    options['UnderlyingSecurityID'] = sc.Ticker_to_ID(options['UnderlyingSecurityID'])['SecurityID']

    # underlying security price
    options['UnderlyingPrice'] = get_underlying_price(options)

    options['Price'] = options.apply(lambda x: 
              calc_price(x.OptionType, x.UnderlyingPrice, x.OptionStrike, x.Tenor, x.RiskFree, x.IV), axis=1)
        
    xl.add_df_to_excel(options, wb, 'Options', index=False)

# In[ ]:
def test(positions):
    
    wb = xw.Book('Book1')    
    
    params, positions = tools.load_test_portfolio()
    
    
    positions = tools.read_positions(wb)
    options = positions[positions.is_option]
    
    cob = datetime.datetime(2024, 6, 30)
    
    # underlying security price
    prices = get_underlying_price(options, cob)
    positions['UnderlyingPrice'] = tools.df_series_merge(options, prices, 'UnderlyingID') 
    
    positions['RiskFreeRate']  = 0.0375
    

    positions['Tenor'] = positions.apply(lambda x: (x.MaturityDate - cob).days/365, axis=1)

    greeks = calc_option_greeks(positions)
    xl.add_df_to_excel(greeks, wb, 'Greeks')

    ids = ['T10000002']
    ts = var_utils.get_dist(ids, 'VOL')
    xl.add_df_to_excel(ts, wb, 'vol_ts')
