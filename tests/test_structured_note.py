# -*- coding: utf-8 -*-
"""
Created on Tue Oct 14 21:20:03 2025

@author: mgdin
"""

"""
UBS
STRUCTURED INVESTMENTS
Opportunities in U.S. and International Equities
Trigger Callable Contingent Yield Securities due July 21, 2027
$3,048,000 Based on the worst performing of the Nasdaq-100 Index®, the Russell 2000® Index and the S&P 500® Index
"""
import pandas as pd
import numpy as np
import xlwings as xw
from scipy import optimize

from utils import xl_utils
from models import binary_option, equity_options
from security import security_info
from utils import var_utils

wb_path = r'C:\Users\mgdin\dev\TRG_App\Models\StructuredNote\UBS - Contingent Yield Security.xlsx'

    
    
    
def test():
    wb = xw.Book(wb_path)
    config = xl_utils.read_dict(wb, 'config')
    
    # read replicate portfolio    
    positions = xl_utils.read_df_from_excel(wb, 'Replicate')
    positions['Tenor'] = positions.apply(lambda x: (x['Maturity'] - x['Price Date']).days/365, axis=1)

    # calculate implied volatility, and values of each legs
    iv = calc_iv(1000, positions)
    positions['ImpliedVol'] = iv
    positions['Value'] = calc_st_value(positions, sigma=iv)

    # calculate greeks
    deltas, gammas = calc_greeks(positions)
    positions['Delta'] = deltas
    positions['Gamma'] = gammas
    
    xl_utils.add_df_to_excel(positions, wb, 'df', index=False)

    # generate slides
    shocks = config['Slide Shocks'].split(',')
    shocks = [float(x) for x in shocks]
    slides = calc_slides(positions, shocks)
    xl_utils.add_df_to_excel(slides, wb, 'slides', index=False)


    # get underlying proxy
    ticker = config['Underlying Proxy']    
    sec_info = security_info.get_ID_by_Ticker([ticker])    
    sec_id = sec_info.iloc[0]['SecurityID']    
    und_dist = var_utils.get_dist([sec_id])

    # generate dist
    dist = np.interp(und_dist, xp=slides['shock'], fp=slides['value'])
    df = pd.DataFrame({sec_id: und_dist.iloc[:,0], 'st_node': dist.ravel()})

    xl_utils.add_df_to_excel(df, wb, 'dist')    
###############################################################################    
def calc_st_value(positions, sigma=0.2):
    values = []
    for i, row in positions.iterrows():
        ty, qt, S, K, T, r = row[['Option Type', 'Quantity', 'Underlying Price', 'Strike', 'Tenor', 'RiskfreeRate']]
        if ty == 'Binary Call':
            values.append( binary_option.CALL(S, K, T, r, sigma) * qt)
        elif ty == 'Binary Put':
            values.append (binary_option.PUT(S, K, T, r, sigma) * qt )
        elif ty == 'Call':
            values.append(equity_options.BS_CALL(S, K, T, r, sigma) * qt)
        elif ty == 'Put':
            values.append(equity_options.BS_PUT(S, K, T, r, sigma) * qt)
        else:
            values.append(None)
    return values

def calc_value(positions, sigma=0.2):
    value = calc_st_value(positions, sigma)
    return sum(value)
    
def calc_iv(price, positions, x0=0.2):
    def f(sigma):
        return calc_value(positions, sigma) - price

    try:
        sigma = optimize.newton(f, x0, maxiter=500, tol=0.001)
    except RuntimeError:
        sigma = np.nan
    
    return sigma

def calc_greeks(positions):
    
    deltas = []
    gammas = []
    for i, row in positions.iterrows():
        ty, qt, S, K, T, sigma, r = row[['Option Type', 'Quantity', 'Underlying Price', 'Strike', 'Tenor', 'ImpliedVol', 'RiskfreeRate']]

        delta, gamma = 0, 0
        if ty == 'Binary Call':
            delta, gamma = binary_option.call_delta_gamma(S, K, T, r, sigma)
        elif ty == 'Binary Put':
            delta, gamma = binary_option.put_delta_gamma(S, K, T, r, sigma)
        elif ty == 'Call':
            delta, gamma = equity_options.call_delta_gamma(S, K, T, r, sigma)
        elif ty == 'Put':
            delta, gamma = equity_options.put_delta_gamma(S, K, T, r, sigma)
        else:
            delta, gamma = None, None
            
        deltas.append(delta * qt)
        gammas.append(gamma * qt)
    return deltas, gammas

def calc_slides(positions, shocks):
    
    # shocks = [-0.99, -0.3, -0.2, -0.15, -0.1, -0.05, 0, 0.05, 0.1, 0.15, 0.2, 0.3, 1]
    
    pos = positions.copy()
    S0 = positions.iloc[0]['Underlying Price']
    sigma = positions.iloc[0]['ImpliedVol']
    V0 = calc_value(positions, sigma)  

    values = []    
    for s in shocks:
        pos['Underlying Price'] = S0 * (1+s)
        value = calc_value(pos, sigma)        
        values.append(value / V0 - 1) # percentage return

    df = pd.DataFrame({'shock': shocks, 'value': values})
    return df
