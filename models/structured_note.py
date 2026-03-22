# -*- coding: utf-8 -*-
"""
Created on Thu Oct 16 16:01:13 2025

@author: mgdin
"""

import pandas as pd
import numpy as np
import xlwings as xw
from scipy import optimize


from security import security_info
from database import db_utils
from utils import xl_utils, date_utils, stat_utils, tools, var_utils, data_utils
from models import model_utils
from models import risk_factors
from models import binary_option, equity_options

from models import MODEL_WORKBOOK_DIR

DATA = {}

def run_model_wb():
    
    model_id, submodel_id = 'M_20251031', 'StructuredNote.1'  
    
    # workbook
    wb = xw.Book(MODEL_WORKBOOK_DIR / model_id / f'{submodel_id}.xlsx')

    # read data from workbook
    read_data_wb(wb, model_id, submodel_id)
    
    # calibrate sigma in the evaluation
    calibrate_sigma(wb)
    
    # calculate slides
    generate_slides(wb)
    
    # generate risk distribution
    calc_dist(wb)
    
    # update risk_factor
    model_utils.update_risk_factor(DATA, wb)
    
    # save model
    save_model()


    


    
    
# read data from workbook
def read_data_wb(wb, model_id, submodel_id):
    core_params = model_utils.read_Model_Parameters(model_id)
    
    model_params = tools.read_parameter(wb)
    model_params['Model ID'] = model_id
    model_params['Submodel ID'] = submodel_id
    model_params['Model Type'] = 'StructuredNote'
    model_params['TS Start Date'] = core_params['TS Start Date']
    model_params['TS End Date'] = core_params['TS End Date']
    model_params['Number of Simulations'] = core_params['Number of Simulations']
    
    # override model_id, submodel_id, model_type
    xl_utils.add_dict_to_excel(model_params, wb, 'Parameters')
    
    DATA['Parameters']  = model_params
    DATA['Securities']  = read_securities(wb, 'Securities')
    DATA['Replica']  = read_securities(wb, 'Replica')
    DATA['config'] = xl_utils.read_dict(wb, 'config')

def calibrate_sigma(wb):

    calibration = DATA['Replica']
    calibration['Tenor'] = calibration.apply(lambda x: (x['Maturity'] - x['PriceDate']).days/365, axis=1)

    # calculate implied volatility, and values of each legs
    iv = calc_iv(1000, calibration)
    calibration['ImpliedVol'] = iv
    calibration['Value'] = calc_st_value(calibration, sigma=iv)

    # calculate greeks
    deltas, gammas = calc_greeks(calibration)
    calibration['Delta'] = deltas
    calibration['Gamma'] = gammas
    
    xl_utils.add_df_to_excel(calibration, wb, 'calibration', index=False)
    DATA['calibration']=calibration


def generate_slides(wb):
    config = DATA['config']
    calibration = DATA['calibration']
    
    # generate slides
    shocks = config['Slide Shocks'].split(',')
    shocks = [float(x) for x in shocks]
    slides = calc_slides(calibration, shocks)

    xl_utils.add_df_to_excel(slides, wb, 'slides', index=False)
    DATA['slides']=slides

def calc_dist(wb):
    config = DATA['config']
    slides = DATA['slides']
    securities = DATA['Securities']
    
    # get underlying proxy
    ticker = config['Underlying Proxy']    
    sec_info = security_info.get_ID_by_Ticker([ticker])    
    sec_id = sec_info.iloc[0]['SecurityID']    
    und_dist = var_utils.get_dist([sec_id])

    # generate dist
    dist = np.interp(und_dist, xp=slides['shock'], fp=slides['value'])
    sn_id = securities['SecurityID'].iloc[0]
    df = pd.DataFrame({sec_id: und_dist.iloc[:,0], sn_id: dist.ravel()})
    xl_utils.add_df_to_excel(df, wb, 'dist_calc')    
    DATA['dist_calc'] = df

    dist = df.iloc[:,[1]]
    dist_stat = stat_utils.dist_stat(dist)
    dist_stat.index.name = 'SecurityID'
    
    xl_utils.add_df_to_excel(dist, wb, 'dist')    
    xl_utils.add_df_to_excel(dist_stat, wb, 'dist_stat')    

    DATA['dist'] = dist
    DATA['dist_stat'] = dist_stat

# save model
def save_model():
    DATA.keys()
    model_utils.save_model_data(DATA, 'Parameters')
    model_utils.save_model_data(DATA, 'Securities')
    model_utils.save_model_data(DATA, 'Replica')
    model_utils.save_model_data(DATA, 'calibration')
    model_utils.save_model_data(DATA, 'config', index=True)
    model_utils.save_model_data(DATA, 'slides', index=False)
    model_utils.save_model_data(DATA, 'dist_cal', index=False)
    model_utils.save_model_data(DATA, 'dist', index=True)
    model_utils.save_model_data(DATA, 'dist_stat', index=True)
    model_utils.save_model_data(DATA, 'risk_factors', index=False)

    # save dist
    dist = DATA['dist']
    model_utils.save_dist(DATA, dist, 'PRICE')

##################################################################################################
# analytic
def calc_st_value(positions, sigma=0.2):
    values = []
    for i, row in positions.iterrows():
        ty, qt, S, K, T, r = row[['OptionType', 'Quantity', 'UnderlyingPrice', 'Strike', 'Tenor', 'RiskfreeRate']]
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
        ty, qt, S, K, T, sigma, r = row[['OptionType', 'Quantity', 'UnderlyingPrice', 'Strike', 'Tenor', 'ImpliedVol', 'RiskfreeRate']]

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
    S0 = positions.iloc[0]['UnderlyingPrice']
    sigma = positions.iloc[0]['ImpliedVol']
    V0 = calc_value(positions, sigma)  

    values = []    
    for s in shocks:
        pos['UnderlyingPrice'] = S0 * (1+s)
        value = calc_value(pos, sigma)        
        values.append(value / V0 - 1) # percentage return

    df = pd.DataFrame({'shock': shocks, 'value': values})
    return df
    
#####################################################################################
# auxilary
def read_securities(wb, tab='Securities'):
    
    securities = xl_utils.read_df_from_excel(wb, tab)
    
    # rename columns
    securities.columns = [x.replace(' ', '') for x in securities.columns]
    
    return securities
