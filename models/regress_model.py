# -*- coding: utf-8 -*-
"""
Created on Tue Sep 10 16:24:23 2024

@author: mgdin
"""

import pandas as pd
import numpy as np
import xlwings as xw
from pathlib import Path

from trg_config import config
from models import model_utils
from models import bond_risk as br
from models import risk_factors as rf
from security import security_info
from mkt_data import mkt_timeseries, mkt_data_info
from utils import xl_utils, date_utils, stat_utils, tools, var_utils, data_utils

FILE_DIR = Path(r'C:\Users\mgdin\dev\TRG_App\Models\RegressModel')

DATA = {}

def run_model():
    wb = xw.Book(FILE_DIR / 'M_20240531.Regress.1.xlsx')    
    read_data_wb(wb)

    # get historical timeseries    
    get_timeseries(wb)
    
    # generate risk distribution
    gen_dist(wb)
    
    # save model
    save_model()

    # save dist
    dist = DATA['security_dist']
    model_utils.save_dist(DATA, dist, 'PRICE')

    # save PE data
    df = xl_utils.read_df_from_excel(wb, 'PE', index=True)
    data_utils.save_stat(df, 'private_equity')    
    
# read inputs from a workbook    
def read_data_wb(wb):
    model_params = tools.read_parameter(wb)
    
    DATA['Parameters']  = model_params
    DATA['Securities']  = xl_utils.read_df_from_excel(wb, 'Securities')
    # DATA['IndexDates']  = model_utils.read_index_dates(model_params)
    DATA['CoreFactors'] = xl_utils.read_df_from_excel(wb, 'CoreFactors')

    
    
    # hist price data
    # prices = xl_utils.read_df_from_excel(wb, 'HistPrices', index=True, addr='A4') 
    # prices.index = pd.to_datetime(prices.index)
    
    # # drop columns with all na, and replace 0 with np.nan
    # prices = prices.dropna(axis=1, how='all')
    # prices.replace(0, np.nan, inplace=True)

    # DATA['hist_prices'] = prices

    # # PE Index
    # core_factors = xl_utils.read_df_from_excel(wb, 'CoreFactors')
    # DATA['CoreFactors'] = core_factors
    
    # core_factor_prices = xl_utils.read_df_from_excel(wb, 'PE Index', addr='A6', index=True)
    # core_factor_prices.index = pd.to_datetime(core_factor_prices.index)
    # DATA['CoreFactors_Prices'] = core_factor_prices    

def get_timeseries(wb):

    sec_ids1 = DATA['Securities']['SecurityID'].to_list()
    sec_ids2 = DATA['CoreFactors']['SecurityID'].to_list()
    sec_ids = sec_ids1 + sec_ids2
    
    params = DATA['Parameters']
    start_date, end_date = params['TS Start Date'], params['TS End Date']
    
    # historical price time series
    prices = mkt_timeseries.get(sec_ids)
    
    # check missing timeseries
    missing = list(set(sec_ids).difference(prices.columns))
    if len(missing) > 0:
        missing_ids = ', '.join(missing)
        raise Exception(f'missing timeseries for: {missing_ids}')
    
    # business dates
    dates = date_utils.get_bus_dates(start_date, end_date)
    
    # standardize the index using business dates
    df = pd.DataFrame(index=pd.Index(dates, name='Date'))
    timeseries = df.merge(prices, left_index=True, right_index=True, how='left')
    
    DATA['hist_prices'] = timeseries
    xl_utils.add_df_to_excel(timeseries, wb, 'hist_prices')
    
    # hist stat
    prices_ret = timeseries.fillna(method='ffill').pct_change(1)
    hist_stat = stat_utils.hist_stat(prices_ret)
    xl_utils.add_df_to_excel(hist_stat, wb, 'hist_stat', index=True)
    DATA['hist_stat'] = hist_stat


    
# save model data to csv file
def save_model():
    model_utils.save_model_data(DATA, 'Parameters')
    model_utils.save_model_data(DATA, 'Securities')
    model_utils.save_model_data(DATA, 'CoreFactors')
    # model_utils.save_model_data(DATA, 'IndexDates', index=True)
    model_utils.save_model_data(DATA, 'hist_prices', index=True)
    # model_utils.save_model_data(DATA, 'CoreFactors_Prices', index=True)
    model_utils.save_model_data(DATA, 'hist_stat', index=True)
    model_utils.save_model_data(DATA, 'corefactor_dist', index=True)
    model_utils.save_model_data(DATA, 'security_dist', index=True)
    model_utils.save_model_data(DATA, 'security_dist_stat', index=True)
    model_utils.save_model_data(DATA, 'betas', index=True)
    
    model_utils.save_model_info_data(DATA)

def gen_dist(wb):

    # hist prices
    hist_prices = DATA['hist_prices']
    
    # core factor prices    
    core_factors = DATA['CoreFactors']
    core_factor_prices = hist_prices[core_factors['SecurityID']]
    
    # core factors
    cf_dist = core_factor_prices.fillna(method='ffill').pct_change(1).replace(0, np.nan)

    # fill na with random sampling    
    cf_dist = model_utils.fill_na_with_rand_sampling(cf_dist)
    xl_utils.add_df_to_excel(cf_dist, wb, 'cf_dist')
    DATA['corefactor_dist'] = cf_dist
    
    
    """
      to do: 
          1. copy equity regression
          2. generate dist for securites
          3. save daa
    """
    
    
    
    
    # simulate security distributions, assume 0.5 correlation
    sys_dist = cf_dist.iloc[:,0]
    sys_vol = sys_dist.std()
    N = len(sys_dist)
    dist = {}
    betas = {}

    
    # adjusted hist vol
    adj_vol = xl_utils.read_df_from_excel(wb, 'hist_stat', index=True, addr='K1')


    # for sec_id, vol in hist_stat['StdValue'].items():
    for sec_id, (rho, vol) in adj_vol[['rho', 'adj vol']].iterrows():
        #print(sec_id, rho, vol)
        beta = rho * vol / sys_vol
        sigma = vol * np.sqrt(1 - rho**2)
        
        betas[sec_id] = [rho, sys_vol, vol, beta, sigma]
        dist[sec_id] = sys_dist * beta + np.random.normal(0, sigma, N)
    
    dist = pd.concat(dist, axis=1)    
    dist_stat = stat_utils.hist_stat(dist)
    betas = pd.DataFrame(betas, index=['rho', 'sys_vol', 'vol', 'beta', 'sigma']).T
    betas.index.name = 'SecurityID'
    
    xl_utils.add_df_to_excel(dist, wb, 'dist', index=True)
    xl_utils.add_df_to_excel(dist_stat, wb, 'dist_stat', index=True)
    xl_utils.add_df_to_excel(betas, wb, 'betas', index=True)
    
    DATA['security_dist'] = dist
    DATA['security_dist_stat'] = dist_stat
    DATA['betas'] = betas
    
#################################
# temp 
def regress_on_dist(wb):
    securities = DATA['Securities']
    corefactors = DATA['CoreFactors']
    
    hist_prices = DATA['hist_prices']
    hist_pct_ret = hist_prices.fillna(method='ffill').pct_change(1).dropna(how='all').fillna(method='ffill')
        
    # security distribution
    sec_ids = securities['SecurityID']
    Y = var_utils.get_dist(sec_ids, category='PRICE')    
    # Y = hist_pct_ret[sec_ids]
    
    # core factor distribution
    sec_ids = corefactors['SecurityID']
    X = var_utils.get_dist(sec_ids, category='PRICE')    
    # X = hist_pct_ret[sec_ids]


    # replace securityID with ticker
    # Y = Y.rename(columns=securities.set_index('SecurityID')['Ticker'].to_dict())
    X = X.rename(columns=corefactors.set_index('SecurityID')['Ticker'].to_dict())

    # regression    
    columns = X.columns.to_list() + ['R-Sq', 'Vol']
    regress_df  = pd.DataFrame(columns=columns)

    for sec_id in Y.columns:
        print(sec_id)

        df = pd.concat([Y[sec_id], X], axis=1)
        betas, b0, r_sq, y_vol, res =  stat_utils.linear_regression(df)
        regress_df.loc[sec_id] = np.append(betas, [r_sq, y_vol])

    regress_df.index.name = 'SecurityID'

    DATA['regress_df'] = regress_df
    xl_utils.add_df_to_excel(regress_df, wb, 'regress2', index=True)
    
