# -*- coding: utf-8 -*-
"""
Created on Tue Jul 23 15:05:26 2024

@author: mgdin
"""

import pandas as pd
import numpy as np
import xlwings as xw

from models import model_utils
from security import security_info
from mkt_data import mkt_timeseries
from utils import xl_utils, stat_utils, tools, var_utils
from models import MODEL_WORKBOOK_DIR

DATA = {}


def run_model_wb():
    
    model_id, submodel_id = 'M_20251231', 'Benchmark'  
    
    # workbook
    wb = xw.Book(MODEL_WORKBOOK_DIR / model_id / f'{submodel_id}.xlsx')
    
    # read data from workbook
    read_data_wb(wb, model_id, submodel_id)

    # generate benchmark prices based on total return and weights
    generate_hist_prices(wb)

    # generate distributions
    generate_dist(wb)

    # save model
    save_model(wb)

# read inputs from a workbook    
# override model_id, submodel_id, model_type
def read_data_wb(wb, model_id, submodel_id):
    
    core_params = model_utils.read_Model_Parameters(model_id)
    
    model_params = tools.read_parameter(wb)
    model_params['Model ID'] = model_id
    model_params['Submodel ID'] = submodel_id
    model_params['Model Type'] = 'Benchmark'
    model_params['TS Start Date'] = core_params['TS Start Date']
    model_params['TS End Date'] = core_params['TS End Date']
    model_params['Number of Simulations'] = core_params['Number of Simulations']
    
    # override model_id, submodel_id, model_type
    print('updating workbook Parameters...')
    xl_utils.add_dict_to_excel(model_params, wb, 'Parameters')
    
    DATA['Parameters']  = model_params
    DATA['Securities']  = xl_utils.read_df_from_excel(wb, 'Securities')
    DATA['IndexDates']  = model_utils.read_index_dates(model_params)
    DATA['Weights'] = xl_utils.read_df_from_excel(wb, 'Weights', index=True)
    
# generate benchmark prices based on total return and weights
def generate_hist_prices(wb):

    securities = DATA['Securities'].set_index('SecurityID')
    weights = DATA['Weights']
    params = DATA['Parameters']

    tickers = weights.index.tolist()
    tic_sec_ids = security_info.get_ID_by_Ticker(tickers)

    # total returns
    from_date, to_date = model_utils.get_date_range(params)
    sec_ids = tic_sec_ids['SecurityID'].to_list()
    total_returns = mkt_timeseries.get_total_return(sec_ids, from_date, to_date)
    sec_id_to_tic = tic_sec_ids.reset_index().set_index('SecurityID')['Ticker'].to_dict()
    total_returns.rename(columns=sec_id_to_tic, inplace=True)
    total_returns.fillna(0, inplace=True)
    
    xl_utils.add_df_to_excel(total_returns, wb, tab='total_returns')
    DATA['total_returns'] = total_returns

    bm_data = {}
    for sec_id, ticker in securities['Ticker'].items():
        print(sec_id, ticker)
        w = weights[ticker]    
        bm_data[sec_id] = (1+(total_returns*w).sum(axis=1)).cumprod()
    
    bm_prices = pd.concat(bm_data, axis=1)
    xl_utils.add_df_to_excel(bm_prices, wb, tab='bm_prices')
    DATA['bm_prices'] = bm_prices

def generate_dist(wb):

    bm_prices = DATA['bm_prices']
    index_dates = DATA['IndexDates'] 
    
    # benchmark distribution
    bm_dist = bm_prices.pct_change(1)
    bm_dist = bm_dist.reindex(index_dates.index)    
    bm_dist.replace(0, np.nan, inplace=True)
    na_count = bm_dist.isna().sum()
    bm_dist = model_utils.fill_na_with_rand_sampling(bm_dist)
    xl_utils.add_df_to_excel(bm_dist, wb, tab='bm_dist')
    DATA['dist'] = bm_dist
    
    # stats    
    dist_stat = stat_utils.dist_stat(bm_dist)
    dist_stat['tVaR'] = var_utils.calc_tVaR(bm_dist)
    dist_stat['na_count'] = na_count
    xl_utils.add_df_to_excel(dist_stat, wb, tab='dist_stat')
    DATA['dist_stat'] = dist_stat
    
# save model data to csv file
def save_model(wb=None):

    model_utils.save_model_data(DATA, 'Parameters')
    model_utils.save_model_data(DATA, 'Securities')
    model_utils.save_model_data(DATA, 'Weights', index=True)
    model_utils.save_model_data(DATA, 'IndexDates', index=True)
    model_utils.save_model_data(DATA, 'total_returns', index=True)
    model_utils.save_model_data(DATA, 'bm_prices', index=True)
    model_utils.save_model_data(DATA, 'dist', index=True)
    model_utils.save_model_data(DATA, 'dist_stat', index=True)
    
    # save hist prices
    bm_prices = DATA['bm_prices']
    mkt_timeseries.save(bm_prices, 'system', 'PRICE')    
    
    # save dist
    dist = DATA['dist']
    model_utils.save_dist(DATA, dist, 'PRICE')
    
    # update risk_factor table
    model_utils.update_risk_factor(DATA, wb)

def test():
    model_id = 'M_20251031'
    wb = xw.Book(MODEL_WORKBOOK_DIR / model_id / 'Benchmark.xlsx')  
    bms = xl_utils.read_df_from_excel(wb, 'Securities')
    bm_dist = var_utils.get_dist(bms['SecurityID'])
    
    var = bms.set_index('SecurityID')
    var['tVaR'] = var_utils.calc_tVaR(bm_dist)

    wb1 = xw.Book()
    xl_utils.add_df_to_excel(var, wb1, 'VaR')
