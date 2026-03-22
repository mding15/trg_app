# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 13:16:05 2025

@author: mgdin

create VIX distribution

"""

import pandas as pd
import numpy as np
import xlwings as xw


from models import model_utils
from models import risk_factors
from mkt_data import mkt_timeseries
from utils import xl_utils, date_utils, stat_utils, tools, var_utils
from models import MODEL_WORKBOOK_DIR


DATA = {}

def run_model_wb():
    
    # UPDATE THIS !!!
    model_id, submodel_id = 'M_20251231', 'VIX'  
    
    # workbook
    wb = xw.Book(MODEL_WORKBOOK_DIR /model_id / f'{submodel_id}.xlsx')

    # read inputs from a workbook    
    read_data_wb(wb, model_id, submodel_id)

    # get historical data
    get_hist_data(wb)
    
    # generate distributions
    generate_dist(wb)

    # update risk_factors
    update_risk_factor(wb)
    
    # save model
    save_model()


# read inputs from a workbook    
def read_data_wb(wb, model_id, submodel_id):
    core_params = model_utils.read_Model_Parameters(model_id)
    
    model_params = tools.read_parameter(wb)
    model_params['Model ID'] = model_id
    model_params['Submodel ID'] = submodel_id
    model_params['Model Type'] = 'VOL'
    model_params['TS Start Date'] = core_params['TS Start Date']
    model_params['TS End Date'] = core_params['TS End Date']
    model_params['Number of Simulations'] = core_params['Number of Simulations']
    
    # override model_id, submodel_id, model_type
    xl_utils.add_dict_to_excel(model_params, wb, 'Parameters')
    
    DATA['Parameters']  = model_params
    DATA['Securities']  = xl_utils.read_df_from_excel(wb, 'Securities')
    DATA['IndexDates']  = model_utils.read_index_dates(model_params)

    for k in DATA.keys():
        print(k)

def get_hist_data(wb):
    
    # hist price data
    securities = DATA['Securities'] 
    prices = mkt_timeseries.get(securities['SecurityID'].to_list())
    print('writing security historical data to tab [hist]')
    xl_utils.add_df_to_excel(prices, wb, 'hist')
    
    # drop columns with all na, and replace 0 with np.nan
    prices = prices.dropna(axis=1, how='all')
    prices.replace(0, np.nan, inplace=True)

    DATA['hist_prices'] = prices

def generate_dist(wb):

    hist = DATA['hist_prices'] 
    
    # calculate change, hist data is in percentage point, 25 = 25%
    hist_chg = hist.diff(1)    
    print('writing hist_chg ...')
    xl_utils.add_df_to_excel(hist_chg, wb, 'hist_chg')
    
    # replace 0 with np.nan
    hist_chg.replace(0, np.nan, inplace=True)
    
    # fill NA, random re-sampling
    hist_chg = model_utils.fill_na_with_rand_sampling(hist_chg)

    index_dates = DATA['IndexDates'] 
    dist = index_dates.merge(hist_chg, left_index=True, right_index=True, how='left')    
    
    # resample in-case there is NA
    dist = model_utils.fill_na_with_rand_sampling(dist)
    DATA['dist'] = dist
    print('writing dist ...')
    xl_utils.add_df_to_excel(dist, wb, 'dist')
    
    # stats    
    dist_stat = stat_utils.dist_stat(dist)
    DATA['dist_stat'] = dist_stat    
    print('writing dist_stat ...')
    xl_utils.add_df_to_excel(dist_stat, wb, 'dist_stat')


# save model data to csv file
def save_model():
    model_utils.save_model_data(DATA, 'Parameters')
    model_utils.save_model_data(DATA, 'Securities')

    model_utils.save_model_data(DATA, 'hist_prices', index=True)
    model_utils.save_model_data(DATA, 'IndexDates', index=True)
    model_utils.save_model_data(DATA, 'dist', index=True)
    model_utils.save_model_data(DATA, 'dist_stat', index=True)
    
    # save dist
    dist = DATA['dist']
    model_utils.save_dist(DATA, dist, 'VOL')
    
#
# update risk_factor db table    
#

def update_risk_factor(wb):
    
    params = DATA['Parameters']
    dist = DATA['dist']
    model_id = params['Model ID']
    
    sec_ids = dist.columns.to_list()
    db_model_id = model_utils.get_db_model_id(model_id)

    # delta risk factors
    delta_rf = pd.DataFrame()
    
    delta_rf['SecurityID'] = sec_ids
    delta_rf['Category']   = 'VEGA'    
    delta_rf['RF_ID']   = sec_ids  
    delta_rf['Sensitivity']   = 1
    delta_rf['model_id']   = db_model_id
    
    risk_factors.db_insert(delta_rf)
    
    DATA['risk_factors'] = delta_rf
    print('writing risk_factors ...')
    xl_utils.add_df_to_excel(delta_rf, wb, 'risk_factors', index=False)


def test():
    rf_ids = ['T10000002']
    category = 'VOL'
    ts = var_utils.get_dist(rf_ids, category)
