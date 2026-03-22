# -*- coding: utf-8 -*-
"""
Created on Wed Aug 21 16:16:21 2024

@author: mgdin
"""
import pandas as pd
import numpy as np
import xlwings as xw
import datetime
from pathlib import Path

from trg_config import config
from models import model_utils
from models import bond_risk as br
from security import security_info
from mkt_data import mkt_timeseries, mkt_data_info
from utils import xl_utils, date_utils, stat_utils, tools, var_utils
from models import MODEL_WORKBOOK_DIR

# FILE_DIR = Path(r'C:\Users\mgdin\dev\TRG_App\Models\FX')

DATA = {}

def run_model_wb():
    # UPDATE THIS !!!
    model_id, submodel_id = 'M_20251231', 'FX.1'  

    # workbook
    wb = xw.Book(MODEL_WORKBOOK_DIR /model_id / f'{submodel_id}.xlsx')
    
    # load required model data 
    read_data_wb(wb, model_id, submodel_id)
    
    # generate distributions
    generate_dist(wb)
    
    # save model
    save_model(wb)

    
# read inputs from a workbook    
def read_data_wb(wb, model_id, submodel_id):
    core_params = model_utils.read_Model_Parameters(model_id)
    
    model_params = tools.read_parameter(wb)
    model_params['Model ID'] = model_id
    model_params['Submodel ID'] = submodel_id
    model_params['Model Type'] = 'FX'
    model_params['TS Start Date'] = core_params['TS Start Date']
    model_params['TS End Date'] = core_params['TS End Date']
    model_params['Number of Simulations'] = core_params['Number of Simulations']
    
    # override model_id, submodel_id, model_type
    print('updating wookbook Parameters ...')
    xl_utils.add_dict_to_excel(model_params, wb, 'Parameters')
    
    
    DATA['Parameters']  = model_params
    DATA['Securities']  = xl_utils.read_df_from_excel(wb, 'Securities')
    DATA['IndexDates']  = model_utils.read_index_dates(model_params)
    
    # hist price data
    prices = xl_utils.read_df_from_excel(wb, 'hist', index=True) 
    prices.index = pd.to_datetime(prices.index)
    
    # drop columns with all na, and replace 0 with np.nan
    prices = prices.dropna(axis=1, how='all')
    prices.replace(0, np.nan, inplace=True)

    DATA['hist_prices'] = prices

def generate_dist(wb):

    hist = DATA['hist_prices'] 
    
    # calculate percentage change
    hist_chg = hist.pct_change(1)    

    # replace 0 with np.nan
    hist_chg.replace(0, np.nan, inplace=True)
    
    # all convert return relative to base ccy
    # e.g. USD/JPY: 100 -> 105, USD is 5% stronger, convert back to USD would be -5%/(1+5%)
    hist_chg = - hist_chg / (1+hist_chg)
    
    # re-sampling for NA
    hist_chg = model_utils.fill_na_with_rand_sampling(hist_chg)

    index_dates = DATA['IndexDates'] 
    dist = index_dates.merge(hist_chg, left_index=True, right_index=True, how='left')    

    # resample in-case there is NA
    dist = model_utils.fill_na_with_rand_sampling(dist)
    DATA['dist'] = dist
    
    xl_utils.add_df_to_excel(dist, wb, 'dist')
    
    # stats    
    dist_stat = stat_utils.dist_stat(dist)
    DATA['dist_stat'] = dist_stat    
    xl_utils.add_df_to_excel(dist_stat, wb, 'dist_stat')


# save model data to csv file
def save_model(wb=None):
    model_utils.save_model_data(DATA, 'Parameters')
    model_utils.save_model_data(DATA, 'Securities')
    model_utils.save_model_data(DATA, 'IndexDates', index=True)

    model_utils.save_model_data(DATA, 'hist_prices', index=True)
    model_utils.save_model_data(DATA, 'dist', index=True)
    model_utils.save_model_data(DATA, 'dist_stat', index=True)
    
    # save dist
    dist = DATA['dist']
    model_utils.save_dist(DATA, dist, 'FX')

    # update risk_factor table
    model_utils.update_risk_factor(DATA, wb)

