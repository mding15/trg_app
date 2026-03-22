# -*- coding: utf-8 -*-
"""
Created on Sun Jul 14 15:03:24 2024

@author: mgdin

Model Private Credit Risk

"""

import pandas as pd
import numpy as np
import xlwings as xw

from models import model_utils
from mkt_data import mkt_timeseries
from utils import xl_utils, stat_utils, tools
from models import risk_factors
from models import MODEL_WORKBOOK_DIR

DATA = {}

def run_model_wb():
    
    model_id, submodel_id = 'M_20251231', 'PrivateCredit.1'  
    
    # workbook
    wb = xw.Book(MODEL_WORKBOOK_DIR / model_id / f'{submodel_id}.xlsx')

    # load data
    read_data_wb(wb, model_id, submodel_id)
    
    # generate dist
    gen_dist(wb)
    
    # update risk_factor
    update_risk_factor(wb)
    
    # save model
    save_model()

def gen_dist(wb):

    # prices = DATA['hist_prices']    
    core_factor_prices = DATA['CoreFactors_Prices']
    
    # core factors distribution
    corefactor_pct = core_factor_prices.fillna(method='ffill').pct_change(1).replace(0, np.nan).dropna()
    index_dates = DATA['IndexDates'] 
    cf_dist = index_dates.merge(corefactor_pct, left_index=True, right_index=True, how='left')
    
    # fill na with random sampling    
    cf_dist = model_utils.fill_na_with_rand_sampling(cf_dist)
    xl_utils.add_df_to_excel(cf_dist, wb, 'cf_dist')
    DATA['corefactor_dist'] = cf_dist

    cf_stat = stat_utils.dist_stat(cf_dist)
    xl_utils.add_df_to_excel(cf_stat, wb, 'cf_stat')
    DATA['corefactor_stat'] = cf_stat

    # simulate security distributions, assume 0.5 correlation
    betas = xl_utils.read_df_from_excel(wb, 'betas')
    cf_id = cf_dist.columns[0]
    sys_dist = cf_dist.reset_index(drop=True).iloc[:,0]
    N = len(sys_dist)
    dist = {cf_id: sys_dist}
    for b in betas.itertuples():
        # print(b.SecurityID, b.beta, b.sigma)
        dist[b.SecurityID] = sys_dist * b.beta + np.random.normal(0, b.sigma, N)
    
    dist = pd.concat(dist, axis=1)    
    dist_stat = stat_utils.dist_stat(dist)
    
    xl_utils.add_df_to_excel(dist, wb, 'dist', index=True)
    xl_utils.add_df_to_excel(dist_stat, wb, 'dist_stat', index=True)
    
    DATA['dist'] = dist
    DATA['dist_stat'] = dist_stat
    DATA['betas'] = betas
    
def PC_Index(wb):
    
    sec_id = 'T10000963' # BBB Spread
    bbb_spread = mkt_timeseries.get([sec_id], category='MACRO')
    bbb_spread.columns = ['BBB_Spread']    
    
    sp_leverage_loan = DATA['CoreFactors_Prices']
        
    pc_index= sp_leverage_loan.merge(bbb_spread, left_index=True, right_index=True, how='left')
    pc_index.fillna(method='ffill', inplace=True)
    xl_utils.add_df_to_excel(pc_index, wb, 'PC Index', index=True, addr='G6')   
    
    pc_index.pct_change(1).std()
    pc_index.iloc[:1044,:].pct_change(1).std()
    
# read inputs from a workbook    
def read_data_wb(wb, model_id, submodel_id):
    
    # model parameters
    model_params = tools.read_parameter(wb)

    # model core parameters
    core_params = model_utils.read_Model_Parameters(model_id)
    
    # update model parameters
    for name in ['Model ID', 'TS Start Date', 'TS End Date', 'Number of Simulations']:
        model_params[name] = core_params[name]

    model_params['Submodel ID'] = submodel_id

    # update model parameters
    print('updating wookbook Parameters...')
    xl_utils.add_dict_to_excel(model_params, wb, 'Parameters')
    
    DATA['Parameters']  = model_params
    DATA['Securities']  = xl_utils.read_df_from_excel(wb, 'Securities')
    DATA['IndexDates']  = model_utils.read_index_dates(model_params)

    # PC Index
    core_factors = xl_utils.read_df_from_excel(wb, 'CoreFactors')
    DATA['CoreFactors'] = core_factors
    
    core_factor_prices = xl_utils.read_df_from_excel(wb, 'PC Index', index=True)
    core_factor_prices.index = pd.to_datetime(core_factor_prices.index)
    core_factor_prices.columns = core_factors['Ticker'].to_list()
    DATA['CoreFactors_Prices'] = core_factor_prices


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
    delta_rf['Category']   = 'DELTA'    
    delta_rf['RF_ID']   = sec_ids  
    delta_rf['Sensitivity']   = 1
    delta_rf['model_id']   = db_model_id
    
    risk_factors.db_insert(delta_rf)
    
    DATA['risk_factors'] = delta_rf
    xl_utils.add_df_to_excel(delta_rf, wb, 'risk_factors', index=False)
    
# save model data to csv file
def save_model():
    model_utils.save_model_data(DATA, 'Parameters')
    model_utils.save_model_data(DATA, 'Securities')
    model_utils.save_model_data(DATA, 'CoreFactors')
    model_utils.save_model_data(DATA, 'IndexDates', index=True)
    model_utils.save_model_data(DATA, 'CoreFactors_Prices', index=True)
    model_utils.save_model_data(DATA, 'corefactor_dist', index=True)
    model_utils.save_model_data(DATA, 'corefactor_stat', index=True)
    model_utils.save_model_data(DATA, 'betas', index=False)
    model_utils.save_model_data(DATA, 'dist', index=True)
    model_utils.save_model_data(DATA, 'dist_stat', index=True)
    model_utils.save_model_data(DATA, 'risk_factors', index=False)

    # save dist
    dist = DATA['dist']
    model_utils.save_dist(DATA, dist, 'PRICE')

    

