# -*- coding: utf-8 -*-
"""
Created on Fri Jul 12 15:41:32 2024

@author: mgdin
"""
import pandas as pd
import numpy as np
import xlwings as xw
from models import model_utils

from models import risk_factors as rf
from security import security_info
from database import db_utils
from mkt_data import mkt_timeseries, mkt_data_info
from utils import xl_utils, date_utils, stat_utils, tools, var_utils, data_utils
from models import risk_factors
from models import MODEL_WORKBOOK_DIR

DATA = {}

def run_model_wb():
    
    # UPDATE THIS!!!
    model_id, submodel_id = 'M_20251231', 'PrivateEquity.1'  
    
    # workbook
    wb = xw.Book(MODEL_WORKBOOK_DIR / model_id / f'{submodel_id}.xlsx')

    # read data from workbook
    read_data_wb(wb, model_id, submodel_id)
    
    # analyze hist prices and make assessment of vol
    hist_prices_analysis(wb)
    
    # generate risk distribution
    gen_dist(wb)
    
    # update risk_factor
    update_risk_factor(wb)
    
    # update db table private_equity
    update_db_equity_equity(wb)
    
    # save model
    save_model()
    
    
# analyze hist prices and make assessment of vol
def hist_prices_analysis(wb):
    securities = DATA['Securities']
    prices = DATA['hist_prices']    
    
    # hist prices    
    prices  = prices.dropna(axis=0, how='all')
    prices = prices.fillna(method='ffill')
    # xl_utils.add_df_to_excel(prices, wb, 'Prices')

    # missing prices
    missing = list(set(securities['SecurityID']).difference(prices))
    prices[missing]=0
    
    # percentage returns
    prices_ret = prices.pct_change(1).replace(0, np.nan)
    xl_utils.add_df_to_excel(prices_ret, wb, 'prices_ret', index=True)
    
    # hist stat
    hist_stat = stat_utils.hist_stat(prices_ret)
    xl_utils.add_df_to_excel(hist_stat, wb, 'hist_stat', index=True)
    DATA['hist_stat'] = hist_stat

def gen_dist(wb):

    # inputs
    index_dates = DATA['IndexDates']
    core_factor_prices = DATA['CoreFactors_Prices']
    betas = xl_utils.read_df_from_excel(wb, 'betas')
    
    # core factors dist
    corefactor_pct = core_factor_prices.fillna(method='ffill').pct_change(1).replace(0, np.nan).dropna()
    cf_dist = index_dates.merge(corefactor_pct, left_index=True, right_index=True, how='left')

    # fill na with random sampling    
    cf_dist = model_utils.fill_na_with_rand_sampling(cf_dist)
    xl_utils.add_df_to_excel(cf_dist, wb, 'cf_dist')
    DATA['corefactor_dist'] = cf_dist

    cf_stat = stat_utils.dist_stat(cf_dist)
    xl_utils.add_df_to_excel(cf_stat, wb, 'cf_stat')
    DATA['corefactor_stat'] = cf_stat
    
    # simulate security distributions, assume 0.5 correlation
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
    
def PE_Index(wb):

    from_date, to_date = model_utils.get_date_range(DATA['Parameters'])
    spx = mkt_timeseries.get_by_tickers(['SPX'],from_date, to_date, 'PRICE')
    spx.dropna(inplace=True)

    pe = DATA['CoreFactors_Prices']
    pe.columns=['PE Index']
    
    df = spx.merge(pe, left_index=True, right_index=True, how='left')
    xl_utils.add_df_to_excel(df, wb, 'PE Index', index=True, addr='K1')   
    

##################################################################################################    
# preprocess
    
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

    # PE Index
    core_factors = xl_utils.read_df_from_excel(wb, 'CoreFactors')
    DATA['CoreFactors'] = core_factors
    
    # hist price data
    prices = xl_utils.read_df_from_excel(wb, 'HistPrices', index=True) 
    prices.index = pd.to_datetime(prices.index)
    
    # drop columns with all na, and replace 0 with np.nan
    prices = prices.dropna(axis=1, how='all')
    prices.replace(0, np.nan, inplace=True)

    DATA['hist_prices'] = prices

    # core factor hist prices    
    core_factor_prices = xl_utils.read_df_from_excel(wb, 'PE Index', index=True)
    missing =  set(core_factors['SecurityID']).difference(core_factor_prices)
    if missing:
        print(f'missing core factor prices: {missing}')
    
    core_factor_prices.index = pd.to_datetime(core_factor_prices.index)
    DATA['CoreFactors_Prices'] = core_factor_prices
    

##################################################################################################    
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
    
# insert data to private_equity table
def update_db_equity_equity(wb):
    params = DATA['Parameters']
    model_id = params['Model ID']

    df = xl_utils.read_df_from_excel(wb, 'private_equity')
    df['model_id']=model_id
    DATA['private_equity']=df
    db_utils.insert_df('private_equity', df)
    
# save model data to csv file
def save_model():

    model_utils.save_model_data(DATA, 'Parameters')
    model_utils.save_model_data(DATA, 'Securities')
    model_utils.save_model_data(DATA, 'CoreFactors')
    model_utils.save_model_data(DATA, 'IndexDates', index=True)
    model_utils.save_model_data(DATA, 'hist_prices', index=True)
    model_utils.save_model_data(DATA, 'CoreFactors_Prices', index=True)
    model_utils.save_model_data(DATA, 'hist_stat', index=True)
    model_utils.save_model_data(DATA, 'corefactor_dist', index=True)
    model_utils.save_model_data(DATA, 'corefactor_stat', index=True)
    model_utils.save_model_data(DATA, 'betas', index=False)
    model_utils.save_model_data(DATA, 'dist', index=True)
    model_utils.save_model_data(DATA, 'dist_stat', index=True)
    model_utils.save_model_data(DATA, 'risk_factors', index=False)
    model_utils.save_model_data(DATA, 'private_equity', index=False)

    # save dist
    dist = DATA['dist']
    model_utils.save_dist(DATA, dist, 'PRICE')
    

    
    
    