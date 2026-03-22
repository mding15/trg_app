# -*- coding: utf-8 -*-
"""
Created on Mon Jul  1 17:18:12 2024

@author: mgdin
"""

import numpy as np
import xlwings as xw

from models import model_utils
from mkt_data import mkt_timeseries
from utils import stat_utils, tools, xl_utils
from models import MODEL_WORKBOOK_DIR

DATA = {}


# model_id, submodel_id = 'M_20251031', 'IR.1'
def run_ir_model(model_id, submodel_id, securities):
    
    model_id, submodel_id = 'M_20251231', 'IR.1'
    
    # workbook
    wb = xw.Book(MODEL_WORKBOOK_DIR / model_id / f'{submodel_id}.xlsx')
    
    # model parameters
    params = read_parameter(wb, model_id, submodel_id)
    DATA['Parameters']  = params

    # ir securities
    securities = tools.read_positions(wb, 'Securities')
    DATA['Securities']  = securities

    # date index
    index_dates = model_utils.read_index_dates(params)
    DATA['IndexDates'] = index_dates
    
    # get hist yield    
    sec_ids = securities['SecurityID']
    from_date, end_date = model_utils.get_date_range(params)
    ir_hist = mkt_timeseries.get(sec_ids, from_date, end_date, category='YIELD')
    DATA['ir_hist'] = ir_hist
    xl_utils.add_df_to_excel(ir_hist, wb, 'ir_hist')
    
    # data transfermation
    ir_hist = ir_hist.fillna(method='ffill')
    ir_dist  = ir_hist.diff(1)
    ir_dist.replace(0, np.nan, inplace=True)    
    ir_dist = ir_dist.merge(index_dates, left_index=True, right_index=True, how='outer')
    
    # random sampling to fill the NAs
    dist = model_utils.fill_na_with_rand_sampling(ir_dist)

    # pick data based on index dates
    dist  = dist.loc[index_dates.index]
    DATA['dist'] = dist
    xl_utils.add_df_to_excel(dist, wb, 'dist')

    # stats
    dist_stat = stat_utils.hist_stat(dist)
    dist_stat.index.name = 'SecurityID'
    DATA['dist_stat'] = dist_stat
    xl_utils.add_df_to_excel(dist_stat, wb, 'dist_stat')

    
    # save model
    save_model(DATA)


# read and update parameters
def read_parameter(wb, model_id, submodel_id):
    
    core_params = model_utils.read_Model_Parameters(model_id)
    
    model_params = tools.read_parameter(wb)
    model_params['Model ID'] = model_id
    model_params['Submodel ID'] = submodel_id
    model_params['Model Type'] = 'IR'
    model_params['Model Name'] = 'IR Model'
    model_params['TS Start Date'] = core_params['TS Start Date']
    model_params['TS End Date'] = core_params['TS End Date']
    model_params['Number of Simulations'] = core_params['Number of Simulations']
    
    # override model_id, submodel_id, model_type
    print('updating wookbook Parameters...')
    xl_utils.add_dict_to_excel(model_params, wb, 'Parameters')
    
    return model_params
    

# save model data to csv file
def save_model(DATA):
    model_utils.save_model_data(DATA, 'Parameters')
    model_utils.save_model_data(DATA, 'Securities')
    model_utils.save_model_data(DATA, 'IndexDates', index=True)
    
    model_utils.save_model_data(DATA, 'ir_hist', index=True)
    model_utils.save_model_data(DATA, 'dist', index=True)
    model_utils.save_model_data(DATA, 'dist_stat', index=True)

    # save dist
    dist = DATA['dist']
    model_utils.save_dist(DATA, dist, 'IR')

    
