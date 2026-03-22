# -*- coding: utf-8 -*-
"""
Created on Tue Jun 11 11:56:52 2024

@author: mgdin
"""
import pandas as pd
import xlwings as xw

from trg_config import config
from models import model_utils
from models import risk_factors
from security import security_info
from mkt_data import mkt_timeseries
from utils import xl_utils, var_utils, tools
from models import MODEL_WORKBOOK_DIR

def run_model_wb():
    # UPDATE THIS !!!
    model_id, submodel_id = 'M_20251231', 'Cash.1'  

    # workbook
    wb = xw.Book(MODEL_WORKBOOK_DIR /model_id / f'{submodel_id}.xlsx')

    # model parameters
    params = read_model_params(wb, model_id, submodel_id)

    # generate USD cash distribution
    gen_usd_cash_dist(params)

    # load cash securities
    securities = xl_utils.read_df_from_excel(wb, 'Securities')
    
    # map the security to USD.CCY 
    DATA = run_model(securities, model_id)
    
    # write risk_factor to excel
    risk_factors = DATA['risk_factors']
    xl_utils.add_df_to_excel(risk_factors, wb, 'risk_factors', index=False)
    
    # samve model
    DATA['Parameters']=params
    save_model(DATA)

def read_model_params(wb, model_id, submodel_id):
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

    return model_params
    
    
# map cash Securities to USD.CCY which has zero distribution    
# fx risk is handled seperately
def run_model(securities, model_id=None):
    
    if model_id is None:
        model_id = var_utils.get_default_model_id()
    
    DATA = create_model(securities, model_id)

    # USD Cash Security
    usd = security_info.get_ID_by_Ticker(['USD.CCY'])
    usd_id = usd['SecurityID'].iloc[0]

    # remove USD.CCY
    securities = securities[securities['SecurityID']!=usd_id]
    
    if len(securities) == 0:
        return DATA

    sec_ids = securities['SecurityID']
    db_model_id = model_utils.get_db_model_id(model_id)
    
    # map all cash to usd
    rfactors = DATA['risk_factors']
        
    rfactors['SecurityID'] = sec_ids
    rfactors['Category']   = 'DELTA'    
    rfactors['RF_ID']   = usd_id
    rfactors['Sensitivity']   = 1
    rfactors['model_id']   = db_model_id
    
    risk_factors.db_insert(rfactors)
    
    return DATA
        

def create_model(securities, model_id):

    model_params = model_utils.read_Model_Parameters(model_id)
    model_params['Model Type'] = 'Cash'
    
    DATA = {'Parameters': model_params}
    DATA['Securities']   = securities
    DATA['risk_factors'] = risk_factors.empty_risk_factors()

    return DATA
       
############################################################################################

def gen_usd_cash_dist(params):

    # USD Cash Security
    usd = security_info.get_ID_by_Ticker(['USD.CCY'])
    usd_id = usd['SecurityID'].iloc[0]
    
    # check if the distribution already exists
    dist =    mkt_timeseries.get([usd_id])
    if not dist.empty:
        print('USD Cash distribution exists, skipped!')
        return
        
    # create cash distribution
    num_simulation = int(params['Number of Simulations'])
    dist = pd.DataFrame(columns=[usd_id], index=range(num_simulation))
    dist.fillna(0, inplace=True)
    
    # Save dist
    DATA = {'Parameters': params}
    model_utils.save_dist(DATA, dist, 'PRICE')

    # update risk_factor in db
    update_risk_factor(params, dist)
    
#
# update risk_factor db table    
#
def update_risk_factor(params, dist):
    
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
    
# save model data to csv file
def save_model(DATA):
    
    model_utils.save_model_data(DATA, 'Parameters')
    model_utils.save_model_data(DATA, 'Securities')
    model_utils.save_model_data(DATA, 'risk_factors')


