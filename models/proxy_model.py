# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 21:14:44 2024

@author: mgdin

simulate distribution based on a proxy

"""

import pandas as pd
import numpy as np
import xlwings as xw
from pathlib import Path

from models import model_utils
from mkt_data import mkt_timeseries
from utils import xl_utils, stat_utils, tools , var_utils

from trg_config import config
from security import security_info
from models import risk_factors

from models import MODEL_WORKBOOK_DIR

DATA = {}

def run_model_wb():
    
    # UPDATE THIS !!!
    model_id, submodel_id = 'M_20251231', 'Proxy.1'  

    # workbook
    wb = xw.Book(MODEL_WORKBOOK_DIR / model_id / f'{submodel_id}.xlsx')

    # load data
    read_data_wb(wb, model_id, submodel_id)

    # simulate distribution
    sim_dist(DATA)

    # update securities
    securities = DATA['Securities']
    xl_utils.add_df_to_excel(securities, wb, 'Securities', index=False)
    
    # update risk_factor
    update_risk_factor(DATA)

    # calculate distributioin
    save_model(DATA)
    
    

def run_model(securities, model_id=None, submodel_id=None):
    # securities = test_data()
        
    # create model
    DATA = create_model(securities, model_id, submodel_id)
    
    # calculate distributioin
    sim_dist(DATA)
    
    # update risk_factor
    update_risk_factor(DATA)

    # calculate distributioin
    save_model(DATA)

    return DATA


# read inputs from a workbook    
def read_data_wb(wb, model_id, submodel_id):
    core_params = model_utils.read_Model_Parameters(model_id)
    
    model_params = tools.read_parameter(wb)
    model_params['Model ID'] = model_id
    model_params['Submodel ID'] = submodel_id
    model_params['Model Type'] = 'Proxy'
    model_params['TS Start Date'] = core_params['TS Start Date']
    model_params['TS End Date'] = core_params['TS End Date']
    model_params['Number of Simulations'] = core_params['Number of Simulations']
    
    # override model_id, submodel_id, model_type
    xl_utils.add_dict_to_excel(model_params, wb, 'Parameters')
    
    DATA['Parameters']  = model_params
    DATA['Securities']  = read_securities(wb)
    DATA['IndexDates']  = model_utils.read_index_dates(model_params)
    DATA['CoreFactors'] = xl_utils.read_df_from_excel(wb, 'CoreFactors')
    
    CoreFactors = DATA['CoreFactors']   
    df = mkt_timeseries.get(CoreFactors['SecurityID']) 
    DATA['Proxy_HistPrices'] = df
    xl_utils.add_df_to_excel(df, wb, 'Proxy_HistPrices', index=True)

    DATA['risk_factors'] = risk_factors.empty_risk_factors()


def read_securities(wb):
    
    securities = xl_utils.read_df_from_excel(wb, 'Securities')
    
    # rename columns
    securities.columns = [x.replace(' ', '') for x in securities.columns]
    
    return securities


def test_data():
    file_path = config['DATA_DIR'] / 'test' / 'upload_security' / 'PC_MS_Portfolio.xlsx'
    
    securities = pd.read_excel(file_path, sheet_name='Security', dtype={'ISIN': str, 'CUSIP': str, 'SEDOL': str} )
    
    # normalize column names
    securities.columns = [x.replace(' ', '') for x in securities.columns]
    
    # rename columns
    securities = securities.rename(columns={'Cusip': 'CUSIP'})
    
    # special treatment on CUSIP
    if 'CUSIP' in securities:
        securities['CUSIP'] = securities['CUSIP'].apply(lambda x: x[-9:] if isinstance(x, str) else x) 
    securities['SecurityID'] = security_info.get_SecurityID_by_ref(securities)


    
    proxy_securities = pd.read_excel(file_path, sheet_name='Proxy', dtype={'Correlation': float, 'Vol Multiple': float})
    proxy_securities = proxy_securities.merge(securities[['ID', 'SecurityID']], on='ID', how='left')
    proxy_securities.columns = [x.replace(' ', '') for x in proxy_securities.columns]
    
    return proxy_securities

def test():
    securities = test_data()
    model = run_model(securities)
    

    
# used in batch 
def create_model(securities, model_id, submodel_id):
    
    if model_id is None:
        model_id = var_utils.get_default_model_id()

    if submodel_id is None:
        submodel_id = f'Proxy.{tools.file_ts()}'
    
    model_params = model_utils.read_Model_Parameters(model_id)
    model_params['Submodel ID'] = submodel_id
    model_params['Model Type'] = 'Proxy'
    
    DATA = {'Parameters': model_params}
    DATA['Securities']   = securities
    DATA['risk_factors'] = risk_factors.empty_risk_factors()
    
    return DATA
     
# simulate distribution 
def sim_dist(DATA):
    
    # securities
    df = DATA['Securities']
    
    # add proxy SecurityID
    tickers = df.loc[df['ProxySecurityID'].isna()]['ProxyTicker']
    df2 = security_info.get_ID_by_Ticker(tickers)
    df['ProxySecurityID'].fillna(df['ProxyTicker'].map(df2.set_index('Ticker')['SecurityID']), inplace=True)
    df.loc[df['ProxySecurityID'].isna(), 'Message']='Proxy is not valid'
    
    proxy_ids = df['ProxySecurityID'].unique()
    proxy_dist = var_utils.get_dist(proxy_ids)
    missing = set(proxy_dist.columns).difference(proxy_ids)
    df.loc[df['ProxySecurityID'].isin(missing), 'Message']='Proxy is not modeled'
    DATA['Securities']=df
    
    df = df[df['Message'].isna()]    
    df = df.set_index('SecurityID')

    N = len(proxy_dist)
    dist = {}
    betas = pd.DataFrame(columns=['SecurityName', 'rho', 'vol', 'proxy_vol', 'beta', 'sigma', 'sim_vol'])
    betas.index.name = 'SecurityID'
    for sec_id, row in df.iterrows():
        print(sec_id)
        rho      = row['Correlation']
        scaler   = row['VolMultiple']
        proxy_id = row['ProxySecurityID']
        securityName = row['SecurityName']
    
        sys = proxy_dist[proxy_id]
        proxy_vol = sys.std()
        vol = proxy_vol * scaler
        sigma = vol * np.sqrt(1-rho**2)
        beta = rho * scaler 
        
        sim_dist = sys * beta + np.random.normal(0, sigma, N)
        sim_vol = sim_dist.std()
        dist[sec_id] = sim_dist
        betas.loc[sec_id] = [securityName, rho, vol, proxy_vol, beta, sigma, sim_vol]
        
    dist = pd.concat(dist, axis=1)
    dist_stat = stat_utils.dist_stat(dist)
    DATA['dist'] = dist
    DATA['dist_stat'] = dist_stat
    DATA['betas'] = betas

#
# update risk_factor db table    
#
def update_risk_factor(DATA):
    
    params = DATA['Parameters']
    dist = DATA['dist']
    model_id = params['Model ID']
    
    sec_ids = dist.columns.to_list()
    db_model_id = model_utils.get_db_model_id(model_id)

    # delta risk factors
    rf = DATA['risk_factors']
    
    rf['SecurityID'] = sec_ids
    rf['Category']   = 'DELTA'    
    rf['RF_ID']   = sec_ids  
    rf['Sensitivity']   = 1
    rf['model_id']   = db_model_id
    
    risk_factors.db_insert(rf)
        

# save model data to csv file
def save_model(DATA):
    
    model_utils.save_model_data(DATA, 'Parameters')
    model_utils.save_model_data(DATA, 'Securities')
    model_utils.save_model_data(DATA, 'CoreFactors')
    model_utils.save_model_data(DATA, 'Proxy_HistPrices')

    model_utils.save_model_data(DATA, 'dist', index=True)
    model_utils.save_model_data(DATA, 'dist_stat', index=True)
    model_utils.save_model_data(DATA, 'betas', index=True)
    model_utils.save_model_data(DATA, 'risk_factors')
    
    # save dist
    dist = DATA['dist']
    model_utils.save_dist(DATA, dist, 'PRICE')

###################################################################################

def run_model_old():
    FILE_DIR = Path(r'C:\Users\mgdin\dev\TRG_App\Models\Proxy')
    wb = xw.Book(FILE_DIR / 'M_20240531.Proxy.1 - FIAE Bond Funds CLP.xlsx')    
    read_data_wb(wb)
    
    # generate distributions
    generate_dist(wb)
    
    # save model
    save_model()

    # save dist
    dist = DATA['dist']
    model_utils.save_dist(DATA, dist, 'PRICE')


def generate_dist(wb):

    securities  = DATA['Securities'].set_index('SecurityID')
    
    index_dates = DATA['IndexDates'] 

    # proxy hist prices
    proxy_prices = DATA['Proxy_HistPrices']
    proxy_ret = proxy_prices.fillna(method='ffill').pct_change(1)
    proxy_ret.replace(0, np.nan)
    proxy_stat = stat_utils.hist_stat(proxy_ret)
    xl_utils.add_df_to_excel(proxy_stat, wb, 'proxy_stat', index=True)
    DATA['proxy_stat'] = proxy_stat
    
    # proxy dist
    proxy_dist = index_dates.merge(proxy_ret, left_index=True, right_index=True, how='left')
    proxy_dist = proxy_dist.fillna(0)
    xl_utils.add_df_to_excel(proxy_dist, wb, 'proxy_dist', index=True)
    DATA['proxy_dist'] = proxy_dist
    
    N = len(proxy_dist)
    dist = {}
    betas = pd.DataFrame(columns=['rho', 'vol', 'proxy_vol', 'beta', 'sigma'])
    for sec_id, row in securities.iterrows():
        # print(sec_id)
        rho      = row['Correl']
        scaler   = row['Scaler']
        proxy_id = row['Proxy']
        if proxy_id not in proxy_dist:
            raise Exception(f'missing proxy: {proxy_id}')
    
        sys = proxy_dist[proxy_id]
        proxy_vol = sys.std()
        vol = proxy_vol * scaler
        sigma = vol * np.sqrt(1-rho**2)
        beta = rho * scaler 
        betas.loc[sec_id] = [rho, vol, proxy_vol, beta, sigma]
        dist[sec_id] = sys * beta + np.random.normal(0, sigma, N)
        
    dist = pd.concat(dist, axis=1)
    xl_utils.add_df_to_excel(dist, wb, 'dist', index=True)
    DATA['dist'] = dist
    
    # stats    
    dist_stat = stat_utils.hist_stat(dist)
    xl_utils.add_df_to_excel(dist_stat, wb, 'dist_stat', index=True)
    DATA['dist_stat'] = dist_stat

    # betas
    xl_utils.add_df_to_excel(betas, wb, 'betas', index=True)    
    DATA['betas'] = betas
    
