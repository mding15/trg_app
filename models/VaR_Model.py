# -*- coding: utf-8 -*-
"""
Created on Thu Dec 25 12:41:27 2025

@author: mgdin
"""

import pandas as pd
import numpy as np
import xlwings as xw

from models import model_utils
from utils import xl_utils, stat_utils, tools, date_utils
from models import MODEL_WORKBOOK_DIR
from database import db_utils
from mkt_data import mkt_timeseries


DATA = {}

def create_new_model():
    model_id = 'M_20251231'
    
    # workbook
    wb = xw.Book(MODEL_WORKBOOK_DIR / model_id / 'VaR_Model.xlsx')

    # read inputs from a workbook    
    read_data_wb(wb)
    
    # create a new VaR hdf file and insert a new row in db table risk_model
    create_VaR_file()
    
    # create core factors
    generate_core_factors(wb)

    # save model
    save_model()

def generate_core_factors(wb):
    
    model_params = DATA['Parameters']
    corefactors  = DATA['CoreFactors']

    # corefactor timeseries    
    sec_ids = corefactors['SecurityID'].unique()
    hist_prices = mkt_timeseries.get(sec_ids)
    id_ticker_map = corefactors.drop_duplicates('SecurityID').set_index('SecurityID')['Ticker'].to_dict()
    hist_prices = hist_prices.rename(columns=id_ticker_map)
 
    start_date, end_date = model_utils.get_date_range(model_params)
    hist_prices=hist_prices.ffill()[(hist_prices.index>=start_date) & (hist_prices.index <= end_date)]
    DATA['corefactor_timeseries'] = hist_prices
    xl_utils.add_df_to_excel(hist_prices, wb, 'corefactor_timeseries')

    # generate index dates
    generate_IndexDates(model_params, wb)

    # generate core dist
    gen_core_factors_dist(wb)


def gen_core_factors_dist(wb):
    
    hist_prices = DATA['corefactor_timeseries']
    index_dates = DATA['IndexDates']
    
    prices = hist_prices.ffill()
    pct_return = prices.pct_change(1)
    pct_return = pct_return - pct_return.mean()
    log_return = np.log( 1 + pct_return )
    log_return.replace(0, np.nan, inplace=True)    
    
    # reindex
    core_dist = log_return.reindex(index_dates.index)
    core_dist = model_utils.fill_na_with_rand_sampling(core_dist)
    
    # de-mean
    core_dist = core_dist - core_dist.mean()
    xl_utils.add_df_to_excel(core_dist, wb, 'core_dist')
    
    DATA['core_dist'] = core_dist
    
    # stat
    dist_stat = stat_utils.dist_stat(core_dist)
    xl_utils.add_df_to_excel(dist_stat, wb, 'dist_stat')
    DATA['core_dist_stat'] = dist_stat

def generate_IndexDates(model_params, wb):

    start_date, end_date = model_utils.get_date_range(model_params)        
    dates = date_utils.get_bus_dates(start_date, end_date)

    # random sampling    
    n_sim = model_params['Number of Simulations']
    dates = dates[2:] # start from the third date 
    index_dates = np.random.choice(dates, size=n_sim, replace=False)
    index_dates = sorted(index_dates)
    df = pd.DataFrame({'Date': index_dates})
    xl_utils.add_df_to_excel(df, wb, 'index_dates', index=True)

    df = df.set_index('Date')
    DATA['IndexDates'] = df
    
    
# create a new VaR hdf file and insert a new row in db table risk_model    
def create_VaR_file():
    
    model_params = DATA['Parameters'] 
    model_id = model_params['Model ID']
    
    # create VaR file
    model_utils.create_var_model(model_params)
    
    # check if the new VaR file has been created correctly
    params = model_utils.read_Model_Parameters(model_id)
    print(tools.convert_to_json_str(params))
    
    # insert into db table: risk_model
    insert_db_risk_model(model_id, model_params['Description'])

    
# read inputs from a workbook    
def read_data_wb(wb):
    model_params = read_parameter(wb)
    DATA['Parameters'] = model_params
    
    corefactors = xl_utils.read_df_from_excel(wb, 'CoreFactors')
    DATA['CoreFactors'] = corefactors
    
def read_parameter(wb):
    model_params = tools.read_parameter(wb)
    model_params['Number of Simulations'] = int(model_params['Number of Simulations'])
    return model_params 

# model_id = 'M_20251031'        
# description="model calibration in Oct 2025"
def insert_db_risk_model(model_id, description="model calibration"):
    df = db_utils.get_sql_df(f"select * from risk_model where model_name = '{model_id}'")
    if df.empty:
        table_df = pd.DataFrame({'model_name':[model_id], 'description': description})
        db_utils.insert_df('risk_model', table_df, key_column='model_name')
        print(f'insert 1 row into risk_model for {model_id}')
    else:
        print(f'skipped! found {model_id} in table risk_model')

def save_model():
    model_params = DATA['Parameters']
    corefactors = DATA['CoreFactors']
    corefactor_timeseries = DATA['corefactor_timeseries']
    index_dates = DATA['IndexDates']
    core_dist   = DATA['core_dist']

    model_utils.save_corefactors(model_params, corefactors)
    model_utils.save_cf_timeseries(model_params, corefactor_timeseries)
    model_utils.save_index_dates(model_params, index_dates)
    model_utils.save_corefactor_dist(model_params, core_dist)
    model_utils.save_model_model_data(DATA, 'core_dist_stat', index=True)
    
#######################################################################################
# 
from utils import var_utils
from trg_config import config
def dist_stat(wb):
    model_id = 'M_20251231'
    # model_id = var_utils.get_model_id()
    print(f'model_id: {model_id}')
    
    # get all distribution IDs for model id    
    var_utils.set_model_id(model_id)
    dist_ids = var_utils.list_dist()

    data=[]
    for cat in ['PRICE', 'SPREAD', 'IR', 'FX', 'VOL']:
        print(cat)
        sec_ids = dist_ids[dist_ids['Category']==cat]['SecurityID'].to_list()
        dists = var_utils.get_dist(sec_ids, cat)

        stat = stat_utils.dist_stat(dists)
        df = dist_ids[dist_ids['Category']==cat]
        df = df.merge(stat, left_on='SecurityID', right_index=True, how='left')
        data.append(df)
    stat = pd.concat(data)

    wb = xw.Book(MODEL_WORKBOOK_DIR / model_id / 'VaR_Model.xlsx')
    xl_utils.add_df_to_excel(stat, wb, 'model_stat', index=False)
    

def get_risk_factor(wb):
    
    df = db_utils.get_sql_df("select * from risk_factor rf where model_id = 3")
    df['Category2'] = df['Category'].apply(lambda x: 'PRICE' if x=='DELTA' else x)
    xl_utils.add_df_to_excel(df, wb, 'risk_factor', index=False)

def load_model():
    wb = xw.Book('Bond Model.xlsx')
    wb = xw.Book()

    model_id, submodel_id = 'M_20240531', 'Credit.4'
    data_dir = config['MODEL_DIR'] / model_id / submodel_id
    files = list(data_dir.glob('*.csv'))
    for f in files:
        print(f.name)
        df = pd.read_csv(f)
        
        xl_utils.add_df_to_excel(df, wb, tab=f.stem, index=False)


def analyze_ts(wb):
    wb = xw.Book('Book2')
    df = xl_utils.read_df_from_excel(wb, 'securities')
    sec_ids = df['SecurityID'].to_list()
    hist_prices = mkt_timeseries.get(sec_ids)

    
    vol_total, vol_start, vol_end = {}, {}, {}
    for c, t in hist_prices.items():
        t = t.dropna().pct_change(1)
        vol_total[c] = t.std()
        vol_start[c] = t.iloc[:250].std()
        vol_end[c] = t.iloc[-250:].std()
    
    df1 = pd.DataFrame.from_dict(vol_total, orient='index', columns=['vol_total'])
    df2 = pd.DataFrame.from_dict(vol_start, orient='index', columns=['vol_start'])
    df3 = pd.DataFrame.from_dict(vol_end, orient='index', columns=['vol_end'])
    vol_df = pd.concat([df1, df2, df3], axis=1)

    xl_utils.add_df_to_excel(vol_df, wb, 'vol', index=True)
        
    xl_utils.add_df_to_excel(hist_prices, wb, 'hist_prices', index=True)
    
    