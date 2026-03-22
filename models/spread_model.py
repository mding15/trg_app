# -*- coding: utf-8 -*-
"""
Created on Sun Dec 28 15:24:06 2025

@author: mgdin
"""

import pandas as pd
import numpy as np
import xlwings as xw

from trg_config import config
from models import model_utils
from utils import xl_utils, stat_utils, tools
from models import MODEL_WORKBOOK_DIR
from security import security_info
from mkt_data import mkt_timeseries

DATA = {}
    
def run_model():
    # UPDATE THIS!!!
    model_id, submodel_id = 'M_20251231', 'Spread.1'  
    
    # workbook
    wb = xw.Book(MODEL_WORKBOOK_DIR /model_id / f'{submodel_id}.xlsx')

    # load model data required
    load_data(wb, model_id, submodel_id)
    
    # equity regression and residual    
    residual, regress_df = get_eq_residuals(wb)

    # simulate distribution
    simulate_dist(wb)
    
    # save model data to csv file
    save_model()


def simulate_dist(wb):
    
    betas = xl_utils.read_df_from_excel(wb, 'betas')
    DATA['betas'] = betas
    
    benchmark_dist = DATA['benchmark_dist']
    benchmark_dist.reset_index(drop=True, inplace=True)
    
    # ido esp taken from equity residuals 
    eq_esp = DATA['eq_esp']
    eq_esp.reset_index(drop=True, inplace=True)
    
    
    dist_data = {}
    for b in betas.itertuples():
        print(b.SecurityID, b.benchmark, b.beta, b.sigma, b.Spread)
        bench_dist = benchmark_dist[b.benchmark]
        esp = eq_esp[b.SecurityID]
        if b.Grade == 'HG':
            dist_data[b.SecurityID] = b.beta * bench_dist - b.sigma * esp
        else:
            dist_data[b.SecurityID] = (b.beta * bench_dist - b.sigma) * b.Spread
        
    dist = pd.concat(dist_data, axis=1)
    dist_stat = stat_utils.dist_stat(dist)

    DATA['dist'] = dist
    DATA['dist_stat'] = dist_stat

    xl_utils.add_df_to_excel(dist, wb, 'dist')
    xl_utils.add_df_to_excel(dist_stat, wb, 'dist_stat')
    

def load_data(wb, model_id, submodel_id):
    # model parameters
    params = tools.read_parameter(wb)

    # model core parameters
    core_params = model_utils.read_Model_Parameters(model_id)
    
    # update model parameters
    for name in ['Model ID', 'TS Start Date', 'TS End Date', 'Number of Simulations']:
        params[name] = core_params[name]

    # update model parameters
    print('updating wookbook Parameters...')
    xl_utils.add_dict_to_excel(params, wb, 'Parameters')

    # index dates
    index_dates = model_utils.read_index_dates(params)
    xl_utils.add_df_to_excel(index_dates.reset_index(), wb, 'IndexDates')
    
    DATA['Parameters']  = params
    DATA['IndexDates']  = index_dates
    DATA['Securities']  = xl_utils.read_df_from_excel(wb, 'Securities')
    
    # benchmark data
    df = read_benchmark_data('Sectors')
    xl_utils.add_df_to_excel(df, wb, 'Sectors', index=False)
    DATA['Sectors'] = df
    
    df = read_benchmark_data('Ratings')
    xl_utils.add_df_to_excel(df, wb, 'Ratings', index=False)
    DATA['Ratings'] = df

    df = read_benchmark_data('rating_vol')    
    xl_utils.add_df_to_excel(df, wb, 'rating_vol', index=False)
    DATA['rating_vol'] = df
    
    df = read_benchmark_data('dist', index_col=0)
    xl_utils.add_df_to_excel(df, wb, 'benchmark_dist', index=True)
    DATA['benchmark_dist'] = df

    df1 = pd.DataFrame(df.std(), columns=['benchmark_vol'])
    df1.index.name = 'benchmark'
    xl_utils.add_df_to_excel(df1, wb, 'Benchmark', index=True)
    DATA['Benchmark'] = df1

    # issuer equity distribution    
    eq_sec_dist = read_benchmark_data('eq_sec_dist', index_col=0)
    eq_sec_dist.index = pd.to_datetime(eq_sec_dist.index)
    xl_utils.add_df_to_excel(eq_sec_dist, wb, 'eq_sec_dist', index=True)
    DATA['eq_sec_dist'] = eq_sec_dist


# run parent equity regression, and return the residuals
# equity residuals
beta_columns = ['Ticker','Sector', 'Beta', 'R-Sq', 'Vol']
def get_eq_residuals(wb):
    
    securities = DATA['Securities']
    index_dates = DATA['IndexDates'] 

    # securities that have issuer ticker
    securities = securities[~securities['IssuerTicker'].isna()]

    if len(securities) == 0:
        return pd.DataFrame(index=index_dates.index), pd.DataFrame(columns=beta_columns)
    
    # equity timeseries
    eq_securities = security_info.get_ID_by_Ticker(securities['IssuerTicker'])    
    eq_hist = mkt_timeseries.get(eq_securities['SecurityID'], category='PRICE')
    eq_hist = eq_hist.rename(columns= eq_securities.set_index('SecurityID')['Ticker'].to_dict())
    
    eq_dist = eq_hist.merge(index_dates, left_index=True, right_index=True, how='outer')
    eq_dist = eq_dist.fillna(method='ffill')
    eq_dist = eq_dist.pct_change(1)
    eq_dist = eq_dist.loc[index_dates.index] 
    # has 0 values, replace with random sampling
    eq_dist.replace(0, np.nan, inplace=True)    
    eq_dist = model_utils.fill_na_with_rand_sampling(eq_dist)
    
    DATA['eq_dist'] = eq_dist
    xl_utils.add_df_to_excel(eq_dist, wb, 'eq_dist')

    # benchmark eq dist
    eq_sec_dist = DATA['eq_sec_dist']

    # sectors
    sectors = DATA['Sectors']
    sectors_tck = sectors.set_index('Sector')['Ticker'].to_dict()

    # regress against sector index

    regress_df  = pd.DataFrame(columns=beta_columns)
    residual_df = pd.DataFrame(index=index_dates.index)

    # i=0
    for i in range(len(securities)):
        sec_id, sector, ticker = securities.iloc[i][['SecurityID', 'Sector', 'IssuerTicker']]
        
        sec_tck = sectors_tck[sector]
        X = eq_sec_dist[sec_tck]
        Y = eq_dist[ticker]
    
        df = pd.concat([Y, X], axis=1)
        betas, b0, r_sq, y_vol, res =  stat_utils.linear_regression(df)
    
        regress_df.loc[sec_id] = [ticker, sec_tck, betas[0], r_sq, y_vol]
        res_df = pd.DataFrame(data = res, columns = [sec_id], index=index_dates.index)
        residual_df = pd.concat([residual_df, res_df], axis=1)

    DATA['eq_regress'] = regress_df
    DATA['eq_residuals'] = residual_df
    xl_utils.add_df_to_excel(regress_df, wb, 'eq_regress')
    xl_utils.add_df_to_excel(residual_df, wb, 'eq_residuals')

    # normalize residuals
    vols = residual_df.std()
    means = residual_df.mean()
    eq_esp = (residual_df - means) / vols
    DATA['eq_esp'] = eq_esp
    xl_utils.add_df_to_excel(eq_esp, wb, 'eq_esp')
    
    return eq_esp, regress_df



# xl_utils.add_df_to_excel(df, wb, 'df')
###############################################################################################
def read_benchmark_data(name, index_col=None):
    model_params = DATA['Parameters']
    data_dir = config['MODEL_DIR'] / model_params['Model ID'] / 'Credit_Benchmark'
    if not data_dir.exists():
        raise Exception(f'You need to run model Credit.0 before run this model: {data_dir}')
        
    filename = data_dir / f'{name}.csv'    
    df = pd.read_csv(filename, index_col=index_col)
    return df
    
# save model data to csv file
def save_model():
    
    model_utils.save_model_data(DATA, 'Parameters')
    model_utils.save_model_data(DATA, 'Securities')
    model_utils.save_model_data(DATA, 'Sectors')
    model_utils.save_model_data(DATA, 'Ratings')
    model_utils.save_model_data(DATA, 'rating_vol', index=False)
    model_utils.save_model_data(DATA, 'benchmark_dist', index=True)
    model_utils.save_model_data(DATA, 'Benchmark', index=True)
    model_utils.save_model_data(DATA, 'betas', index=False)
    model_utils.save_model_data(DATA, 'dist', index=True)
    model_utils.save_model_data(DATA, 'dist_stat', index=True)


    # save dist
    dist = DATA['dist']
    model_utils.save_dist(DATA, dist, 'SPREAD')
    