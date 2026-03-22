# -*- coding: utf-8 -*-
"""
Created on Wed Dec 24 12:26:52 2025

@author: mgdin
"""

import pandas as pd
import numpy as np
import xlwings as xw

from trg_config import config
from models import model_utils
from utils import xl_utils, stat_utils, tools
from models import MODEL_WORKBOOK_DIR
from database import db_utils

DATA = {}
    
def run_model():
    # UPDATE THIS !!!
    model_id, submodel_id = 'M_20251231', 'SpreadGeneric.1'  
    
    # workbook
    wb = xw.Book(MODEL_WORKBOOK_DIR /model_id / f'{submodel_id}.xlsx')

    # load model core data
    load_model_core_data(wb, model_id, submodel_id)
    
    # read inputs from a workbook    
    read_data_wb(wb)
    
    # update benchmark data
    load_benchmark(wb)
    
    #  simulate distribution
    simulate_dist(wb)
    
    # save model data to csv file
    save_model()


def simulate_dist(wb):
    
    betas = xl_utils.read_df_from_excel(wb, 'betas')
    DATA['betas'] = betas
    
    benchmark_dist = DATA['benchmark_dist']
    n_sim = len(benchmark_dist)
    
    dist_data = {}
    for b in betas.itertuples():
        bench_dist = benchmark_dist[b.benchmark]
        esp = np.random.randn(n_sim)
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
    

######################################################################################################################
def get_bond_securities(wb):
    sql = """
    select si."SecurityID", si."SecurityName", si."Currency", si."AssetClass", si."AssetType",  
    bi."MaturityDate", bi."IssuerTicker", bi."Rating", bi."Sector", bi."Country", bi."CouponRate", bi."CouponType", bi."PaymentFrequency", bi."Callable",
    bi."CallDate", bi."Formula", bi."Putable" , bi."DayCountBasis", bi."DatedDate", bi."FirstInterestPayment"  
    from security_info si left join bond_info bi on bi."SecurityID" = si."SecurityID"
    where si."AssetClass"='Bond' and si."AssetType"='Bond' and si."Currency" = 'USD'

    """
    df = db_utils.get_sql_df(sql)

    xl_utils.add_df_to_excel(df, wb, 'tmp', index=False)

######################################################################################################################
# pull benchmark data into excel
def load_benchmark(wb):

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

    
def read_benchmark_data(name, index_col=None):
    model_params = DATA['Parameters']
    data_dir = config['MODEL_DIR'] / model_params['Model ID'] / 'Credit_Benchmark'
    if not data_dir.exists():
        raise Exception(f'You need to run model Credit.0 before run this model: {data_dir}')
        
    filename = data_dir / f'{name}.csv'    
    df = pd.read_csv(filename, index_col=index_col)
    return df



# load model data
def load_model_core_data(wb, model_id, submodel_id):
    
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

# read inputs from a workbook    
def read_data_wb(wb):

    DATA['Securities']  = xl_utils.read_df_from_excel(wb, 'Securities')

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
    