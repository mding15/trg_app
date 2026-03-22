# -*- coding: utf-8 -*-
"""
Created on Mon Mar  3 08:22:48 2025

@author: mgdin
"""

import pandas as pd
import numpy as np
import xlwings as xw
from pathlib import Path
import datetime

from trg_config import config
from models import model_utils
from security import security_info as sc
from mkt_data import mkt_timeseries, mkt_data_info
from utils import xl_utils, date_utils, stat_utils, tools, var_utils
from utils import df_utils
from database import db_utils
from models import risk_factors
from models import MODEL_WORKBOOK_DIR


 
TS_MIN_LEN = 100

def run_model_wb():
    model_id, submodel_id = 'M_20251231', 'Equity.3'  
    
    # workbook
    wb = xw.Book(MODEL_WORKBOOK_DIR / model_id / f'{submodel_id}.xlsx')

    # model parameters
    model_params = read_parameter(wb, model_id, submodel_id)

    # read security
    # get_eq_securities(wb, model_params)
    securities = tools.read_positions(wb, 'Securities')

    # get hist prices
    hist_prices = get_hist_prices(securities, model_params)

    # load required DATA
    DATA = load_data(model_params, securities, hist_prices)
    write_load_data_to_xl(DATA, wb)

    # calculate distributioin
    sim_dist(DATA)
    write_sim_data_to_xl(DATA, wb)
    
    # calculate distributioin
    save_model(DATA)

    
# wb = xw.Book('Book1')
# xl_utils.add_df_to_excel(prices, wb, 'df')

# model_id, submodel_id = 'M_20240531', 'Equity.12'
# filename = 'ModelTest1.xlsx'
# hist_prices=eq_prices
# securities=equities
def run_model(securities, hist_prices, model_id=None, submodel_id=None):
    
    if model_id is None:
        model_id = var_utils.get_default_model_id()

    if submodel_id is None:
        submodel_id = f'Equity.{tools.file_ts()}'
        
    # create model parameters
    model_params = create_params(model_id, submodel_id)

    # load required DATA
    DATA = load_data(model_params, securities, hist_prices)
    
    # calculate distributioin
    sim_dist(DATA)
    
    # calculate distributioin
    save_model(DATA)
    
    return DATA
    
    
def create_params(model_id, submodel_id):
    model_params = model_utils.read_Model_Parameters(model_id)
    model_params['Submodel ID'] = submodel_id
    model_params['Model Type'] = 'Equity'
    return model_params
 
    
def load_data(model_params, securities, hist_prices):
    # create container
    DATA = {'Parameters': model_params}

    # add model data
    DATA['CoreFactors']  = model_utils.load_corefactors(model_params)
    DATA['IndexDates']   = model_utils.read_index_dates(model_params)
    DATA['CoreFactors_Prices'] = model_utils.read_cf_timeseries(model_params)
    DATA['corefactor_dist']    = model_utils.read_corefactor_dist(model_params)
    
    # read security upload excel file
    DATA['Securities']   = securities
    DATA['hist_prices']  = hist_prices
    
    # xl_utils.add_df_to_excel(prices, wb, 'prices')
    
    DATA['security_timeseries']  = get_security_timeseries(DATA)
    DATA['core_factors_timeseries']  = get_core_factors_timeseries(DATA)
    

    return DATA    
    
def sim_dist(DATA):
    
    # run regression against core_factors 
    run_regression(DATA)
    
    # core factors log returns
    core_factors = gen_core_factors_dist(DATA)
    
    # security betas to core factors
    betas = DATA['regress_df']
    residuals = gen_residuals(DATA)

    # N = num of simulation; nc = number of core factors
    N, nc = core_factors.shape

    # systematic returns
    sys_dist = np.dot(core_factors, betas.iloc[:,:nc].T)
    sys_dist = pd.DataFrame(sys_dist, columns=betas.index)
    DATA['sys_dist'] = sys_dist

    # idiosyncratic returns
    residuals = residuals[sys_dist.columns]

    # total log returns
    log_ret = sys_dist.values + residuals.values
    log_ret = pd.DataFrame(log_ret, columns=sys_dist.columns)
    simulated_dist = np.exp(log_ret) - 1

    stat_df = DATA['stat_df']
    
    
    stat_df['resi_cnt']  = DATA['residual_count']
    stat_df['sys_vol']   = sys_dist.std()        
    stat_df['idio_vol']  = residuals.std()        
    stat_df['total_vol'] = log_ret.std()        
    stat_df['sim_vol'] = simulated_dist.std()        

    DATA['simulated_dist'] = simulated_dist    
    

def gen_core_factors_dist(DATA):
    if 'corefactor_dist' in DATA:
        return DATA['corefactor_dist']

    prices = DATA['core_factors_timeseries']
    
    prices = prices.ffill()
    pct_return = prices.pct_change(1)
    pct_return = pct_return - pct_return.mean()
    log_return = np.log( 1 + pct_return )
    log_return.replace(0, np.nan, inplace=True)    

    
    # reindex
    index = DATA['IndexDates'] 
    core_dist = log_return.reindex(index.index)
    core_dist = model_utils.fill_na_with_rand_sampling(core_dist)
    DATA['corefactor_dist'] = core_dist

    # xl_utils.add_df_to_excel(core_dist, wb, 'df')
    return core_dist

# generate residuals
def gen_residuals(DATA):    

    betas = DATA['regress_df']
    residuals = DATA['residual_df']
    index = DATA['IndexDates']
    
    residuals = index.merge(residuals, left_on='Date', right_index=True, how='left')

    # check if beta and residuals have different sec list
    diff = set(betas.index).difference(residuals.columns)
    if diff:
        print('Error: betas and residuals have different security list')

    # number of residuals that are not na
    residual_count = residuals.count()
    DATA['residual_count'] = residual_count

    params = DATA['Parameters']
    num_simulation = params['Number of Simulations']
    
    # fill na with normal draw that std = res_vol    
    for sec_id, num in residual_count.items():
        if num == num_simulation: # if no missing data
            continue
        
        n = num_simulation - num
        rsq, vol = betas.loc[sec_id][['R-Sq', 'Vol']]
        res_vol = vol * np.sqrt(1-rsq)
        
        eps = np.random.normal(0, res_vol, n)
        residuals.loc[residuals[sec_id].isna(), sec_id] = eps
        
    DATA['idio_dist']  = residuals
    
    return residuals


def get_security_timeseries(DATA):
    
    params = DATA['Parameters']
    Securities = DATA['Securities']
    sec_ids = Securities['SecurityID'].to_list()
    
    # historical price time series
    prices = DATA['hist_prices']
    start_date, end_date = params['TS Start Date'], params['TS End Date']
    prices = prices[(prices.index >= start_date) & (prices.index <= end_date)]
    
    # check missing timeseries
    missing = list(set(sec_ids).difference(prices.columns))
    if missing:
        missing_ids = ', '.join(missing)
        raise Exception(f'missing timeseries for: {missing_ids}')
    
    # business dates
    dates = date_utils.get_bus_dates(start_date, end_date)
    
    # standardize the index using business dates
    df = pd.DataFrame(index=pd.Index(dates, name='Date'))
    
    security_timeseries = df.merge(prices, left_index=True, right_index=True, how='left')

    return security_timeseries

def get_core_factors_timeseries(DATA):
    params = DATA['Parameters']
    
    # historical time series
    prices = DATA['CoreFactors_Prices']

    # business dates
    start_date, end_date = params['TS Start Date'], params['TS End Date']
    dates = date_utils.get_bus_dates(start_date, end_date)
    
    # standardize the index using business dates
    df = pd.DataFrame(index=pd.Index(dates, name='Date'))
    prices = df.merge(prices, left_index=True, right_index=True, how='left')
    
    return prices

# run regression against core factors
def run_regression(DATA):

    Y = DATA['security_timeseries']
    X = DATA['core_factors_timeseries'] 

    # xl_utils.add_df_to_excel(X, wb, 'X')
    # xl_utils.add_df_to_excel(Y, wb, 'Y')
    # xl_utils.add_df_to_excel(df, wb, 'df')

    # filter out timeseries that have length less than TS_MIN_LEN
    c = Y.count()
    sec_ids_no_ts = c[c<TS_MIN_LEN].index
    Y.drop(sec_ids_no_ts, axis=1, inplace=True)  
    
    # drop the securities that have short timeseries    
    df = DATA['Securities']
    df1 = df[df['SecurityID'].isin(sec_ids_no_ts)].copy()
    df1['Exception'] = 'No timeseries'
    DATA['exception'] = df1
    DATA['Securities'] = df[~df['SecurityID'].isin(sec_ids_no_ts)]
    
    # results dataframes
    columns = X.columns.to_list() + ['R-Sq', 'Vol']
    regress_df  = pd.DataFrame(columns=columns)
    residual_df = pd.DataFrame()

    n = len(Y.columns)
    for i in range(n):
        # print(i)
        # i = 0
        df = pd.concat([Y.iloc[:,[i]], X], axis=1)
        df = data_transform(df)
        betas, b0, r_sq, y_vol, res =  stat_utils.linear_regression(df)
        
        sec_id = df.columns[0]
        regress_df.loc[sec_id] = np.append(betas, [r_sq, y_vol])
        res_df = pd.DataFrame(data = res, columns = [sec_id], index=df.index)
        
        residual_df = pd.concat([residual_df, res_df], axis=1)
        if i%100 == 99:
            print(f'securities regressed: {i+1}')
    print(f'securities regressed: {n}')
            
    regress_df.index.name = 'SecurityID'
    DATA['X'] = X
    DATA['Y'] = Y
    DATA['regress_df'] = regress_df
    DATA['residual_df'] = residual_df
    DATA['stat_df'] = calc_ts_stat(DATA)
    

def data_transform(df):
    df = df.dropna()
    
    # calculate log returns
    df= df.pct_change(1)
    df = df.dropna()
    
    # de-mean
    #df = df - df.mean()
    
    # log return
    df = np.log(1+df)

    return df    

#
# timeseries stat
#
def calc_ts_stat(DATA):
    params = DATA['Parameters']
    
    # concat core_factors and securities
    cf = DATA['CoreFactors'].set_index('Ticker')[['Name']].rename(columns={'Name':'SecurityName'})
    sec = DATA['Securities'].set_index('SecurityID')[['SecurityName']]
    df = pd.concat([cf, sec])
    df.index.name = 'SecurityID'
    
    # concat prices
    cf_prices = DATA['core_factors_timeseries']
    sc_prices = DATA['security_timeseries'] 
    prices = pd.concat([cf_prices, sc_prices], axis=1)
    
    # calc stats
    df['start_date'] = date_utils.get_first_date(prices)
    df['end_date']   = date_utils.get_last_date(prices)
    df['length']     = prices.count()
    df1 = prices.ffill().pct_change(1)
    df['mean'] = df1.mean()
    df['max']  = df1.max()
    df['min']  = df1.min()
    df['vol']  = model_utils.calc_ts_vol(prices)
    
    # var window vol
    num_simulation = params['Number of Simulations']
    prices1 = prices.iloc[-num_simulation:]
    df['cur_vol']  = model_utils.calc_ts_vol(prices1)

    return df

    
def get_data_dir(model_params):
    data_dir = config['MODEL_DIR'] / model_params['Model ID'] / model_params['Submodel ID']
    return tools.get_folder(data_dir)

##############################################################################################
# preprocess

# read and update parameters
def read_parameter(wb, model_id, submodel_id):
    
    core_params = model_utils.read_Model_Parameters(model_id)
    
    model_params = tools.read_parameter(wb)
    model_params['Model ID'] = model_id
    model_params['Submodel ID'] = submodel_id
    model_params['Model Type'] = 'Equity'
    model_params['TS Start Date'] = core_params['TS Start Date']
    model_params['TS End Date'] = core_params['TS End Date']
    model_params['Number of Simulations'] = core_params['Number of Simulations']
    
    # override model_id, submodel_id, model_type
    print('updating wookbook Parameters...')
    xl_utils.add_dict_to_excel(model_params, wb, 'Parameters')
    
    return model_params
    

def update_parameters(wb, model_params):
    
    df = df_utils.dict_to_df(model_params)
        
    df.loc['TS Start Date', 'Value'] = df.loc['TS Start Date', 'Value'].date()
    df.loc['TS End Date', 'Value'] = df.loc['TS End Date', 'Value'].date()
    
    xl_utils.add_df_to_excel(df, wb, 'Parameters', index=True)    
    
# add parameter in the 'Model' folder
def add_Parameter_in_Model_folder():
    model_params = {
        'Model ID': 'M_20240531',
        'TS Start Date': datetime.datetime(2010,1,1),
        'TS End Date': datetime.datetime(2024,5,31),
        'Number of Simulations': 1044
        }
    
    model_dir = config['MODEL_DIR'] / model_params['Model ID']
    file_path = model_dir / 'Model' / 'Parameters.csv'
    tools.save_parameter_csv(model_params, file_path)


def get_eq_securities(wb, model_params):
    sql = """
    select si."SecurityID", si."SecurityName", si."Currency", si."AssetClass", si."AssetType", si."Ticker" 
    from security_info_view si where si."AssetClass" = 'Bond' and si."AssetType" in ('Fund', 'ETF')															

    """

    df = xl_utils.read_df_from_excel(wb, 'Securities')
    sec_ids = df['SecurityID'].unique()
    sec_id_str = ",".join([f"'{x}'" for x in sec_ids])
    print(sec_id_str)
    sql = f"""
    select * from security_info_view si where si."SecurityID" in ({sec_id_str})
    """
    # print(sql)
    
    securities = db_utils.get_sql_df(sql)

    securities = securities[['SecurityID', 'SecurityName', 'Currency', 'AssetClass', 'AssetType', 'Ticker']]

    if securities.duplicated(['SecurityID']).sum() > 0:
        raise Exception("get_eq_securities: found duplicated SecurityID")

    hist_prices = get_hist_prices(securities, model_params)    

    stat = calc_hist_price_stat(hist_prices)
    
    df = securities.merge(stat[['StartDate', 'EndDate', 'Length', 'vol']], left_on='SecurityID', right_index=True, how='left')
    xl_utils.add_df_to_excel(df, wb, 'temp', index=False)        

def get_hist_prices(securities, model_params):
    # get hist prices
    sec_ids = securities['SecurityID'].to_list()
    hist_prices = mkt_timeseries.get(sec_ids)

    start_date, end_date = model_utils.get_date_range(model_params)
    hist_prices = hist_prices[(hist_prices.index>=start_date) & (hist_prices.index <= end_date)]
    
    hist_prices.replace(0, np.nan, inplace=True)    
    return hist_prices    
    
def calc_hist_price_stat(hist_prices):

    # calc stat
    stat = stat_utils.hist_stat(hist_prices)    
    stat['vol'] = model_utils.calc_ts_vol(hist_prices)

    return stat
    
def write_load_data_to_xl(DATA, wb):
    name = 'CoreFactors'
    xl_utils.add_df_to_excel(DATA[name], wb, name, index=False)
    
    name = 'IndexDates'
    xl_utils.add_df_to_excel(DATA[name].reset_index(), wb, name, index=True)
    
    name = 'CoreFactors_Prices'
    xl_utils.add_df_to_excel(DATA[name], wb, name, index=True)
    
    name = 'corefactor_dist'
    xl_utils.add_df_to_excel(DATA[name], wb, name, index=True)
    
    hist_prices = DATA['hist_prices']
    xl_utils.add_df_to_excel(hist_prices.iloc[:,:100], wb, 'hist_prices', index=True)
    
    hist_stat = calc_hist_price_stat(hist_prices) 
    xl_utils.add_df_to_excel(hist_stat, wb, 'hist_stat', index=True)

def write_sim_data_to_xl(DATA, wb):
    name = 'exception'
    xl_utils.add_df_to_excel(DATA[name], wb, name, index=False)
    
    name = 'regress_df'
    xl_utils.add_df_to_excel(DATA[name], wb, name, index=True)

    name = 'stat_df'
    xl_utils.add_df_to_excel(DATA[name], wb, name, index=True)
    
    
    for name in ['idio_dist', 'sys_dist', 'simulated_dist']:
        df = stat_utils.dist_stat(DATA[name])
        xl_utils.add_df_to_excel(df, wb, name, index=True)
    
    
##########################################################################################
# save model data to csv file

def save_model(DATA):
    model_utils.save_model_data(DATA, 'Parameters')
    model_utils.save_model_data(DATA, 'Securities')
    model_utils.save_model_data(DATA, 'CoreFactors')
    model_utils.save_model_data(DATA, 'IndexDates', index=True)
    model_utils.save_model_data(DATA, 'CoreFactors_Prices', index=True)
    model_utils.save_model_data(DATA, 'hist_prices', index=True)
    model_utils.save_model_data(DATA, 'security_timeseries', index=True)
    model_utils.save_model_data(DATA, 'core_factors_timeseries', index=True)
    model_utils.save_model_data(DATA, 'exception', index=True)
    model_utils.save_model_data(DATA, 'corefactor_dist', index=True)
    model_utils.save_model_data(DATA, 'simulated_dist', index=True)
    model_utils.save_model_data(DATA, 'stat_df', index=True)
    model_utils.save_model_data(DATA, 'regress_df', index=True)
    model_utils.save_model_data(DATA, 'residual_df', index=True)
    model_utils.save_model_data(DATA, 'idio_dist', index=True)
    model_utils.save_model_data(DATA, 'sys_dist', index=True)

    # save dist
    dist = DATA['simulated_dist']
    model_utils.save_dist(DATA, dist, 'PRICE')
    
    # update risk_factor
    update_risk_factor(DATA)



#
# update risk_factor db table    
#
def update_risk_factor(DATA):
    
    params = DATA['Parameters']
    dist = DATA['simulated_dist']
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
    
    
    
######################################################
# TEST
def test():
    # generate dist for securitis in filename
    securities, hist_prices = get_test_data()
    
    run_model(securities, hist_prices)
    
    # extract dist
    var_utils.set_model_id() 
    sec_ids = ['T10000951', 'T10001582', 'T10000952']
    df = var_utils.get_dist(sec_ids)
    
    wb = xw.Book('Book1')
    xl_utils.add_df_to_excel(df, wb, 'df')

def get_test_data():
    
    data_dir = config['MODEL_DIR'] / 'M_20240531' / 'Equity.12'
    securities = pd.read_csv(data_dir / 'Securities.csv')
    prices = pd.read_csv(data_dir / 'hist_prices.csv')
    
    prices.index = pd.to_datetime(prices.index)
    prices = prices.dropna(axis=1, how='all')
    prices.replace(0, np.nan, inplace=True)
    
    return securities, prices