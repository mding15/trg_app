# -*- coding: utf-8 -*-
"""
Created on Sat Oct 11 20:46:24 2025

@author: mgdin
"""

import pandas as pd
import numpy as np
import xlwings as xw

from models import model_utils
from utils import xl_utils, date_utils, stat_utils, tools, var_utils

def test():
    wb = xw.Book(r'C:\Users\mgdin\dev\TRG_App\Models\HedgeFund\HedgeFund.xlsx')

    # read input
    funds, monthly_returns = read_input(wb)

    # model init
    model_id, submodel_id = 'M_20240531', 'HedgeFund.1'
    DATA = model_init(model_id, submodel_id, funds, monthly_returns)

    # run regression against core_factors 
    run_regression(DATA)

    # simulate distribution
    sim_dist(DATA)

    write_to_xl(DATA, wb)
    # xl_utils.add_df_to_excel(Y, wb, 'df', index=True)

    # save distributioin
    save_model(DATA, wb)


def run_model(funds, monthly_returns, model_id=None, submodel_id=None):
    
    if model_id is None:
        model_id = var_utils.get_default_model_id()

    if submodel_id is None:
        submodel_id = f'Equity.{tools.file_ts()}'

    # model init
    DATA = model_init(model_id, submodel_id, funds, monthly_returns)

    # run regression against core_factors 
    run_regression(DATA)

    # simulate distribution
    sim_dist(DATA)

    # save distributioin
    save_model(DATA)
    
    return DATA
    
    
###############################################################################
def read_input(wb):
    
    funds      = xl_utils.read_df_from_excel(wb, 'FundInfo')
    fund_class = funds['FundClass'].iloc[0]
    sec_id     = funds['SecurityID'].iloc[0]
    monthly_returns = read_monthly_return(wb, fund_class)
    monthly_returns = monthly_returns.rename(columns={fund_class: sec_id})
    
    return funds, monthly_returns

def read_monthly_return(wb, fund_class):
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    month_int = {v:i+1 for i, v in enumerate(months)}

    df = xl_utils.read_df_from_excel(wb, 'MonthlyReturn')

    # filter data
    df = df[df['FundClass']==fund_class]
    df = df[months + ['Year']]

    # convert return in table to 1-dim time series
    ts = pd.melt(df, id_vars='Year', value_vars=months, var_name='mmm', value_name=fund_class)

    ts = ts[~ts[fund_class].isna()]
    ts['Date'] = ts.apply(lambda x: date_utils.month_end(int(x.Year), month_int[x.mmm]), axis=1)
    # ts['Date'] = ts['Date'].apply(date_utils.bus_date)
    ts['Date'] = pd.to_datetime(ts['Date'])
    ts = ts[['Date', fund_class]].sort_values(by=['Date'])

    return ts.set_index('Date')

    # xl_utils.add_df_to_excel(ts, wb, 'ts', index=False)

    
def create_params(model_id, submodel_id):
    model_params = model_utils.read_Model_Parameters(model_id)
    model_params['Submodel ID'] = submodel_id
    model_params['Model Type'] = 'HedgeFund'
    return model_params

def model_init(model_id, submodel_id, securities, monthly_returns):

    # create model parameters
    model_params = create_params(model_id, submodel_id)

    # create container
    DATA = model_utils.load_data(model_params)
    
    # remove return timeseries ouside the date range
    start_dt, end_dt = model_params['TS Start Date'], model_params['TS End Date']
    monthly_returns = monthly_returns.loc[start_dt:end_dt]

        
    DATA['Securities']     = securities
    
    DATA['security_timeseries']  = monthly_returns
    DATA['core_factors_timeseries']  = corefactor_monthly_returns(DATA)

    return DATA    


def corefactor_monthly_returns(DATA):
    
    # corefactor hist prices
    prices = DATA['CoreFactors_Prices']

    # # date range
    # start_date, end_date = get_date_range(DATA)

    # # standardize dates
    # prices = standardize_dates(prices, start_date, end_date)
    
    # fill na
    prices = prices.fillna(method='ffill').fillna(method='bfill')

    prices.index = pd.to_datetime(prices.index)
    month_end_prices = prices.resample('M').last()
    
    monthly_returns = month_end_prices.pct_change()

    return monthly_returns

# run regression against core factors
def run_regression(DATA):

    X = DATA['core_factors_timeseries']
    Y = DATA['security_timeseries'] 
    
    # results dataframes
    columns = X.columns.to_list() + ['R-Sq', 'Vol']
    regress_df  = pd.DataFrame(columns=columns)
    residual_df = pd.DataFrame()

    n = len(Y.columns)
    for i in range(n):
        # print(i)
        # i = 0
        df = pd.concat([Y.iloc[:,[i]], X], axis=1)
        df = df.dropna()
        # log return
        df = np.log(1+df)

        betas, b0, r_sq, y_vol, res =  stat_utils.linear_regression(df)
        
        sec_id = df.columns[0]
        regress_df.loc[sec_id] = np.append(betas, [r_sq, y_vol])
        res_df = pd.DataFrame(data = res, columns = [sec_id], index=df.index)
        
        residual_df = pd.concat([residual_df, res_df], axis=1)
        if i%100 == 99:
            print(f'securities regressed: {i+1}')
    print(f'securities regressed: {n}')
            
    regress_df.index.name = 'SecurityID'
    DATA['regress_df'] = regress_df
    DATA['residual_df'] = residual_df
    DATA['stat_df'] = calc_ts_stat(DATA)
    

# simulate distribution
def sim_dist(DATA):
    
    # core factors log returns
    core_factors = DATA['corefactor_dist']
    
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
    
    stat_df['sys_vol']   = sys_dist.std()        
    stat_df['idio_vol']  = residuals.std()        
    stat_df['total_vol'] = log_ret.std()        
    stat_df['sim_vol'] = simulated_dist.std()        

    DATA['dist'] = simulated_dist    

# generate residuals
def gen_residuals(DATA):    

    betas = DATA['regress_df']
    corefactor_dist = DATA['corefactor_dist']
    residuals = pd.DataFrame(index=corefactor_dist.index)
    
    for sec_id, row in betas.iterrows():
        rsq, vol = row[['R-Sq', 'Vol']]
        res_vol = vol * np.sqrt(1-rsq) / np.sqrt(22) # scale down to daily vol

        eps = np.random.normal(0, res_vol, len(residuals))
        residuals[sec_id] = eps

    DATA['idio_dist']  = residuals
    
    return residuals

    
###############################################################################
# auxilary
def standardize_dates(prices, start_date, end_date):
    dates = date_utils.get_bus_dates(start_date, end_date)
    
    # standardize the index using business dates
    df = pd.DataFrame(index=pd.Index(dates, name='Date'))
    
    standardized_timeseries = df.merge(prices, left_index=True, right_index=True, how='left')
    
    return standardized_timeseries

# return (start_date, end_date)    
def get_date_range(DATA):
    model_params = DATA['Parameters']
    start_dt, end_dt = model_params['TS Start Date'], model_params['TS End Date']
    return start_dt, end_dt    
    
def write_to_xl(DATA, wb):
    skip_index=['Securities', 'CoreFactors']
    for tab in DATA.keys():
        print(tab)
        df = DATA[tab]
        add_df_to_excel(df, wb, tab, tab not in skip_index)

# tab = 'Parameters'
def add_df_to_excel(df, wb, tab, index=False):
    if isinstance(df, dict):
        df = pd.DataFrame.from_dict(df, orient='index', columns=['Value'])
        df.index.name = 'Name'
        index=True
    xl_utils.add_df_to_excel(df, wb, tab, index)    

#
# timeseries stat
#
def calc_ts_stat(DATA):
    
    # concat core_factors and securities
    cf = DATA['CoreFactors'].set_index('Ticker')[['Name']].rename(columns={'Name':'SecurityName'})
    sec = DATA['Securities'].set_index('SecurityID')[['SecurityName']]
    df = pd.concat([cf, sec])
    df.index.name = 'SecurityID'
    
    # concat prices
    cf_ts = DATA['core_factors_timeseries']
    sc_ts = DATA['security_timeseries'] 
    ts = pd.concat([cf_ts, sc_ts], axis=1)
    
    # calc stats
    df['start_date'] = date_utils.get_first_date(ts)
    df['end_date']   = date_utils.get_last_date(ts)
    df['length']     = ts.count()
    df['mean'] = ts.mean()
    df['max']  = ts.max()
    df['min']  = ts.min()
    df['vol']  = ts.std()
    
    return df

##########################################################################################
# save model data to csv file

def save_model(DATA, wb=None):

    # save model data to csv files
    model_utils.save_model(DATA, skip_index = ['Securities', 'CoreFactors'])

    # save dist
    dist = DATA['dist']
    model_utils.save_dist(DATA, dist, 'PRICE')

    # save log
    model_utils.save_model_info_data(DATA)
    
    # update risk_factor
    model_utils.update_risk_factor(DATA, wb)

