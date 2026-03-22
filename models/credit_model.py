# -*- coding: utf-8 -*-
"""
Created on Tue Jun 25 13:43:24 2024

@author: mgdin
"""

import pandas as pd
import numpy as np
import xlwings as xw
from pathlib import Path

from trg_config import config
from models import model_utils, bond_risk
from security import security_info
from mkt_data import mkt_timeseries, mkt_data_info
from utils import xl_utils, date_utils, stat_utils, tools, var_utils

FILE_DIR = Path(r'C:\Users\mgdin\dev\TRG_App\Models\Bond')
securities_columns = ['SecurityID', 'Sector', 'IssuerTicker', 'Rating', 'Spread']

DATA = {}
    
def run_model():
   
    wb = xw.Book(FILE_DIR / 'FIAE International Bonds.xlsx')
    
    # check if any sec_id has been modeled
    check_existing(wb)

    # generate spread distribution
    bond_spread(wb)
    

def bond_spread(wb):

    read_data_wb(wb)

    # model index dates    
    index_dates = DATA['IndexDates'] 
    n_sim = len(index_dates)

    # securities
    securities = DATA['Securities']

    # eq residual    
    residual, regress_df = get_eq_residuals(wb)

    # for bonds that have no issuer tickers, 
    missing = list(set(securities['SecurityID']).difference(residual.columns))
    if len(missing) > 0:
        
        # generate standard normal draws
        df = pd.DataFrame(np.random.randn(n_sim, len(missing)), columns=list(missing), index=index_dates.index)
        residual = pd.concat([residual, df], axis=1)

        # assume R-sq = 0.5
        df = pd.DataFrame(index=missing, columns=regress_df.columns)
        df['R-Sq'] = 0.5
        regress_df = pd.concat([regress_df, df])
            
        
    # volatility by rating
    rating_vol = DATA['rating_vol'] 
    rating_map = DATA['rating_map']
    benchmark_stat = DATA['benchmark_stat']
    
    # calculate spread
    bonds = calc_yield(wb)
    securities['Spread'] = tools.df_series_merge(securities, bonds['Spread'], 'SecurityID') 
    securities['Spread'] = securities['Spread'] * 100 # unit is in percentage point
    
    # betas
    betas = securities[['SecurityID', 'IssuerTicker', 'Sector', 'Rating', 'Spread']].copy()
    betas['Sector'] = betas['Sector'].apply(lambda x: x.replace(' ', '_'))
    betas['MRating'] = betas['Rating'].apply(lambda x: rating_map.loc[x, 'Major_Rating'])
    betas['Grade'] = betas['Rating'].apply(lambda x: rating_map.loc[x, 'Grade'])
    betas['benchmark'] = betas['Grade'] + "_" + np.where(betas['Sector']=='NoSector', 'All_Sectors', betas['Sector'])
    betas['benchmark_vol']  = betas['benchmark'].apply(lambda x: benchmark_stat.loc[x, 'StdValue'])
    betas['bond_vol'] = betas['MRating'].apply(lambda x: rating_vol.loc[x, 'Volatility'])
    betas['r-sq'] = betas['SecurityID'].apply(lambda x: regress_df.loc[x, 'R-Sq'])
    betas['corr'] = np.sqrt(betas['r-sq'])
    betas['beta'] = betas['corr'] * betas['bond_vol'] / betas['benchmark_vol']
    betas['sigma'] = np.sqrt(1-betas['r-sq']) * betas['bond_vol']
    betas['scale'] = np.where(betas['Grade']=='HG', 1, betas['Spread'])
    DATA['betas'] = betas
    xl_utils.add_df_to_excel(betas, wb, 'betas')
    
    
    # simulate bond spread
    benchmark_dist = DATA['benchmark_dist']
    
    dist_data = {}
    for i in range(len(betas)):
        sec_id, benchmark, beta, sigma, scale = betas.iloc[i][['SecurityID', 'benchmark', 'beta', 'sigma', 'scale']]
        bench_dist = benchmark_dist[benchmark]
        esp = residual[sec_id]
        dist_data[sec_id] = (beta * bench_dist - sigma * esp) * scale
    dist = pd.concat(dist_data, axis=1)
    
    DATA['sp_dist'] = dist
    DATA['sp_dist_stat'] = stat_utils.hist_stat(dist)
    xl_utils.add_df_to_excel(DATA['sp_dist'], wb, 'sp_dist')
    xl_utils.add_df_to_excel(DATA['sp_dist_stat'], wb, 'sp_dist_stat')
    
    # save model
    save_model()

    # save dist
    var_utils.save_dist(dist, 'SPREAD')

# calc bond yield
def calc_yield(wb):
    params = DATA['Parameters']
    securities = DATA['Securities'].set_index('SecurityID')
    
    bonds = securities[['MaturityDate', 'CouponRate', 'PaymentFrequency']].rename(
        columns={'MaturityDate': 'Maturity', 'CouponRate':'Coupon', 'PaymentFrequency': 'Frequency'})
    
    start_date, end_date = params['TS Start Date'], params['TS End Date']
    hist_prices = mkt_timeseries.get(securities.index, start_date, end_date, category='PRICE')
    bonds['Price'] = hist_prices.ffill().loc[end_date]
    bonds['Price'] = bonds['Price'] / 100        
    bonds['PriceDate'] = end_date

    # calc tenor and yield
    bonds['Tenor'] = bonds.apply(lambda x: (x['Maturity'] - x['PriceDate']).days/365, axis=1)        
    bonds['Yield'] = bonds.apply(lambda x: bond_risk.bond_yield(x['Coupon'], x['Tenor'], x['Frequency'], x['Price']), axis=1)
    
    # get risk-free rate
    bonds['RiskFreeRate'] = bond_risk.calc_riskfree_rate(bonds, end_date)
    bonds['Spread'] = bonds['Yield'] - bonds['RiskFreeRate']
    xl_utils.add_df_to_excel(bonds, wb, 'Yield')
    DATA['bonds'] = bonds
    
    return bonds


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
    xl_utils.add_df_to_excel(eq_hist, wb, 'eq_hist')
    
    eq_dist = eq_hist.merge(index_dates, left_index=True, right_index=True, how='outer')
    eq_dist = eq_dist.fillna(method='ffill')
    eq_dist = eq_dist.pct_change(1)
    eq_dist = eq_dist.loc[index_dates.index]
    DATA['eq_dist'] = eq_dist
    xl_utils.add_df_to_excel(eq_dist, wb, 'eq_dist')

    # eq sec dist
    eq_sec_dist = DATA['eq_sec_dist']
    xl_utils.add_df_to_excel(eq_sec_dist, wb, 'eq_sec_dist')

    # sectors
    sectors = DATA['Sectors']
    sectors_tck = sectors.set_index('Sector')['Ticker'].to_dict()

    # regress against sector index

    regress_df  = pd.DataFrame(columns=beta_columns)
    residual_df = pd.DataFrame(index=index_dates.index)
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


# save model data to csv file
def save_model():
    model_utils.save_model_data(DATA, 'Parameters')
    model_utils.save_model_data(DATA, 'Securities')

    model_utils.save_model_data(DATA, 'eq_dist', index=True)
    model_utils.save_model_data(DATA, 'eq_sec_dist', index=True)
    model_utils.save_model_data(DATA, 'eq_regress', index=True)
    model_utils.save_model_data(DATA, 'eq_residuals', index=True)
    model_utils.save_model_data(DATA, 'eq_esp', index=True)
    model_utils.save_model_data(DATA, 'benchmark_dist', index=True)
    model_utils.save_model_data(DATA, 'benchmark_stat', index=True)
    model_utils.save_model_data(DATA, 'rating_vol', index=True)
    model_utils.save_model_data(DATA, 'rating_map', index=True)
    model_utils.save_model_data(DATA, 'betas')
    model_utils.save_model_data(DATA, 'sp_dist', index=True)
    model_utils.save_model_data(DATA, 'sp_dist_stat', index=True)
    
    model_utils.save_model_info_data(DATA)
    
def read_credit0_data(name, index_col=None):
    model_params = DATA['Parameters']
    data_dir = config['MODEL_DIR'] / model_params['Model ID'] / 'Credit.0'
    if not data_dir.exists():
        raise Exception(f'You need to run model Credit.0 before run this model: {data_dir}')
        
    filename = data_dir / f'{name}.csv'    
    df = pd.read_csv(filename, index_col=index_col)
    return df

def read_securities(wb):
    df  = xl_utils.read_df_from_excel(wb, 'Securities')
    df['Sector'].fillna('NoSector', inplace=True)    
    return df

# read inputs from a workbook    
def read_data_wb(wb):
    model_params = tools.read_parameter(wb)
    
    DATA['Parameters']  = model_params
    DATA['Securities']  = read_securities(wb)
    DATA['rating_map']  = xl_utils.read_df_from_excel(wb, 'rating_map', index=True)
    DATA['IndexDates']  = model_utils.read_index_dates(model_params)
    
    DATA['Sectors'] = read_credit0_data('Sectors')
    DATA['rating_vol'] = read_credit0_data('rating_vol', index_col=0)
    
    eq_sec_dist = read_credit0_data('eq_sec_dist', index_col=0)
    eq_sec_dist.index = pd.to_datetime(eq_sec_dist.index)
    DATA['eq_sec_dist'] = eq_sec_dist

    benchmark_dist = read_credit0_data('sp_dist', index_col=0)
    benchmark_dist.index = pd.to_datetime(benchmark_dist.index)
    DATA['benchmark_dist'] = benchmark_dist

    benchmark_stat =  read_credit0_data('sp_dist_stat', index_col=0)
    benchmark_stat.loc['HG_NoSector'] = benchmark_stat.loc['HG_All_Sectors']
    benchmark_stat.loc['HY_NoSector'] = benchmark_stat.loc['HY_All_Sectors']
    DATA['benchmark_stat'] = benchmark_stat

    check_data(DATA)
    

def check_data(DATA):
    securities = DATA['Securities']
    sectors = DATA['Sectors'] 
    
    # make sure all sectors are validate
    sectors = sectors['Sector'].to_list() + ['NoSector']
    diff = list(set(securities['Sector']).difference(sectors))
    if len(diff) > 0:
        diff_list = ', '.join(diff)
        raise Exception(f'Unknown sectors: [{diff_list}]')

def check_existing(wb):
    # list all distribution IDs
    dist_list = var_utils.list_dist()
    dist_list = dist_list[dist_list['Category']=='SPREAD']
    
    # join
    securities = read_securities(wb)
    securities = securities.merge(dist_list, on='SecurityID', how='left')
    xl_utils.add_df_to_excel(securities, wb, 'Securities', index=False)
    
    