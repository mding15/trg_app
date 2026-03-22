# -*- coding: utf-8 -*-
"""
Created on Mon Jun 24 09:25:33 2024

@author: mgdin

Purpose:
    Generate distribution for credit benchmarks
    
"""
import xlwings as xw
import pandas as pd
import numpy as np

from models import model_utils
from mkt_data import mkt_timeseries
from utils import xl_utils, stat_utils, var_utils, tools
from trg_config import config
from models import MODEL_WORKBOOK_DIR

DATA = {}

#    
# download spread data from FED web site
#
def update_FED_data():
    
    # open the workbook
    file_path = MODEL_WORKBOOK_DIR / 'Data' / 'FED' / 'FRED Corporate.xlsx'
    fed_wb = xw.Book(file_path)
    
    # download data by following the instructions in "INFO" tab
    #
    csv_folder = config['DATA_DIR'] / 'FED' / '20251204'
    
    # read link the ticker to security_id
    securities = tools.read_positions(fed_wb, 'Security')
    
    # save 
    # read hist timeseries from each tab
    data = []
    for row in securities.itertuples():
        sec_id, ticker = row.SecurityID, row.Ticker
        print(sec_id, ticker)
        file_path = csv_folder / f'{ticker}.csv'
        df = pd.read_csv(file_path,index_col=0)
        df.columns = [sec_id] # rename column by sec_id
        df.index.name = 'Date'
        data.append(df)
 
    df = pd.concat(data, axis=1)
    
    # save the data into hdf file
    mkt_timeseries.save(df, 'FED', category='MACRO')
    

#
# main function: create credit spread benchmarks
#
def run_credit_benchmarks():
    
    # update this !!!
    model_id = 'M_20251231'
    
    # model inputs
    wb = xw.Book(MODEL_WORKBOOK_DIR / f'{model_id}' / 'Credit_Benchmark.xlsx')

    # load model core data
    load_model_core_data(wb, model_id)

    # read data for the model        
    read_data_wb(wb)

    # update corefactors
    update_corefactors(wb)
    
    # update sector equity data
    update_sector_equity(wb)

    # regress sector equity against SPY  
    sector_equity_regress(wb)

    # simulate sector benchmarks
    simulation(wb)
    
    # save model data to csv file
    save_model()
    


# simulate sector benchmarks
def simulation(wb):
    
    rating_vol = xl_utils.read_df_from_excel(wb, 'rating_vol')
    DATA['rating_vol'] = rating_vol
        
    betas = xl_utils.read_df_from_excel(wb, 'betas')
    DATA['betas'] = betas

    index_dates = DATA['IndexDates']
    core_dists = DATA['core_dist']
    sector_esp = DATA['sector_esp']

    # simulate sector benchmarks
    dist = pd.DataFrame(index=index_dates.index)
    for b in betas.itertuples():
        core_dist = core_dists[b.corefactor]        
        esp = sector_esp[b.Ticker]
        # spread is negative correlated to equity price
        dist[b.benchmark] = b.beta * core_dist - b.sigma * esp 
    
    DATA['dist'] = dist
    xl_utils.add_df_to_excel(dist, wb, 'dist')    
    
    # dist statistics
    dist_stat = stat_utils.dist_stat(dist)
    DATA['dist_stat'] = dist_stat
    xl_utils.add_df_to_excel(dist_stat, wb, 'dist_stat')    
    


# update sector equity data
def update_sector_equity(wb):    
    
    # get Sector price hist
    eq_sectors = DATA['Sectors']  
    eq_sec_hist = mkt_timeseries.get(eq_sectors['SecurityID'], category='PRICE')
    eq_sec_hist = eq_sec_hist.rename(columns= eq_sectors.set_index('SecurityID')['Ticker'].to_dict())
    
    # sector return timeseries
    index_dates = DATA['IndexDates']
    eq_sec_dist = eq_sec_hist.merge(index_dates, left_index=True, right_index=True, how='outer')
    eq_sec_dist = eq_sec_dist.fillna(method='ffill')
    eq_sec_dist = eq_sec_dist.pct_change(1)
    eq_sec_dist.replace(0, np.nan, inplace=True)
    eq_sec_dist = model_utils.fill_na_with_rand_sampling(eq_sec_dist)
    eq_sec_dist = eq_sec_dist.loc[index_dates.index]
    DATA['eq_sec_dist'] = eq_sec_dist
    xl_utils.add_df_to_excel(eq_sec_dist, wb, 'eq_sec_dist')

    # hist stat
    sector_stat = stat_utils.dist_stat(eq_sec_dist)
    DATA['sector_stat'] = sector_stat
    xl_utils.add_df_to_excel(sector_stat, wb, 'sector_stat')

    # corr matrix
    corr = eq_sec_dist.corr()
    DATA['corr'] = corr
    xl_utils.add_df_to_excel(corr, wb, 'sector_corr')


# regress sector equity against SPY
def sector_equity_regress(wb):

    eq_sec_dist = DATA['eq_sec_dist']

    # regress against SPY, last column is SPY
    Y = eq_sec_dist.iloc[:,:-1]
    X = eq_sec_dist.iloc[:,-1:]
    
    regress_df, residual_df = regress(Y, X)
    DATA['sector_regress'] = regress_df
    DATA['secotr_residuals'] = residual_df
    xl_utils.add_df_to_excel(regress_df, wb, 'sector_regress')
    xl_utils.add_df_to_excel(residual_df, wb, 'secotr_residuals')

    # normalize residuals
    vols = residual_df.std()
    means = residual_df.mean()
    sector_esp = (residual_df - means) / vols
    
    # regressor esp is needed for All_Sector
    regressor = eq_sec_dist.columns[-1]
    sector_esp[regressor] = 0
    
    DATA['sector_esp'] = sector_esp
    xl_utils.add_df_to_excel(sector_esp, wb, 'sector_esp')
    

# update core factors
def update_corefactors(wb):    
    
    # model parameters    
    model_params = DATA['Parameters']

    # model index dates    
    index_dates = DATA['IndexDates'] 

    # core factors
    core_factor = DATA['CoreFactors']
    
    # core hist data
    core_hist = mkt_timeseries.get(core_factor['SecurityID'], category='MACRO')
    core_hist = core_hist.rename(columns= core_factor.set_index('SecurityID')['Ticker'].to_dict())
    
    start_date, end_date = model_utils.get_date_range(model_params)
    core_hist = core_hist.ffill()
    core_hist = core_hist[(core_hist.index >= start_date) & (core_hist.index <= end_date)]
    xl_utils.add_df_to_excel(core_hist, wb, 'core_hist')

    # core dist
    # add index_dates
    core_dist = core_hist.merge(index_dates, left_index=True, right_index=True, how='outer')
    core_dist['US_Corporate']     = core_dist['US_Corporate'].diff(1)
    core_dist['BBB_US_Corporate'] = core_dist['BBB_US_Corporate'].diff(1)
    core_dist['US_High_Yield']    = core_dist['US_High_Yield'].pct_change(1)

    # random sampling for 0 values
    core_dist.replace(0, np.nan, inplace=True)
    core_dist = model_utils.fill_na_with_rand_sampling(core_dist)
    
    # pick values for index_dates
    core_dist = core_dist.loc[index_dates.index]    

    # de-mean
    core_dist = core_dist - core_dist.mean()

    # save to the data    
    DATA['core_dist'] = core_dist

    # write to excel
    xl_utils.add_df_to_excel(core_dist, wb, 'core_dist')

    # statistics    
    core_stat = stat_utils.dist_stat(core_dist)
    DATA['core_stat'] = core_stat
    xl_utils.add_df_to_excel(core_stat, wb, 'core_stat')


# read inputs from a workbook    
def read_data_wb(wb):

    DATA['Securities']  = xl_utils.read_df_from_excel(wb, 'Securities')
    DATA['Sectors']     = xl_utils.read_df_from_excel(wb, 'Sectors')    
    DATA['Ratings']     = xl_utils.read_df_from_excel(wb, 'Ratings')    
    DATA['CoreFactors'] = xl_utils.read_df_from_excel(wb, 'CoreFactors')

    # DATA['rating_vol']  = xl_utils.read_df_from_excel(wb, 'rating_vol', index=True)

# load model data
def load_model_core_data(wb, model_id):
    
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
    

# save model data to csv file
def save_model():
    
    model_utils.save_model_data(DATA, 'Parameters')
    model_utils.save_model_data(DATA, 'Securities')
    model_utils.save_model_data(DATA, 'CoreFactors')
    model_utils.save_model_data(DATA, 'Sectors')
    model_utils.save_model_data(DATA, 'Ratings')
    model_utils.save_model_data(DATA, 'core_dist', index=True)
    model_utils.save_model_data(DATA, 'core_stat', index=True)
    model_utils.save_model_data(DATA, 'eq_sec_dist', index=True)
    model_utils.save_model_data(DATA, 'sector_stat', index=True)
    model_utils.save_model_data(DATA, 'corr',        index=True)
    model_utils.save_model_data(DATA, 'sector_regress', index=True)
    model_utils.save_model_data(DATA, 'secotr_residuals', index=True)
    model_utils.save_model_data(DATA, 'sector_esp', index=True)
    model_utils.save_model_data(DATA, 'betas', index=False)
    model_utils.save_model_data(DATA, 'dist', index=True)
    model_utils.save_model_data(DATA, 'dist_stat', index=True)
    model_utils.save_model_data(DATA, 'rating_vol', index=False)

    # replace ticker with SecurityID
    dist = DATA['dist']
    securities = DATA['Securities']
    dist = dist.rename(columns=securities.set_index('Ticker')['SecurityID'].to_dict())

    # save dist
    model_utils.save_dist(DATA, dist, 'SPREAD')

def regress(Y, X):
    
    # results dataframes
    columns = X.columns.to_list() + ['R-Sq', 'Vol']
    regress_df  = pd.DataFrame(columns=columns)
    residual_df = pd.DataFrame()

    n = len(Y.columns)
    for i in range(n):
        df = pd.concat([Y.iloc[:,[i]], X], axis=1)
        betas, b0, r_sq, y_vol, res =  stat_utils.linear_regression(df)
        
        sec_id = df.columns[0]
        regress_df.loc[sec_id] = np.append(betas, [r_sq, y_vol])
        res_df = pd.DataFrame(data = res, columns = [sec_id], index=df.index)
        
        residual_df = pd.concat([residual_df, res_df], axis=1)

    return regress_df, residual_df


##################################################################################################################
#
# obsolete, replaced by spreadsheet calculation
#


#
# replaced by spreadsheet calculation in tab 'rating_vol'
#
def calc_rating_vol(wb):
    
    # rating vol
    rating_vol = DATA['rating_vol']

    core_stat = DATA['core_stat']
    hg_benchmark = core_stat.loc['BBB_US_Corporate', 'std']
    hy_benchmark = core_stat.loc['US_High_Yield', 'StdValue']
    rating_vol['Benchmark'] =  rating_vol['Grade'].apply(lambda x: hg_benchmark if x=='HG' else hy_benchmark)
    rating_vol['Volatility'] = rating_vol['Benchmark'] * rating_vol['Scale']
    DATA['rating_vol'] = rating_vol
    xl_utils.add_df_to_excel(rating_vol, wb, 'rating_vol', index=True)

#
# replaced by spreadsheet calculation 
# 
def calc_betas(wb):
    eq_sectors = DATA['Sectors']
    sector_stat = DATA['sector_stat']
    core_stat = DATA['core_stat']
    corr = DATA['corr']

    eq_sectors = DATA['eq_sectors']
    hg_sector = eq_sectors[eq_sectors['Ticker'] != 'SPY'][['Sector', 'Ticker']].copy()
    hg_sector['Sector'] = hg_sector['Sector'].apply(lambda x: x.replace(' ', '_'))
    hg_sector = hg_sector.set_index('Ticker')
    hg_sector['grade'] = 'HG'
    hg_sector['eq_vol'] = sector_stat['std']
    hg_sector['spy_vol'] = sector_stat.loc['SPY','std']
    hg_sector['scale'] = hg_sector['eq_vol'] / hg_sector['spy_vol']
    hg_sector['benchmark_vol'] = core_stat.loc['BBB_US_Corporate', 'std']
    hg_sector['sp_vol'] = hg_sector['benchmark_vol'] * hg_sector['scale']
    hg_sector['corr'] = corr['SPY']
    hg_sector['beta'] = hg_sector['corr'] * hg_sector['sp_vol'] / hg_sector['benchmark_vol']

    hy_sector = eq_sectors[eq_sectors['Ticker'] != 'SPY'][['Sector', 'Ticker']].copy()
    hy_sector['Sector'] = hy_sector['Sector'].apply(lambda x: x.replace(' ', '_'))
    hy_sector = hy_sector.set_index('Ticker')
    hy_sector['grade'] = 'HY'
    hy_sector['eq_vol'] = sector_stat['std']
    hy_sector['spy_vol'] = sector_stat.loc['SPY','std']
    hy_sector['scale'] = hy_sector['eq_vol'] / hy_sector['spy_vol']
    hy_sector['benchmark_vol'] = core_stat.loc['US_High_Yield', 'std']
    hy_sector['sp_vol'] = hy_sector['benchmark_vol'] * hy_sector['scale']
    hy_sector['corr'] = corr['SPY']
    hy_sector['beta'] = hy_sector['corr'] * hy_sector['sp_vol'] / hy_sector['benchmark_vol']
    
    sector_beta = pd.concat([hg_sector, hy_sector])
    DATA['sector_beta'] = sector_beta
    xl_utils.add_df_to_excel(sector_beta, wb, 'sector_beta')    