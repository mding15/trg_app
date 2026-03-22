# -*- coding: utf-8 -*-
"""
Created on Sun Dec 28 17:10:22 2025

@author: mgdin

UF bond model: model yield

"""

import pandas as pd
import numpy as np
import xlwings as xw

from trg_config import config
from models import model_utils
from utils import xl_utils, stat_utils, tools
from models import MODEL_WORKBOOK_DIR
from models import bond_risk as br

DATA = {}
    
def run_model():
    # UPDATE THIS !!!
    model_id, submodel_id = 'M_20251231', 'UF_Model.1'  
    
    # workbook
    wb = xw.Book(MODEL_WORKBOOK_DIR /model_id / f'{submodel_id}.xlsx')

    # load model data required
    load_data(wb, model_id, submodel_id)

    # calculate yield history
    model_yield_chg_hist(wb)
    
    # regression yield against AAA yield benchmark
    regress_issuer(wb)
    
    # simulate distribution
    simulate_dist(wb)
    
    # save model data to csv file
    save_model()


def simulate_dist(wb):
    
    uf_benchmark = DATA['UF_Benchmark']
    uf_vol = uf_benchmark.std()

    # create uf distribution    
    uf = DATA['IndexDates'].copy()
    uf['UF'] = uf_benchmark
    idx = uf['UF'].isna()
    uf.loc[idx, 'UF'] = np.random.normal(0, uf_vol, sum(idx))
    
    xl_utils.add_df_to_excel(uf, wb, 'bench_dist', index=True)    
    DATA['uf_dist'] = uf

    # fill missing residuals
    residuals_fillna(wb)

    # create distributions for all issuers
    betas = DATA['betas']
    residuals = DATA['residuals']
    issuer_dist = {}
    for issuer, beta in betas['Beta'].items():
        # print(issuer, beta)
        issuer_dist[issuer] = uf['UF'] * beta + residuals[issuer]

    issuer_dist = pd.concat(issuer_dist, axis=1)
    xl_utils.add_df_to_excel(issuer_dist, wb, 'issuer_dist', index=True)

    # create distributions by sec_id
    bonds = DATA['Securities']
    bonds = bonds.set_index('SecurityID')
    bond_dist = {}
    for sec_id, issuer in bonds['IssuerTicker'].items():
        # print(sec_id, issuer)
        if issuer in issuer_dist:
            bond_dist[sec_id] = issuer_dist[issuer]
        
    # missing sec_ids
    missing = set(bonds.index).difference(bond_dist.keys())

    # simulate assuming vol to be the 75% quantile of the vol distribution    
    vol  = betas['Vol'].quantile(0.75)    # conservitively take 75% quantile
    rsq  = betas['R-Sq'].quantile(0.5)    # median rsq
    rho  = np.sqrt(rsq)    
    beta = rho * vol / uf_vol
    res_vol = vol * np.sqrt(1-rsq)
    n = len(uf)
    for sec_id in missing:
        res = np.random.normal(0, res_vol, n)
        bond_dist[sec_id] = uf['UF'] * beta + res
    
    dist = pd.concat(bond_dist, axis=1)
    dist = dist * 0.01 # unit is in percentage points
    xl_utils.add_df_to_excel(dist, wb, 'dist', index=True)    
    DATA['dist'] = dist

    # stats    
    dist_stat = stat_utils.dist_stat(dist)
    
    res_count = DATA['residual_count']    
    res_count = pd.DataFrame(res_count.rename('res_count'))
    res_count = bonds[['IssuerTicker']].merge(res_count, left_on='IssuerTicker', right_index=True, how='left')
    res_count.fillna(0, inplace=True)
    dist_stat['res_count'] = res_count['res_count']
    
    xl_utils.add_df_to_excel(dist_stat, wb, 'dist_stat')
    DATA['dist_stat'] = dist_stat
    
# calculate yield history
def model_yield_chg_hist(wb):
    
    # bond data
    bonds = DATA['Securities']
    
    # hist price data
    prices = DATA['hist_prices']

    
    hist_stat = stat_utils.hist_stat(prices)
    xl_utils.add_df_to_excel(hist_stat, wb, 'hist_stat', index=True)
    DATA['hist_prices_stat'] = hist_stat

    # bond that has hist prices
    sec_ids = list(set(bonds['SecurityID']) & set(prices.columns))
    
    # calc hist yield    
    yield_hist = {}
    yield_chg  = {}

    bonds = bonds.set_index('SecurityID')
    for sec_id in sec_ids:
        
        print(sec_id)
        maturity, coupon, freq = bonds.loc[sec_id][['MaturityDate', 'CouponRate', 'PaymentFrequency']]
        hist = prices[[sec_id]]
        hist = hist.dropna().rename(columns={sec_id: 'Price'}).reset_index()
        min_price = hist['Price'].min()
        hist['Price2'] = np.where(min_price>80, hist['Price']/100, 1) # if price<80, use par 
        
        hist['Tenor'] = hist['Date'].apply(lambda x: (maturity - x).days/365.25)
        hist = hist[hist['Tenor'] > 0.5].copy() # only calculate tenor more than 6 months
        if len(hist) > 0:
            hist['Yield'] = hist.apply(lambda x: br.bond_yield(coupon, x['Tenor'], freq, x['Price2']), axis=1)
            hist['Duration'] = hist.apply(lambda x: br.bond_duration(x['Yield'], coupon, x['Tenor'], freq), axis=1)
            hist['Price'].fillna(method='ffill', inplace=True)
            hist['Price_pct'] = hist['Price'].pct_change(1)
            hist['Yield_chg'] = -hist['Price_pct'] / hist['Duration']
            
            hist = hist.set_index('Date')
            yield_hist[sec_id] = hist['Yield']
            yield_chg[sec_id]  = hist['Yield_chg']
    
        # xl_utils.add_df_to_excel(hist, wb, 'hist_test', index=True)

    yield_hist = pd.concat(yield_hist, axis=1)    
    xl_utils.add_df_to_excel(yield_hist, wb, 'yld_hist', index=True)
    DATA['yield_hist'] = yield_hist
    
    yield_chg = pd.concat(yield_chg, axis=1) * 10000   
    yield_chg.replace(0, np.nan, inplace=True)
    xl_utils.add_df_to_excel(yield_chg, wb, 'yld_chg', index=True)
    DATA['yield_change'] = yield_chg
    
    # stat
    yield_stat = stat_utils.hist_stat(yield_chg)
    xl_utils.add_df_to_excel(yield_stat, wb, 'yld_chg_stat', index=True)
    DATA['yield_hist_stat'] = yield_stat

    # aggregate by issuer    
    issuers = bonds.loc[yield_chg.columns]['IssuerTicker'].reset_index().rename(columns={'index':'SecurityID'})
    
    issuer_yield_chg = {}
    for issuer in issuers['IssuerTicker'].unique():
        #issuer = 'BBCREDITO'
        print(issuer)
        sec_ids = issuers[issuers['IssuerTicker'] == issuer]['SecurityID'].to_list()
        issuer_yield_chg[issuer] = yield_chg[sec_ids].mean(axis=1)        

    df = pd.concat(issuer_yield_chg, axis=1)
    xl_utils.add_df_to_excel(df, wb, 'issuer_yld_chg')       
    DATA['issuer_yield_change'] = df
    
    issuer_stat = stat_utils.hist_stat(df)
    xl_utils.add_df_to_excel(issuer_stat, wb, 'issuer_yld_stat')       
    DATA['issuer_yield_stat'] = issuer_stat

    # AAA issuer data for UF Benchmark
    aaa_issuers = bonds[bonds['Rating']=='AAA']['IssuerTicker'].unique()

    # take the ones that have at least 100 days of data    
    aaa_issuers = list(set(aaa_issuers) & set(issuer_stat[issuer_stat['Length'] > 100].index))
    
    aaa_yld_chg = df[aaa_issuers].copy()
    aaa_yld_chg['UF_Benchmark'] = aaa_yld_chg.mean(axis=1)
    xl_utils.add_df_to_excel(aaa_yld_chg, wb, 'AAA_yld_chg')       
    DATA['AAA_yield_change'] = aaa_yld_chg

    uf_benchmark = aaa_yld_chg['UF_Benchmark'].copy()
    DATA['UF_Benchmark'] = uf_benchmark


# regression yield against AAA yield benchmark
def regress_issuer(wb):
    
    Y = DATA['issuer_yield_change']
    X = DATA['UF_Benchmark']
    X = X.fillna(method='ffill').dropna()

    regress_df  = pd.DataFrame(columns=['Beta', 'R-Sq', 'Vol'])
    residuals = {}
    
    for issuer in Y.columns:
        # print(issuer)

        df = pd.concat([Y[issuer], X], axis=1)
        df = df.dropna()
        if len(df)<100:
            print(f'skip {issuer}, short data:', len(df))
            continue
            
        betas, b0, r_sq, y_vol, res =  stat_utils.linear_regression(df)
        
        regress_df.loc[issuer] = [betas[0], r_sq, y_vol]
        residuals[issuer] = pd.Series(res, index=df.index)

    regress_df.index.name = 'Issuer'
    xl_utils.add_df_to_excel(regress_df, wb, 'betas', index=True)
    DATA['betas'] = regress_df

    # residuals
    res_df = pd.concat(residuals, axis=1)    
    index_dates = DATA['IndexDates'] 
    res_df = index_dates.merge(res_df, left_index=True, right_index=True, how='left')    
    DATA['res_df'] = res_df
    xl_utils.add_df_to_excel(res_df, wb, 'res_df', index=True)
    
    res_count = res_df.count()
    DATA['residual_count'] = res_count

# fillna in residual    
def residuals_fillna(wb):
    res_df = DATA['res_df']
    betas  = DATA['betas']
    
    missing = list(set(betas.index).difference(res_df))
    if len(missing) > 0:
        missing_issuers = ', '.join(missing)
        raise Exception(f'missing issuers in residuals: {missing_issuers}')
    
    # fill na with normal draw that std = res_vol    
    for issuer, row in betas[['R-Sq', 'Vol']].iterrows():
        # print(issuer)
        rsq, vol = row
        res_vol = vol * np.sqrt(1-rsq)

        idx = res_df[issuer].isna()
        res_df.loc[idx, issuer] = np.random.normal(0, res_vol, sum(idx))

    DATA['residuals'] = res_df
    xl_utils.add_df_to_excel(res_df, wb, 'residuals', index=True)
    
    
#################################################################################################
def load_data(wb, model_id, submodel_id):
    # model parameters
    params = tools.read_parameter(wb)

    # model core parameters
    core_params = model_utils.read_Model_Parameters(model_id)
    
    # update model parameters
    for name in ['Model ID', 'TS Start Date', 'TS End Date', 'Number of Simulations']:
        params[name] = core_params[name]

    params['Submodel ID'] = submodel_id

    # update model parameters
    print('updating wookbook Parameters...')
    xl_utils.add_dict_to_excel(params, wb, 'Parameters')


    DATA['Parameters']  = params
    DATA['Securities']  = xl_utils.read_df_from_excel(wb, 'Securities')

    # index dates
    index_dates = model_utils.read_index_dates(params)
    xl_utils.add_df_to_excel(index_dates.reset_index(), wb, 'IndexDates')
    DATA['IndexDates']  = index_dates

    # rating data
    df = read_benchmark_data('Ratings')
    xl_utils.add_df_to_excel(df, wb, 'Ratings', index=False)
    DATA['Ratings'] = df

    # hist price data
    prices = xl_utils.read_df_from_excel(wb, 'hist_prices', index=True) 
    prices.index = pd.to_datetime(prices.index)
    
    # drop columns with all na, and replace 0 with np.nan
    prices = prices.dropna(axis=1, how='all')
    prices.replace(0, np.nan, inplace=True)

    DATA['hist_prices'] = prices
    
###################################




    ####################################    
    # benchmark data
    # df = read_benchmark_data('Sectors')
    # xl_utils.add_df_to_excel(df, wb, 'Sectors', index=False)
    # DATA['Sectors'] = df
    

    # df = read_benchmark_data('rating_vol')    
    # xl_utils.add_df_to_excel(df, wb, 'rating_vol', index=False)
    # DATA['rating_vol'] = df
    
    # df = read_benchmark_data('dist', index_col=0)
    # xl_utils.add_df_to_excel(df, wb, 'benchmark_dist', index=True)
    # DATA['benchmark_dist'] = df

    # df1 = pd.DataFrame(df.std(), columns=['benchmark_vol'])
    # df1.index.name = 'benchmark'
    # xl_utils.add_df_to_excel(df1, wb, 'Benchmark', index=True)
    # DATA['Benchmark'] = df1

    # # issuer equity distribution    
    # eq_sec_dist = read_benchmark_data('eq_sec_dist', index_col=0)
    # eq_sec_dist.index = pd.to_datetime(eq_sec_dist.index)
    # xl_utils.add_df_to_excel(eq_sec_dist, wb, 'eq_sec_dist', index=True)
    # DATA['eq_sec_dist'] = eq_sec_dist




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
    model_utils.save_model_data(DATA, 'Ratings')
    
    model_utils.save_model_data(DATA, 'hist_prices', index=True)
    model_utils.save_model_data(DATA, 'hist_prices_stat', index=True)
    model_utils.save_model_data(DATA, 'yield_hist', index=True)
    model_utils.save_model_data(DATA, 'yield_change', index=True)
    model_utils.save_model_data(DATA, 'yield_hist_stat', index=True)
    model_utils.save_model_data(DATA, 'issuer_yield_change', index=True)
    model_utils.save_model_data(DATA, 'issuer_yield_stat', index=True)
    model_utils.save_model_data(DATA, 'AAA_yield_change', index=True)
    model_utils.save_model_data(DATA, 'betas', index=True)
    model_utils.save_model_data(DATA, 'res_df', index=True)    
    model_utils.save_model_data(DATA, 'residuals', index=True)
    model_utils.save_model_data(DATA, 'uf_dist', index=True)
    model_utils.save_model_data(DATA, 'dist', index=True)
    model_utils.save_model_data(DATA, 'dist_stat', index=True)

    # save dist
    dist = DATA['dist']
    model_utils.save_dist(DATA, dist, 'SPREAD')
    