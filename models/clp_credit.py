# -*- coding: utf-8 -*-
"""
Created on Fri Jul 12 09:07:51 2024

@author: mgdin
"""

import pandas as pd
import numpy as np
import xlwings as xw
import datetime
from pathlib import Path

from trg_config import config
from models import model_utils
from models import bond_risk as br
from security import security_info
from mkt_data import mkt_timeseries, mkt_data_info
from utils import xl_utils, date_utils, stat_utils, tools, var_utils

FILE_DIR = Path(r'C:\Users\mgdin\dev\TRG_App\Models\Bond')
securities_columns = ['SecurityID', 'Sector', 'IssuerTicker', 'Rating', 'Spread']

DATA = {}

def run_model():
    wb = xw.Book(FILE_DIR / 'FIAE Domestic Bonds.xlsx')    
    read_data_wb(wb)
    
    # calculate yield history and create benchmark
    model_yield_chg_hist(wb)
    
    # generate distributions
    generate_dist(wb)
    
    # save model
    save_model()

    # save dist
    dist = DATA['bond_dist']
    model_utils.save_dist(DATA, dist, 'SPREAD')

# read inputs from a workbook    
def read_data_wb(wb):
    model_params = tools.read_parameter(wb)
    
    DATA['Parameters']  = model_params
    DATA['Securities']  = xl_utils.read_df_from_excel(wb, 'Securities')
    DATA['rating_map']  = xl_utils.read_df_from_excel(wb, 'rating_map', index=True)
    DATA['IndexDates']  = model_utils.read_index_dates(model_params)
    
    # hist price data
    prices = xl_utils.read_df_from_excel(wb, 'HistPrices', index=True, addr='A13') 
    prices.index = pd.to_datetime(prices.index)
    
    # drop columns with all na, and replace 0 with np.nan
    prices = prices.dropna(axis=1, how='all')
    prices.replace(0, np.nan, inplace=True)

    DATA['hist_prices'] = prices

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
    # sec_id = sec_ids[0]
    for sec_id in sec_ids:
        
        print(sec_id)
        maturity, coupon, freq = bonds.loc[sec_id][['MaturityDate', 'CouponRate', 'PaymentFrequency']]
        hist = prices[[sec_id]]
        hist = hist.dropna().rename(columns={sec_id: 'Price'}).reset_index()
        min_price = hist['Price'].min()
        hist['Price2'] = np.where(min_price>80, hist['Price']/100, 1) # if price<80, use par 
        
        hist['Tenor'] = hist['Date'].apply(lambda x: (maturity - x).days/365.25)
        hist = hist[hist['Tenor'] > 0.5] # only calculate tenor more than 6 months
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
    
    # adj vol
    yield_stat['StdValue Adj'] = yield_stat['StdValue']
    idx = yield_stat['Length'] < 100
    yield_stat.loc[idx, 'StdValue Adj'] = yield_stat['StdValue'] / np.sqrt(10) # two weeks
    
    xl_utils.add_df_to_excel(yield_stat, wb, 'yield_stat', index=True)
    DATA['yield_hist_stat'] = yield_stat

def generate_dist(wb):

    yield_chg = DATA['yield_change'] 
    yield_stat = DATA['yield_hist_stat']

    index_dates = DATA['IndexDates'] 
    dist = index_dates.merge(yield_chg, left_index=True, right_index=True, how='left')    

    # fill na with normal draw that std = hist vol   
    for sec_id, vol in yield_stat['StdValue Adj'].items():
        # print(sec_id)

        idx = dist[sec_id].isna()
        dist.loc[idx, sec_id] = np.random.normal(0, vol, sum(idx))

    # missing sec_ids
    bonds = DATA['Securities']
    missing = set(bonds['SecurityID']).difference(dist)

    # simulate assuming vol to be the 75% quantile of the vol distribution    
    dist_stat = stat_utils.hist_stat(dist)
    vol  = dist_stat['StdValue'].quantile(0.75)    # conservitively take 75% quantile
    n = len(dist)
    for sec_id in missing:
        dist[sec_id] = np.random.normal(0, vol, n)

    # convert from bps to pct point
    dist = dist / 100
    
    # stats    
    dist_stat = stat_utils.hist_stat(dist)
    
    dist_stat['yield_count'] = yield_chg.count()
    dist_stat.fillna(0, inplace=True)
    xl_utils.add_df_to_excel(dist_stat, wb, 'dist_stat')
    DATA['bond_dist_stat'] = dist_stat

    xl_utils.add_df_to_excel(dist, wb, 'dist', index=True)    
    DATA['bond_dist'] = dist
    
# save model data to csv file
def save_model():
    model_utils.save_model_data(DATA, 'Parameters')
    model_utils.save_model_data(DATA, 'Securities')

    model_utils.save_model_data(DATA, 'rating_map', index=True)
    model_utils.save_model_data(DATA, 'hist_prices', index=True)
    model_utils.save_model_data(DATA, 'hist_prices_stat', index=True)
    model_utils.save_model_data(DATA, 'yield_hist', index=True)
    model_utils.save_model_data(DATA, 'yield_change', index=True)
    model_utils.save_model_data(DATA, 'yield_hist_stat', index=True)
    model_utils.save_model_data(DATA, 'bond_dist_stat', index=True)
    model_utils.save_model_data(DATA, 'bond_dist', index=True)
    
    model_utils.save_model_info_data(DATA)
    