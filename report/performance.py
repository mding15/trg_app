# -*- coding: utf-8 -*-
"""
Created on Mon Mar 25 13:59:13 2024

@author: mgdin
"""
import datetime
import pandas as pd
import numpy as np

from mkt_data import mkt_timeseries
from utils import date_utils, mkt_data, tools, stat_utils

def calc_performance(DATA):

    num_years = 3
    positions = DATA['Positions']

    # total mv
    total_mv = positions['MarketValue'].sum()
    cash = positions[positions['AssetClass']=='Cash']['MarketValue'].sum()

    # date range
    end_date = DATA['Parameters']['AsofDate']
    # end_date = datetime.datetime(2025,3,24)
    from_date = date_utils.add_years(end_date, -num_years)
    print('from_date:', from_date.strftime('%Y-%m-%d'), ' end_date:', end_date.strftime('%Y-%m-%d'))

    # get price history and calculate performance
    sec_ids = positions[positions['AssetClass'] != 'Cash']['SecurityID'].unique()
    # price_hist = mkt_timeseries.get_hist(sec_ids, from_date, end_date)
    price_hist = mkt_timeseries.get(sec_ids, from_date, end_date)
    DATA['PriceHist']   = price_hist
    
    stat = stat_utils.hist_stat(price_hist)
    DATA['hist_price_stat'] = stat
    
    # only take securities that have more than 60% dates of total days
    stat1 = stat[ stat['Length']> 250 * num_years * 0.6] 

    if len(stat1) == 0:
        print('found zero securities that have hist prices')
            
    # missing price securities
    missing = list(set(sec_ids).difference(stat1.index))
    if len(missing) > 0:
        msg = 'History MarketValue: missing historical prices for: ' + ', '.join(missing)
        print(msg)

    # caluclate hist mv for a subset
    sec_ids = stat1.index.to_list()        
    price_hist = price_hist[sec_ids]    
    
    # fill the missing prices
    price_hist = price_hist.ffill().bfill()
    
    # Calculate market value history
    pos = positions.set_index('SecurityID')
    subset_mv = pos.loc[sec_ids, 'MarketValue'].sum()
    scale = (total_mv - cash) / subset_mv
    if len(price_hist) > 0 and len(sec_ids) > 0:
        price_scaled = price_hist / price_hist.iloc[-1]
        mv = np.dot(price_scaled.values, pos.loc[sec_ids, 'MarketValue'].values)
        mv_hist = pd.DataFrame(mv, columns=['MarketValue'], index=price_hist.index )    
        mv_hist = mv_hist * scale  + cash
    else:
        mv_hist = pd.DataFrame(columns=['MarketValue'])    

    DATA['MV_Hist'] = mv_hist
    
    



def find_prior_year_end(dates):
    if len(dates) == 0:
        return None
    
    e_date = dates[-1]
    e_year = datetime.datetime(e_date.year-1, 12, 31)
    i = min(range(len(dates)), key=lambda i: abs((dates[i]-e_year).days))
    return dates[i]

def calc_returns(positions, price_hist):
    
    performance = positions[['SecurityID']].drop_duplicates(subset=['SecurityID'])
    performance = performance.set_index('SecurityID')
    
    if len(price_hist) > 0:
        y_end = find_prior_year_end (price_hist.index)
        performance['1D'] = price_hist.iloc[-1] / price_hist.iloc[-2] -1
        performance['1W'] = price_hist.iloc[-1] / price_hist.iloc[-5] -1
        performance['1M'] = price_hist.iloc[-1] / price_hist.iloc[-20] -1
        performance['YTD'] = price_hist.iloc[-1] / price_hist.loc[y_end] -1
        performance['1Y'] = price_hist.iloc[-1] / price_hist.iloc[0] -1
    
    pos_performance = positions[['pos_id', 'SecurityID', 'AssetClass', 'AssetType', 'MarketValue']]
    pos_performance = pos_performance.merge(performance, left_on='SecurityID', right_index=True, how='left')
    
    return pos_performance

# pos=positions
def calc_mv_hist(pos, price_hist):
    
    # securities in both pos and price_hist
    sec_ids = list(set.intersection(set(pos['SecurityID']), set(price_hist.columns)))
    pos = pos.set_index('SecurityID')
    
    price_hist = price_hist[sec_ids]
    
    if len(price_hist) > 0:
        price_scaled = price_hist / price_hist.iloc[-1]
        mv = np.dot(price_scaled.values, pos.loc[sec_ids, 'MarketValue'].values)
        mv_hist = pd.DataFrame(mv, columns=['MarketValue'], index=price_hist.index )    
        cash = pos[pos['AssetClass']=='Cash']['MarketValue'].sum()
        mv_hist = mv_hist + cash
    else:
        mv_hist = pd.DataFrame(columns=['MarketValue'])    
    return mv_hist

def calc_performance2(DATA):
    positions = DATA['Positions']
    end_date = DATA['Parameters']['AsofDate']
    from_date = date_utils.add_years(end_date, -1)
    print('from_date:', from_date.strftime('%Y-%m-%d'), ' end_date:', end_date.strftime('%Y-%m-%d'))

    # get price history and calculate performance
    sec_ids = positions[positions['AssetClass'] != 'Cash']['SecurityID'].unique()
    price_hist = mkt_data.get_market_data(sec_ids, from_date, end_date, 'PRICE')
    
    # missing price securities
    missing = list(set(sec_ids).difference(price_hist.columns))
    if len(missing) > 0:
        msg = 'Missing historical prices for: ' + ', '.join(missing)
        print(msg)
        tools.add_log(DATA, "calc_performance", msg)
        
        # fill with last price from position
        last_prices = DATA['LastPrices']        
        price_hist.loc[price_hist.index[-1], missing] = last_prices.loc[missing, 'LastPrice']
        
        
    # fill the missing prices
    price_hist = price_hist.ffill().bfill()
    DATA['PriceHist']   = price_hist
    
    DATA['Performance'] = calc_returns(positions, price_hist)

    # Calculate market value history
    DATA['MV_Hist'] = calc_mv_hist(positions, price_hist)
    
    benchmark_performance(DATA)

def benchmark_performance(DATA):

    if 'MV_Hist' not in DATA:
        DATA['Return_1Y'] = pd.DataFrame(columns=['Portfolio', 'Benchmark'])
        return
        
    mv_hist = DATA['MV_Hist']
    mv_hist = mv_hist.ffill()

    return_1Y = pd.DataFrame(columns=['Portfolio', 'Benchmark'], index=mv_hist.index)
    DATA['Return_1Y'] = return_1Y

    if len(mv_hist) == 0:
        return
    
    params = DATA['Parameters']
    bm_ticker = params['Benchmark'] 
    
    d1, d2 = mv_hist.index[0], mv_hist.index[-1]
    spx = mkt_timeseries.get_by_tickers([bm_ticker], d1, d2)
    spx.ffill(inplace=True)

    return_1Y['Portfolio'] = mv_hist / mv_hist.iloc[0] -1
    return_1Y['Benchmark'] = spx / spx.iloc[0] -1 
    return_1Y = return_1Y.reset_index()
    
    DATA['Return_1Y'] = return_1Y
    


    
def test(DATA):
    calc_performance(DATA)
    
#     import xl_utils as xl    
#     import xlwings as xw
#     df = DATA['Performance']
#     wb = xw.Book()
#     xl.add_df_to_excel(df, wb, 'performance')