# -*- coding: utf-8 -*-
"""
Created on Fri Mar 28 12:58:48 2025

@author: mgdin
"""
import pandas as pd
import numpy as np
from utils import stat_utils, var_utils

def back_test(DATA):
    # mv_hist = DATA['MV_Hist']
    mv_hist = calc_hist_mv(DATA)
    total_var = DATA['TotalVaR']
    T = DATA['Parameters']['HorizonDays']
    
    var, vol = calc_var(DATA)
    
    back_test = pd.DataFrame(index=mv_hist.index)
    # back_test['TS_Vol'] = total_var['Volatility']
    # back_test['TS_VaR'] = -total_var['tVaR%'] * np.sqrt(21/T) 
    back_test['TS_Vol'] = vol * np.sqrt(252)
    back_test['TS_VaR'] = -var * np.sqrt(21) 
    
    back_test['TS_Return'] = mv_hist['MarketValue'].pct_change(21)
    back_test = back_test.dropna(subset=['TS_Return'])
    back_test['exceptions'] = (back_test['TS_Return'] < back_test['TS_VaR']).astype(int)

    back_test.index.name = 'TS_Date'
    back_test = back_test.reset_index()
    
    DATA['BackTest']=back_test

def calc_var(DATA):
    positions  = DATA['Positions']
    # exclude Alternative
    positions = positions[positions['AssetClass'] != 'Alternative']

    PnL = DATA['PnL']
    PnL = PnL[positions['pos_id']]

    tot_mv =   positions['MarketValue'].sum()
    tot_pl = PnL.sum(axis=1)
    tot_pl = pd.DataFrame(tot_pl, columns=['TotalVaR'])
    
    var = var_utils.calc_tVaR(tot_pl) / tot_mv
    vol = tot_pl.std() / tot_mv
    return var.iloc[0,0], vol.iloc[0]
    
def calc_hist_mv(DATA):
    num_years = 3
    positions  = DATA['Positions']
    price_hist = DATA['PriceHist']

    # exclude Alternative
    positions = positions[positions['AssetClass'] != 'Alternative']
    
    # total mv
    total_mv = positions['MarketValue'].sum()
    cash = positions[positions['AssetClass']=='Cash']['MarketValue'].sum()

    # non-cash security IDs
    sec_ids = positions[positions['AssetClass'] != 'Cash']['SecurityID'].unique()
    
    
    
    # stat
    stat = stat_utils.hist_stat(price_hist)
    
    # only take securities that have more than 60% dates of total days
    stat = stat[ stat['Length']> 250 * num_years * 0.6] 

    if len(stat) == 0:
        print('found zero securities that have hist prices')

    # security that has hist_price
    sec_ids1 = list(set(sec_ids) & set(stat.index))
    
    # missing price securities
    missing = list(set(sec_ids).difference(sec_ids1))
    if len(missing) > 0:
        msg = 'Backtest: missing historical prices for: ' + ', '.join(missing)
        print(msg)
    
    # caluclate hist mv for a subset
    price_hist = price_hist[sec_ids1]    
    
    # fill the missing prices
    price_hist = price_hist.ffill().bfill()
    
    # Calculate market value history
    pos = positions.set_index('SecurityID')
    subset_mv = pos.loc[sec_ids1, 'MarketValue'].sum()
    scale = (total_mv - cash) / subset_mv
    if len(price_hist) > 0 and len(sec_ids) > 0:
        price_scaled = price_hist / price_hist.iloc[-1]
        mv = np.dot(price_scaled.values, pos.loc[sec_ids1, 'MarketValue'].values)
        mv_hist = pd.DataFrame(mv, columns=['MarketValue'], index=price_hist.index )    
        mv_hist = mv_hist * scale  + cash
    else:
        mv_hist = pd.DataFrame(columns=['MarketValue'])    

    return mv_hist
    
