# -*- coding: utf-8 -*-
"""
Created on Mon Mar 25 16:33:21 2024

@author: mgdin
"""
import pandas as pd
import numpy as np

from utils import var_utils
from security import security_info

def calc_sharpe_ratio(DATA):
    T = DATA['Parameters']['HorizonDays']
    positions = DATA['Positions']
    
    # expected returns
    positions['ExpectedReturn'].fillna(0.02, inplace=True)

    # position VaR 
    var = DATA['VaR']
    positions = positions.merge(var, on='pos_id', how='left')

    # total Market Value
    tot_mv = positions['MarketValue'].sum()

    # weights
    positions['Weight'] = positions['MarketValue'] / tot_mv

    # return countribution
    positions['Return Contribution'] = positions['ExpectedReturn'] * positions['Weight']
    
    # total return
    total_return = np.dot(positions['MarketValue'], positions['ExpectedReturn']) / tot_mv

    # split into zero_vol and non_zero_vol
    zero_vol_pos = positions[positions['STD'] == 0]
    positions = positions[positions['STD'] > 0].copy()
    
    # total
    total = positions[['Marginal_STD', 'Marginal_VaR',	'Marginal_tVaR']].sum()
    tot_std, tot_var, tot_tvar = total
    
    # Annualized volatility in percentage to the market value
    positions['Volatility'] = positions['STD'] / abs(positions['MarketValue']) * np.sqrt(252/T)
    total.loc['Volatility'] = tot_std / tot_mv * np.sqrt(252/T)
    
    # Annualized marginal volatility in percentage to the market value
    positions['Marginal_Vol'] = positions['Marginal_STD'] / tot_mv * np.sqrt(252/T)
    
    # Annualized percentage VaR with respect to MarketValue
    positions['VaR%']  = positions['VaR']  / abs(positions['MarketValue']) 
    positions['tVaR%'] = positions['tVaR'] / abs(positions['MarketValue']) 
    total.loc['VaR%']  = tot_var / tot_mv
    total.loc['tVaR%'] = tot_tvar / tot_mv

    # Percentage of VaR Contribution to the total VaR
    positions['VaR Contribution']  = positions['Marginal_VaR'] / tot_var
    positions['tVaR Contribution'] = positions['Marginal_tVaR'] / tot_tvar 
    
    # total return
    total.loc['ExpectedReturn'] = total_return

    # riskfree rate
    df = DATA['Static']
    rf_rate = df.loc['Riskfree Rate']['Value']

    # Sharpe Ratio to Volatility
    positions['SR Vol'] = (positions['ExpectedReturn'] - rf_rate ) / positions['Volatility']
    total.loc['SR Vol'] = (total['ExpectedReturn'] - rf_rate ) / total['Volatility']

    # extreme low valatility assign 0 Sharpe ratio
    positions.loc[positions['Volatility'] < 1e-4, 'SR Vol'] = 0
    
    # Sharpe Ratio to VaR
    positions['SR VaR']  = (positions['ExpectedReturn'] - rf_rate) / positions['VaR%'] / np.sqrt(252/T) 
    positions['SR tVaR'] = (positions['ExpectedReturn'] - rf_rate) / positions['tVaR%'] / np.sqrt(252/T) 
    total.loc['SR VaR'] = (total['ExpectedReturn'] - rf_rate ) / total['VaR%'] / np.sqrt(252/T) 
    total.loc['SR tVaR'] = (total['ExpectedReturn'] - rf_rate ) / total['tVaR%'] / np.sqrt(252/T) 
    
    # Ratio of return to the marginal VaR in percentage
    positions['SR mVaR'] = (positions['ExpectedReturn'] - rf_rate) / (positions['Marginal_VaR']/positions['MarketValue'] * np.sqrt(252/T))

    # extreme low VaR assign 0 Sharpe ratio
    positions.loc[positions['VaR%'] < 1e-4, ['SR VaR', 'SR mVaR']] = [0, 0]

    # add back zero vol positions
    positions = pd.concat([positions, zero_vol_pos])
    
    # fill the blank only for numeric columns
    numeric_cols = positions.select_dtypes(include=[np.number]).columns 
    positions[numeric_cols] = positions[numeric_cols].fillna(0)
    
    # benchmark risk
    bm_risk = calc_benchmark_risk(DATA)
    DATA['BechmarkRisk'] = bm_risk
    
    # update DATA
    DATA['Positions'] = positions
    DATA['TotalVaR'] = total
    
    # xl.add_df_to_excel(positions, wb, 'test_pos')    

    
def calc_benchmark_risk(DATA):
    params = DATA['Parameters']
    T = params['HorizonDays']
    bench_ticker = params['Benchmark']
    bench_exp_returns = params['BenchmarkExpectedReturn']
    bench_returns = get_return_by_ticker([bench_ticker])
    
    # riskfree rate
    df = DATA['Static']
    rf_rate = df.loc['Riskfree Rate']['Value']

    metrics = {}
   
    metrics['Vol']  = bench_returns.std()[0] * np.sqrt(252)
    metrics['VaR%']  = var_utils.calc_tVaR(bench_returns).iloc[0,0] * np.sqrt(T)
    metrics['ExpRet'] = bench_exp_returns
    metrics['SR Vol'] = (bench_exp_returns - rf_rate) / metrics['Vol']
    metrics['SR VaR'] = (bench_exp_returns - rf_rate) / (metrics['VaR%'] * np.sqrt(252/T))

    return metrics

# tickers = [bench_ticker]
def get_return_by_ticker(tickers, category='PRICE'):
    # tickers = ['BM_20_80']
    #var_utils.VaR_file
    ticker_ids = security_info.get_ID_by_Ticker(tickers)
    returns = var_utils.get_dist(ticker_ids['SecurityID'], category=category)
    # handling missing case
    if len(returns) == 0:
        tic_str = ", ".join(tickers)
        raise Exception(f'can not find distribution for benchmark {tic_str}')
    
    returns.columns = ticker_ids['Ticker'].to_list()
    returns.index.name = 'Date'
    return returns
        