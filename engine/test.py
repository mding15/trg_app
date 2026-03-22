# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 11:57:27 2025

@author: mgdin
"""
import sys
sys.path.insert(0, r'C:\Users\mgdin\OneDrive\Documents\dev\claude\trg_app')

import xlwings as xw
import pandas as pd
from pathlib import Path
from utils import tools
from utils import xl_utils as xl
from api import scrubbing_portfolio
from preprocess import read_portfolio
from engine import VaR_engine as engine
from report import powerbi as pbi
from api import portfolios

def test_engine():
    
    # option 1: read portfolio from Excel
    wb = xw.Book('Model2.xlsx')
    params, positions = read_input(wb)
    
    # option 2: read portfolio by port_id
    port_id = 5329
    positions, params, unknown_positions, limit = portfolios.load_from_db(port_id)
    
    # run engine
    DATA = engine.calc_VaR(positions, params)

    # write DATA to excel
    wb = xw.Book('Book3')
    engine.write_to_excel(wb, DATA)

    # test report
    results = pbi.generate_report(DATA)
    excel_file = Path(r'C:\Users\mgdin\Downloads\pbi_output.xlsx')
    pbi.write_results_xl(results, excel_file)
    wb = xw.Book(excel_file)
    


def test_pre_process():
    
    wb = xw.Book('Book1.xlsx')
    params, positions = read_input(wb)
    
    # engine    

    DATA = engine.create_DATA(positions, params)
    engine.pre_process(DATA)      
    
    # position_VaR(DATA)
    engine.calc_sensitivities(DATA)
    
    # positions = DATA['Positions']
    # xl.add_df_to_excel(positions, wb, 'pos')

def test_position_VaR():
    wb = xw.Book('Model2.xlsx')
    params, positions = read_input(wb)

    # engine    

    DATA = engine.create_DATA(positions, params)
    engine.pre_process(DATA)      
    
    # Calculate sensitivities, such as: duration, convexity 
    engine.calc_sensitivities(DATA)
    
    # calc risk factors
    DATA['RiskFactor'] = engine.calc_risk_factors(DATA)
    xl.add_df_to_excel(DATA['RiskFactor'], wb, 'RiskFactor')

    # calc risk factor PnL
    DATA['RF_PnL'] = engine.calc_RF_PnL(DATA)
    cat_pl = DATA['RF_PnL']
    for cat in cat_pl:
        print(cat)
        xl.add_df_to_excel(cat_pl[cat], wb, f'{cat}_PnL')
    
    # calc position VaR
    DATA['VaR'] = engine.calc_position_VaR(DATA)
    xl.add_df_to_excel(DATA['VaR'], wb, 'VaR', index=False)
    
    # Risk Factor VaR
    DATA['RF_VaR'] = engine.calc_rf_VaR(DATA)
    xl.add_df_to_excel(DATA['RF_VaR'], wb, 'RF_VaR', index=False)
    
    
    

def read_input(wb):
    
    params, positions = read_portfolio.read_input_xl(wb)
    
    # check inputs
    params, positions = scrubbing_portfolio.scrub_data(params, positions)
    
    return params, positions 


def test_data():
    data = {'SecurityID': 'T10000108', 
            'SecurityName': 'SPDR S&P 500 ETF TRUST', 
            'Quantity': 100, 
            'MarketValue': 66900, 
            'ExpectedReturn': 0.15, 
            'Class': 'Equity', 
            'SC1': 'EQ ETF', 
            'SC2': None,
            'Country': 'United States', 
            'Region': 'North America', 
            'Sector': 'Basket', 
            'Industry': 'Basket', 
            'OptionType': None, 
            'PaymentFrequency': None, 
            'MaturityDate': None, 
            'OptionStrike': None, 
            'UnderlyingSecurityID': None, 
            'CouponRate': None, 
            'is_option': False, 
            'UnderlyingID': None, 
            'LastPrice': 669, 
            'LastPriceDate': '9/30/2025', 
            'AssetClass': 'Equity', 
            'AssetType': 'ETF', 
            'Currency': 'USD',
            'ISIN': 'US78462F1030',
            'CUSIP': '78462F103',
            'Ticker': 'SPY',
            }

    positions = pd.DataFrame([data])

    params = {'AsofDate': '2025-09-30', 
            'ReportDate': '2025-09-30', 
            'RiskHorizon': '1 Month', 
            'TailMeasure': '95% TailVaR', 
            'ReturnFrequency': 'Daily', 
            'Benchmark': 'BM_60_40', 
            'ExpectedReturn': 'Upload', 
            'BaseCurrency': 'USD', 
            'port_id': 5344, 
            'PortfolioName': 'SPY'}
    
    return params, positions

def test():
    params, positions = test_data()
    DATA = engine.calc_VaR(positions, params)

if __name__ == '__main__':
    test()
