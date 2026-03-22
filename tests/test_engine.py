# -*- coding: utf-8 -*-
"""
Created on Fri May 17 15:45:59 2024

@author: mgdin
"""
import pandas as pd
import numpy as np
import xlwings as xw
from pathlib import Path
import traceback

# for testing purpose
# from api import app, db
# app.app_context().push()

from engine import VaR_engine as engine
from preprocess import read_portfolio
from models import bond_risk as br
from report import powerbi as pbi
from utils import test_utils, xl_utils, var_utils, mkt_data
from trg_config import config
from api import scrubbing_portfolio
from api import portfolios    

def test_engine():
    
    file_path = config['DATA_DIR'] / 'test' / 'portfolios' / 'Model2.xlsx'
    wb = xw.Book(file_path)
    
    # read parameters and positions from file
    params, positions = read_input(wb)
    
    # run engine
    DATA = engine.calc_VaR(positions, params)

    # write DATA to excel
    wb = xw.Book()
    test_utils.write_DATA_to_xl(DATA, wb)

    # test report
    results = pbi.generate_report(DATA)
    excel_file = Path(r'C:\Users\mgdin\Downloads\pbi_output.xlsx')
    pbi.write_results_xl(results, excel_file)
    wb = xw.Book(excel_file)
    

def test_save_portfolios():
    file_path = Path(r'C:\Users\mgdin\Downloads') / 'Model-2 (4).xlsx'
    params, positions = portfolios.read_input_file(file_path)    
    portfolios.check_input(params, positions)
    DATA = engine.calc_VaR(positions, params)    


    
def test_calc_agg_pl(positions, PnL):
    # Construct hierarchy P/L
    hierarchy = ['Class', 'SC1', 'SC2', 'SecurityID']
    for col in hierarchy:
        positions[col].fillna('', inplace=True)
    hier_keys = positions.loc[PnL.columns, hierarchy]
    paths = hier_keys.apply(lambda x: '|'.join(x), axis=1).to_list()
    pos_pl = pd.DataFrame(PnL.values, columns=paths)


def debug(wb, DATA):
    bonds = DATA['Bonds']
    xl_utils.add_df_to_excel(bonds, wb, 'Bonds', index=True)

    positions = DATA['Positions']
    xl_utils.add_df_to_excel(positions, wb, 'CalcPositions', index=True)
    
    DATA.keys()

def test_bond(wb, DATA):
    bonds = DATA['Bonds']
    xl_utils.add_df_to_excel(bonds, wb, 'Bonds', index=True)

    rpt_date = DATA['Parameters']['AsofDate']
    positions = DATA['Positions']

    last_prices = DATA['LastPrices']
    bonds['Price'] = last_prices['LastPrice'] / 100 # convert bond price to par price=1
    
    
    bonds['Price'] = np.where((bonds['Price']<0.8) | (bonds['Price']>1.5) , 1, bonds['Price']) # cap 1.5, floor=0.8
    sec_ids = bonds.index.tolist()

    # bond tenor
    bonds['Tenor']    = bonds.apply(lambda x: (x['MaturityDate'] - rpt_date).days/365.25, axis=1)
    bonds['IR_Tenor'] = bonds.apply(lambda x: (x['MaturityDate'] - rpt_date).days/365.25, axis=1)
    
    # calculate sensitivities
    bonds['Yield']     = bonds.apply(lambda x: br.bond_yield(x['CouponRate'], x['Tenor'], x['PaymentFrequency'] , x['Price']), axis=1)
    bonds['Duration']  = bonds.apply(lambda x: br.bond_duration(x.Yield, x.CouponRate, x.Tenor, x['PaymentFrequency']), axis=1)
    bonds['Convexity'] = bonds.apply(lambda x: br.bond_convexity(x.Yield, x.CouponRate, x.Tenor, x['PaymentFrequency']), axis=1)
    
    
#
# test distribution
#    
def test_distribution():
    wb = xw.Book('Book1')
    positions = engine.read_positions(wb)
    params = engine.read_params(wb)
    
    positions = engine.build_positions(positions, params)
    xl_utils.add_df_to_excel(positions, wb, 'calc_positions', index=False)

    # set var model
    var_utils.set_client_id(params['ClientID'])

    # Calculate sensitivities, such as: duration, convexity 
    positions = engine.calc_sensitivities(positions)
    
    # calc risk factors
    risk_factors = engine.calc_risk_factors(positions)
    
    cat = 'DELTA'
    rf_id_list = risk_factors[risk_factors['Category']==cat]['RF_ID'].to_list()
    dist = var_utils.get_dist(rf_id_list, cat)
    missing = set(rf_id_list).difference(set(dist.columns))
    xl_utils.add_df_to_excel(dist, wb, 'dist')

    # security_df = security.get_security_by_ID(positions['SecurityID'])
    # xl_utils.add_df_to_excel(security_df, wb, 'Security', index=False)

# engine.calc_var
def test_var_calc():
    wb = engine.open_template()
    wb = xw.Book('Book1')
    
    positions = engine.read_positions(wb)
    params = engine.read_params(wb)

    try:
        DATA = engine.calc_VaR(positions, params)
        test_utils.write_DATA_to_xl(DATA, wb)
    except Exception as e:
        traceback.print_exc()
    
    positions = DATA['Positions']    
    sec_id_list= positions['SecurityID'].to_list()
    
    # prices
    prices = mkt_data.get_market_data(sec_id_list)    
    xl_utils.add_df_to_excel(prices, wb, 'Prices')
    
    # distribution
    risk_factors = DATA['RiskFactor']
    rf_id_list = risk_factors[risk_factors['Category']=='DELTA']['RF_ID'].to_list()

    dist = var_utils.get_dist(rf_id_list, 'PRICE')    
    xl_utils.add_df_to_excel(dist, wb, 'Dist4Y')

    # dist_list = var_utils.list_dist()
    # xl.add_df_to_excel(dist_list, wb, 'DistList')

    print(var_utils.VaR_file)
    #var_utils.set_version('1.1')    
    var_utils.set_version('4Y')    
    
    DATA.keys()    
    pnl = DATA['RF_PnL']['DELTA']
    xl_utils.add_df_to_excel(pnl, wb, 'RF_PnL')
    
# In[]
def read_input(wb):
    
    params, positions = read_portfolio.read_input_xl(wb)
    
    # check inputs
    params, positions = scrubbing_portfolio.scrub_data(params, positions)
    
    return params, positions 

