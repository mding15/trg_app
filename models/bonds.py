# -*- coding: utf-8 -*-
"""
Created on Tue Jun 18 11:36:07 2024

@author: mgdin
"""

import pandas as pd
import numpy as np
import xlwings as xw
from pathlib import Path

from trg_config import config
from models import model_utils
from models import bond_risk as br
from models import risk_factors as rf
from security import security_info
from mkt_data import mkt_timeseries, mkt_data_info
from utils import xl_utils, date_utils, stat_utils, tools, var_utils

BOND_FILE = config['SEC_DIR'] / 'bonds.csv' 
FILE_DIR = Path(r'C:\Users\mgdin\dev\TRG_App\Models\Bond')
DATA = {}

# default VaR model
var_utils.set_model_id() 

def bond_var():
    wb = xw.Book(FILE_DIR / 'Bond2.3.xlsx')    
    bonds = tools.read_positions(wb, 'Bonds')
    
    # calc bond sensitivity
    bonds['Tenor'] = bonds.apply(lambda x: (x['Maturity'] - x['PriceDate']).days/365, axis=1)
    sens = calc_bond_sensitivities(bonds)
    bonds[sens.columns] = sens

    # risk factor
    risk_factors = calc_risk_factors(bonds)

    # Check securities that are missing risk factors    
    
    xl_utils.add_df_to_excel(bonds, wb, 'Bonds', index=True)

def calc_PnL(bonds, risk_factors):
    PnL = {}
    
    for cat in ['SPREAD', 'IR']:
        cat = 'SPREAD'
        factors = risk_factors[risk_factors.Category == cat]
        
        rf_ids = factors['RF_ID'].unique()

        # get ts and drop RFs that do not have ts
        ts = var_utils.get_dist(rf_ids, cat)
        # check missing ts
        
        # drop missing ts refs
        factors = factors[factors.RF_ID.isin(ts)]

        # calculate P/L
        pl = pd.DataFrame(factors.Sensitivity.values * ts[factors.RF_ID].values, columns=factors.SecurityID, index=ts.index)
        PnL[cat] = pl

        
# bonds: columns = ['Coupon', 'Tenor', 'Price']
def calc_bond_sensitivities(bonds):
    bonds = bonds[['Tenor', 'Price', 'Coupon']].copy()
  
    bonds['Status'] = 'OK'
    bonds.loc[bonds['Tenor'] < 0.001, 'Status'] = 'Bad Tenor'
    bonds.loc[(bonds['Price'] > 1.5) | (bonds['Price'] < 0.2) , 'Status'] = 'Bad Price'

    idx = (bonds['Status'] == 'OK')
    bonds.loc[idx, 'Yield']     = bonds.loc[idx].apply(lambda x: br.bond_yield(x['Coupon'], x['Tenor'], x['Price']), axis=1)
    bonds.loc[idx, 'Duration']  = bonds.loc[idx].apply(lambda x: br.bond_duration(x.Yield, x.Coupon, x.Tenor), axis=1)
    bonds.loc[idx, 'Convexity'] = bonds.loc[idx].apply(lambda x: br.bond_convexity(x.Yield, x.Coupon, x.Tenor), axis=1)

    return bonds[['Yield', 'Duration', 'Convexity', 'Status']]


def calc_risk_factors(securities):

    bonds = securities[(securities['AssetClass'] == 'Bond') & (securities['AssetType'] == 'Bond')]
    
    # Bond risk_factors
    spread_rf = rf.spread_risk_factors(bonds['SecurityID'])
    ir_rf = rf.ir_risk_factors(bonds)
    risk_factors = pd.concat([spread_rf, ir_rf], ignore_index=True)

    return risk_factors

    
    