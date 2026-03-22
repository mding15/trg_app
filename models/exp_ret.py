# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 14:02:47 2024

@author: mgdin
"""

import pandas as pd
import numpy as np
import xlwings as xw
from pathlib import Path

from trg_config import config
from security import security_info
from utils import xl_utils, data_utils


def run():
    sec_expret = generate_exp_ret()

    wb = xw.Book('Book2')    
    xl_utils.add_df_to_excel(sec_expret, wb, 'ExpRet', index=False)

def generate_exp_ret():

    securities = security_info.get_security_by_ID()
    
    # exclude assets
    securities= securities[~securities['AssetClass'].isin(['Macro', 'Riskfactor'])]
    securities= securities[~securities['AssetType'].isin(['Index', 'YieldCurve'])]
    
    # select columns
    securities = securities[['SecurityID', 'SecurityName', 'AssetClass', 'AssetType']]
    
    # assign exp_ret based on assets
    df = data_utils.load_stat('AssetClassReturns')
    
    sec_expret = securities.merge(df, on=['AssetClass', 'AssetType'], how='left')
    missing = sec_expret[sec_expret['ExpRet'].isna()]
    if len(missing) > 0:
        asset_class = missing[['AssetClass', 'AssetType']].drop_duplicates(subset=['AssetClass', 'AssetType'])
        print("missing asset class/type")
        print(asset_class)
    
    sec_expret = sec_expret.set_index('SecurityID')
    data_utils.save_stat(sec_expret, 'ExpectedReturn')
    
    return sec_expret
