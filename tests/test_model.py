# -*- coding: utf-8 -*-
"""
Created on Tue Oct  7 15:33:50 2025

@author: mgdin
"""

import pandas as pd
import xlwings as xw
from pathlib import Path

from trg_config import config
from models import equity_model
from utils import xl_utils
from utils import stat_utils, var_utils
from security import security_info
from mkt_data import mkt_timeseries

def test_equity_model(wb):
    
    wb = xw.Book(r'C:\Users\mgdin\dev\TRG_App\Models\Equity\EquityModel.Test.xlsx')
    
    # read input
    securities, hist_prices = read_input_equity_model(wb)
    
    # equity model
    model_id, submodel_id = 'M_20240531', 'Equity.test'

    # create model parameters
    model_params = equity_model.create_params(model_id, submodel_id)

    # load required DATA
    DATA = equity_model.load_data(model_params, securities, hist_prices)

    # calculate distributioin
    equity_model.sim_dist(DATA)
    
    # write DATA to excel
    write_to_xl(DATA, wb)
    
    return DATA

###############################################################################    
def read_input_equity_model(wb):
    # read new security data from tab 'Securities'
    securities = xl_utils.read_df_from_excel(wb, 'InputSecurity')
    #securities['SecurityID'] = security_info.get_SecurityID_by_ref(securities)
    for col in ['SecurityID', 'SecurityName']:
        if col not in securities:
            raise Exception(f'SecurityList: missing column: {col}')
            
    # read timeseries
    hist_prices = xl_utils.read_df_from_excel(wb, 'InputPrice', index=True)        
    
    return securities, hist_prices
    

def write_to_xl(DATA, wb):

    for tab in ['Parameters', 'Securities','CoreFactors']:
        add_df_to_excel(DATA, wb, tab)

    for tab in ['IndexDates', 'CoreFactors_Prices','hist_prices', 'security_timeseries',
                'core_factors_timeseries', 'exception', 'corefactor_dist', 'simulated_dist',
                'stat_df', 'regress_df', 'residual_df', 'idio_dist', 'sys_dist']:
        add_df_to_excel(DATA, wb, tab, index=True)

# tab = 'Parameters'
def add_df_to_excel(DATA, wb, tab, index=False):
    df = DATA[tab]
    if isinstance(df, dict):
        df = pd.DataFrame.from_dict(df, orient='index', columns=['Value'])
        df.index.name = 'Name'
        index=True
    xl_utils.add_df_to_excel(df, wb, tab, index)    
    

def get_hist_prices(wb):
    
    sec_ids = ['T10000001', 'T10000004', 'T10000458']
    df = mkt_timeseries.get(sec_ids)
    xl_utils.add_df_to_excel(df, wb, 'ts')    
    
    
