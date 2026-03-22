# -*- coding: utf-8 -*-
"""
Created on Fri Mar 21 12:20:57 2025

@author: mgdin
"""
import pandas as pd
import xlwings as xw

from security import security_info
from utils import xl_utils, var_utils

def test():
    wb = xw.Book('Book3')
    df = var_utils.list_dist()
    xl_utils.add_df_to_excel(df, wb, 'var_list')
   
def get_dist(wb):
    sec_ids = ['T10001566']
    df = var_utils.get_dist(sec_ids)

    tickers = ['SPY', 'IYR', 'Reuters PE', 'SnP PC']
    df = security_info.get_ID_by_Ticker(tickers)
    sec_dict = df.set_index('Ticker')['SecurityID'].to_dict()
    sec_ids = df['SecurityID'].to_list()
    dist = var_utils.get_dist(sec_ids)

    xl_utils.add_df_to_excel(dist, wb, 'dist')

    # save dist 
    var_utils.save_dist(dist, category='PRICE')
