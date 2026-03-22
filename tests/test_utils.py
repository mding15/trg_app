# -*- coding: utf-8 -*-
"""
Created on Fri May 17 11:56:01 2024

@author: mgdin
"""
import xlwings as xw

from trg_config import config
from utils import xl_utils, tools, data_utils, date_utils, hdf_utils, mkt_data, var_utils


def test_data_utils():

    wb = xw.Book()
    
    # tools
    tools.timestamp()

    # data_utils    
    data_utils.load_stat('static_data')
    
    # date_utils
    date_utils.get_cob()

    # hdf_utils
    df = hdf_utils.list(config['mkt_file'])
    xl_utils.add_df_to_excel(df, wb, 'mkt_file')

    # mkt_data
    df = mkt_data.test() 
    xl_utils.add_df_to_excel(df, wb, 'mkt_data')
    
    # var_utils
    df = var_utils.get_dist(['T10000001'])
    xl_utils.add_df_to_excel(df, wb, 'var_dist')
