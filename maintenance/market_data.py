# -*- coding: utf-8 -*-
"""
Created on Tue Aug 19 11:39:01 2025

@author: mgdin
"""
import xlwings as xw

from utils import xl_utils
from mkt_data import sync_mkt_data
from security import security_info
from models import sync_dist

def copy_remote_mkt_data():
    sec_list = []
    tickers=['SPY', 'AAPL']
    
    sync_mkt_data.sync_mkt_data(sec_list, tickers)


def copy_remote_dist_data():

    sec_list = ['T10000108', 'T10001565', 'T10001566']

    # df = sync_dist.api_get_dist_data(sec_list)
    sync_dist.copy_from_server(sec_list)    
    

# copy market data from server to local
def copy_mkt_data_for_portfolio(file_path):
    # file_path =  config['TEST_DIR'] / 'portfolios' / 'Demo.xlsx'

    # get positions    
    wb = xw.Book(file_path)
    positions = xl_utils.read_df_from_excel(wb, '1. Positions')

    # get sec_list
    df = security_info.get_SecurityID_by_ref(positions)
    sec_list = df['SecurityID'].tolist()

    sync_mkt_data.sync_mkt_data(sec_list, [])

def test_copy_mkt_data_for_portfolio():
    # file_path =  config['TEST_DIR'] / 'portfolios' / 'Demo.xlsx'
    # file_path = r"C:\Users\mgdin\OneDrive - tailriskglobal.com\Documents - TRG Project\Shared\Projects\Demos\Demo1\FI_E_62_38.xlsx"
    file_path = r"C:\Users\mgdin\OneDrive - tailriskglobal.com\Documents - TRG Project\Shared\Members\Michael\Demo Portfolios\FI_E 60_40.xlsx"

    copy_mkt_data_for_portfolio(file_path)

def test(data):
    wb = xw.Book()
    xl_utils.add_df_to_excel(data, wb, 'df')
