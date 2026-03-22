# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 10:54:17 2024

@author: mgdin
"""
from pandas import pd
import xlwings as xw
from utils import xl_utils
from database import db_utils

from detl import yh_extract


def xl_book():
    wb = xw.Book('Book1')

def test_api_hist_price(wb):
    tickers = ['MSFT', 'PYPL']
    df = yh_extract.api_hist_price(tickers)
    xl_utils.add_df_to_excel(df, wb, 'hist_price', index=False)
    df = xl_utils.read_df_from_excel(wb, 'hist_price')

    # extract historical prices and save to db table stock_price
    yh_extract.extract_hist_prices(tickers)

    # query database
    query = "select * from yh_stock_price where ticker in ('MSFT', 'PYPL')"
    df = db_utils.get_sql_df(query)
    xl_utils.add_df_to_excel(df, wb, 'hist_price2', index=False)


    
