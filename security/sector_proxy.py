# -*- coding: utf-8 -*-
"""
Created on Tue Sep 10 20:16:58 2024

@author: mgdin
"""


import pandas as pd
import xlwings as xw
from trg_config import config
from utils import xl_utils, date_utils, tools
from security import security_info

TABLE_FILE = config['SEC_DIR'] / 'sector_proxy.csv'
TABLE_COLUMNS = ['Sector','Ticker', 'SecurityID', 'UpdateDate'] # ticker=proxy ticker, securityID=proxy securityID

def save_table(df):
    df.to_csv(TABLE_FILE, index=False)

def read_table():
    if TABLE_FILE.exists():
        df = pd.read_csv(TABLE_FILE)
    else:
        df = pd.DataFrame(columns=TABLE_COLUMNS)

    return df

def get():
    return read_table()

def add_data(new_data):
    df = read_table()
    
    # update date
    new_data['UpdateDate'] = date_utils.today()

    sectors = new_data['Sector'].to_list()
    
    # drop existing data
    df = df[~df['Sector'].isin(sectors)]
    
    # Concat the new sectors
    df = pd.concat([df, new_data[TABLE_COLUMNS]], ignore_index=True)
    
    # save to file
    save_table(df)
    
    print(f'created new data to file {TABLE_FILE}', len(new_data))
    
def view():
    wb = xw.Book()
    df = get()    
    
    xl_utils.add_df_to_excel(df, wb, 'Proxy', index=False)    
    
    
def xl_add_new():
    wb = xw.Book('BasketSectorModel.xlsx')
    df = xl_utils.read_df_from_excel(wb, 'Proxy')    
    add_data(df)
    
    view()
    
def test():
    wb = xw.Book('BasketSectorModel.xlsx')
    new_data = xl_utils.read_df_from_excel(wb, 'Proxy')    

    add_data(new_data)
