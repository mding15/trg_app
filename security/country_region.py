# -*- coding: utf-8 -*-
"""
Created on Wed Sep  4 12:00:03 2024

@author: mgdin
"""

import pandas as pd
import xlwings as xw
from trg_config import config
from utils import xl_utils, date_utils

COUNTRY_FILE = config['SEC_DIR'] / 'country_region.csv'
COUNTRY_TABLE_COLUMNS = ['Country','Region', 'UpdateDate']

def save_country_region(df):
    df.to_csv(COUNTRY_FILE, index=False)

def read_country_region():
    if COUNTRY_FILE.exists():
        df = pd.read_csv(COUNTRY_FILE)
    else:
        df = pd.DataFrame(columns=COUNTRY_TABLE_COLUMNS)

    return df

# sec_ids: SecurityID list
def get():
    df = read_country_region()
    return df

def add_country_region(countries):
    df = read_country_region()
    
    # update date
    countries['UpdateDate'] = date_utils.today()
    
    # Concat the new sectors
    df = pd.concat([df, countries[COUNTRY_TABLE_COLUMNS]], ignore_index=True)
    
    # override existing country
    df = df.drop_duplicates(subset=['Country'], keep='last')
    
    # save to file
    save_country_region(df)
    
    print('created new countries:', len(countries))
    
    
def test():
    wb = xw.Book('FIAE Security Sectors.xlsx')
    df = xl_utils.read_df_from_excel(wb, 'SectorList', addr='Q1')    
    
    add_country_region(df)
