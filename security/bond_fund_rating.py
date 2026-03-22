# -*- coding: utf-8 -*-
"""
Created on Sat Sep 21 15:56:45 2024

@author: mgdin
"""

import pandas as pd
import xlwings as xw
from trg_config import config
from utils import xl_utils, date_utils, tools
from security import security_info

TABLE_FILE = config['SEC_DIR'] / 'bond_fund_rating.csv'
TABLE_COLUMNS = ['SecurityID','Rating', 'Weight', 'UpdateDate']

def save_table(df):
    df.to_csv(TABLE_FILE, index=False)

def read_table():
    if TABLE_FILE.exists():
        df = pd.read_csv(TABLE_FILE)
    else:
        df = pd.DataFrame(columns=TABLE_COLUMNS)

    return df

# sec_ids: SecurityID list
def get(sec_ids=None):
    df = read_table()
    if sec_ids is None:
        return df
    else:
        return df[df['SecurityID'].isin(sec_ids)]

def add_data(new_data):
    df = read_table()
    
    # update date
    new_data['UpdateDate'] = date_utils.today()

    # securityID for the new_data
    sec_ids = new_data['SecurityID'].to_list()
    
    # drop existing data
    df = df[~df['SecurityID'].isin(sec_ids)]
    
    # Concat the new sectors
    df = pd.concat([df, new_data[TABLE_COLUMNS]], ignore_index=True)
    
    # save to file
    save_table(df)
    
    print(f'created new data to file {TABLE_FILE}', len(new_data))
    

BOND_FUND_RATING_XL_FILE = config['HOME_DIR'].parent / 'Models' / 'IndustrySector' / 'BondFundRating.xlsx'
def view():
    wb = xw.Book(BOND_FUND_RATING_XL_FILE)
    df = get()    

    # get basket name
    sec_ids = df['SecurityID'].to_list()
    securities = security_info.get_security_by_ID(sec_ids)
    
    # add basket name
    df = df.merge(securities[['SecurityID', 'SecurityName']], on='SecurityID', how='left')
    df = tools.df_move_columns(df, ['SecurityName'])
    xl_utils.add_df_to_excel(df, wb, 'FundRating', index=False)    
    
    
def xl_add_new():
    wb = xw.Book(BOND_FUND_RATING_XL_FILE)
    df = xl_utils.read_df_from_excel(wb, 'Update')    
    
    # make sure weights add up to 100%
    tw = df.groupby(by=['SecurityID'])['Weight'].sum()
    bad = tw[(tw>1.02) | (tw<0.95)]
    if len(bad)>0:
        bad_ids = bad.index.tolist()
        raise Exception(f'weight does not add up to 100%, SecurityID: {bad_ids}')

    # scale to 100%
    df['Scale'] = tools.df_series_merge(df, tw, key='SecurityID')
    df['Weight'] = df['Weight'] / df['Scale']

    # make sure sectors are unique within SecurityID
    df = df.groupby(by=['SecurityID', 'Rating'])['Weight'].sum()
    df = df.reset_index()    
    
    add_data(df)
    
    view()
    
