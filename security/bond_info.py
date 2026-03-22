# -*- coding: utf-8 -*-
"""
Created on Mon Jul  1 11:41:39 2024

@author: mgdin
"""
import pandas as pd
import xlwings as xw
from trg_config import config
from utils import xl_utils, date_utils

BOND_FILE = config['SEC_DIR'] / 'bonds.csv'


def save_bond_info(df):
    df.to_csv(BOND_FILE, index=False)

def read_bond_info():
    df = pd.read_csv(BOND_FILE)
    df['MaturityDate'] = pd.to_datetime(df['MaturityDate'])
    return df


def get(sec_ids=None):
    df = read_bond_info()
    
    if sec_ids is not None:
        df = df[df['SecurityID'].isin(sec_ids)]
    
    return df


def add_bonds(new_bonds):
    df = read_bond_info()
    
    # wb = xw.Book('Securities.xlsx')
    # new_bonds = xl_utils.read_df_from_excel(wb, 'Treasury')    

    # new bond securityIDs
    sec_ids = new_bonds['SecurityID'].to_list()

    # columns
    columns = list(set(new_bonds) & set(df))
    new_bonds = new_bonds[columns]

    # remove existing 
    df = df[~df['SecurityID'].isin(sec_ids)]
    
    # add date
    new_bonds['AddDate']    = date_utils.today()
    new_bonds['UpdateDate'] = date_utils.today()
    
    # concat new bonds
    df = pd.concat([df, new_bonds])
    
    # save to file
    save_bond_info(df)
    
    print('created new bonds:', len(new_bonds))
    
BOND_INFO_XL_FILE = config['HOME_DIR'].parent / 'Models' / 'Securities' / 'BondInfo.xlsx'
def view():
    wb = xw.Book(BOND_INFO_XL_FILE)
    df = get()    
    
    # add basket name
    # df = tools.df_move_columns(df, ['SecurityName'])
    xl_utils.add_df_to_excel(df, wb, 'BondInfo', index=False)    
    
def xl_add_new():
    wb = xw.Book(BOND_INFO_XL_FILE)
    df = xl_utils.read_df_from_excel(wb, 'Update')    
    
    add_bonds(df)
    
    view()
