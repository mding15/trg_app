# -*- coding: utf-8 -*-
"""
Created on Wed Sep  4 09:41:59 2024

@author: mgdin
"""

import pandas as pd
import xlwings as xw
from trg_config import config
from utils import xl_utils, date_utils

SECTOR_FILE = config['SEC_DIR'] / 'security_sectors.csv'
SECTOR_TABLE_COLUMNS = ['SecurityID','Sector','Industry','Country', 'UpdateDate']

def save_security_sector(df):
    df.to_csv(SECTOR_FILE, index=False)

def read_security_sector():
    if SECTOR_FILE.exists():
        df = pd.read_csv(SECTOR_FILE)
    else:
        df = pd.DataFrame(columns=SECTOR_TABLE_COLUMNS)

    return df

# sec_ids: SecurityID list
def get(sec_ids):
    df = read_security_sector()
    if sec_ids is None:
        return df
    else:
        return df[df['SecurityID'].isin(sec_ids)]

def add_security_sector(sec_sectors):
    df = read_security_sector()
    
    # update date
    sec_sectors['UpdateDate'] = date_utils.today()
    
    # Concat the new sectors
    df = pd.concat([df, sec_sectors[SECTOR_TABLE_COLUMNS]], ignore_index=True)
    
    # override existing sector info
    df = df.drop_duplicates(subset=['SecurityID'], keep='last')
    
    # save to file
    save_security_sector(df)
    
    print('created new sectors:', len(sec_sectors))
    

def view():
    file_path = config['HOME_DIR'].parent / 'Models' / 'IndustrySector' / 'SecuritySectors.xlsx'
    wb = xw.Book(file_path) 
    
    # refresh
    df = read_security_sector()    
    xl_utils.add_df_to_excel(df, wb, 'Sectors', index=False)    
    
def update():
    file_path = config['HOME_DIR'].parent / 'Models' / 'IndustrySector' / 'SecuritySectors.xlsx'
    wb = xw.Book(file_path) 
    new_sectors = xl_utils.read_df_from_excel(wb, 'Update')  
    add_security_sector(new_sectors)
    
    
    
def test():
    wb = xw.Book('FIAE Security Sectors.xlsx')
    new_sectors = xl_utils.read_df_from_excel(wb, 'Sectors')    

    add_security_sector(new_sectors)

    df = read_security_sector()    
    df1 = df.groupby(by=['Sector']).count()['SecurityID']
    xl_utils.add_df_to_excel(df1, wb, 'df1')
    