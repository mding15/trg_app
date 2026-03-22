# -*- coding: utf-8 -*-
"""
Created on Thu Sep  5 13:45:28 2024

@author: mgdin
"""

import pandas as pd
import xlwings as xw
from pathlib import Path
from trg_config import config
from utils import xl_utils

SOURCE_DATA_DIR  = Path(r'C:\Users\mgdin\OneDrive - tailriskglobal.com\TRG\Data\ETF')
ETF_DIR = config['HOME_DIR'].parent / 'Models' /'ETF'

def extract_constituents():
    wb = xw.Book(ETF_DIR / 'ETF constituents.xlsx')
    wb = xw.Book('Book1')
    source_files = xl_utils.read_df_from_excel(wb, 'Files')
    for filename in source_files['File']:
        print(filename)
        file_path = SOURCE_DATA_DIR / filename
        if not file_path.exists():
            print('*** not exists ***')
 
    # BlackRock
    etf_constituents = BlackRock(source_files)
    xl_utils.add_df_to_excel(etf_constituents, wb, 'BlackRock', index=False)

    # Invesco
    etf_constituents = Invesco(source_files)
    xl_utils.add_df_to_excel(etf_constituents, wb, 'Invesco', index=False)

    # StateStreet
    etf_constituents = StateStreet(source_files)
    xl_utils.add_df_to_excel(etf_constituents, wb, 'StateStreet', index=False)
    
def BlackRock(source_files):
    files = source_files[source_files['Format'] == 'BlackRock'].set_index('SecurityID')['File']
    etf_constituents = pd.DataFrame()
    for sec_id, filename in files.items():
        print(sec_id, filename)
        file_path = SOURCE_DATA_DIR / filename      
        wb1 = xw.Book(file_path)
        sht = wb1.sheets['Holdings']
        as_of_date = sht.range('A1').value
        etf_name = sht.range('A2').value
        df = xl_utils.read_df_from_excel(wb1, 'Holdings', 'A8')
        df['SecurityID'] = sec_id
        df['ETF Name'] = etf_name
        df['AS_OF_DATE'] = as_of_date
        wb1.close()
        etf_constituents = pd.concat([etf_constituents, df], ignore_index=True)
    
    return etf_constituents
    
def Invesco(source_files):
    files = source_files[source_files['Format'] == 'Invesco'].set_index('SecurityID')['File']
    etf_constituents = pd.DataFrame()
    for sec_id, filename in files.items():
        print(sec_id, filename)
        file_path = SOURCE_DATA_DIR / filename      
        df = pd.read_csv(file_path)
        df['SecurityID'] = sec_id
        etf_constituents = pd.concat([etf_constituents, df], ignore_index=True)
    
    return etf_constituents

def StateStreet(source_files):
    files = source_files[source_files['Format'] == 'State Street'].set_index('SecurityID')['File']
    etf_constituents = pd.DataFrame()
    for sec_id, filename in files.items():
        print(sec_id, filename)
        file_path = SOURCE_DATA_DIR / filename      
        wb1 = xw.Book(file_path)
        sht = wb1.sheets['holdings']
        as_of_date = sht.range('B3').value[7:]
        etf_name = sht.range('B1').value
        df = xl_utils.read_df_from_excel(wb1, 'holdings', 'A5')
        df['SecurityID'] = sec_id
        df['ETF Name'] = etf_name
        df['AS_OF_DATE'] = as_of_date
        wb1.close()
        etf_constituents = pd.concat([etf_constituents, df], ignore_index=True)
    return etf_constituents

