# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 10:32:19 2024

@author: mgdin
"""
import pandas as pd
import xlwings as xw

from trg_config import config
from detl import bb_extract
from security import security_info
from utils import hdf_utils as hdf
from utils import xl_utils as xl



def get_mkt_data(sec_list, category='PRICE'):
    
    # read data from mkt file
    data = hdf.read(sec_list, category, config.mkt_file)
    return data

def get_bb_security():
    #wb = open_security_wb()
    wb = xw.Book('Book2')
    
    # get bb security
    df = xl.read_df_from_excel(wb, 'Ticker')
    sec_id_list = df['Ticker'].to_list()
    filename = bb_extract.download_security(sec_id_list, 'TICKER')

    filename = config['BB_DIR'] / 'SecurityData' / 'SecurityData.20240403.21.34.16.csv'
    bb_securities = pd.read_csv(filename)
    xl.add_df_to_excel(bb_securities, wb, 'BB Security', index=False)      
    
    new_securities = bb_securities[['NAME', 'ID_ISIN', 'ID_CUSIP', 'TICKER', 'ID_BB_UNIQUE', 'ID_BB_GLOBAL', 'CRNCY', 'SECURITY_TYP']].copy()
    new_securities = new_securities.rename(columns={'ID_ISIN': 'ISIN', 'ID_CUSIP': 'CUSIP', 'TICKER': 'Ticker', 'ID_BB_UNIQUE': 'BB_UNIQUE', 
                                                    'ID_BB_GLOBAL': 'BB_GLOBAL', 'CRNCY':'Currency', 'NAME':'SecurityName'})

    new_securities['SecurityID'] = security_info.get_SecurityID_by_ref(new_securities)
    xl.add_df_to_excel(new_securities, wb, 'NewSecurity', index=False)  
    
    existing_securities = new_securities[~new_securities['SecurityID'].isna()]
    new_securities      = new_securities[new_securities['SecurityID'].isna()]

    # add new xref
    existing_securities['Source'] = 'BB'
    #security.add_xref(existing_securities)
    
    # assign values for AssetClass, AssetType
    asset_class = xl.read_df_from_excel(wb, 'AssetClass')
    new_securities = new_securities.merge(asset_class[['Ticker', 'AssetClass', 'AssetType']], on='Ticker', how='left')
    
    # add new securities
    new_securities['Source'] = 'BB'
    security_info.create_security_and_xref(new_securities)
    
    # view all securities
    securities = security_info.get_securities_with_xref() 
    xl.add_df_to_excel(securities, wb, 'securities', index=False)   
    
    
def open_security_wb():
    wb = xw.Book()
    df = pd.DataFrame({'Ticker': ['TSLA', 'AMZN']})
    xl.add_df_to_excel(df, wb, 'Ticker', index=False)  
    return wb    