# -*- coding: utf-8 -*-
"""
Created on Sat Jan 10 17:13:56 2026

@author: mgdin
"""
import pandas as pd
import xlwings as xw

from security import security_info
from utils import xl_utils, tools
from trg_config import config

def add_new_security():
    wb = xw.Book('Book1')

    # read upload_security template
    df = read_security_template()
    xl_utils.add_df_to_excel(df, wb, 'Security')

    # read security data from wookbook, then create new security in database 
    create_new_security_from_wb(wb)
    
    
# read security data from wookbook, then create new security in database 
def create_new_security_from_wb(wb):

    # Upload security to Database
    securities = read_security(wb) 

    # create new security in database
    new_securities, new_xref_df = security_info.create_security_and_xref(securities)
    
    # write outputs
    xl_utils.add_df_to_excel(new_securities, wb, 'new_securities', index=False)
    xl_utils.add_df_to_excel(new_xref_df, wb, 'new_xref_df', index=False)


def read_security_template():
    file_path = config['PUBLIC_DIR'] / 'security_upload_template.xlsx'
    df = pd.read_excel(file_path, sheet_name='Security')        
    return df

def read_security(wb):
        
    # read new security data from tab 'Security'
    securities = tools.read_positions(wb, 'Security')

    # normalize column names
    securities.columns = [x.replace(' ', '') for x in securities.columns]
    
    return securities
