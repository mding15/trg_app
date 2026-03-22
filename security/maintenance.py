# -*- coding: utf-8 -*-
"""
Created on Sun Mar  3 16:16:25 2024

@author: mgdin
"""
import pandas as pd
import xlwings as xw

from utils import xl_utils as xl
from security import security_info

    
# use this to add new securities    
# this includes interative procedure
def created_new_securitity():
    wb = open_excel_template()
    # wb = xw.Book('Book4')
    
    # add new security to the excel template file
    # .....

    # check if securities exist in the current database
    check_exist_security(wb)
    
    # remove existing securities from excel
    # ....
    
    # create new security in database
    create_new_security(wb)
    
    # view all securities and xrefs    
    view_all_securities(wb)
    
    # view joined securities and xrefs    
    view_security_with_ref(wb)

def view_all_securities(wb):
    wb = xw.Book('Book2')
    securities = security_info.get_securities()
    xref = security_info.get_xref()
    xl.add_df_to_excel(securities, wb, 'securities_', index=False)  
    xl.add_df_to_excel(xref, wb, 'xref_', index=False)  

def view_security_with_ref(wb):
    
    securities = security_info.get_securities_with_xref()
    xl.add_df_to_excel(securities, wb, 'securities', index=False)  
    
# DELETE securities (include xrefs)
# filename contains a list of SecurityIDs
#
def DELETE_Security(filename):

    df = pd.read_csv(filename)
    sec_ids = df['SecurityID'].tolist()
    #sec_ids = ['T10000991']
    security_info._DELETE_securities(sec_ids)
    
    
# create new security in database
def create_new_security(wb):

    new_securities = xl.read_df_from_excel(wb, 'Security')
    security_info.create_security_and_xref(new_securities)
    xl.add_df_to_excel(new_securities, wb, 'Security', index=False)  

    
    
def check_exist_security(wb):
    # check if securities exist in the current database
    new_securities = xl.read_df_from_excel(wb, 'Security')
    new_securities['SecurityID'] = security_info.get_SecurityID_by_ref(new_securities)
    new_securities['Exist'] = ~new_securities['SecurityID'].isna()
    xl.add_df_to_excel(new_securities, wb, 'Security', index=False)  
    
def open_excel_template():
    wb = xw.Book()

    df = security_info.get_securities_with_xref(['T10000014'])
    df = df.drop(columns=['SecurityID','DateAdded'])
    xl.add_df_to_excel(df, wb, 'Security', index=False)    
    return wb
    
#################################################################################
# generate yf_sec_list.csv file for YF data downloading process
def get_yf_sec_list():

    xref = security_info.get_xref()
    xref = xref[xref['REF_TYPE']=='YF_ID']
    securities = security_info.get_securities_with_xref(xref['SecurityID'], ['YF_ID'])
    securities = securities[['YF_ID', 'SecurityID', 'SecurityName']]
    
    return securities
    
#################################################################################
def test():
    
    get_yf_sec_list()
    wb = xw.Book('Book3')
    df = xl.read_df_from_excel(wb, 'Security')
    df.to_csv(index=False)

