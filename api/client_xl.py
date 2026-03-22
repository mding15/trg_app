# -*- coding: utf-8 -*-
"""
Description:
    All functions used to test TRG Excel Client

Created on Sun Mar 17 21:15:17 2024

@author: mgdin
"""
import datetime
import xlwings as xw
import pandas as pd
import json

from trg_config import config
from api import client, data_pack
from utils import xl_utils as xl
from security import maintenance as sm

TEST_DATA_DIR = config['SRC_DIR'] / 'test_data'

def read_df_from_excel(wb, tab, addr='A1', index=False):
    sht = wb.sheets[tab]
    return sht.range(addr).options(pd.DataFrame, expand='table', index=index).value

# write data to excel
def add_df_to_excel(df, wb, tab, index=True, addr='A1'):
            
    if not tab in [sht.name for sht in wb.sheets]:
        wb.sheets.add(tab, after=wb.sheets[-1])
        
    sht = wb.sheets[tab]
    sht.range(addr).expand('table').clear_contents()
    sht.range(addr).options(index=index).value = df

def read_params(wb):
    params = read_df_from_excel(wb, 'Parameters', index=True)
    params.index = [x.replace(' ', '')  for x in params.index]
    params = params['Value'].to_dict()
    rpt_date = params['AsofDate'] 
    if isinstance(rpt_date, datetime.datetime):
        params['AsofDate'] = rpt_date.strftime('%Y-%m-%d')
    return params

def read_positions(wb):
    positions = read_df_from_excel(wb, 'Positions')
    return positions.to_csv(index=False)

def gen_input_data(wb):
    params = read_params(wb)
    
    data = {'Request': 'CalculateVaR'}
    data['ClientID'] = params['ClientID']
    data['PortfolioID'] = params['PortfolioID']
    
    data['Parameters'] = params
    data['Positions'] = read_positions(wb)
    return data

def save_portfolio_to_json(wb, filename):
    #wb = xw.Book('TRG Risk Assessment.xlsm')
    payload = gen_input_data(wb)
    with open(filename, 'w') as f:
        json.dump(payload, f)    
    print('saved to:', filename)

def read_portfolio_from_json(filename):
    with open(filename, 'r') as f:
        payload = json.load(f)    
    return payload

def test_AddSecurity():
    # wb = xw.Book('Book4')
    wb = sm.open_excel_template()
    new_securities = xl.read_df_from_excel(wb, 'Security')
    
    input_data ={
        "ClientID": '123',
        "Request":   'AddSecurities',
        "NewSecurity": new_securities.to_csv(index=False)
        }

    
    response = client.api_request('add_security', input_data)

    new_sec  = data_pack.extract_df(response, 'NewSecurity')
    new_xref = data_pack.extract_df(response, 'NewXref')
    existing = data_pack.extract_df(response, 'ExistingSecurity')

    xl.add_df_to_excel(new_sec, wb, 'new_sec', index=False)
    xl.add_df_to_excel(new_xref, wb, 'new_xref', index=False)
    xl.add_df_to_excel(existing, wb, 'existing', index=False)

    response.keys()

def test_GetSecurity():

    sec_id_list = [['ISIN', 'LU0119620176'], ['CUSIP','74340XBN0'],['Ticker','GOOG'], ['TRG_ID', 'T10000425']]
    input_data ={
        "ClientID": '123',
        "Request":   'GetSecurity',
        "SecurityIdList": sec_id_list
        }
    
    response = client.api_request('data_request', input_data)
    securities = data_pack.extract_df(response, 'Security')

    wb = xw.Book('Book1')
    xl.add_df_to_excel(securities, wb, 'get_sec', index=False)

    
    
def test():
    filename = TEST_DATA_DIR / 'Demo.Model_1.json'
    
    # wb = xw.Book('TRG Risk Assessment.xlsm')
    # payload = gen_input_data(wb)
    # save_portfolio_to_json(wb, filename)
    
    payload = read_portfolio_from_json(filename)
    response = client.api_request('calculate', payload)

    # save payload
    data_pack.save_input_data(payload)
    
    # save powerbi files
    data_pack.write_pbi(response)
        


