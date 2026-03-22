# -*- coding: utf-8 -*-
"""
Created on Sun Mar 17 16:53:43 2024

@author: mgdin
"""
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import json
import xlwings as xw

from trg_config import config
from utils import xl_utils as xl
from security import security_info
from api import client, client_xl, request_handler, data_pack

TEST_DATA_DIR = config['SRC_DIR'] / 'test_data'
username = 'test123'

def test_portfolio_name():
    return TEST_DATA_DIR / 'Demo.Model_1.json'

def create_test_portfolio():
    wb = xw.Book('TRG Risk Assessment.xlsm')
    filename = test_portfolio_name()
    client_xl.save_portfolio_to_json(wb, filename)

def test_portfolio():
    filename = test_portfolio_name()
    payload = client_xl.read_portfolio_from_json(filename)
    return payload

def save_results_CalculateVaR(response):
    filename = TEST_DATA_DIR / 'Demo.Model_1.response.json'
    with open(filename, 'w') as f:
        json.dump(response, f, indent=4)    
    print('saved to:', filename)
    
def create_results_CalculateVaR():
    payload = test_portfolio()
    response, status = request_handler.CalculateVaR(payload)
    save_results_CalculateVaR(response)
        
def test_results_CalculateVaR():
    filename = TEST_DATA_DIR / 'Demo.Model_1.response.json'
    with open(filename, 'r') as f:
        response = json.load(f)    
    return response
    
def test_CalculateVaR():
    wb = xw.Book('test2.xlsx')
    input_data = client_xl.gen_input_data(wb)

    #input_data = test_portfolio()
    
    try:
        response, status = request_handler.CalculateVaR(input_data)
        print(f'status: {status}')
    except Exception as e:
        print(e)
        
    # expected = test_results_CalculateVaR()
    # if json.dumps(response) == json.dumps(expected):
    #     print('\ntest_CalculateVaR: OK')
    # else:
    #     print('\ntest_CalculateVaR: Failed')
    

def test_MarketData():
    
    route = 'data_request'
    
    # 'GetHistory', 'GetSecurityList'
    request_type = 'GetHistory'  
    
    
    input_data ={
        "Client ID": '123',
        "Request":   'MarketData',
        "Type":      request_type,
        "Data Category":  'PRICE',
        "From Date":    '2018-01-01',
        "To Date":      '2023-12-31',
        "SecurityID": ['T10000022', 'T10000026']
        }

    response, status = request_handler.get_response(route, username, input_data)
    
    if status == 200:
        print('Success!')
        df = data_pack.extract_df(response, 'DATA')
        df.head()
    else:
        print(status)
        print(response)


def test_AddSecurities():
    df = security_info.get_securities_with_xref(['T10000014'])
    new_securities = df.drop(columns=['SecurityID','DateAdded'])
    
    wb = xw.Book('Book4')
    new_securities = xl.read_df_from_excel(wb, 'Security')

    
    input_data ={
        "ClientID": '123',
        "Request":   'AddSecurities',
        "NewSecurity": new_securities.to_csv(index=False)
        }

    response, status = request_handler.AddSecurities(input_data)
    
    new_sec  = data_pack.extract_df(response, 'NewSecurity')
    new_xref = data_pack.extract_df(response, 'NewXref')
    existing = data_pack.extract_df(response, 'ExistingSecurity')

    xl.add_df_to_excel(new_sec, wb, 'new_sec', index=False)
    xl.add_df_to_excel(new_xref, wb, 'new_xref', index=False)
    xl.add_df_to_excel(existing, wb, 'existing', index=False)

def test_GetSecurities():
    sec_id_list = [['ISIN', 'LU0119620176'], ['CUSIP','74340XBN0'],['Ticker','GOOG'], ['TRG_ID', 'T10000425']]
    input_data ={
        "ClientID": '123',
        "Request":   'GetSecurity',
        "SecurityIdList": sec_id_list
        }

    response, status = request_handler.GetSecurities(input_data)
    if status == 200:
        print(response)

    wb = xw.Book('Book1')
    df = data_pack.extract_df(response, 'Security')
    xl.add_df_to_excel(df, wb, 'securities', index=False)
    

def test_api():
   
    payload = test_portfolio()
    response = client.api_request('calculate', payload)
