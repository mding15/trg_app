# -*- coding: utf-8 -*-
"""
Created on Tue Jul 23 11:29:16 2024

@author: mgdin
"""
import os
import datetime
import json
import pandas as pd
import xlwings as xw
from pathlib import Path

from trg_config import config
from api import data_pack, request_handler_ft, client
from security import security_info
from engine import VaR_engine as engine
from report import powerbi as pbi
from utils import xl_utils, data_utils, tools


TEST_DATA_DIR = Path(r'C:\Users\mgdin\dev\TRG_App\test\test_portfolios')

def test():

    wb = xw.Book(TEST_DATA_DIR / 'Fintree Prototype.xlsx')
    
def test_api(wb):
    # generate input data
    input_data = gen_input_data(wb)
    
    # get token
    username = os.environ['ft_username']
    password = os.environ['ft_password']
    token = client.get_token(username, password)

    # call api
    response = client.api_request(token, 'risk_calculator', input_data)

    wb = xw.Book()
    for name in ['Portfolio Risk', 'Allocation', 'Region Risk']:
        print(name)
        df = data_pack.extract_df(response, name)
        xl_utils.add_df_to_excel(df, wb, tab=name, index=False)


    print(json.dumps(response, indent=4))
    print(json.dumps(input_data, indent=4))

    
def test_var(wb):
    
    # generate input data
    input_data = gen_input_data(wb)
    
    # generate params, positions for the VaR engine
    params, positions = request_handler_ft.generate_engine_inputs(input_data)

    # run engine
    try:
        DATA = engine.calc_VaR(positions, params)
        
        response, code = request_handler_ft.ft_riskcalc_response(DATA)
        
    except Exception as e:
        print('VaR Engine Error:', str(e))

    
    # write engine results
    # engine.write_to_excel(wb, DATA)

    # add posVaR to the results
    # wb = xw.Book()
    # write_results_xl(results, wb)
    
    df = tools.extract_df(response, 'Region Risk')
    web1 = xw.Book('Book2')
    xl_utils.add_df_to_excel(df, web1, 'Region')    
#########################################################################################    
def read_params(wb):
    params = xl_utils.read_df_from_excel(wb, 'Parameters', index=True)
    params = params['Value'].to_dict()
    rpt_date = params['Report Date'] 
    if isinstance(rpt_date, datetime.datetime):
        params['Report Date'] = rpt_date.strftime('%Y-%m-%d')
        
    if 'Positions' in params:
        del params['Positions']
    
    return params

def read_positions(wb):
    positions = xl_utils.read_df_from_excel(wb, 'Positions')
    return positions.to_csv(index=False)

def gen_input_data(wb):
    data = read_params(wb)
    data['Positions'] = read_positions(wb)
    return data
    
# write results to xl
def write_results_xl(results, wb):
    #wb = xw.Book()
    for name in results:
        book = results[name]
        for tab in book:
            xl_utils.add_df_to_excel(book[tab], wb, tab, index=False)    
            
        