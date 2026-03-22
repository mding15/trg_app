# -*- coding: utf-8 -*-
"""
Created on Sun Mar 17 21:45:02 2024

@author: mgdin

"""

import datetime
import xlwings as xw
import pandas as pd
import numpy as np
import os
from pathlib import Path
from io import StringIO

import requests

host = 'https://engine.tailriskglobal.com'
#host = 'http://localhost:5050'

HOME_DIR=Path(os.environ['USERPROFILE'])

#
# create a token
#
def get_token(username, password):

    login_data = {
        'username': username,
        'password': password,
    }
    
    print(f'\nConnecting host: {host} ......')
    response = requests.post(f'{host}/api/login', json=login_data)
    if response.status_code == 200:
        token = response.json()['token']
        print(token)
    elif response.status_code == 401:
        remove_credential()
        raise Exception(f'Login failed {response.status_code}')
    else:
        msg = response.json()['message']
        raise Exception(f'Login failed {response.status_code}: {msg}')
        
    return token


def credential_file():
    filename = HOME_DIR / '.trg' / 'crd.txt'
    return filename
def remove_credential():
    filename =credential_file()
    if filename.exists():
        filename.unlink()

def get_credential():
    filename =credential_file()
    if not filename.exists():
        return None
    else:
        cred = []
        with open(filename, 'r') as f:
            for line in f:
                cred.append(line.strip())
    return cred[:2]
        
def api_calculate(payload, username, password):
    token = get_token(username, password)
    params = {
        'token': token
    }
    
    response = requests.post(f'{host}/api/calculate', params=params, json=payload)
    
    if response.status_code == 200: # success
        data = response.json()
        return data
    else:
        data = response.json()
        if 'Error' in data:
            msg = data['Error']
        raise Exception(f'Request failed {response.status_code}: {msg}')
    


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
    
    def cusip_float(x):
        x = int(x)
        return f'{x: 09}'
    
    # remove ' from the front of CUSIP, and convert float to string
    if 'CUSIP' in positions:
        positions['CUSIP'] = positions['CUSIP'].apply(lambda x: x[1:] if isinstance(x, str) and x[0]=="'" else x)
        positions['CUSIP'] = positions['CUSIP'].apply(lambda x: cusip_float(x) if (isinstance(x, float) and (x is not np.nan)) else x)
        

    return positions.to_csv(index=False)

def gen_input_data(wb):
    global host
    
    params = read_params(wb)
    
    # set host server 
    if 'Host' in params:
        host = params['Host'] 
    else:
        host = 'https://engine.tailriskglobal.com'
        
    
    data = {'Request': 'CalculateVaR'}
    data['ClientID'] = params['ClientID']
    data['PortfolioID'] = params['PortfolioID']
    
    data['Parameters'] = params
    data['Positions'] = read_positions(wb)
    return data


def extract_df(input_data, name):
    csv_data = input_data[name]
    df = pd.read_csv(StringIO(csv_data))  
    return df

def extract_book(book_raw):
        book = {}
        for tab in book_raw:
            book[tab] = extract_df(book_raw, tab)
        return book
    
def write_book(book, name, output_folder):
    outfile = output_folder / f'{name}.xlsx'
    
    with pd.ExcelWriter(outfile) as writer:
        for tab in book:
            book[tab].to_excel(writer, sheet_name=tab, index=False)
    print('saved file:', outfile)

def extract_error_positions(response):
    pbi_data = response['PBI_DATA']
    for name in pbi_data:
        if name == 'Error_Positions':
            book = extract_book(pbi_data[name])
    return book['Positions']

def write_pbi(response, out_folder):
    pbi_data = response['PBI_DATA']
    for name in pbi_data:
        print(name)
        book = extract_book(pbi_data[name])
        write_book(book, name, out_folder)
    
def get_pbi_folder(portfolio_id):
    
    # trg folder
    out_folder = HOME_DIR / 'TailRiskGlobal' / portfolio_id

    # make sure the folder exists    
    out_folder.mkdir(parents=True, exist_ok=True)

    return out_folder

def excel_calculator(wb):
    #wb = xw.Book('VaRCalculator.xlsm')
    
    # clear message
    wb.sheets['Run'].range('MSG_RUN').value = 'Connecting TRG ...'
    wb.sheets['Run'].range('MSG_RUN').offset(1,0).clear_contents()

    cred = get_credential()
    if cred is None:
        wb.sheets['Run'].range('MSG_RUN').value = 'Login ...'
        return
    else:
        username, password = cred
    
    # prepare for the input data
    payload = gen_input_data(wb)
    
    # call api 
    try:
        response = api_calculate(payload, username, password)

        # unpacking the results
        wb.sheets['Run'].range('MSG_RUN').value = 'Unpacking results ...'
        portfolio_id = response['PortfolioID']
        out_folder = get_pbi_folder(portfolio_id) 
        write_pbi(response, out_folder)
        # error = extract_error_positions(response)
        # add_df_to_excel(error, wb, 'Error', index=False)
        
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        wb.sheets['Run'].range('MSG_RUN').value = f'{timestamp}: The calculation has been successfully completed.'
        wb.sheets['Run'].range('MSG_RUN').offset(1,0).value = 'The results are saved to'
        wb.sheets['Run'].range('MSG_RUN').offset(2,0).value = f'{out_folder}'
    except Exception as e:
        wb.sheets['Run'].range('MSG_RUN').value = str(e)
    
    
    
def CalculateVaR():

    wb = xw.Book.caller()

    excel_calculator(wb)


