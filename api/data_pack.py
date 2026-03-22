# -*- coding: utf-8 -*-
"""
Created on Mon Mar 18 12:22:58 2024

@author: mgdin
"""
import json
import pandas as pd
from io import StringIO

from trg_config import config
from utils import xl_utils, tools



def pack_book(book):
    book_pk = {}
    for tab in book:
        book_pk[tab] = book[tab].to_csv(index=False)
        
    return book_pk

def pack_pbi(results):
    package = {}
    for name in results:
        package[name]=pack_book(results[name])
    return package
        
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
    return book

def write_pbi(response, out_folder=None):
    if out_folder is None:
        out_folder = get_pbi_folder(response)
        
    report = extract_pbi_report(response)
    for name, book in report.items():
        write_book(book, name, out_folder)
    
def extract_pbi_report(response):
    pbi_data = response['PBI_DATA']
    
    report = {}
    for name in pbi_data:
        print(name)
        book = extract_book(pbi_data[name])
        report[name] = book
        
    return report
    
def get_pbi_folder(response):
    client_id = response['ClientID']
    portfolio_id = response['PortfolioID']
    
    out_folder = config['DATA_DIR'] / 'powerbi' / client_id / portfolio_id
    out_folder.mkdir(parents=True, exist_ok=True)
    return out_folder

def write_to_xl(response, wb):
    pbi_data = response['PBI_DATA']
    for name in pbi_data:
        print(name)
        book = extract_book(pbi_data[name])
        for tab in book:
            tab_name = f'{name}!{tab}'
            xl_utils.add_df_to_excel(book[tab], wb, tab_name[:31], index=False)

def save_input_data(input_data):
    client_id = input_data['ClientID']
    portfolio_id = input_data['PortfolioID']

    ts = tools.file_ts()
    filename = config['PORT_DIR'] / f'{client_id}.{portfolio_id}.{ts}.json'
    with open(filename, 'w') as f:
        json.dump(input_data, f)

# filename = config['SRC_DIR'] / 'test_data' / 'Demo.Model_1.json'    
def read_input_data(filename):
    with open(filename, 'r') as f:
        data = json.load(f)
    return data

#
# request = 'CalculateVaR'
#
def pack_payload(request, positions, params):
    
    data = {'Request': request}

    data['ClientID'] = params['ClientID']
    data['PortfolioID'] = params['PortfolioID']
    
    data['Parameters'] = tools.convert_to_json(params)
    data['Positions']  = positions.to_csv(index=False)
    return data


# filename = config['PORT_DIR'] / 'Demo.Model_1.json'
# filename = config['PORT_DIR'] / f'{client_id}.{portfolio_id}.json'
def unpack_payload(filename):
    data = read_input_data(filename)
        
    params = data['Paramaters']
    positions = extract_df(data, 'Positions')
    return positions, params
        
       
