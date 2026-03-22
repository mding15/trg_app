# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 20:20:13 2024

@author: mgdin
"""

import pandas as pd
from utils import xl_utils, tools
from trg_config import config
from engine import validate_positions
import os

POSITIONS_TAB = '1. Positions'
PARAMEMTERS_TAB = '3. Parameters'
LIMIT_TAB = '4. Limit'


def read_input_xl(wb):
    # wb = xw.Book('Hedged Portfolio.xlsx')
    params = xl_utils.read_df_from_excel(wb, 'Parameters')
    positions = tools.read_positions(wb)
    
    return process_input(params, positions)

# read params and positions from file
def read_input_file(file_path):
    #check file size, if the size is over, then throw a exception
    #size limit 100KB
    file_size = os.path.getsize(file_path)
    max_size = 100 * 1024  # 100KB in bytes
    if file_size > max_size:
        raise Exception("File size exceeds the maximum allowed size of 100KB")

    print('start to read:')
    check_sheet(file_path, sheet_name=PARAMEMTERS_TAB)
    check_sheet(file_path, sheet_name=POSITIONS_TAB)
    check_sheet(file_path, sheet_name=LIMIT_TAB)
    
    params = pd.read_excel(file_path, sheet_name=PARAMEMTERS_TAB)
    
    positions = pd.read_excel(file_path, sheet_name=POSITIONS_TAB, dtype={'ISIN': str, 'CUSIP': str, 'Ticker': str, 'Cusip': str} )
    positions = positions.where(pd.notnull(positions), None)
    
    limit = pd.read_excel(file_path, sheet_name=LIMIT_TAB)
    
    return process_input(params, positions, limit)

def test_read_input_file():
    file_path = config['TEST_DIR'] / 'clients' / 'Test1.xlsx'
    read_input_file(file_path)
    
def df_max_length(df_series, max_len):
    return df_series.apply(lambda x: str(x) if not pd.isna(x) else None).str.slice(0, max_len)

def process_input(params, positions, limit):

    # Remove space in column names
    positions.columns = [x.replace(' ', '') for x in positions.columns]

    # only two columns
    params = params.iloc[:, :2]
    # drop blank value in the first column 
    params = params[~params.iloc[:,0].isna()].copy()   
    
    # convert params to dict
    params.index = params.iloc[:,0].apply(lambda x: x.replace(' ', ''))
    params = params.iloc[:,1].to_dict()
    
    # check input file against template
    check_against_template(params, positions)

    # rename columns
    positions = positions.rename(columns={'AssetClass': 'userAssetClass', 'Currency': 'userCurrency', 'Cusip': 'CUSIP'})

    if 'CUSIP' in positions:
        positions['CUSIP'] = positions['CUSIP'].apply(lambda x: x[-9:] if isinstance(x, str) else x) 

    # remove blank row    
    blank_rows = positions['SecurityName'].isna() & positions['ISIN'].isna() & positions['CUSIP'].isna() & positions['Ticker'].isna()
    positions = positions[~blank_rows]
    
    # max length
    positions['SecurityName'] = df_max_length(positions['SecurityName'], 100)
    positions['ID'] = df_max_length(positions['ID'], 50)
    positions['ISIN'] = df_max_length(positions['ISIN'], 20)
    positions['CUSIP'] = df_max_length(positions['CUSIP'], 20)
    positions['Ticker'] = df_max_length(positions['Ticker'], 20)
    positions['userAssetClass'] = df_max_length(positions['userAssetClass'], 50)
    positions['userCurrency'] = df_max_length(positions['userCurrency'], 20)

    # check errors
    error_message = []

    # parameters
    params, errors = validate_positions.check_parameters(params)
    if errors:
        error_message.append('\nThe Parameter tab has the following errors:')
        error_message.extend(errors)
        
    # positions
    positions, errors = validate_positions.check_positions(positions)
    if errors:
        error_message.append('\nThe Position has the following errors:')
        error_message.extend(errors)
        
    if error_message:
        error_message.insert(0, 'Your input file has errors!')
        raise Exception('\n'.join(error_message))

    # Process limit data
    limit.columns = [x.replace(' ', '') for x in limit.columns]  # Clean column names
    limit = limit.iloc[:, :2]
    limit = limit[~limit.iloc[:,0].isna()].copy()
    # Keep original limit category names with spaces for display
    limit.index = limit.iloc[:,0]  # Don't remove spaces from limit categories
    limit = limit.iloc[:,1].to_dict()

    return params, positions, limit

def params_to_df(params):
    df = pd.DataFrame({'Parameters': list(params.keys()),
                       'Values': list(params.values())})
    
    return df    
    	

def get_template():
    file_path = config['PUBLIC_DIR'] / 'input_template.xlsx'
    if file_path.exists() == False:
        raise Exception('missing file: ' + str(file_path))

    params = pd.read_excel(file_path, sheet_name=PARAMEMTERS_TAB)
    positions = pd.read_excel(file_path, sheet_name=POSITIONS_TAB)

    # remove NA
    params = params[~params['Parameters'].isna()]    
    
    # remove space
    requried_params =  set([x.replace(' ', '') for x in params['Parameters']])
    requried_positions = set([x.replace(' ', '') for x in positions.columns])    

    return requried_params, requried_positions    
    
def check_sheet(file_path, sheet_name):
    print(f'{file_path}')
    print(f'{sheet_name}')
    if xl_utils.has_sheet(file_path, sheet_name=sheet_name) == False:
        raise Exception(f"Input file does not have sheet name: {sheet_name}")
    
# check params and positions, make sure they match template
def check_against_template(params, positions):

    # get template required fields
    requried_params, requried_positions = get_template()   

    # check parameters
    missing = set(requried_params).difference(params.keys())
    if missing:
        print(f'Missing Parameters:\n{",".join(missing)}')
        raise Exception(f'Missing Parameters:\n{",".join(missing)}')

    # check positions
    missing = set(requried_positions).difference(positions.columns)
    if missing:
        print(f'Missing Position columns: {list(missing)}')
        raise Exception(f'Missing Position columns:\n{list(missing)}')