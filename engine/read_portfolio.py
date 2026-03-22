# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 20:20:13 2024

@author: mgdin
"""

import pandas as pd
from utils import xl_utils, tools
from trg_config import config


def read_input_xl(wb):
    # wb = xw.Book('Hedged Portfolio.xlsx')
    params = xl_utils.read_df_from_excel(wb, 'Parameters')
    positions = tools.read_positions(wb)
    
    return process_input(params, positions)

# read params and positions from file
def read_input_file(file_path):
    params = pd.read_excel(file_path, sheet_name='Parameters')
    positions = pd.read_excel(file_path, sheet_name='Positions')

    
    return process_input(params, positions)
    
def process_input(params, positions):

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

    # remove blank row    
    blank_rows = positions['SecurityName'].isna() & positions['ISIN'].isna() & positions['CUSIP'].isna() & positions['Ticker'].isna()
    positions = positions[~blank_rows]
    
    return params, positions

def params_to_df(params):
    df = pd.DataFrame({'Parameters': list(params.keys()),
                       'Values': list(params.values())})
    
    return df    
    	

def get_template():
    file_path = config['PUBLIC_DIR'] / 'portfolio_template.xlsx'
    if file_path.exists() == False:
        raise Exception('missing file: ' + str(file_path))

    params = pd.read_excel(file_path, sheet_name='Parameters')
    positions = pd.read_excel(file_path, sheet_name='Positions')

    # remove NA
    params = params[~params['Parameters'].isna()]    
    
    # remove space
    requried_params =  set([x.replace(' ', '') for x in params['Parameters']])
    requried_positions = set([x.replace(' ', '') for x in positions.columns])    

    return requried_params, requried_positions    
    
    
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
        raise Exception(f'Missing Position columns: {list(missing)}')