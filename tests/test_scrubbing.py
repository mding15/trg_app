# -*- coding: utf-8 -*-
"""
Created on Tue Feb 18 16:07:38 2025

@author: mgdin
"""
import pandas as pd
from trg_config import config
from preprocess import read_portfolio
from api import scrubbing_portfolio
from utils import tools


TEST_PORT_ID = 100

def test_scrubbing_portfolio():
    client_id = '1015'
    group_id = '16'
    file_name = 'PC_MS_Portfoliov2.20250216.213357.xlsx'
    
    file_path = config['CLIENT_DIR'] / client_id / group_id / file_name
    # wb = xw.Book(file_path)
    # wb = xw.Book('Model2.xlsx')
    
    try:
        # read input file
        # params, positions = read_portfolio.read_input_xl(wb)
        params, positions = read_portfolio.read_input_file(file_path)
        
        # check inputs
        params, positions = scrubbing_portfolio.scrub_data(params, positions)
        
        # save params and positions to csv files
        save_portfolio_by_port_id(file_path, params, positions)
        
    except Exception as e:
        error_file = scrubbing_portfolio.get_error_filename(file_path)
        write_error(str(e), error_file)
        
    return params, positions


def save_portfolio_by_port_id(file_path, params, positions):
    folder = file_path.parent
    port_id = TEST_PORT_ID
    tools.save_parameter_csv(params,    folder / f'{port_id}.params.csv')
    tools.save_positions_csv(positions, folder / f'{port_id}.positions.csv')
    
def write_error(error, file_path=None):
    if not file_path:
        file_path = tools.gen_file_name_in_downloads('test_error.csv')
    
    with open(file_path, 'w', newline='') as f:
        f.write(str(error))
        print(f'write errors to file: {str(file_path)}')
#######
def test():
    client_id = '1015'
    group_id = '16'
    file_name = 'PC_MS_Portfoliov2.20250216.213357.xlsx'
    
    file_path = config['CLIENT_DIR'] / client_id / group_id / file_name
