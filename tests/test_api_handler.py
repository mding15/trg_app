# -*- coding: utf-8 -*-
"""
Created on Wed May 29 12:42:59 2024

@author: mgdin
"""
import os
import io
from pathlib import Path
import xlwings as xw
from flask import request


from api import app
from trg_config import config
from database.models import User as User
from database import model_aux as db
from report import powerbi as pbi

from api import client_xl, data_pack, request_handler, request_handler_ft
from utils import test_utils, xl_utils, tools
from database import pbi_db_insert as pbi_db
from api import portfolios 
from api import scrubbing_portfolio
from api import run_calculation as run_calc
from preprocess import read_portfolio, portfolio_utils
from engine import VaR_engine

username=os.environ['test_username']
TEST_PORT_ID = 100
    
def test_handle_upload_portfolio():
    upload_file_path = config['DATA_DIR'] / 'test' / 'portfolios' / 'UnHedge_Portfolio.xlsx'

    with app.test_request_context('/upload', method='POST', 
                                  data={'file': (xl_ioBytes(upload_file_path), upload_file_path.name)},
                                  content_type='multipart/form-data'):
        portfolios.handle_upload_portfolio(request, username)
        
    
    
def test_handle_upload_portfolio_by_steps():
    
    infile_path = config['DATA_DIR'] / 'test' / 'portfolios' / 'Model2.xlsx'
    
    # database objects    
    user = User.query.filter_by(username=username).first()
    client = user.client
    pgroup = db.get_port_group(user)

    # save file    
    file_path, port_name = test_save_portfolio_file(infile_path, client, pgroup)
    
    # add to database
    # port_id = portfolios.db_add_portfolio(user, client, pgroup, file_path, port_name)
    
    # scrub data
    #scrubbing_portfolio.scrubbing_portfolio(port_id)        
    params, positions = test_scrubbing_portfolio_without_db(file_path)
    params, positions = load_portfolio_by_port_id(file_path)
    
    
    # calculate VaR
    # run_calc.run_calculation(port_id, username)
    run_calculation_without_db(params, positions)


def test_save_portfolio_file(file_path, client, pgroup):
    filename = file_path.name
    with app.test_request_context('/upload', method='POST', 
                                  data={'file': (xl_ioBytes(file_path), filename)},
                                  content_type='multipart/form-data'):
        file_path, port_name = portfolios.save_portfolio_file(request, client, pgroup)
    
    return file_path, port_name

def test_scrubbing_portfolio_without_db(file_path):
    # file_path = config['CLIENT_DIR'] / '1' / '3' / 'Model1.20250120.162530.xlsx'
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

def run_calculation_without_db(params, positions):
    
    DATA = VaR_engine.calc_VaR(positions, params)
    if 'Error' in DATA:
        raise Exception(DATA['Error'])

    # generate PBI Report
    results = pbi.generate_report(DATA)
    
    # write results to the input file
    port_id = TEST_PORT_ID
    file_path = portfolio_utils.get_folder_by_port_id(port_id) / f'{port_id}.pbi.xlsx'
    pbi.write_results_xl(results, file_path)



def test_api_handler():
    wb = test_utils.template_test_portfolio()
    wb = xw.Book('VaRCalculator.xlsm')
    

    input_data  = client_xl.gen_input_data(wb)
    route = 'calculate'
    response, status = request_handler.get_response(route, username, input_data)
    
    data_pack.write_pbi(response)

    
    # insert the report into database
    creator = 'Prueba'
    report_description = "Prueba Reports"
    reports = data_pack.extract_pbi_report(response)
    pbi_db.insert_report_to_db(reports, creator, report_description)
    
    # insert static        
    static_reports = {}
    static_reports['DimClasses'] = reports['DimClasses']
    pbi_db.insert_static_to_db(static_reports)
    
def test_api_handler_ft():
    username = os.environ['ft_username']
    wb = xw.Book('Fintree Prototype.xlsx')

    input_data  = request_handler_ft.generate_input(wb)
    route = 'risk_calculator'
    response, status = request_handler_ft.get_response(route, username, input_data)

    for name in ['Portfolio Risk', 'Allocation']:
        print(name)
        df = data_pack.extract_df(response, name)
        xl_utils.add_df_to_excel(df, wb, tab=name, index=False)
        
def write_error(error, file_path=None):
    if not file_path:
        file_path = tools.gen_file_name_in_downloads('test_error.csv')
    
    with open(file_path, 'w', newline='') as f:
        f.write(str(error))
    print(f'write errors to file: {str(file_path)}')
    
def save_portfolio_by_port_id(file_path, params, positions):
    folder = file_path.parent
    port_id = TEST_PORT_ID
    tools.save_parameter_csv(params,    folder / f'{port_id}.params.csv')
    tools.save_positions_csv(positions, folder / f'{port_id}.positions.csv')

def load_portfolio_by_port_id(file_path):
    folder = file_path.parent
    port_id = TEST_PORT_ID
    params = tools.read_parameter_csv(folder / f'{port_id}.params.csv')
    positions = tools.read_positions_csv(folder / f'{port_id}.positions.csv')
        
    return params, positions
####

# In[1] auxilary for testing upload file 
   
# read xl into Bytes IO stream
def xl_ioBytes(filename):
    with open(filename, "rb") as f:
        excel_data = f.read()

    return io.BytesIO(excel_data)
    
# save file in the request to /downloads folder    
def save_upload_file(req):
    if 'file' not in req.files:
        raise Exception('No file part')
    
    file = req.files['file']
    if file.filename == '':
        raise Exception('No selected file')
    if file:
        file_path = tools.gen_file_name_in_downloads('download_test.xlsx')
        file.save(file_path)
