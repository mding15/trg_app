# -*- coding: utf-8 -*-
"""
Created on Fri May 31 15:50:33 2024

@author: mgdin
"""

import json
import logging
import datetime
import uuid
import traceback
import pandas as pd

from trg_config import config
from security import security_info
from api import data_pack
from engine import VaR_engine as engine
from report import powerbi as pbi
from report import fintree
from utils import tools, data_utils

from api import api_status_codes as api_code
from api import request_handler as rh
from api.logging_config import get_logger

# logger
logger = get_logger(__name__)
# ts = tools.file_ts()
# logfile = config['LOG_DIR']  / f"api_ft.{ts}.log"
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)
# formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
# file_handler = logging.FileHandler(logfile)
# file_handler.setFormatter(formatter)
# logger.addHandler(file_handler)


# main function, dispatch requests to various functions
def get_response(route, username, input_data):
    print('get_response...')
    print(f'route: {route}, username:{username}')
    logger.info('===== New Request =====')
    logger.info(f'route: {route}')
    logger.info(f'username: {username}')
    
    data_str = json.dumps(input_data, indent=4)
    logger.info(data_str)

    try:
        if route == 'test':
            response, status = api_test(username, input_data)
        elif route == 'risk_calculator':
            response, status = risk_calculator(username, input_data)

        else:
            response, status = rh.Fatal_Error(username, input_data)
            
    except rh.HandlerException as e:
        print(e)
        response, status = e.response()

    logger.info('===== Response =====')
    logger.info(f'Status: {status}')
    logger.info(json.dumps(response, indent=4))
        
    return response, status

################################################################################################
def risk_calculator(username, input_data):

    # generate params, positions for the VaR engine
    params, positions = generate_engine_inputs(input_data)
    
    # save input_data
    portfolio_id = params['PortfolioID']
    portfolio_dir = rh.get_portfolio_dir(username, portfolio_id)    
    ts = tools.file_ts()
    rh.save_input_data(portfolio_dir, input_data, ts)
    
    # run engine
    try:
        DATA = engine.calc_VaR(positions, params)
        response, code = ft_riskcalc_response(DATA)
        
    except Exception as e:
        response['Error'] = str(e)
        print('VaR Engine Error:', str(e))
        traceback.print_exc()
        code = api_code.ERROR_CALCULATION_FAILED
    
    
    rh.save_response_data(portfolio_dir, response, code, ts)
    return response, code

################################################################################################
INPUT_DATA = {
    "Request": "RiskCalculator",
    "Client ID": "C12345",
    "Portfolio Name": "Growth Model",
    "Portfolio ID": "Model_1",
    "Report Date": "2024-03-30",
    "Risk Horizon": "Month",
    "Confidence Level": 0.95,
    "Benchmark": "BM_20_80",
    "Benchmark Expected Return": 0.048,
    "Benchmark Name": "Benchmark 20% Equity, 80% Bond",
    "Positions": "Security Name,Nemo,ISIN,Market Value,Last Price,Last Price Date,Asset Currency\niShares Core MSCI Pacific ETF,IPAC.P,US46434V6965,100000.0,71.277892,2024-05-31,USD\niShares Asia 50 ETF,AIA.O,US4642884302,100000.0,68.7291209999999,2024-05-31,USD\nCash,USD.CCY,,100000.0,1.0,2024-05-31,USD\n"
    }

def generate_engine_inputs(input_data):
    # extract params, positions
    params, positions = extract_params_positions(input_data)

    # process params, positions
    params = process_params(params)
    positions = process_positions(positions)

    return params, positions    
    
    

def extract_params_positions(input_data):
    diff = set(INPUT_DATA.keys()).difference(input_data.keys())
    if len(diff) > 0:
        diff_str = str(diff)
        raise rh.HandlerException(f'missing parameters: {diff_str}', api_code.ERROR_INPUT_DATA)
    
    params = tools.dict_ex_subset(input_data, ['Positions'])
    positions = data_pack.extract_df(input_data, 'Positions')

    return params, positions    

def process_params(params):
    # remove space from keys
    params = dict([(k.replace(' ', ''), v) for k, v in params.items()])
    
    # convert report date to datetime
    params['AsofDate'] = pd.to_datetime(params['AsofDate'])
    
    return params
    
POSITION_COLUMNS = ['Security Name', 'Nemo', 'ISIN', 'Market Value', 'Last Price', 'Last Price Date', 'Asset Currency']
def process_positions(positions):
    # check missing columns    
    missing = set(POSITION_COLUMNS).difference(positions)
    if len(missing) > 0:
        miss_str = ', '.join(list(missing))
        raise rh.HandlerException(f'Poisition missing columns: {miss_str}', api_code.ERROR_INPUT_DATA)
    
    # remove spce from column names
    positions.columns = [x.replace(' ', '') for x in positions.columns]

    # rename columns
    positions.rename(columns={'Nemo': 'Ticker', 'AssetCurrency': 'Currency'}, inplace=True)
    
    # SecurityID
    positions['SecurityID'] = security_info.get_SecurityID_by_ref(positions)
    
    # ExpectedReturn
    df = data_utils.load_stat('ExpectedReturn')
    positions['ExpectedReturn'] = tools.df_series_merge(positions, df['ExpectedReturn'], key='SecurityID')        

    # asset attributes
    df = data_utils.load_stat('FintreeAssets')    
    positions['Class'] = tools.df_series_merge(positions, df['Asset Class'], key='SecurityID')
    positions['SC1'] = tools.df_series_merge(positions, df['Asset Sub Class'], key='SecurityID')
    positions['SC2'] = tools.df_series_merge(positions, df['Nemo'], key='SecurityID')
    positions['Country'] = tools.df_series_merge(positions, df['Country'], key='SecurityID')
    positions['Region'] = tools.df_series_merge(positions, df['Region'], key='SecurityID')
    positions['Sector'] = "Unknown"
    positions['Industry'] = "Unknown"

    # xl_utils.add_df_to_excel(positions, wb, 'test', index=False)
    return positions

RISKCALC_RESPONSE = {
    "Request": "RiskCalculator",
    "Request ID": "8015f938-e",
    "Status": "Success",
    "Client ID": "C12345",
    "Portfolio ID": "Model_1",
    "Allocation": "Class,Allocation,Risk Contribution",
    "Portfolio Risk": "Name,Volatility,VaR,Sharpe Ratio - Vol,Sharpe Ratio - VaR",
    "Region Risk": "Region,Allocation,Risk Contribution",
}

def ft_riskcalc_response(DATA):
    
    params = DATA['Parameters']
    
    # pbi data
    results = fintree.generate_report(DATA)        

    # extract ft results
    # portfolio_risk = results['Fact_AggTable_Master']['Fact_Agg Table']
    # portfolio_risk = portfolio_risk[['Type', 'Vol_P', 'VaR_P', 'SR Vol', 'SR VaR']]
    # portfolio_risk = portfolio_risk.rename(columns={'Type': 'Name', 
    #                                 'Vol_P': 'Volatility', 'VaR_P' : 'VaR', 
    #                                 'SR Vol': 'Sharpe Ratio - Vol',
    #                                 'SR VaR': 'Sharpe Ratio - VaR'})

    # posVaR = DATA['Positions']
    # allocation = posVaR.groupby(by=['Class']).sum()[['Weight', 'mgtVaR%']]
    # allocation.columns = ['Allocation', 'VaR']
    # allocation = allocation.reset_index()    
    
    # response
    response = {}
    response['Request'] = params['Request']
    response['Request ID'] = str(uuid.uuid4())[:10]
    response['Client ID'] = params['ClientID']
    response['Portfolio ID'] = params['PortfolioID']
    response['Portfolio Risk'] = results['Portfolio Risk'].to_csv(index=False)
    response['Allocation'] = results['Allocation'].to_csv(index=False)
    response['Region Risk'] = results['Region Risk'].to_csv(index=False)
    
    response['Status'] = 'Success'
    code = 200

    return response, code

##############################################################################    
def api_test(username, input_data):
    return RISKCALC_RESPONSE, 200
    

##############################################################################
# test
import xlwings as xw
from utils import xl_utils

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

def generate_input(wb):
    data = read_params(wb)
    data['Positions'] = read_positions(wb)
    return data

def test():

    wb = xw.Book('Fintree Prototype.xlsx')
    input_data = generate_input(wb)

    username = 'ftest555'
    response, code = get_response('risk_calculator', username, input_data)

    if code != 200:
        print(response['Error'])
        
    for name in ['Portfolio Risk', 'Allocation']:
        print(name)
        df = data_pack.extract_df(response, name)
        xl_utils.add_df_to_excel(df, wb, tab=name, index=False)

    
    
    