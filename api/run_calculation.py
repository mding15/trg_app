# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 13:27:46 2024

@author: mgdin
"""

import pandas as pd
from api import portfolios
from engine import VaR_engine
from preprocess import portfolio_utils
from report import powerbi as pbi
from database import model_aux as db
from database import sync_report_mapping
    
def run_calculation(port_id, username, insert_msss=True):
    try:
        # port_id = 2641

        # read input file
        params, positions, unknown_positions = portfolios.load_portfolio_by_port_id(port_id)
        
        DATA = VaR_engine.calc_VaR(positions, params)
        if 'Error' in DATA:
            raise Exception(DATA['Error'])

        # generate PBI Report
        DATA['port_id'] = port_id
        results = pbi.generate_report(DATA)
        
        # write results to the input file
        file_path = portfolio_utils.get_folder_by_port_id(port_id) / f'{port_id}.pbi.xlsx'
        pbi.write_results_xl(results, file_path)
        
        # write results to database
        if insert_msss:
            # report_description = portfolio_description(params)
            report_description = params['PortfolioName']
            pbi.insert_results_to_db(results, port_id, username, report_description)
        
        # update status
        if len(unknown_positions):
            status = 'partial'
        else:
            status = 'success'
        db.update_portfolio_status(port_id, status=status, report_id=port_id)    
        sync_report_mapping.sync_delta()
        
    except Exception as e:
        # update status
        db.update_portfolio_status(port_id, status='pending', message=str(e))    
        raise Exception(str(e))

    
def portfolio_description(params):

    pname = params['PortfolioName']
    as_of_date = pd.to_datetime(params['AsofDate']).strftime('%Y-%m-%d')
    risk_horizon = params['RiskHorizon']
    tail_measure = params['TailMeasure']
    benchmark = params['Benchmark']
    description = f'{pname}: {as_of_date}, {risk_horizon}, {tail_measure}, {benchmark}'
    return description

def test():
    username = 'test1@trg.com'
    port_id = 2545
    
    run_calculation(port_id, username)