"""
Created on Sun Feb  2 14:08:45 2025

@author: mgdin



1. scp_data.py - copy data from AWS to local

2. test_scrubbing


"""
import os
import pandas as pd
import xlwings as xw
from pathlib import Path
from trg_config import config
from api import app
app.app_context().push()

from api import request_handler
from api import portfolios, scrubbing_portfolio
from api import create_account
from api import run_calculation 
from utils import xl_utils, tools
from utils import var_utils
from engine import VaR_engine
from report import powerbi as pbi
from security import security_info
from preprocess import upload_security, portfolio_utils
from preprocess import scrubbing_portfolio as scrub
from process import process
from database import db_utils, model_aux, ms_sql_server
from database import sync_report_mapping
from database.models import User as User
from mkt_data import mkt_timeseries
from report import preprocess, performance, sharpe_ratio, back_test

username = os.environ['test_username']
def test():
    print('running test ....')
    adhoc_debugt()

def test_main(df):
    wb = xw.Book('sp_hist_price_div.xlsx')
    df = xl_utils.read_df_from_excel(wb, 'hist_dividend', index=True)    
    
    wb = xw.Book('Book1')
    
    xl_utils.add_df_to_excel(df, wb, 'positions', index=False)
    
    test_upload_portfolio()

    test_upload_security(wb)

    test_create_account()

    delete_portfolios()
    
    delete_user()


########################################################################################################
# test upload_portfolio, it trigger data scrubbing and VaR calculation
def test_process_account(account_id, as_of_date):
    account_id = 1004
    as_of_date = '2024-01-31'
    process.process_account(account_id, as_of_date)


def test_upload_portfolio():
    port_file_path = config['DATA_DIR'] / 'test' / 'portfolios' / 'Test1.xlsx'
    port_file_path = Path.home() / 'Downloads' / 'MS_11.10.2025.xlsx'
    
    track='true'
    if track == 'true':
        portfolios.test_upload_portfolio(port_file_path, username, track='true')
    else:
        # save portfolio
        user, file_path, port_name = portfolios.test_save_portfolio_file(port_file_path, username)
        
        # run portfolio
        port = test_process_portfolio(user, file_path, port_name)
        print(port)

def test_process_portfolio(user, file_path, port_name, to_pickle=False):

    # user = User.query.filter_by(username=username).first()
    
    # create a new portfolio in table portfolio_info
    port = process.create_portfolio(user, file_path.name, port_name)

    # read input file
    params, positions, limit = process.read_input_file(port)

    # scrube data        
    params, positions, unknown_positions = scrub.scrubbing_portfolio(params, positions, limit)        
    
    # pickle portfolio
    if to_pickle:
        tools.pickle_port(positions, params, unknown_positions, limit)
        # positions, params, unknown_positions, limit = tools.pickle_load_port()


    # calculate VaR
    process.calc_portfolio(username, params, positions, unknown_positions, limit)

    return port


def test_by_port_id():
    
    port_id = 5338
    positions, params, unknown_positions, limit = portfolios.load_from_db(port_id)

    # calculate VaR
    process.calc_portfolio(username, params, positions, unknown_positions, limit)

##############################################################

def process_calc_portfolio(username, params, positions, unknown_positions):
    port_id = params['port_id']
    DATA = VaR_engine.calc_VaR(positions, params)
    if 'Error' in DATA:
        raise Exception(DATA['Error'])

    # generate PBI Report
    DATA['port_id'] = port_id
    DATA['position_exception'] = unknown_positions
    results = pbi.generate_report(DATA)
    
    # write results to the input file
    file_path = portfolio_utils.get_folder_by_port_id(port_id) / f'{port_id}.pbi.xlsx'
    pbi.write_results_xl(results, file_path)
    
    # write results to database
    report_description = params['PortfolioName']
    pbi.insert_results_to_db(results, port_id, username, report_description)
    
    # update status
    if len(unknown_positions):
        status = 'partial'
    else:
        status = 'success'
    model_aux.update_portfolio_status(port_id, status=status, report_id=port_id)    
    sync_report_mapping.sync_delta()
    

def rerun_portfolio():
    port_id = 5232
    process.rerun_portfolio(port_id)

def adhoc_debugt():
    port_id = 5269
    user, port, file_path = get_port_info(port_id)
    
    # read input file
    params, positions = process.read_input_file(port)

    # scrube data        
    params, positions, unknown_positions = scrub.scrubbing_portfolio(params, positions)        

    # calculate VaR
    # process.calc_portfolio(user.username, params, positions, unknown_positions)

    
    port_id = params['port_id']
    DATA = VaR_engine.calc_VaR(positions, params)
    if 'Error' in DATA:
        raise Exception(DATA['Error'])

    # generate PBI Report
    DATA['port_id'] = port_id
    DATA['position_exception'] = unknown_positions
    results = pbi.generate_report(DATA)

    # write to db
    report_id = port_id
    results = pbi.process_nonetype_dataframes(results)
    results_processed = pbi.process_dataframes_with_report_id(results, report_id)
    
    table_name = 'dm_port_consolidated'
    df = results_processed[table_name]
    outfile = config['TEST_DIR'] / f"{table_name}.csv"
    df.to_csv(outfile)
    
    print(f'saving data to database {table_name} ...')
    ms_sql_server.insert_df(table_name, df)
    print('done!')
    
def delete_portfolios():
    port_id_list = [5021]
    portfolios.delete_portfolios(port_id_list)
    
def read_portfolio():
    port_id = 5105
    port = model_aux.get_portfolio_by_id(port_id)
    params, positions = process.read_input_file(port)
    
    outfile = config['TEST_DIR'] / "test_positions.csv"
    positions.to_csv(outfile)
    
def get_port_info(port_id):
    # port_id = 5104
    user = model_aux.get_user_by_port_id(port_id)
    port = model_aux.get_portfolio_by_id(port_id)
     
    file_path = portfolio_utils.get_folder_by_port_id(port_id) / port.filename   

    print(f'port_id: {port_id}')
    print(f'username: {user.username}')    
    print(f'port_name: {port.port_name}')    
    print(f'file_path: {file_path}')

    return user, port, file_path
    
def test_scrubbing_portfolio():
    port_id =  5063
    scrubbing_portfolio.scrubbing_portfolio(port_id)


    
def test_run_calculation():
    port_id =  5037
    process.run_calculation_by_port_id(port_id)
    
def test_calc_Var(wb):
    port_id =  5020
    params, positions, unknown_positions = process.read_portfolio_from_db(port_id)
    DATA = VaR_engine.calc_VaR(positions, params)
    if 'Error' in DATA:
        print(DATA['Error'])

    write_results(DATA, wb)
    
    
def test_pbi(DATA, port_id):
    
    DATA['port_id'] = port_id
    
    pbi.preprocess(DATA)
    
    # generate PowerBI data
    results = pbi.gen_pbi_data_master(DATA)    

    # write results to the input file
    file_path = portfolio_utils.get_folder_by_port_id(port_id) / f'{port_id}.pbi.xlsx'
    pbi.write_results_xl(results, file_path)
    
    # write results to database
    report_description = 'test'
    report_id = pbi.insert_results_to_db(results, port_id, username, report_description)

    # df = results['dm_fact_d_MgPositions']
    # xl_utils.add_df_to_excel(df, wb, 'mg_pos', index=True)
    
def test_mkt_timeseries():
    port_id = 4785
    # read input file
    params, positions, unknown_positions = portfolios.load_portfolio_by_port_id(port_id)

    # get timeseries for sec_ids
    sec_ids = positions[positions['AssetClass'] != 'Cash']['SecurityID'].unique()
    price_hist = mkt_timeseries.get_hist(sec_ids)
    
def test_run_calculation():

    port_id = 4785

    # read input file
    params, positions, unknown_positions = portfolios.load_portfolio_by_port_id(port_id)
    
    DATA = VaR_engine.calc_VaR(positions, params)
    if 'Error' in DATA:
        raise Exception(DATA['Error'])

    # generate PBI Report
    DATA['port_id'] = port_id
    # results = pbi.generate_report(DATA)
    pbi.check_input(DATA)
    
    # check required data
    preprocess.check_input(DATA)
    
    # position exception
    preprocess.read_position_exception(DATA)
    
    #  Performance, calculate past 1Y returns
    print('calculate Performance....')
    performance.calc_performance(DATA)

    # calculate Sharp Ratio, add to pos_var dataframe
    print('calculate Sharpe Ratio')
    sharpe_ratio.calc_sharpe_ratio(DATA)

    # calculate back_test
    print('calculate back test...')
    back_test.back_test(DATA)

    # generate results
    results = pbi.gen_pbi_data_master(DATA)    
    
    
    # write results to the input file
    file_path = portfolio_utils.get_folder_by_port_id(port_id) / f'{port_id}.pbi.xlsx'
    pbi.write_results_xl(results, file_path)
    
    # write results to database
    # report_description = portfolio_description(params)
    report_description = params['PortfolioName']
    pbi.insert_results_to_db(results, port_id, username, report_description)
    
    # update status
    if len(unknown_positions):
        status = 'partial'
    else:
        status = 'success'
    model_aux.update_portfolio_status(port_id, status=status, report_id=port_id)    
    sync_report_mapping.sync_delta()


##################################################################################################
def test_upload_security(wb):

    # file_path = config['DATA_DIR'] / 'test' / 'upload_security' / 'PC_MS_Portfolio.xlsx'
    file_path = config['DATA_DIR'] / 'test' / 'upload_security' / 'UploadSecurity.4.xlsx'

    results = upload_security.upload_security(file_path)
    
    write_results(results, wb)

##################################################################################################
from utils import date_utils, stat_utils
import datetime
import numpy as np

    

    # df = stat
    # xl_utils.add_df_to_excel(price_scaled, wb, 'price_scaled', index=True)
    
##################################################################################################
def db_temp(wb):
    query = """
select si.*  from security_info si 
left join security_attribute sa on si."SecurityID" = sa.security_id 
where sa.security_id is null 
and si."AssetType" not in ('Index','YieldCurve') and si."AssetClass" not in ('Benchmark', 'Macro', 'Riskfactor')
    """
    df = db_utils.get_sql_df(query)
    
    xl_utils.add_df_to_excel(df, wb, 'security', index=False)
    
    
    db_utils.insert_df('security_attribute', df)

    df['securityid']
    df['paymentfrequency'].unique()

    df['paymentfrequency'].fillna(0, inplace=True)
    
    df_save = df
    
    
##################################################################################################
# user and related tables
def test_create_account():
   
    username = os.environ['test_username']
    
    data = {
        'firstName'  : 'test',
        'lastName'   : 'test',
        'email'      :  username,
        'companyName': 'test company'
        }
    create_account.create_account(data)    
    
    
def delete_user(username):
    
    # username = 'test@trg.com'
    
    # delete
    user = model_aux.get_user(username)  
    client = user.client
    pgroups = client.port_groups

    # delete portfolios
    port_id_list = []
    pgroup_id_list = []
    for pgroup in pgroups:
        pgroup_id_list.append(pgroup.pgroup_id)
        for port in pgroup.portfolios:
            port_id_list.append(port.port_id)

        
    input_data = { 'PORT_ID_LIST': port_id_list }
    request_handler.delete_portfolios(username, input_data)
    
    # delete entitlement
    db_utils.delete('user_entitilement', 'port_group_id', pgroup_id_list)
    
    # delete pgroup
    for pgroup_id in pgroup_id_list:
        model_aux.delete_pgroup(pgroup_id)

    # delete user
    model_aux.delete_user(user)
        
    # delete client
    model_aux.delete_client(client)
    

def fix_entitilement():
    users =  get_all_users() 
    for user in users:
        print(user.username)
        add_entitlements(user)
    
    # sync sql server database
    sync_report_mapping.sync_delta_thread()

def add_entitlements(user):
    
    client = user.client
    pgroups = client.port_groups        
    
    # add permissions
    for pgroup in pgroups:
        for permission in ['view', 'upload', 'download']:
            model_aux.save_entitlements(user.user_id, pgroup.pgroup_id, permission)
    
    
def get_all_users():
    username_list = db_utils.get_sql_df('select username from "user"')
    
    users = []
    for username in username_list['username']:
        users.append(model_aux.get_user(username))
    
    return users
    
##################################################################################################
# auxilaries
# results = DATA
def write_results(results, wb):
    for tab, df in results.items():
        if tab in ['RF_PnL']:
            continue
        if isinstance(df, dict):
            df = pd.DataFrame([df])
        
        xl_utils.add_df_to_excel(df, wb, tab)


def write_port_to_xl(wb, params, positions, unknown_positions):
    if isinstance(params, dict):
        params_df = tools.dict_to_df(params)
    else:
        params_df = params

    xl_utils.add_df_to_excel(params_df, wb, 'params', index=False)
    xl_utils.add_df_to_excel(positions, wb, 'positions', index=False)
    xl_utils.add_df_to_excel(unknown_positions, wb, 'unknown_positions', index=False)
    

        
def excel(positions):
    wb = xw.Book('Book1')
    xl_utils.add_df_to_excel(positions, wb, 'positions', index=False)
    df = xl_utils.read_df_from_excel(wb, 'Ticker')    

    df.drop_duplicates(subset=['securityid'], inplace=True)
    
##################################################################################################
# TEMP functions    
def get_security():
    sec_ids = set()
    
    file_path = config['TEST_DIR'] / 'portfolios' 

    for file in file_path.glob("*.xlsx"):
        print(file.name)
        df = pd.read_excel(file, 'Positions')
        df['SecurityID'] = security_info.get_SecurityID_by_ref(df)
        sec_ids.update(df['SecurityID'])
    
    sec_list = security_info.get_securities_with_xref(sec_ids)
    
    # xl_utils.add_df_to_excel(sec_list, wb, 'sec_list', index=False)

    
def temp():
    wb = xw.Book('Book2')
    df = xl_utils.read_df_from_excel(wb, 'Tickers')
    df['SecurityID'] = security_info.get_SecurityID_by_ref(df)
    xl_utils.add_df_to_excel(df, wb, 'Tickers')
    
    sec_ids = df['SecurityID']
    
    df = security_info.get_securities_with_xref(sec_ids)
    xl_utils.add_df_to_excel(df, wb, 'securities')
    df.loc[df['YF_ID'].isna(), 'YF_ID']
    
    sec_ids= ['T10000885']    
    df = var_utils.get_dist(sec_ids)
    

    
    
    
    
    
    
    
    
    
    
    
    
    
        