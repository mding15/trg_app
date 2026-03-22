# -*- coding: utf-8 -*-
"""
Created on Sat May 31 13:11:06 2025

@author: mgdin
"""
import pandas as pd
from pathlib import Path

from trg_config import config
from preprocess import read_portfolio, portfolio_utils, scrubbing_portfolio as scrub
from database import model_aux, db_utils
from database import sync_report_mapping
from engine import VaR_engine
from report import powerbi as pbi
from utils import date_utils
from process import process_accounts
from api import email2

from api.logging_config import get_logger
logger = get_logger(__name__)

SYS_USERNAME = 'sys@trg.com'

# run a process from an input file
def process_portfolio(user, file_path, port_name):

    # create a new portfolio in table portfolio_info
    port = create_portfolio(user, file_path.name, port_name)

    # read input file
    params, positions, limit = read_input_file(port)

    # scrube data        
    params, positions, unknown_positions, limit = scrub_portfolio_with_error_handling(port, params, positions, limit)        
    
    # calculate VaR
    calc_portfolio(user.username, params, positions, unknown_positions, limit, insert_msss=True)

    return port
    
# re-run from port_id
def rerun_portfolio(port_id, insert_msss=True):
    port = model_aux.get_portfolio_by_id(port_id)
    if not port:
        raise Exception(f'Invalid port_id: {port_id}')

    user = model_aux.get_user_by_id(port.created_user_id)

    # read input file
    params, positions, limit = read_input_file(port)

    # scrube data        
    params, positions, unknown_positions, limit = scrub_portfolio_with_error_handling(port, params, positions, limit)        
    
    # calculate VaR
    calc_portfolio(user.username, params, positions, unknown_positions, limit, insert_msss=True)

#
# entry function called from scheduler
#
# as_of_date = date_utils.parse_date('2025-05-16')
# account_id = 1001
#
def process_account(account_id, as_of_date):
    
    if isinstance(as_of_date, str):
        as_of_date = date_utils.parse_date(as_of_date)
    
    user = get_user_by_account(account_id)
    # user = model_aux.get_user(SYS_USERNAME)    

    # get account portfolio
    params, positions, limit = process_accounts.get_account_portfolio(account_id, as_of_date)

    # get portfolio
    port = model_aux.get_port_by_account(account_id, as_of_date)
    
    # create a new portfolio in table portfolio_info
    if not port:
        port_name = params['PortfolioName']
        filename = 'batch'
        port = create_portfolio(user, filename, port_name, account_id, as_of_date, is_batch=True)
        

    # update portfolio
    positions['port_id'] = port.port_id
    params['port_id'] = port.port_id        
    limit['port_id'] = port.port_id

    # scrube data        
    params, positions, unknown_positions, limit = scrub_portfolio_with_error_handling(port, params, positions, limit)        
    
    # calculate VaR
    calc_portfolio(user.username, params, positions, unknown_positions, limit, insert_msss=True, account_id=account_id)
   
# account_id = 1004
def get_user_by_account(account_id):
    account = db_utils.get_sql_df(f'select * from account where account_id = {account_id}')
    user_id = int(account.owner_id.iloc[0])
    user = model_aux.get_user_by_id(user_id)
    return user


##########################################################################################
def run_calculation_by_port_id(port_id, insert_msss=True):
    user = model_aux.get_user_by_port_id(port_id)
    params, positions, unknown_positions, limit = read_portfolio_from_db(port_id)
    calc_portfolio(user.username, params, positions, unknown_positions, insert_msss, limit)


# username=user.username
def calc_portfolio(username, params, positions, unknown_positions, limit, insert_msss=True, account_id=None):
    port_id = params['port_id']
    try:
        # Add account_id to params if provided (for tracked portfolios)
        if account_id:
            params['account_id'] = account_id
        
        DATA = VaR_engine.calc_VaR(positions, params)
        if 'Error' in DATA:
            raise Exception(DATA['Error'])

        # generate PBI Report
        DATA['port_id'] = port_id
        DATA['position_exception'] = unknown_positions
        DATA['limit'] = limit  # Add limit data to DATA
        results = pbi.generate_report(DATA)
        
        # write results to the input file
        file_path = portfolio_utils.get_folder_by_port_id(port_id) / f'{port_id}.pbi.xlsx'
        pbi.write_results_xl(results, file_path)
        
        # write results to database
        if insert_msss:
            report_description = params['PortfolioName']
            pbi.insert_results_to_db(results, port_id, username, report_description)
        
        # update status
        if len(unknown_positions):
            status = 'partial'
        else:
            status = 'success'
        model_aux.update_portfolio_status(port_id, status=status, report_id=port_id)    
        sync_report_mapping.sync_delta()
    
        print(f'successfully processed port: {port_id}')
    except Exception as e:
        # update status
        model_aux.update_portfolio_status(port_id, status='pending', message=str(e))
        
        # send email to support team for pending
        try:
            # get portfolio info to get port_name
            port = model_aux.get_portfolio_by_id(port_id)
            port_name = port.port_name if port else None
            user_id = port.created_user_id if port else None
            user_name = port.created_by if port else None
            send_pending_notification_email(port_id, port_name, str(e), user_id, user_name)
            
        except Exception as email_error:
            print(f"Failed to get user info for email: {email_error}")
        
        raise Exception(str(e))

##########################################################################################
def read_input_file(port):

    # get file path
    file_path = get_file_path(port)
    
    try:
        # read input file
        params, positions, limit = read_portfolio.read_input_file(file_path)

        positions['port_id'] = port.port_id
        params['port_id'] = port.port_id        
        params['PortfolioName'] = port.port_name
        limit['port_id'] = port.port_id

        return params, positions, limit
        
    except Exception as e:
        # save error to file
        error_file = get_error_filename(file_path)
        write_error_to_file(str(e), error_file)
        
        # update db
        relative_path = error_file.relative_to(config['CLIENT_DIR'])
        relative_path = Path(*relative_path.parts[1:])
        model_aux.update_portfolio_status(port.port_id, status='error', message=str(relative_path))
        
        raise Exception('Input file error')
    

def scrub_portfolio_with_error_handling(port, params, positions, limit):
    
    try:
        # scrube data
        params, positions, unknown_positions = scrub.scrubbing_portfolio(params, positions, limit)
        return params, positions, unknown_positions, limit
        
    except Exception as e:
        model_aux.update_portfolio_status(port.port_id, status='pending', message=str(e))

        try:
            port_id = port.port_id
            port_name = port.port_name if port else None
            user_id = port.created_user_id if port else None
            user_name = port.created_by if port else None
            send_pending_notification_email(port_id, port_name, str(e), user_id, user_name)
            
        except Exception:
            user_id = None
            user_name = None

        
        raise Exception('Portfolio scrubbing error')


def get_file_path(port):
    file_path = portfolio_utils.get_port_file_path(port)
    if not file_path.exists():
        model_aux.update_portfolio_status(port.port_id, status='error', message=f'file not found: {str(file_path)}')
        raise Exception(f'file not found: {str(file_path)}')
    return file_path


    
def get_params(port_id):
    df = db_utils.get_sql_df(f'select * from port_parameters where port_id = {port_id}')    
    params = df.T.iloc[:,0].to_dict()
    return params

def get_positions(port_id):
    df = db_utils.get_sql_df(f'select * from port_positions where port_id = {port_id} and unknown_security = False ')
    df['MaturityDate'] = pd.to_datetime(df['MaturityDate'])
    df['LastPriceDate'] = pd.to_datetime(df['LastPriceDate'])
    df['is_option'].fillna(False, inplace=True)
    
    positions = df[df['unknown_security'] == False]
    unknown_positions = df[df['unknown_security']]
    
    return positions, unknown_positions

def get_limit(port_id):
    df = db_utils.get_sql_df(f'select * from port_limit where port_id = {port_id}')
    if df.empty:
        return {}
    else:
        limit = {}
        for _, row in df.iterrows():
            limit[row['limit_category']] = row['limit_value']
        limit['port_id'] = port_id
        return limit

def read_portfolio_from_db(port_id):
    params = get_params(port_id)
    positions, unknown_positions = get_positions(port_id)
    limit = get_limit(port_id)
    return params, positions, unknown_positions, limit
    
        
######################################
# database functions
# filename = file_path.name
def create_portfolio(user, filename, port_name, account_id=None, as_of_date=None, is_batch=False):
    client = user.client
    pgroup = model_aux.get_port_group(user)

    port = model_aux.add_portfolio(data={
        'port_name': port_name,
        'filename':  filename,
        'created_by': f'{user.firstname} {user.lastname}',
        'status': 'running',
        'port_group_id': pgroup.pgroup_id,
        'created_user_id': user.user_id,
        'account_id': account_id,
        'as_of_date': as_of_date,
        'is_batch': is_batch
        })
    
    logger.info(f'created a new portfolio, port_id: {port.port_id}')
    
    return port
#########################################
# auxiliary functions
def get_error_filename(file_path):
    error_file = file_path.parent / f'{file_path.stem}.errors.csv'
    return error_file

def write_error_to_file(error, error_file):
    with open(error_file, 'w', newline='') as f:
        f.write(str(error))
    print(f'write errors to file: {str(error_file)}')


def send_pending_notification_email(port_id, port_name, error_message, user_id=None, user_name=None):
    """Added by AI, send portfolio pending notification email"""
    try:
        # send email
        email2.send_portfolio_status_notification(
            port_id=port_id,
            port_name=port_name,
            status='pending',
            error_message=error_message,
            user_id=user_id,
            user_name=user_name
        )
        
        print(f"Portfolio pending notification email sent to support team for portfolio {port_id}")
        
    except Exception as email_error:
        print(f"Failed to send portfolio pending notification email: {email_error}")
