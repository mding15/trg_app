# -*- coding: utf-8 -*-
"""
Created on Sat Nov  2 11:57:25 2024

@author: mgdin

API: upload_portfolio implementation:
    - save excel file in the designed folder
    - add portfolio summary into database
"""

import pandas as pd
import datetime
from flask import jsonify
from werkzeug.utils import secure_filename
from pathlib import Path

from trg_config import config
from database.models import User as User
from database import model_aux, db_utils
from report import powerbi as pbi
from database import sync_report_mapping
from api import email2 as email2_util
from utils import tools
from preprocess import portfolio_utils, read_portfolio
from process import process, process_accounts


from api.logging_config import get_logger
logger = get_logger(__name__)

support_emails = config['SUPPORT_EMAILS']

ALLOWED_EXTENSIONS = {'xlsx'}
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def handle_upload_portfolio(route, username, request):

    logger.info('===== New Request =====')
    logger.info(f'route: {route}')
    logger.info(f'username: {username}')

    try:
        user = User.query.filter_by(username=username).first()
        client = user.client
        pgroup = model_aux.get_port_group(user)

        # Check for track over time parameters
        track_over_time = request.form.get('track_over_time')
        
        logger.info(f'track_over_time: {track_over_time}')

        # if check_permission(username, 'upload_portfolio', group_id)==False:
        #     group_name = models.get_group_name(group_id)
        #     message = f'You do not have permission to upload portfolio to this group: {group_name}'
        #     raise Exception(message)

        # save file
        logger.info('===== save portfolio file =====')
        file_path, port_name = save_portfolio_file(request, client, pgroup)
        
        # Handle account tracking if enabled - save the positions and parameters to account_positions and account_run_parameters
        if track_over_time == 'true':
            save_portfolio_for_tracking(file_path, user, port_name)
        
        # Process portfolio using Excel file data (normal case)
        logger.info('===== process portfolio =====')
        port = process.process_portfolio(user, file_path, port_name)

        # send notification email    
        if not config['DEBUG']:
            logger.info('===== send notifiction email =====')
            email_notification(port.port_id, port.port_name, username, client.client_name)
        
        logger.info('===== upload portfolio success =====')
        return jsonify({'message': 'upload portfolio successed'}), 200
    
    except Exception as e:
        message = str(e)
        logger.error(message)
        return jsonify({'error': message}), 402


def rerun_portfolio(port_id, insert_msss=True):
    process.rerun_portfolio(port_id, insert_msss)
    
def save_portfolio_file(request, client, pgroup):

    if 'file' not in request.files:
        raise Exception('No file part')

    file = request.files['file']
    if file.filename == '':
        raise Exception('No selected file')

    if allowed_file(file.filename) == False:
        raise Exception(f'file extension is not allowed: {file.filename}')
    
    if file:
        file_path = save_file(client.client_id, pgroup.pgroup_id, file)
        port_name = Path(file.filename).stem
        
    return file_path, port_name

# Handle account tracking if enabled - save the positions and parameters to account_positions and account_run_parameters
def save_portfolio_for_tracking(file_path, user, port_name):
    logger.info('===== processing account tracking =====')
    # Read Excel data directly (no scrubbing needed)
    params, positions, limit = read_portfolio.read_input_file(file_path)
    
    # Check if account exists by portfolio name (Excel filename without extension)
    existing_account = process_accounts.find_account_by_name(user.user_id, port_name)
    
    is_new_account = False
    if existing_account is None:
        # Create new account
        logger.info(f'===== creating new account for portfolio: {port_name} =====')
        account = process_accounts.create_account_for_tracking(user, port_name, user.client_id)
        account_id = account.account_id
        is_new_account = True
    else:
        # Use existing account
        account_id = existing_account['account_id']
        is_new_account = False
        logger.info(f'===== using existing account_id: {account_id} for portfolio: {port_name} =====')
    
    # Save data to account tables (raw Excel data, no scrubbing)
    logger.info('===== saving account data =====')
    # Only save parameters for new accounts, skip for existing accounts
    process_accounts.save_account_run_parameters(account_id, params, is_new_account)
    # Always save positions (with same-day replacement logic)
    process_accounts.save_account_positions(account_id, positions)
    # Save limit data to account_limit table
    process_accounts.save_account_limit(account_id, limit)
    
    logger.info('===== account tracking completed =====')
    
######################################
def delete_portfolios(port_id_list):
    
    # delete from portfolio_info
    model_aux.delete_portfolios(port_id_list)
    
    # delete from port_positions, port_params
    delete_from_db(port_id_list)
    
    # delete pbi report
    pbi.delete_report_from_db(port_id_list)
    
    # sync sql server database
    sync_report_mapping.sync_delta_thread()


        
######################################
def email_notification(port_id, portfolio_name, username, company_name):
    subject = "Portfolio Upload Notification"
    receiver_emails = support_emails

    # read_email_template
    template_file = config['TEMPLATES_DIR'] / 'portfolio_upload_email.html'
    try:
        with open(template_file, "r") as file:
            content = file.read()
    except FileNotFoundError:
        raise Exception(f"Error: The template file {template_file} was not found.")
    
    content = content.replace("[port_id]"           , str(port_id))
    content = content.replace("[portfolio_name]"    , portfolio_name)
    content = content.replace("[username]"          , username)
    content = content.replace("[company_name]"      , company_name)
    
    email2_util.send_email(receiver_emails, subject, "", content, cc=[], bcc=[])

            
######################################
# database functions
def db_add_portfolio(user, client, pgroup, file_path, port_name):
    
    # market_value = positions['MarketValue'].sum()
    # number_of_positions = positions['SecurityName'].count()

    port = model_aux.add_portfolio(data={
        'port_name': port_name,
        'filename':  file_path.name,
        'created_by': f'{user.firstname} {user.lastname}',
        'status': 'processing',
        'port_group_id': pgroup.pgroup_id,
        'created_user_id': user.user_id,
        })
    
    logger.info(f'added portfolio to db, port_id: {port.port_id}')
    
    return port


# folder = Path(r'C:\DATA\trgapp_data\clients\1015\16')
# port_id=3089
# params = tools.read_parameter_csv(folder / f'{port_id}.params.csv')
# positions = tools.read_positions_csv(folder / f'{port_id}.positions.csv')
# unknown_positions = tools.read_positions_csv(folder / f'{port_id}.unknown_positions.csv')


# save position and parameters into database
def save_to_db(port_id, params, positions, limit):
    
    # save params
    df = pd.DataFrame([params])
    db_utils.insert_df('port_parameters', df, 'port_id')

    # save positions
    df = positions
    db_utils.insert_df('port_positions', df, 'port_id')
    
    # save limit
    df = pd.DataFrame([limit])
    db_utils.insert_df('port_limit', df, 'port_id')
    
    # positions['unknown_security']=False
    # unknown_positions['unknown_security']=True
    # df = pd.concat([positions, unknown_positions], ignore_index=True)
    # df['port_id']=port_id
    # db_utils.insert_df('port_positions', df, 'port_id')


def delete_from_db(port_id_list):
    db_utils.delete('port_parameters', 'port_id', port_id_list)
    db_utils.delete('port_positions', 'port_id', port_id_list)
    db_utils.delete('port_limit', 'port_id', port_id_list)
    
def load_from_db(port_id=5327):
    positions = db_utils.get_sql_df('select * from port_positions where port_id=%(port_id)s', {'port_id': port_id})
    df = db_utils.get_sql_df('select * from port_parameters where port_id=%(port_id)s', {'port_id': port_id})
    params = df.iloc[0].to_dict()
    df = db_utils.get_sql_df('select * from port_limit where port_id=%(port_id)s', {'port_id': port_id})
    limit = df.set_index('limit_category')['limit_value'].to_dict()
    
    unknown_positions = positions[positions['unknown_security']]
    positions = positions[positions['unknown_security']==False]

    positions['is_option'].fillna(False, inplace=True)

    return positions, params, unknown_positions, limit


# In[] save params and portfolio to csv files
def save_portfolio_by_port_id(port_id, params, positions, unknown_positions):
    folder = portfolio_utils.get_folder_by_port_id(port_id)
    tools.save_parameter_csv(params,    folder / f'{port_id}.params.csv')
    tools.save_positions_csv(positions, folder / f'{port_id}.positions.csv')
    tools.save_positions_csv(unknown_positions, folder / f'{port_id}.unknown_positions.csv')

def load_portfolio_by_port_id(port_id):
    folder = portfolio_utils.get_folder_by_port_id(port_id)
    params = tools.read_parameter_csv(folder / f'{port_id}.params.csv')
    positions = tools.read_positions_csv(folder / f'{port_id}.positions.csv')
    unknown_positions = tools.read_positions_csv(folder / f'{port_id}.unknown_positions.csv')
    return params, positions, unknown_positions
    
def load_position_exception_by_port_id(port_id):
    folder = portfolio_utils.get_folder_by_port_id(port_id)
    unknown_positions = pd.read_csv(folder / f'{port_id}.unknown_positions.csv')
    return unknown_positions
    

def get_group_folder(client_id, pgroup_id):
    return portfolio_utils.get_group_folder(client_id, pgroup_id)

def save_file(client_id, pgroup_id, file):
    filename = secure_filename(file.filename)
    name, ext = filename.rsplit('.', 1)
    ts = datetime.datetime.now().strftime('%Y%m%d.%H%M%S')
    filename = f'{name}.{ts}.{ext}'    
    file_path = portfolio_utils.get_file_path(client_id, pgroup_id, filename)
    
    logger.info(f'saving filepath: {file_path}')
    file.save(file_path)
    
    return file_path


def get_port_params(file_path):
    
    df = pd.read_excel(file_path, sheet_name='Parameters')
    params = df.set_index(df.columns[0])[df.columns[1]].to_dict()
    
    df = pd.read_excel(file_path, sheet_name='Positions')
    params['Market Value'] = df['MarketValue'].sum()
    params['Number of Positions'] = df['SecurityName'].count()
    return params
    
#######################################################################################################
#
# TEST  
#
import io
from api import app
from flask import request

# read xl into Bytes IO stream
def xl_ioBytes(filename):
    with open(filename, "rb") as f:
        excel_data = f.read()

    return io.BytesIO(excel_data)

    # username = 'test1@trg.com'
    # port_file_path = Path.home() / 'Downloads' / 'Demo.xlsx'

# track = 'true', 'false'
def test_save_portfolio_file(port_file_path, username):

    with app.app_context():
        user = User.query.filter_by(username=username).first()
        client = user.client
        pgroup = model_aux.get_port_group(user)
        
    with app.test_request_context('/upload', method='POST', 
                                  data={'file': (xl_ioBytes(port_file_path), port_file_path.name)},
                                  content_type='multipart/form-data'):
        file_path, port_name = save_portfolio_file(request, client, pgroup)
    return user, file_path, port_name


# track = 'true', 'false'
def test_upload_portfolio(port_file_path, username, track='false'):
    try:
        ctx = create_test_context(port_file_path, track)
        route = '/upload_portfolio'
        handle_upload_portfolio(route, username, request)
    finally:
        # close the context
        ctx.pop()

# track = 'true', 'false'
def create_test_context(port_file_path, track='false'):
    ctx = app.test_request_context('/upload', method='POST', 
                                   data={'file': (xl_ioBytes(port_file_path), port_file_path.name),
                                         'track_over_time': track
                                         }, 
                                   content_type='multipart/form-data')
    ctx.push()
    return ctx
    

def test():
    username = 'test1@trg.com'
    port_file_path = Path.home() / 'Downloads' / 'PC MS Portfoliov3.xlsx'
    
    test_upload_portfolio(port_file_path, username)
