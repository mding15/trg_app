# -*- coding: utf-8 -*-
"""
Created on Wed Mar  5 21:02:34 2025

@author: mgdin
"""

import pandas as pd
import datetime
from flask import jsonify, request
from werkzeug.utils import secure_filename
from pathlib import Path
import json

from trg_config import config
from database.models import User as User
from database import pg_create_connection
from api import api_status_codes as api_code
from utils import tools, xl_utils, web_utils
from preprocess import upload_security


from api.logging_config import get_logger
logger = get_logger(__name__)

support_emails = config['SUPPORT_EMAILS']

ALLOWED_EXTENSIONS = {'xlsx'}
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def upload(route, username, request):

    logger.info('===== New Request =====')
    logger.info(f'route: {route}')
    logger.info(f'username: {username}')

    user = User.query.filter_by(username=username).first()

    try:
        if route == 'upload_portfolio':
            response, status = upload_portfolio(user, request)
    
        elif route == 'upload_security':
            response, status = handle_upload_security(user, request)
            
        else:
            response, status = Unknown_Route_Error(route, user)
            

    except Exception as e:
        message = str(e)
        logger.error(message)
        return jsonify({'error': message}), 402
    
    logger.info('===== Response =====')
    logger.info(f'Status: {status}')
    logger.info(json.dumps(response, indent=4))
        
    return jsonify(response), status

################################################################################################
def upload_portfolio(user, request):
    pass
    
def handle_upload_security(user, request):
    upload_id = None
    try:
        logger.info('handle upload_security ...')
        
        # check permission    
        check_permission(user, 'upload_security')
    
        # save file
        file_path, name = save_security_file(request)
    
        # add an entry to table security_upload
        upload_id = db_add_security_upload(user, name, file_path.name)        
    
        # process upload security
        results = upload_security.upload_security(file_path)
    
        # save results
        result_file_path = save_upload_security_results(results, file_path)
    
        # db update status to success
        db_upload_security_status(upload_id, status='success', result_file=result_file_path.name)
        
        return {'message': 'upload security successed'}, 200

    except Exception as e:
        # save error to file
        error_file = get_error_filename(file_path)
        write_error_to_file(str(e), error_file)
        
        # update db
        if upload_id:
            db_upload_security_status(upload_id, status='error', error_file=error_file.name)
        
        raise e

        
def check_permission(user, route):
    if route == 'upload_security':
        if user.role != 'superadmin':
            raise Exception(f'user {user.username} does not have permission to upload security')
    
 
    
def Unknown_Route_Error(route, user):
    
    print(f'Panic: fatal Error, unknown route: {route}')

    # pack response
    response = {
        "Status": 'failed',
        "Error:": f'Server encontered unknown route: {route}'
    }

    return response, api_code.ERROR_REQUEST_UNKNOWN

####################################################################################################
# upload security functions
def get_security_folder():
    folder = config['MODEL_DIR'] / 'Upload'
    return tools.get_folder(folder)

def save_security_file(request):
    folder = get_security_folder()
    file_path, name = save_file(request, folder)
    
    return file_path, name


def db_add_security_upload(user, name, filename):
    conn = pg_create_connection()

    data = {
        'upload_name': name, 
        'filename': filename, 
        'status': 'processing', 
        'created_by': f'{user.firstname} {user.lastname}',
        'created_user_id': user.user_id
        }    
    

    columns = ', '.join(data.keys())
    placeholders = ", ".join(["%s"] * len(data.keys()))
    sql = f"INSERT INTO upload_security ({columns}) VALUES ({placeholders}) RETURNING upload_id;"

    with conn.cursor() as cur:
        cur.execute(sql, tuple(data.values()))
        upload_id = cur.fetchone()[0]
        conn.commit()
    conn.close()
    
    logger.info(f'added upload_security to db, upload_id: {upload_id}')
    
    return upload_id

def db_upload_security_status(upload_id, status, result_file='', error_file=''):
    conn = pg_create_connection()
    
    sql = 'UPDATE upload_security SET status = %s,  result_filename = %s, err_filename=%s WHERE upload_id = %s;'
    with conn.cursor() as cur:
        cur.execute(sql, (status, result_file, error_file, upload_id))
        conn.commit()
    conn.close()
    
# save results to a file    
def save_upload_security_results(results, file_path):

    filename = file_path.stem
    outfile = file_path.parent / f'{filename}.results.xlsx'
    xl_utils.write_book_to_xl2(results, outfile)
    return outfile
    
    

####################################################################################################
# file utility functions    
def save_file(request, folder):

    if 'file' not in request.files:
        raise Exception('No file part')

    file = request.files['file']
    if file.filename == '':
        raise Exception('No selected file')

    if allowed_file(file.filename) == False:
        raise Exception(f'file extension is not allowed: {file.filename}')
    
    if file:
        file_path, filename = get_file_path(file, folder)
        file.save(file_path)
        logger.info(f'saving filepath: {file_path}')    
        
    return file_path, filename

def get_file_path(file, folder):
    filename = secure_filename(file.filename)
    name, ext = filename.rsplit('.', 1)
    ts = datetime.datetime.now().strftime('%Y%m%d.%H%M%S')
    filename = f'{name}.{ts}.{ext}'    
    file_path = folder / filename
    
    return file_path, name

####################################################################################################
# write error to csv file
def get_error_filename(file_path):
        error_file = file_path.parent / f'{file_path.stem}.errors.csv'
        return error_file

def write_error_to_file(error, error_file):
        with open(error_file, 'w', newline='') as f:
            f.write(str(error))
        print(f'write errors to file: {str(error_file)}')

####################################################################################################
# TEST
        
def test():
    username = 'mding@trg.com'

    # upload security
    route = 'upload_security'
    file_path = Path.home() / 'Downloads' / 'ModelTest1.xlsx'
    try:
        ctx = web_utils.upload_request_ctx(file_path)
        upload(route, username, request)
    finally:
        ctx.pop()

