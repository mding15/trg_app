# -*- coding: utf-8 -*-
"""
Created on Mon Mar 18 14:47:56 2024

@author: mgdin
"""
import xlwings as xw
import requests
import json
import os
from pathlib import Path

from trg_config import config
from api import client, client_xl, test_handler, data_pack, request_handler_ft, create_account 
from database import model_aux as db 
from database import pg_create_connection
from utils import tools, xl_utils, test_utils
from security import security_info

username = os.environ['test_username']
password = os.environ['test_password']

admin_username = os.environ['api_username']
admin_password = os.environ['api_password']

host = 'https://engine.tailriskglobal.com'
# host = 'http://localhost:8000'
# host = 'http://54.82.69.244' # dev2
client.set_host(host)


def request_get(url, name):
    response = requests.get(url)
    if response.status_code == 200:
        message = response.json()[name]
        print(message)
    else:
        print('Request failed', response.status_code)
        msg = response.json()['message']
        print('Error:', msg)


def request_post(url, data, name='message'):
    print(url)
    response = requests.post(url, json=data)
    if response.status_code == 200:
        msg = response.json()[name]
        print(msg)
    else:
        print('Request failed', response.status_code)
        msg = response.json()['message']
        print('Error:', msg)

    
def test_login(username, password):
    print(f'username: {username}')
    login_data = {
        'username': username,
        'password': password,
    }
    print(f'host:{host}/api/login')
    response = requests.post(f'{host}/api/login', json=login_data)
    if response.status_code == 200:
        token = response.json()['token']
        print(token)
    else:
        print('Request failed', response.status_code)
        msg = response.json()['message']
        print('Error:', msg)

    print(response.text)
    return token

    
def test_register():
    firstName = 'Michael'
    lastName  = 'Ding'
    email     = 'mgding@gmail.com'
    companyName = 'TRG'
    
    data = {
        'firstName': firstName,
        'lastName' : lastName,
        'email': email,
        'companyName': companyName,
    }

    db_delete_user(email)
    
    url = f'{host}/api/register'
    print(url)
    response = requests.post(url, json=data)
    if response.status_code == 200:
        message = response.json()['message']
        print(message)
    else:
        print('Request failed', response.status_code)
        msg = response.json()['message']
        print('Error:', msg)


def test_reset_password():

    user = db.get_user(username)
    token = create_account.get_reset_token(user)
    
    # verify token
    url=f'{host}/api/verify_token/{token}'
    print(url)
    request_get(url, 'email')    

    # Reset password
    url=f'{host}/api/reset_password'
    data = {
        'token': token,
        'password': password,
    }
    request_post(url, data, 'message')
    
    # web link
    url = f'http://{host}/resetPassword/{token}'
    print(url)

def test_sso_cookie():
    token = test_login(username, password)
    params = {'token': token, 'withCredentials': True  }
    
    url=f'{host}/api/set_sso_cookie'
    print(url)
    
    try:
        response = requests.post(url, params=params)
        
        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            
            print("--- header ----")
            print(response.headers)
            print('--- cookies ---')
            print(response.cookies)
            print(data)
        else:
            print(f"Failed: {response.status_code}")
            print("Response:", response.text)
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")    
    
def test_upload_portfolio():
    
    token = test_login(username, password)
    params = {'token': token }
    
    url=f'{host}/api/upload_portfolio'
    print(url)
    
    # file_path = config['TEST_DIR'] / 'portfolios' / 'Test1.xlsx'
    file_path = Path.home()/'Downloads'/'Test_portfolio 1.xlsx'
    
    with open(file_path, 'rb') as file:
        files = {'file': file}
        try:
            response = requests.post(url, files=files, params=params)
            
            # Check if the request was successful
            if response.status_code == 200:
                message = response.json()['message']
                print(message)
            else:
                print(f"Failed: {response.status_code}")
                print("Response:", response.text)
        
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")

def test_rerun_portfolio():
    
    token = test_login()
    params = {'token': token }
    
    port_id = 4113
    url=f'{host}/api/rerun_portfolio/{port_id}'
    print(url)
    
    try:
        response = requests.get(url, params=params)
        
        # Check if the request was successful
        if response.status_code == 200:
            message = response.json()['message']
            print(message)
        else:
            print(f"Failed: {response.status_code}")
            print("Response:", response.text)
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    
def test_run_account():
    account_id =1005
    as_of_date = '2024-07-31'
    
    token = test_login(admin_username, admin_password)
    params = {'token': token }
    
    url=f'{host}/api/run_account/{account_id}/{as_of_date}'
    print(url)
    
    try:
        response = requests.get(url, params=params)
        
        # Check if the request was successful
        if response.status_code == 200:
            message = response.json()['message']
            print(message)
        else:
            print(f"Failed: {response.status_code}")
            print("Response:", response.text)
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
def test_scrubbing_portfolio():
    
    token = test_login()
    params = {'token': token }
    
    url=f'{host}/api/scrubbing_portfolio'
    print(url)
    
    port_id = 1521
    data = {
        'PORT_ID': port_id,
    }
    
    try:
        response = requests.post(url, params=params, json=data)
        
        # Check if the request was successful
        if response.status_code == 200:
            message = response.json()['message']
            print(message)
        else:
            print(f"Failed: {response.status_code}")
            print("Response:", response.text)
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")    


def test_user_approval_data():
    token = test_login()
    params = {'token': token }
    
    url=f'{host}/api/user_approval/data'
    print(url)
    
    try:
        response = requests.post(url, params=params, json={})
        
        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            print(data)
        else:
            print(f"Failed: {response.status_code}")
            print("Response:", response.text)
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")    

def test_update_user_approval():
    token = test_login()
    params = {'token': token }
    
    url=f'{host}/api/user_approval/update'
    print(url)
    
    data = {
        "updates": [
           {
               "user_id": 1051,
               "approval": "pending",
               "role": "user"
           },
           {
               "user_id": 1053,
               "approval": "revoked",
               "role": "user"
           }
       ]
    }

    try:
        response = requests.post(url, params=params, json=data)
        
        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            print(data)
        else:
            print(f"Failed: {response.status_code}")
            print("Response:", response.text)
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")    
    

def test_get_entitlement_data():
    token = test_login()
    params = {'token': token }        

    
    url=f'{host}/api/get_entitlement'
    # url=f'{host}/api/mytest'
    print(url)
    
    
    response = requests.get(url, params=params)
    
    try:
        response = requests.get(url, params=params)
        
        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            print(data)
        else:
            print(f"Failed: {response.status_code}")
            print("Response:", response.text)
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")    
    
def test_get_entitlement_1client():
    token = test_login()
    params = {'token': token }        

    url=f'{host}/api/get_entitlement_1client'
    print(url)
    
    try:
        response = requests.post(url, params=params, json={'client_id': 1013})
        
        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            print(data)
        else:
            print(f"Failed: {response.status_code}")
            print("Response:", response.text)
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")    


def test_run_calculation():
    
    token = test_login()
    params = {'token': token }
    
    url=f'{host}/api/run_calculation'
    print(url)
    
    port_id = 1489
    data = {
        'PORT_ID': port_id,
    }
    
    try:
        response = requests.post(url, params=params, json=data)
        
        # Check if the request was successful
        if response.status_code == 200:
            message = response.json()['message']
            print(message)
        else:
            print(f"Failed: {response.status_code}")
            print("Response:", response.text)
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")    


def test_get_dashboard():
    params = {'token': test_login() }

    url=f'{host}/api/get_dashboard'
    print(url)
    
    try:
        response = requests.post(url, params=params, json={})
        
        # Check if the request was successful
        if response.status_code == 200:
            message = response.json()
            print(json.dumps(message, indent=2))
        else:
            print(f"Failed: {response.status_code}")
            print("Response:", response.text)
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    
    
def test_get_upload_security():
    params = {'token': test_login() }

    url=f'{host}/api/get_upload_security'
    print(url)
    
    try:
        response = requests.post(url, params=params, json={})
        
        # Check if the request was successful
        if response.status_code == 200:
            message = response.json()
            print(json.dumps(message, indent=2))
        else:
            print(f"Failed: {response.status_code}")
            print("Response:", response.text)
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    
def test_download():
    params = {'token': test_login() }
    group_id = 13
    filename = 'Model-1.xlsx.20241113.113701'
    url=f'{host}/api/download/{group_id}/{filename}'
    print(url)

    outfile = Path(r'C:\Users\mgdin\Downloads') / filename
    try:
        response = requests.get(url, params=params)
        
        # Check if the request was successful
        if response.status_code == 200:
            with open(outfile, 'wb') as f:
                f.write(response.content)
        else:
            print(f"Failed: {response.status_code}")
            print("Response:", response.text)
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    

def test_download_file():
    token = test_login()
    params = {'token': token }
    filename = 'portfolio_template.xlsx'
    url=f'{host}/api/download_file/{filename}'
    print(url)

    outfile = Path(r'C:\Users\mgdin\Downloads') / filename
    try:
        response = requests.get(url, params=params)
        
        # Check if the request was successful
        if response.status_code == 200:
            with open(outfile, 'wb') as f:
                f.write(response.content)
        else:
            print(f"Failed: {response.status_code}")
            print("Response:", response.text)
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    
def test_delete_portfolios():
    token = test_login()
    params = {'token': token }
    data = { 
            'PORT_ID_LIST': [2514] 
            }

    url=f'{host}/api/delete_portfolios'
    print(url)

    try:
        response = requests.post(url, params=params, json=data)
        
        # Check if the request was successful
        if response.status_code == 200:
            message = response.json()
            print(json.dumps(message, indent=2))
        else:
            print(f"Failed: {response.status_code}")
            print("Response:", response.text)
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")

    
def test_calc_var(token):
    #wb = test_utils.template_test_portfolio()
    wb = xw.Book('VaRCalculator.xlsm')
    
    payload  = client_xl.gen_input_data(wb)
    response = client.api_request(token, 'calculate', payload)

    # error in response
    if 'Error' in response:
        error = json.loads(response['Error'])
        df = tools.extract_df(error, 'Unknown Securities')    
        xl_utils.add_df_to_excel(df, wb, 'UnknownSecurities', index=False)
        
    # write results to excel
    wb = xw.Book()
    data_pack.write_to_xl(response, wb)
    
    # addhoc
    results = response['Results']
    pos_var = tools.extract_df(results, 'PositionVaR')
    
    xl_utils.add_df_to_excel(pos_var, wb, 'VaR')    
    

    wb = xw.Book('Book8')


def test_ft_api(token):
    
    wb = xw.Book(config['TEST_DIR'] / 'portfolios' / 'Fintree Prototype.xlsx')
    input_data = request_handler_ft.generate_input(wb)

    token = client.get_token(username, password)
    response = client.api_request(token, 'risk_calculator', input_data)

    wb = xw.Book()
    for name in ['Portfolio Risk', 'Allocation']:
        print(name)
        df = data_pack.extract_df(response, name)
        xl_utils.add_df_to_excel(df, wb, tab=name, index=False)


    print(json.dumps(response, indent=4))
    print(json.dumps(input_data, indent=4))
    
 
def test_market_data():
    token = client.get_token(username, password)

    # get timeseries
    tickers=['SPY', 'AAPL']
    sec_list = security_info.get_ID_by_Ticker(tickers)['SecurityID'].to_list()
    
    df = client.api_market_data(token, sec_list)

    print(df.head())

    # get security list
    payload = {"Request":'MarketData', "Type":'GetSecurityList'}
    response = client.api_request(token, 'data_request', payload)
    df = data_pack.extract_df(response, 'DATA')


def test_request_dist():
    token = client.get_token(username, password)

    # get timeseries
    tickers=['SPY', 'BBR75']
    sec_list = security_info.get_ID_by_Ticker(tickers)['SecurityID'].to_list()

    # payload
    payload ={
        "Request":   'Distribution',
        "Category":  'PRICE',
        "SecurityID": sec_list
        }

    response = client.api_request(token, 'data_request', payload)
    df = data_pack.extract_df(response, 'DATA')

    print(df.head())



###################### Auxiliry Functions ######################
def db_delete_user(username):
    
    pg_conn = pg_create_connection()

    sql = """
    delete from "user" u
    where email = %s 
    """
    
    with pg_conn.cursor() as cursor:
        cursor.execute(sql, (username,))
        pg_conn.commit()

def open_template_test_api():
    wb = xw.Book()
    data = test_handler.test_portfolio()    
    positions = data_pack.extract_df(data, 'Positions')
    parameters = tools.dict_to_df(data['Paramaters'])
    xl_utils.add_df_to_excel(positions, wb, 'Positions', index=False)    
    xl_utils.add_df_to_excel(parameters, wb, 'Parameters', index=False)    
    
    return wb

def get_mkt_data_payload(tickers=['SPY', 'AAPL']):
    sec_list = security_info.get_ID_by_Ticker(tickers)['SecurityID'].to_list()
    
    # from_date, to_date = '2018-01-01', '2023-12-31'
    from_date, to_date = None, None
    
    input_data ={
        "Request":   'MarketData',
        "Type":      'GetHistory',
        "Data Category":  'PRICE',
        "From Date":    from_date,
        "To Date":      to_date,
        "SecurityID": sec_list
        }

    return input_data

#############################################################################
# TEST
def test():
    
    token = test_login(username, password)
    print(token)
    params = {'token': token }
    url = f'{host}/api/test'
    
    
    print(url)
    
    payload = {
        'Client ID': '12345',
        'Portfolio Name': 'Test Portfolio'
        }
    
    response = requests.post(url, params=params, json=payload)
    if response.status_code == 200:
        print(response.json())
    else:
        print('Request failed', response.status_code)
        msg = response.json()['message']
        print('Error:', msg)

