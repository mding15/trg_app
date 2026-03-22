# -*- coding: utf-8 -*-
"""
Created on Mon May 26 16:51:05 2025

@author: mgdin
"""
import os
import requests

from trg_config import config
from api import client

email=os.environ['api_username']
username=os.environ['api_username']
password=os.environ['api_password']
host = 'https://engine.tailriskglobal.com'

def api_commands():
    # re-run a portfolio
    port_id = 5327
    rerun_portfolio(port_id)
    
    # run account positions
    account_id = 1008
    as_of_date = '2024-01-31'
    run_account(account_id, as_of_date)
    
    for as_of_date in ['2024-02-28', '2024-03-31', '2024-04-30', '2024-05-31', '2024-06-30']:
        print(as_of_date)
        run_account(account_id, as_of_date)

###############################################################################
def rerun_portfolio(port_id):
    
    params = {'token': api_login() }    
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

def run_account(account_id, as_of_date):
    params = {'token': api_login() }    
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
    
    
    

def api_login():
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

    return token
