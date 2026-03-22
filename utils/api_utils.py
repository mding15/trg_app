# -*- coding: utf-8 -*-
"""
Created on Sun Mar 30 23:16:37 2025

@author: mgdin
"""
import requests
import os
from api import client, client_xl, test_handler, data_pack, request_handler_ft, create_account 

username = os.environ['api_username']
password = os.environ['api_password']


host = 'https://engine.tailriskglobal.com'
# host = 'http://localhost:5050'

def set_local_host():
    global host
    host = 'http://localhost:5050'
    
def set_remote_host():
    global host
    host = 'https://engine.tailriskglobal.com'
    
def login():
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
        msg = response.json()
        print('Error:', msg)

    return token

    
# api request
def request(token, request, payload):
    print(f'connecting {host}/api/login')
    
    params = {
        'token': token
    }
    
    request_url = f'{host}/api/{request}'
    print('api request:', request_url)
    response = requests.post(request_url, params=params, json=payload)
    
    if response.status_code != 200: # failed
        print('Request failed', response.status_code)
    
    return response.json()
    

def rerun_portfolio(port_id):
    token = login()
    params = {'token': token }
    
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
