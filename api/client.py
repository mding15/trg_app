# -*- coding: utf-8 -*-
"""
Created on Sun Mar 17 16:44:42 2024

@author: mgding
"""

import requests
import json
import os
import jwt
from api import data_pack

host = 'https://engine.tailriskglobal.com'
if 'TRG_API_HOST' in os.environ:
    host = os.environ["TRG_API_HOST"]
else:
    host = 'http://localhost:5050'

def set_host(new_host):
    global host
    host = new_host
    
# api request
def api_request(token, request, payload):
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
    

#
# Step 1. get a token from the server
#
def get_token(username, password):
    login_data = {
        'username': username,
        'password': password,
    }
    print(f'host:{host}')
    response = requests.post(f'{host}/api/login', json=login_data)
    if response.status_code == 200:
        token = response.json()['token']
        print(token)
        return token
    else:
        print('Request failed', response.status_code)
        msg = response.json()['message']
        print('Error:', msg)
        return None

def authenticate(email, password):
    login_data = {
        'email': email,
        'password': password,
    }
    print(f'host:{host}')
    url = f'{host}/api/authenticate'
    response = requests.post(url, json=login_data)
    if response.status_code == 200:
        token = response.json()['token']
        print(token)
        return token
    else:
        print('Request failed', response.status_code)
        msg = response.json()['message']
        print('Error:', msg)
        return None

#
# Step 2. call api with the token
#
def call_api_test(token):
    
    payload = {
        'Client ID': '123',
        'Portfolio Name': 'Growth Model',
        'Report Date': '2024-02-25',
        'Risk Horizon': 'Short-term',
        'Confidence Level': 95,
        'Benchmark': 'S&P 500',
        'Positions': [
            'AAPL',
            'GOOGL',
            'AMZN',
        ],
    }
    
    data = api_request(token, 'test', payload)
    if data:
        print(json.dumps(data, indent=4))

def test():
    print('api client test...')
    token = get_token('test', 'risk123')
    if token:
        call_api_test(token)

    SECRET_KEY = os.environ['SECRET_KEY']
    data = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])

#####
# def dumy():
#     params = {
#         'token': token
#     }
    
#     request_url = f'hostname/api/calculate'
#     payload = {
#         'name' : 'mike',
#         'age': 36,
#         'email': 'mike@test.com'
#         }
#     response = requests.post(request_url, params=params, json=payload)



###############################################################################
# client api functions

def api_market_data(token, sec_list):

    # payload
    payload ={
        "Request":   'MarketData',
        "Type":      'GetHistory',
        "Data Category":  'PRICE',
        "From Date":    None,
        "To Date":      None,
        "SecurityID": sec_list
        }
    
    response = api_request(token, 'data_request', payload)
    
    df = data_pack.extract_df(response, 'DATA')
    
    return df

















