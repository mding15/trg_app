# -*- coding: utf-8 -*-
"""
Created on Fri Dec 15 11:49:55 2023

@author: mgdin
"""

import os
import time
import uuid
import binascii
from urllib.parse import urlparse
from pathlib import Path
import json
import re
import pandas as pd
import requests
import datetime
import jwt

from trg_config import config
    

CODE_SUCCESS          = 200
CODE_CREATED          = 201
CODE_NOT_AVAILABLE    = 404

CONFIG_DIR            = config['CNFG_DIR']
REFERENCE_DIR         = config['SRC_DIR'] / 'detl' / 'reference'

def generate_jwt(client_id, client_secret, url, method, region="default"):
    url_parsed = urlparse(url)
    # by default, jwt uses HMACSHA256
    # jwt also does the base64Url encoding internally
    JWT_LIFETIME = 25
    JWT_MAX_CLOCK_SKEW = 180 # Allows for differences between server and client clocks
    now = time.time()
    payload = {
        'iss': client_id,
        'iat': int(now - JWT_MAX_CLOCK_SKEW),
        'nbf': int(now - JWT_MAX_CLOCK_SKEW),
        'exp': int(now + JWT_MAX_CLOCK_SKEW + JWT_LIFETIME),
        'region': region,
        'path': url_parsed.path,
        'method': method,
        'host': url_parsed.hostname,
        'jti': str(uuid.uuid4()),
    }
    key = binascii.unhexlify(client_secret)
    return jwt.encode(payload, key, algorithm="HS256")


# BB Config    
def get_bb_config():
    filename = config['CNFG_DIR'] / 'bloomberg.json'
    with open(filename) as f:
        bb_config = json.load(f)
    bb_config['HistData']['security_file'] = config['HOME_DIR']  / bb_config['HistData']['security_file']
    return bb_config
    
class bb_info:

    def __init__(self, bb_config):

        credential = bb_config["credential"]
        with open(CONFIG_DIR / f'{credential}.json') as f:
            token = json.load(f)
        
        catalog  = bb_config['catalog']
        client_id = token["client_id"]
        client_secret = token["client_secret"]
        host = bb_config['host'] 
        uri  = f"{host}/eap/catalogs/{catalog}"

        self.catalog = catalog
        self.client_id = client_id
        self.client_secret = client_secret
        self.uri = uri
        self.request_uri = f'{uri}/requests/'


def sec_id(id_type, id_value):
    data = {'@type':'Identifier'}
    data['identifierType'] = id_type
    data['identifierValue'] = id_value
    return data

def build_sec_list(id_type, sec_ids):
    sec_list = []
    for id_value in sec_ids:
        sec_list.append(sec_id(id_type, id_value))
    return sec_list

def contains(e_type, e_contains):
    return {
        "@type": e_type,
        "contains": e_contains
        }

def gen_identifier(ds_name):
    # hash reduces likelihood of ID overlaps
    return f"{ds_name}{str(uuid.uuid4())[0:6]}" 

class BB_Exception(Exception):
    def __init__(self, message):
        self.messgae = message
    def _repr__(self):
        return self.message


def check_ds_name(ds_name):
    # ds_name : must begin with a letter and consit only of aphanumeric characters
    if not re.match('[A-Za-z][A-Za-z0-9]*$', ds_name):
        raise BB_Exception(f'bad data set name: {ds_name}. Only aphanumeric characters are valid')

def get_field_list(name, version='v1'):
    '''
        name : SecurityData, HistData
        version : v1 default
    '''
    filename = REFERENCE_DIR / f'BB_{name}.{version}.csv'
    df = pd.read_csv(filename)
    return [{'mnemonic': x} for x in df.columns]

#get_SecurityData_files()
    
# DataRequest
# sec_list = ['USDGBP Curncy', 'USDJPY Curncy']
def DataRequest_Payload(ds_name, identifier, sec_list, sec_id_type='TICKER'):    
    #ds_name = self.ds_name
    # identifier = gen_identifier(ds_name)
    
    payload = {}
    payload['@type'] = 'DataRequest'
    payload['name'] = ds_name
    payload['identifier'] = identifier
    payload['title'] = 'data request'
    
    # universe
    sec_list = build_sec_list(sec_id_type, sec_list)
    payload['universe'] = contains('Universe', sec_list)

    # field list
    filed_list = get_field_list('SecurityData')
    payload['fieldList'] = contains('DataFieldList', filed_list)
    
    payload['trigger'] = {
        "@type": "SubmitTrigger"
    }
    payload['formatting'] = {
        "@type": "MediaType",
        "outputMediaType": "text/csv"
    }

    return payload

# generate HistoryData request
def HistoryData_Payload(ds_name, identifier, sec_list, sec_id_type, start_date, end_date):
    # ds_name = 'HistData'
    # sec_list = ['BBG000N9MNX3']
    
    # request data
    payload = {}
    payload['@type'] = 'HistoryRequest'
    payload['name'] = ds_name
    payload['identifier'] = identifier
    payload['title'] = 'stock prices'
    
    # universe
    sec_list = build_sec_list(sec_id_type, sec_list)
    payload['universe'] = contains('Universe', sec_list)

    # field list
    filed_list = get_field_list('HistData')
    payload['fieldList'] = contains('HistoryFieldList', filed_list)

    payload['trigger'] = {
        "@type": "SubmitTrigger"
    }

    date_range = {
                    "@type": "IntervalDateRange",
                    "startDate": start_date,
                    "endDate": end_date
                    }         
    payload["runtimeOptions" ] = {
        "@type"                : "HistoryRuntimeOptions",
        "dateRange"            : date_range,
        "historyPriceCurrency" : "USD",
        "period"               : "daily"
    }
    
    payload['formatting'] = {
        "@type": "MediaType",
        "outputMediaType": "text/csv"
    }

    #print(json.dumps(payload, indent=4))
    return payload

def datetime_to_str(dt):
    if isinstance(dt, datetime.datetime):
        return dt.strftime("%Y-%m-%d")
    elif isinstance(dt, datetime.date):
        return dt.strftime("%Y-%m-%d")
    elif isinstance(dt, str):
        return dt
    else:
        raise BB_Exception(f'{dt} has wrong format, datetime is expected!')


def ticker_list(sec_list, suffix='US Equity'):
    #sec_list = ['TSLA', 'AZN', 'ALC', 'QCOM', 'LOW', 'SPGI']
    sec_list = [x+' '+suffix for x in sec_list]

    return sec_list

class DL_Request:
    
    def __init__(self):

        # bloomber info
        bb_config = get_bb_config()
        self.bbi  = bb_info(bb_config)

        # response sleep time
        self.RESPONSE_SLEEP_TIME = bb_config['RESPONSE_SLEEP_TIME']
            
    def DataRequest(self, ds_name, sec_list, sec_id_type='TICKER'):
        '''
            ds_name  = 'SecurityData'
            sec_list = ['TSLA US Equity', 'AZN US Equity']
            sec_id_type='TICKER'
        '''
        
        check_ds_name(ds_name)
        identifier = gen_identifier(ds_name)

        bbi = self.bbi
        
        data = DataRequest_Payload(ds_name, identifier, sec_list, sec_id_type)
        jwt_string = generate_jwt(bbi.client_id, bbi.client_secret, bbi.request_uri, method="POST")
        
        response = requests.post(
            bbi.request_uri, json=data, headers={'jwt': jwt_string, 'api-version': '2'})
        if response.status_code == CODE_CREATED:
            print(f'Request {identifier} created successfully')
        else:
            msg = 'DataRequest error:' + json.dumps(response.json())
            raise BB_Exception(msg)
        
        return self.get_response(identifier)

    def HistoryRequest(self, ds_name, sec_list, sec_id_type, start_date, end_date):
        '''
            ds_name  = 'HistData'
            sec_list = ['BBG000N9MNX3']
            sec_id_type='BB_GLOBAL'
            start_date = "2020-01-01"
            end_date = "2023-12-01"
        '''
        check_ds_name(ds_name)
        identifier = gen_identifier(ds_name)
        
        start_date = datetime_to_str(start_date)
        end_date   = datetime_to_str(end_date)
        
        payload = HistoryData_Payload(ds_name, identifier, sec_list, sec_id_type, start_date, end_date)    
        #print(json.dumps(payload, indent=4))
    
        # JWT
        bbi = self.bbi
        jwt_string = generate_jwt(bbi.client_id, bbi.client_secret, bbi.request_uri, method="POST")
    
        response = requests.post(
            bbi.request_uri, json=payload, headers={'jwt': jwt_string, 'api-version': '2'})
        if response.status_code == CODE_CREATED:
            print('Request created successfully')
        else:
            msg = 'HistoryRequest error:' + json.dumps(response.json())
            raise BB_Exception(msg)
        
        return self.get_response(identifier), identifier

            
    # The following code performs a simple poll to check for the response and retrieve it:
    def get_response(self, identifier):
        snapshot_date = datetime.datetime.now().strftime("%Y%m%d")
        file_format = "csv"

        bbi = self.bbi
        uri = bbi.uri
        #identifier = self.identifier
        distribution_uri = f"{uri}/datasets/{identifier}/snapshots/{snapshot_date}/distributions/{identifier}.{file_format}"
        out_txt = None
        
        while True:
            jwt_string = generate_jwt(bbi.client_id, bbi.client_secret, distribution_uri, method="GET")
            response = requests.get(distribution_uri, headers={'jwt': jwt_string, 'api-version': '2'})
            if response.status_code == CODE_NOT_AVAILABLE:
                print(f"Data set {identifier} is not available. Retry in {self.RESPONSE_SLEEP_TIME} seconds...")
                time.sleep(self.RESPONSE_SLEEP_TIME)
                continue
            elif response.status_code == CODE_SUCCESS:
                print(f"Response found on {distribution_uri}.")
                #out_file = self.write_response(identifier, response.text) 
                out_txt = response.text
                #print(response.text)
                break
            else:
                print("Unhandled HTTP status code.")
                print(response.status_code)
                print(response.text)
                break

        return out_txt

############################################################
def test_Connection():
    
    url = "https://api.bloomberg.com/eap/"
    bb_config = get_bb_config()
    bbi  = bb_info(bb_config)
    
    headers = {
    	"JWT": generate_jwt(bbi.client_id, bbi.client_secret, url, "GET"),
     	"api-version": "2"
    }

    # send requet
    response = requests.request("GET", url, headers=headers)

    if response.status_code == 200:
        print('Connection testing is successful!')
    else:
        print('Failed to connect')

def test_DataRequest():
    dr = DL_Request()
    sec_list = ticker_list(['TSLA', 'AZN', 'ALC', 'QCOM', 'LOW', 'SPGI'])
    data = dr.DataRequest('SecurityData', sec_list, 'TICKER')
    print(data)

def test_HistoryRequest():
    
    dr = DL_Request()
    sec_list = ['BBG000BB6WG8', 'BBG000BB9KF2']
    start_date, end_date = '2024-01-02', '2024-01-10'
    data = data = dr.HistoryRequest('History', sec_list, 'BB_GLOBAL', start_date, end_date)
    print(data)
    
    
def test():
    test_Connection()
    
    test_DataRequest()

    test_HistoryRequest()