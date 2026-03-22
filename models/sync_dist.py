# -*- coding: utf-8 -*-
"""
Created on Tue Nov  4 12:21:32 2025

@author: mgdin
"""

from utils import api_utils
from api import data_pack
from utils import var_utils

# copy dist data from server to local
# sec_list = ['T10000108', 'T10001608']
def copy_from_server(sec_list, category='PRICE'):
    
    # get data from remote server
    dist = api_get_dist_data(sec_list, category)
    
    # save dist to local
    var_utils.save_dist(dist, category)

    id_list = ', '.join(dist.columns)
    print(f'saved dist for {id_list}')    

def test_sync_mkt_data():
    sec_list = ['T10000108', 'T10001608']
    copy_from_server(sec_list)

# call api
def api_get_dist_data(sec_list, category='PRICE'):
    token = api_utils.login()

    # payload
    payload ={
        "Request":   'Distribution',
        "Category":   category,
        "SecurityID": sec_list
        }

    response = api_utils.request(token, 'data_request', payload)
    df = data_pack.extract_df(response, 'DATA')
    return df

