# -*- coding: utf-8 -*-
"""
Created on Mon May 13 14:20:05 2024

@author: mgdin
"""

import pandas as pd
import xlwings as xw

from trg_config import config
from utils import tools, xl_utils

CLIENTS_FILENAME = config['DB_DIR'] / 'Clients.csv'
if not CLIENTS_FILENAME.exists():
    client_info = pd.DataFrame( columns = ['client_id', 'client_name', 'created_date'])
    client_info.loc[len(client_info)] = ['C021350', 'test', tools.today()]
    client_info.to_csv(CLIENTS_FILENAME, index=False)


def view_clients():
    wb = xw.Book()
    client_info = load_client_info()
    xl_utils.add_df_to_excel(client_info, wb, 'clients', index=False)

    # add a new client
    client_name = 'test client2'
    add_client(client_name)

    client_info = load_client_info()
    xl_utils.add_df_to_excel(client_info, wb, 'clients', index=False)
    
    
    # manual update client_info
    df = xl_utils.read_df_from_excel(wb, 'clients')
    save_client_info(df)
    
################################################################################    
def load_client_info():
    return pd.read_csv(CLIENTS_FILENAME)

def save_client_info(client_info):
    # convert data types
    #client_info['client_id'] = pd.to_numeric(client_info['client_id'], downcast='integer')
    client_info['created_date'] = pd.to_datetime(client_info['created_date']).apply(lambda x: x.strftime('%Y-%m-%d'))

    # save to csv file    
    client_info.to_csv(CLIENTS_FILENAME, index=False)
    
def add_client(client_name):
    # client_name = 'test client1'
    client_info = load_client_info()
    max_id = client_info['client_id'].max()
    max_id = int(max_id[1:])+1
    client_id = f'C{max_id:06}'
    created_date = tools.today()

    client_info.loc[len(client_info)] = [client_id, client_name, created_date]

    save_client_info(client_info)

def init_client():
    if not CLIENTS_FILENAME.exists():
        client_info = pd.DataFrame( columns = ['client_id', 'client_name', 'created_date'])
        client_info.loc[len(client_info)] = ['C021350', 'test', tools.today()]
        save_client_info(client_info)
                        
################################################################################    
    
def test():
    client_name = 'test client1'
    add_client(client_name)
    
    client_info = load_client_info()
    client_info = client_info.iloc[:-1]
    save_client_info(client_info)
