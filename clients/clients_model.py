# -*- coding: utf-8 -*-
"""
Created on Mon May 13 15:16:11 2024

@author: mgdin
"""

import pandas as pd
import xlwings as xw

from trg_config import config
from utils import tools, xl_utils
CLIENTS_MODEL_FILENAME = config['DB_DIR'] / 'ClientsModel.csv'
if not CLIENTS_MODEL_FILENAME.exists():
    clients_model = pd.DataFrame( columns = ['client_id', 'model_id', 'update_date'])
    clients_model.loc[len(clients_model)] = ['C021350', 'M_20240510', tools.today()]
    clients_model.to_csv(CLIENTS_MODEL_FILENAME, index=False)
	

def view_clients_model():
    wb = xw.Book()
    clients_model = load_clients_model()
    xl_utils.add_df_to_excel(clients_model, wb, 'clients_model', index=False)

    # add a client model
    client_id = 'C123'
    model_id = 'M_123x'
    add_client_model(client_id, model_id)
    
    # manual update clients_model
    df = xl_utils.read_df_from_excel(wb, 'clients_model')
    save_clients_model(df)

    client_id = 'C021351'
    get_model_id(client_id)
    
################################################################################    
def get_model_id(client_id):
    
    models = load_clients_model()
    model = models[models['client_id']==client_id]
    
    if len(model) > 0:
        return model.iloc[0]['model_id']
    else:
        return None
    
def load_clients_model():
    return pd.read_csv(CLIENTS_MODEL_FILENAME)

def save_clients_model(clients_model):
    # convert data types
    # clients_model['client_id'] = pd.to_numeric(clients_model['client_id'], downcast='integer')
    clients_model['update_date'] = pd.to_datetime(clients_model['update_date']).apply(lambda x: x.strftime('%Y-%m-%d'))

    # save to csv file    
    clients_model.to_csv(CLIENTS_MODEL_FILENAME, index=False)
    
def add_client_model(client_id, model_id):
    
    clients_model = load_clients_model()
    update_date = tools.today()

    clients_model.loc[len(clients_model)] = [client_id, model_id, update_date]

    save_clients_model(clients_model)
    
################################################################################    
