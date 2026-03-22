# -*- coding: utf-8 -*-
"""
Created on Tue Jun 11 11:56:52 2024

@author: mgdin
"""
import pandas as pd
import xlwings as xw

from trg_config import config
from utils import xl_utils, var_utils, tools

def copy(source_id, model_id, submodel_id, num_simulation=1044):
    source_id = '4Y'
    model_id = 'M_20240531'
    submodel_id = 'Copy.2'
    num_simulation = 1044
    
    wb = xw.Book('Book9')
    securities = xl_utils.read_df_from_excel(wb, 'Securities', index=False)

    sec_ids = securities['SecurityID'].to_list()
    
    # source distribution
    var_utils.set_model_id(source_id)    
    source_dist = var_utils.get_dist(sec_ids, 'PRICE')
    
    # check missing
    missing = set(source_dist).difference(sec_ids)
    if len(missing)>0:
        mis_secs = ', '.join(list(missing))
        raise Exception(f'Failed to find securities in source {source_id}: [{mis_secs}]')
    
    # save
    model_folder = get_model_folder(model_id, submodel_id)
    
    Parameters = get_model_parameters(source_id, model_id, submodel_id, num_simulation)
    save_model_data(Parameters, 'Parameters', model_folder)
    save_model_data(securities, 'Securities', model_folder)
    save_model_data(source_dist, 'Distribution', model_folder)

    var_utils.set_model_id(model_id)
    var_utils.save_dist(source_dist, 'PRICE')

    # xl_utils.add_df_to_excel(source_dist, wb, 'dist')
    
def save_model_data(df, filename, model_folder):
    file_path = model_folder / f'{filename}.csv'
    df.to_csv(file_path, index=False)
    print('Saved file:', file_path)    

def get_model_parameters(source_id, model_id, submodel_id, num_simulation):
    params = {'Source Model ID': source_id, 
              'Model ID': model_id,
              'Submodel ID': submodel_id,
              'Number of Simulations': num_simulation
              }
    return tools.dict_to_df(params)

def get_model_folder(model_id, submodel_id):
    model_folder = config['MODEL_DIR'] / model_id / submodel_id

    if not model_folder.exists():
        model_folder.mkdir(parents=True, exist_ok=True)

    return model_folder

def create_copy_model_template():
    wb = xw.Book()
    df = pd.DataFrame(columns=['SecurityID','SecurityName','Ticker'])
    xl_utils.add_df_to_excel(df, wb, index=False)
    return wb

    		

    
    
    