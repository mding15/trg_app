# -*- coding: utf-8 -*-
"""
Created on Sun Jun  1 14:23:43 2025

@author: mgdin
"""
from trg_config import config
from database import model_aux


def get_port_file_path(port):
    
    # portfolio_group
    pg = model_aux.get_pgroup_by_id(port.port_group_id)        

    file_path = get_file_path(pg.client_id, pg.pgroup_id, port.filename)
    return file_path

# portfolio file folder
def get_folder_by_port_id(port_id):
    # portfolio_info
    port = model_aux.get_portfolio_by_id(port_id)
    if port is None:
        raise Exception(f'can not find portfolio {port_id}')
    
    # portfolio_group
    pg = model_aux.get_pgroup_by_id(port.port_group_id)        
    
    return get_group_folder(pg.client_id, pg.pgroup_id)
    
# raw portfolio file_path
def get_file_path_by_port_id(port_id):
    # portfolio_info
    port = model_aux.get_portfolio_by_id(port_id)
    if port is None:
        raise Exception(f'can not find portfolio {port_id}')
    
    # portfolio_group
    pg = model_aux.get_pgroup_by_id(port.port_group_id)        

    file_path = get_file_path(pg.client_id, pg.pgroup_id, port.filename)
    return file_path

def get_client_folder(client_id):
    client_folder = config['CLIENT_DIR'] / f'{client_id}'
    if not client_folder.exists():
        client_folder.mkdir(parents=True, exist_ok=True)
            
    return client_folder
def get_group_folder(client_id, pgroup_id):
    group_folder = get_client_folder(client_id) / f'{pgroup_id}'
    if not group_folder.exists():
        group_folder.mkdir(parents=True, exist_ok=True)
            
    return group_folder
    
def get_file_path(client_id, pgroup_id, filename):
    file_path = get_group_folder(client_id, pgroup_id) /  filename
    return file_path   


