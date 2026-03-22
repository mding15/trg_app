# -*- coding: utf-8 -*-
"""
Created on Mon Jul 28 10:00:09 2025

@author: mgdin

Database Auxilary functions
"""

from database import db_utils


############## limit ###################        
def add_limit_var(port_group_id):
    
    query = "select * from limit_var where port_group_id=%(port_group_id)s"
    df = db_utils.get_sql_df(query, {'port_group_id': port_group_id})
    
    if df.empty:
        default_limit = db_utils.get_sql_df(query, {'port_group_id': 0})
        default_limit['port_group_id'] = port_group_id
        db_utils.insert_df('limit_var', default_limit, key_column='port_group_id')
    else:
        print(f'Skipped! limit exists for port_group_id: {port_group_id}')

def test_add_limit_var():
    port_group_id = 1
    add_limit_var(port_group_id)
    
    
def add_limit_concentration(port_group_id):
    #port_group_id=1
    query = "select * from limit_concentration where port_group_id=%(port_group_id)s"
    df = db_utils.get_sql_df(query, {'port_group_id': port_group_id})
    
    if df.empty:
        default_limit = db_utils.get_sql_df(query, {'port_group_id': 0})
        default_limit['port_group_id'] = port_group_id
        db_utils.insert_df('limit_concentration', default_limit, key_column='port_group_id')
    else:
        print(f'Skipped! limit exists for port_group_id: {port_group_id}')
    
    
def test_add_limit_concentration():
    port_group_id = 1
    add_limit_concentration(port_group_id)




def test():
    df = db_utils.get_sql_df("select pgroup_id from portfolio_group")
    pgroup_id_list = df['pgroup_id'].to_list()
    
    for port_group_id in pgroup_id_list:
        print(f"port_group_id: {port_group_id}")
        add_limit_concentration(port_group_id)
    
    from database import model_aux        
    user = model_aux.get_user('test1@trg.com')
    print(user.webdashboard_login)
