# -*- coding: utf-8 -*-
"""
Created on Thu Dec 26 21:01:21 2024

@author: mgdin

Description:
    1 - test user file uploading
    2 - test user file scupping
    
"""
import os
from trg_config import config
from api import app
from api import portfolios
from api import scrubbing_portfolio as scrub
from api import run_calculation as run_calc
from database.models import User
from database import model_aux



app.app_context().push()
def test():

    username = os.environ['test_username']
    filename =  'Model-1.20241227.013418.xlsx'
    port_name = 'Model 1'
    print(f'user: {username}')
    print(f'file: {filename}')
    print(f'portfolio: {port_name}')
    
    user = User.query.filter_by(username=username).first()
    client = user.client
    pgroup = model_aux.get_port_group(user)
    
    # save portfolio file to data/clients folder
    #file_path = portfolios.get_file_path(client.client_id, pgroup.pgroup_id, filename)
    
    # save portfolio to database
    # port_id = portfolios.portfolios.db_add_portfolio(user, client, pgroup, file_path)
        
    port = model_aux.get_portfolio_info(pgroup.pgroup_id, port_name)
    port_id = port.port_id
    print(f'port_id: {port_id}')
    #scrub.scrubbing_portfolio(port_id)
        
    run_calc.run_calculation(port_id, username)
    

    

