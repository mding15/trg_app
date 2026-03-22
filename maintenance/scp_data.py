# -*- coding: utf-8 -*-
"""
Created on Tue Feb 18 13:56:00 2025

@author: mgdin

pip install paramiko scp

"""
import datetime
from pathlib import Path
from database import model_aux
from utils import scp_utils
from trg_config import config

AWS_HOME = Path(r'/home/ec2-user/api/trgapp')
SRC_HOME = Path(r'C:\Users\mgdin\OneDrive\Documents\dev\TRG_App\dev\trgapp')

def copy_code():
    
    local_file = SRC_HOME / 'tests' / 'test.py'

    scp_utils.LOCAL_TO_AWS(local_file, AWS_HOME / local_file.parent.name, 'dev2' )



def copy_clients_file():
    username = 'pcontreras@tailriskglobal.com'
    username = 'test1@trg.com'
    username = 'mgding@gmail.com'
    user = model_aux.get_user(username)

    user_id = 1069
    user = model_aux.get_user_by_id(user_id)
    
    port_id = 4932
    user = model_aux.get_user_by_port_id(port_id)
    
    client = user.client    
    client_id = client.client_id
    
    scp_utils.copy_client_portfolio(client_id)


# Cautious!!!
def COPY_LOCAL_TO_AWS():
    local_file = config['PUBLIC_DIR'] / 'input_template.xlsx'
    local_file = config['PUBLIC_DIR'] / 'security_upload_template.xlsx'
    aws_path = Path('/home/ec2-user/api/data/public')
    
    scp_utils.LOCAL_TO_AWS(local_file, aws_path)

def COPY_REFD_TO_AWS():
    local_file = config['REFD_DIR'] / 'DimClasses.xlsx'
    aws_path = Path('/home/ec2-user/api/data/reference_data')
    
    scp_utils.LOCAL_TO_AWS(local_file, aws_path)
    