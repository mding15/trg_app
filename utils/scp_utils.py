# -*- coding: utf-8 -*-
"""
Created on Sat Mar  8 09:56:06 2025

@author: mgdin

pip install paramiko scp

"""

import paramiko
from scp import SCPClient
from pathlib import Path

from trg_config import config
from detl import yh_extract

hosts = {
    'prod2': 'ec2-54-86-24-102.compute-1.amazonaws.com',
    'dev2' : 'ec2-54-82-69-244.compute-1.amazonaws.com'
    }

keys = {
        'prod2': r"C:\Users\mgdin\local\AWS\KeyPairs\prod2.pem",
        'dev2':  r"C:\Users\mgdin\.ssh\id_rsa"
        }

def test():
    
    client_id = '1020'
    
    copy_client_portfolio(client_id)


###############################################################################################
# copy YH price file from AWS to local
def copy_YH(today):
    
    # today = datetime.datetime.now()    
    file_path = yh_extract.get_eod_file_path(today)
    
    data_dir = config['DATA_DIR']
    aws_file = Path('/home/ec2-user/api/data') / file_path.relative_to(data_dir)
    local_path =  file_path.parent   
    
    aws_to_local(aws_file, local_path)


# copy portfolio file from AWS to local
def copy_client_portfolio(client_id):
    
    # client_id = '1015'
    
    aws_dir = Path('/home/ec2-user/api/data/clients')
    aws_path = aws_dir / f'{client_id}' 

    local_path = config['CLIENT_DIR']
    
    aws_to_local(aws_path, local_path)

###############################################################################################
# use this with CAUTIOUS!!!
def LOCAL_TO_AWS(local_path, aws_path, server='prod2'):
    aws_path = aws_path.as_posix()
    
    ssh_client = create_ssh_client(server)

    """Copy file from local to AWS EC2 """
    with SCPClient(ssh_client.get_transport()) as scp:
        scp.put(local_path, aws_path, recursive=True)
    print(f"Copy from {local_path} to {aws_path}")

def aws_to_local(aws_path, local_path, server='prod2'):
    aws_path = aws_path.as_posix()
    
    ssh_client = create_ssh_client(server)

    """Copy file from AWS EC2 to Windows."""
    with SCPClient(ssh_client.get_transport()) as scp:
        scp.get(aws_path, local_path, recursive=True)
    print(f"Copy from {aws_path} to {local_path}")

def create_ssh_client(server):
    host = hosts[server]
    user = "ec2-user"
    private_key_path = keys[server]
    
    print(f'server: {server}')
    print(f'host: {host}')
    
    """Create an SSH client and connect to the remote host."""
    key = paramiko.RSAKey(filename=private_key_path)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=host, username=user, pkey=key)
    return ssh