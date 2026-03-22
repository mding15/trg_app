# -*- coding: utf-8 -*-
"""
Created on Wed Feb 28 17:39:20 2024

@author: mgdin
"""

from trgapp import config

# config folder
config_files = [
    'token1.json'
    ]



# data folder
data_files =[
    'security/SecurityID.csv',
    'security/security_xref.csv',
    'BB/HistData/sec_list.csv',
    'YF/yf_sec_list.csv',
    ]


def check_file_list(folder, file_list):
    for file in file_list:
        file_path = folder / file
        
        # make sure parent folder exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        if not file_path.exists():
            print('file not found:', file_path)
            
            
def check_files():
    check_file_list(config.CFG_DIR,  config_files)
    check_file_list(config.DATA_DIR, data_files)
    
    
    
