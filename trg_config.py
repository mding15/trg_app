# -*- coding: utf-8 -*-
"""
Created on Tue Jul 25 16:06:30 2023

@author: mgdin
"""
from pathlib import Path
import os
import json
import pandas as pd
import datetime 

current_dir = Path(os.path.dirname(__file__))
# current_dir = Path(os.getcwd())

config = {}

config['SRC_DIR']     = current_dir
config['HOME_DIR']    = current_dir.parent
config['DATA_DIR']    = config['HOME_DIR'] / 'data'
config['CNFG_DIR']    = config['HOME_DIR'] / 'config'
config['LOG_DIR']      = config['HOME_DIR'] / 'log'
config['TEMPLATES_DIR'] = config['SRC_DIR'] / 'templates'
config['DEBUG'] = False

# data dir
def set_data_dir():
    config['MKTD_DIR']    = config['DATA_DIR'] / 'market_data'
    config['MODEL_DIR']   = config['DATA_DIR'] / 'model'
    config['SEC_DIR']     = config['DATA_DIR'] / 'security'
    config['PORT_DIR']    = config['DATA_DIR'] / 'portfolios'
    config['REPT_DIR']    = config['DATA_DIR'] / 'reports'
    config['STAT_DIR']    = config['DATA_DIR'] / 'stat'
    config['REFD_DIR']    = config['DATA_DIR'] / 'reference_data'
    config['CLIENT_DIR']  = config['DATA_DIR'] / 'clients'
    config['VaR_DIR']     = config['DATA_DIR'] / 'var'
    config['BB_DIR']      = config['DATA_DIR'] / 'BB'
    config['YF_DIR']      = config['DATA_DIR'] / 'YF'
    config['YH_DIR']      = config['DATA_DIR'] / 'YH'
    config['DB_DIR']      = config['DATA_DIR'] / 'database'
    config['PUBLIC_DIR']  = config['DATA_DIR'] / 'public'
    config['mkt_file']    = config['MKTD_DIR'] / 'market_data.daily.h5'
    config['sqlite_db']   = config['DB_DIR']   / 'sqlite.db'
    config['bsk_file']    = config['MKTD_DIR'] / 'equity_basket.csv'
    config['TEST_DIR']    = config['DATA_DIR'] / 'test' 
    
    # create directory
    for name in config:
        if name[-4:] == '_DIR':
            folder = config[name]
            if not folder.exists():
                folder.mkdir(parents=True, exist_ok=True)
                print('making directory:', folder)

################################################################################################    
# App config
if 'TRG_CONFIG_FILE' in os.environ:
    config_file = os.environ['TRG_CONFIG_FILE']
else:
    config_file = config['CNFG_DIR'] / 'app_config.json'
    
with open(config_file) as f:
    app_config = json.load(f)
    config.update(app_config)


os.environ["DB_USERNAME"] = app_config["DB_USERNAME"]
os.environ["DB_PASSWORD"] = app_config["DB_PASSWORD"]

os.environ["PRS_USERNAME"] = app_config["PRS_USERNAME"]
os.environ["PRS_PASSWORD"] = app_config["PRS_PASSWORD"]

os.environ["EMAIL_PASSWORD"] = app_config["EMAIL_PASSWORD"]
os.environ["EMAIL_KEY"] = app_config["EMAIL_KEY"]



################################################################################################    
# support_team.json
config_file = config['CNFG_DIR'] / 'support_team.json'
    
if config_file.exists():
    with open(config_file) as f:
        suport = json.load(f)
    config['SUPPORT_EMAILS'] = suport['SUPPORT_EMAIL']
else:
    print(f'Error: file is missing: {config_file}')
    config['SUPPORT_EMAILS'] = ['info@tailriskglobal.com']
    
    

################################################################################################    
# override 
if 'TRG_OVERRIDE' in os.environ:
    config_override_file =Path(os.environ['TRG_OVERRIDE'])
else:
    config_override_file = config['CNFG_DIR'] / 'config_override.json'
    
if config_override_file.exists():
    with open(config_override_file) as f:
        override = json.load(f)

    for name, file_path in override.items():
        if name in config:
            if isinstance(config[name], Path):
                override[name] = Path(file_path)
        
    config.update(override)


    

#########################################################################################################    
set_data_dir()
ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f'{ts} DATA_DIR:', config['DATA_DIR'])

