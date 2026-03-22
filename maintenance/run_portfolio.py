# -*- coding: utf-8 -*-
"""
Created on Sun Mar 30 23:15:10 2025

@author: mgdin
"""

import os
os.chdir(r'C:\Users\mgdin\OneDrive\Documents\dev\TRG_App\dev\trgapp')
import pandas as pd
import xlwings as xw
from pathlib import Path
from trg_config import config
from api import app
app.app_context().push()

from utils import api_utils
from api import portfolios

api_utils.set_local_host()
api_utils.set_remote_host()

# re-run portfolio
def rerun_portfolio():
    port_id = 4974
    api_utils.rerun_portfolio(port_id)
    
# upload portfolio
def upload_portfolio_as_user():
    username = 'mgding@gmail.com'
    port_file_path = Path.home() / 'Downloads' / 'Test1.xlsx'
    portfolios.test_upload_portfolio(port_file_path, username)
    