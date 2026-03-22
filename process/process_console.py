# -*- coding: utf-8 -*-
"""
Created on Fri Jun  6 14:48:51 2025

@author: mgdin
"""

from process import process

def console():
    
    # run account calculation
    account_id = 1005
    as_of_date = '2024-01-31'
    process.process_account(account_id, as_of_date)
