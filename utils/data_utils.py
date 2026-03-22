# -*- coding: utf-8 -*-
"""
Created on Tue May 14 17:07:25 2024

@author: mgdin
"""

import pandas as pd
from trg_config import config
from database import db_utils

# name = 'private_equity'
# name = 'static_data'
def load_stat(name):
    
    df = db_utils.get_sql_df(f"select * from stat_{name}")

    df = df.set_index(df.columns[0])
    return df

##################################################################################
# !!! Decommissioned !!!!
def load_stat2(name):
    # file path
    filepath = stat_filepath(name)
    
    if not filepath.exists():
        return pd.DataFrame()
    else:
        # df1 = pd.read_csv(filepath, index_col=0)
        return pd.read_csv(filepath, index_col=0)
    
# name = 'private_equity'
def save_stat(df, name):
    # load stat
    stat = load_stat(name)
    
    # append
    stat = pd.concat([stat, df])
        
    # drop duplicate index, keep the new ones
    stat = stat.loc[~stat.index.duplicated(keep='last')]
    
    # save
    filepath = stat_filepath(name)
    stat.to_csv(filepath, index=True)
    print('saved to {filepath}', len(stat))

# file path
def stat_filepath(name):
    filepath = config['STAT_DIR'] / (name + '.csv')
    return filepath
    