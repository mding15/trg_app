# -*- coding: utf-8 -*-
"""
Created on Mon Oct 13 17:11:08 2025

@author: mgdin
"""
import pandas as pd

# drop columns
def drop_columns(df, columns):
    columns = set(df.columns) & set(columns)
    return df.drop(columns=columns)

# to: fronat, end
# e.g. df_move_columns(df, ['SecurityID'], to='front'), will move the SecurityID column to the front
def df_move_columns(df, cols, to='front'):

    columns = [col for col in df if col not in cols]
    if to == 'front':
        columns = cols + columns
    else:
        columns = columns + cols
    
    return df[columns]    

#
# convert dict to dataframe
#
def dict_to_df(params, columns=['Name', 'Value']):
    df = pd.DataFrame.from_dict(params, orient='index', columns=columns[1:2])
    df.index.name = columns[0]
    
    return df

###############################################################################
# Test
def test():
    params = {'a': 'X', 'b': 'Y'}
    dict_to_df(params)
