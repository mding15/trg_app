# -*- coding: utf-8 -*-
"""
Created on Mon Mar 25 12:42:31 2024

@author: mgdin
"""

import pandas as pd

from utils import var_utils

##################################################################################
# VaR Aggregation
def calc_pos_var(pl):
    sum_pl = pl.sum(axis=1)
    
    var = pd.DataFrame(var_utils.calc_VaR(pl))
    var['MarginalVaR'] = var_utils.calc_marginal_VaR(pl, sum_pl)
    return var, sum_pl

def calc_pos_tvar(pl):
    sum_pl = pl.sum(axis=1)
    var = pd.DataFrame(var_utils.calc_tVaR(pl))
    var['MarginalVaR'] = var_utils.calc_marginal_tVaR(pl, sum_pl)
    return var, sum_pl

def calc_agg_var(pos_pl):
    if len(pos_pl.columns) == 1:
        var, _ = calc_pos_tvar(pos_pl)
        return var, None
    
    data = []
    for x in pos_pl.columns:
        keys = x.split('|')
        data.append(['|'.join(keys[:-1]), x])
    hierarchy = pd.DataFrame(data, columns=['parent', 'path'])
    
    agg_pl = pd.DataFrame()
    agg_var = pd.DataFrame()
    for p in hierarchy['parent'].unique():
        print(p)
        
        pos_ids = hierarchy[hierarchy['parent']==p]['path']
        pl = pos_pl[pos_ids]
        df, tot_pl = calc_pos_tvar(pl)
        agg_var = pd.concat([agg_var, df])
        agg_pl = pd.concat([agg_pl, tot_pl.rename(p)], axis=1)

    return agg_var, agg_pl

# pl=pos_pl
def calc_agg_pl(pl):
    agg_pl = {}
    for col in set(pl.columns):
        # print(col)
        df = pl[col]
        if isinstance(df, pd.Series):
            agg_pl[col] = df
        else:
            agg_pl[col] = df.sum(axis=1)
        
    return pd.concat(agg_pl, axis=1)

# hierarchy=['AssetClass', 'AssetType', 'SecurityID']
# hierachy=['Rating']
def calc_hierarchy_var(DATA, hierarchy=['AssetClass', 'AssetType', 'SecurityID'], name='Root'):
    positions = DATA['Positions'].set_index('pos_id')
    PnL = DATA['PnL']
    PnL = PnL[positions.index]

    # add pos_id to hierarchy to ensure unique
    hier_keys = positions.loc[PnL.columns, hierarchy]
    paths = hier_keys.apply(lambda x: name + '|' + '|'.join(x), axis=1).to_list()
    pos_pl = pd.DataFrame(PnL.values, columns=paths)
    pos_pl = calc_agg_pl(pos_pl)

    agg_var = pd.DataFrame()  
    while True:
        if pos_pl is None:
            break
    
        var, pos_pl = calc_agg_var(pos_pl)
        agg_var = pd.concat([agg_var, var])
    
    agg_var.index.name = 'Hierarchy'
    return agg_var
