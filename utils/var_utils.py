# -*- coding: utf-8 -*-
"""
Created on Sun Jan  7 17:59:46 2024

@author: mgdin
"""
import pandas as pd
import numpy as np

from trg_config import config
from utils import hdf_utils
from clients import clients_model

# Distribution file
VaR_file  = config['VaR_DIR']  / 'VaR.M0.h5'

# default model_id
default_model_id_file = config['VaR_DIR'] / 'default_model_id.csv'

def get_default_model_id():
    if default_model_id_file.exists():
        default_model_id = pd.read_csv(default_model_id_file)['model_id'].iloc[0]
    else:
        default_model_id = 'M0'    

    return default_model_id


################################################################################
# set model_id before access to VaR_file
def set_model_id(model_id= None):
    global VaR_file
    if model_id is None:
        model_id = get_default_model_id()
        
    VaR_file = config['VaR_DIR']  / f'VaR.{model_id}.h5'
    if not VaR_file.exists():
        raise Exception(f'Cannot find file : {VaR_file}')
    else:
        print(f'VaR model is set to: {VaR_file}')

# set default model_id        
set_model_id()        


########
# meta data
# from_date = '2019-01-01'
# end_date = '2022-12-30'
# length = 1044

def save_metadata(from_date, end_date, length):
    from_date = pd.to_datetime(from_date)
    end_date  = pd.to_datetime(end_date)
    metadata = {
        'from_date'  : from_date,
        'end_date'   : end_date,
        'length': length,
        }
    
    df = pd.DataFrame([metadata])
    hdf_utils.save(df, 'META', VaR_file)
    
def get_metadata():
    return get_dist(['from_date', 'end_date', 'length'], 'META')
    
def create_var_file(from_date='2019-01-01', end_date='2022-12-30', length=1044):
    if VaR_file.exists():
        return
    
    print(f'creating VaR file: {VaR_file}')
    save_metadata(from_date, end_date, length)
    df = pd.DataFrame(index=range(length))
    df['T00000000'] = 0
    hdf_utils.save(df, 'PRICE', VaR_file)
    
def create_var_model(model_id, from_date='2019-01-01', end_date='2022-12-30', length=1044):
    global VaR_file
    VaR_file = config['VaR_DIR']  / f'VaR.{model_id}.h5'
    create_var_file(from_date, end_date, length)
    
def get_VaR_file():
    return VaR_file

def get_model_id():
    filename = get_VaR_file()
    model_id = filename.stem[4:]
    return model_id

    
def set_client_id(client_id):
    
    model_id = clients_model.get_model_id(client_id)
    default_model_id = get_default_model_id()
    if model_id is None:
        print(f'can not find model for client {client_id}, use default model [{default_model_id}]')
        model_id = default_model_id

    print(f'VaR model: {model_id}')    
    set_model_id(model_id)
    
# set_model_id('M_20240331')
def save_dist(dist, category='PRICE'):
    print(f'Saving: {VaR_file}')
    if category == 'DELTA':
        category = 'PRICE'
        
    metadata = get_metadata()
    length = metadata['length'][0]
    if len(dist) != length:
        input_len = len(dist)
        raise Exception(f'input distribution length ({input_len}) does not match meta length ({length})')
        
    dist = dist.reset_index(drop=True)
    
    hdf_utils.save(dist, category, VaR_file)

def get_dist(sec_ids, category='PRICE'):
    if category == 'DELTA':
        category = 'PRICE'
    elif category == 'VEGA':
        category = 'VOL'
        
    return hdf_utils.read(sec_ids, category, VaR_file)

def remove_dist(sec_ids, category='PRICE'):
    hdf_utils.remove(sec_ids, category, VaR_file)
    
    
def list_dist():
    with pd.HDFStore(VaR_file) as store:
        keys = ['/'.join(k.split('/')[-2:]) for k in store.keys()]

    df = pd.DataFrame([x.split('/') for x in keys], columns=['Category', 'SecurityID'])        
    return df

#########################################################################################################
# export / import
def export_dist(sec_ids, filename, category='PRICE'):
    dist = get_dist(sec_ids, category)
    dist.index.name = 'index'
    dist.to_csv(filename, index=True)

def import_dist(filename, category='PRICE'):
    dist = pd.read_csv(filename, index_col=0)
    save_dist(dist, category)
    
#########################################################################################################
# methods to calculate VaR

# CL = 0.95 # 95% confidence level
def calc_VaR(pl, CL=0.95):
    var = pd.DataFrame(pl.quantile(1-CL).rename('VaR')) * (-1)

    return var

# calculate tail VaR
def calc_tVaR(pl, CL=0.95):
    var = pl.quantile(1-CL)
    tvar = pd.DataFrame(columns=['tVaR'])

    #for i in range(len(var)):
    for i in var.index:
        df = pl[i]
        tvar.loc[i] = df.loc[df < var.loc[i]].mean() * (-1)
        
    return tvar 

# marginal VaR
def calc_marginal_VaR(pl, sum_pl=None, CL=0.95):
    window = 0.1
    ub, lb = (1-CL) * (1-window/2), (1-CL) * (1+window/2)
    n1, n2 = int(len(pl) * ub), int(len(pl) * lb)+1
    
    if sum_pl is None:
        sum_pl = pl.sum(axis=1).rename('Total')
    else:
        sum_pl = sum_pl.rename('Total')
        
    total_pl = pd.concat([pl, sum_pl], axis=1)
    m_var = total_pl.sort_values('Total').reset_index().iloc[n1:n2,1:].mean()
    scale = total_pl['Total'].quantile(1-CL) / m_var['Total']
    m_var = m_var * scale * (-1)
    return m_var

# marginal tail VaR
def calc_marginal_tVaR(pl, sum_pl=None, CL=0.95):
    
    if sum_pl is None:
        sum_pl = pl.sum(axis=1).rename('Total__')
    else:
        sum_pl = sum_pl.rename('Total__')
    
    var = sum_pl.quantile(1-CL)
    pl = pd.concat([pl, sum_pl], axis=1)

    tail_idx = pl.index[pl['Total__'] < var]
    mvar = pl.loc[tail_idx].mean() * -1 
    return mvar.iloc[:-1]

# marginal volatility =  Cov(x, total) / sigma(total)
def calc_marginal_vol(pnl):
    if pnl.empty:
        return None
    
    df = pd.concat([pnl, pnl.sum(axis=1).rename('total')], axis=1)
    cov = np.cov(df.T.astype('float32'))
    tot_vol = np.sqrt(cov[-1, -1]) # last column is total
    mg_vol = cov[:,-1] / tot_vol
    mg_vol = pd.DataFrame(mg_vol, index=df.columns, columns=['mgVol'])
    return mg_vol


# aggregate pnl
def calc_agg_pl(pos_pl):
    parents = []
    for x in pos_pl.columns:
        keys = x.split('|')
        parents.append(['|'.join(keys[:-1]), x])
    parents = pd.DataFrame(parents, columns=['parent', 'path'])
    
    agg_pl = pd.DataFrame()
    for p in parents['parent'].unique():
        paths = parents[parents['parent']==p]['path']
        pl = pos_pl[paths]
        pl_sum = pl.sum(axis=1).rename(p)
        agg_pl = pd.concat([agg_pl, pl_sum], axis=1)

    return agg_pl

def test_calc_agg_pl(positions, PnL):
    # Construct hierarchy P/L
    hierarchy = ['Class', 'SC1', 'SC2', 'SecurityID']
    for col in hierarchy:
        positions[col].fillna('', inplace=True)
    hier_keys = positions.loc[PnL.columns, hierarchy]
    paths = hier_keys.apply(lambda x: '|'.join(x), axis=1).to_list()
    pos_pl = pd.DataFrame(PnL.values, columns=paths)

#############################################################################
import sys
from contextlib import redirect_stdout
def cmd_list_dist():
    df = list_dist()
    with redirect_stdout(sys.__stdout__):  # Temporarily redirect to original stdout
        df.to_csv(sys.stdout, index=False)    
    
    
#############################################################################
def test():

    df = list_dist()
    
    sec_ids = ['T10000011']
    returns = get_dist(sec_ids)
    
    set_model_id('M0')
    set_model_id('C021350')
    
    client_id = 'C021350'
    set_client_id(client_id)

    from_date = '2019-05-10'
    end_date = '2023-04-28'
    length = 1000
    save_metadata(from_date, end_date, length)
    
    get_metadata()
    create_var_file()
    

    

#############################################################################
# Create default VaR_file if it does not exist
create_var_file()
