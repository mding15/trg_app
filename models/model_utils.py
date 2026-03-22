# -*- coding: utf-8 -*-
"""
Created on Sun Jan  7 20:40:29 2024

@author: mgdin
"""
import pandas as pd
import xlwings as xw
import datetime

from trg_config import config
from utils import xl_utils
from utils import mkt_data, var_utils, tools
from models import risk_factors

def create_var_model(model_params):
    model_id = model_params['Model ID']
    start_date = model_params['TS Start Date']
    end_date   = model_params['TS End Date']
    num_simulation = model_params['Number of Simulations']
    var_utils.create_var_model(model_id, start_date, end_date, num_simulation)
    
    # save model params to csv file
    model_dir = config['MODEL_DIR'] / model_params['Model ID'] / 'Model'
    if not model_dir.exists():
        model_dir.mkdir(parents=True, exist_ok=True)
    filename = model_dir/'Parameters.csv'
    tools.save_parameter_csv(model_params, filename)

def get_model_params(model_id=None):
    if model_id is None:
        model_id = var_utils.get_default_model_id()
    var_utils.set_model_id(model_id)
    df = var_utils.get_metadata()
    model_params = {'Model ID': model_id,
                    'TS Start Date': df['from_date'].iloc[0].to_pydatetime(),
                    'TS End Date': df['end_date'].iloc[0].to_pydatetime(),
                    'Number of Simulations': df['length'].iloc[0]
                    }
    
    return model_params        
    
# sub model data folder
def get_model_sub_dir(model_params):
    data_dir = config['MODEL_DIR'] / model_params['Model ID'] / model_params['Submodel ID']
    if not data_dir.exists():
        data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir

# read index dates        
def read_index_dates(model_params):

    file_path = config['MODEL_DIR'] / model_params['Model ID'] / 'Model' / 'index_dates.csv'
    df = pd.read_csv(file_path)
    dates = pd.to_datetime(df['Date'])
    index_dates = pd.DataFrame(index=dates)
    # start_dt, end_dt = index_dates.index[0], index_dates.index[-1]
    # print('start:', start_dt.strftime('%Y-%m-%d'), ' end:', end_dt.strftime('%Y-%m-%d'))
    # print('length:', len(index_dates))
    
    return index_dates

def save_index_dates(model_params, index_dates):
    file_path = config['MODEL_DIR'] / model_params['Model ID'] / 'Model' / 'index_dates.csv'
    index_dates.index.name='Date'
    index_dates.to_csv(file_path, index=True)
    print(f'save_index_dates: {file_path}')

def calc_ts_vol(prices):
    # calculate vol
    vol_df = pd.DataFrame(columns=['vol'])
    for sec_id in prices:
        df = prices[[sec_id]]
        vol = (df / df.shift(1)).dropna().std().iloc[0]
        vol_df.loc[sec_id] = vol

    return vol_df['vol']

# fill na with random sampling    
def fill_na_with_rand_sampling(df, replace=True):

    # sec_id = df.columns[0]
    for sec_id in df.columns:
        idx = df[sec_id].isna()
        n = sum(idx)
        rand_samples = df.loc[~idx, sec_id].sample(n=n, replace=replace).values
        df.loc[idx, sec_id] = rand_samples
    
    return df

def load_data(model_params):
    # create container
    DATA = {'Parameters': model_params}

    # add model data
    DATA['CoreFactors']  = load_corefactors(model_params)
    DATA['IndexDates']   = read_index_dates(model_params)
    DATA['CoreFactors_Prices'] = read_cf_timeseries(model_params)
    DATA['corefactor_dist']    = read_corefactor_dist(model_params)

    return DATA
    
def get_date_range(model_params):
    start_date, end_date = model_params['TS Start Date'], model_params['TS End Date']
    return start_date, end_date


####################################################################
# model_info
MODEL_INFO_COLUMNS = ['SecurityID', 'ModelID', 'SubModelID', 'SecurityName', 'AssetClass', 'AssetType', 'Currency', 'UpdateDate']
MODEL_INFO_FILEPATH = config['MODEL_DIR'] / 'model_info.csv'
def get_model_info():
    if not MODEL_INFO_FILEPATH.exists():
        info = pd.DataFrame(columns = MODEL_INFO_COLUMNS)
        info.to_csv(MODEL_INFO_FILEPATH, index=False)
    else:
        info = pd.read_csv(MODEL_INFO_FILEPATH)

    return info
    
def update_model_info(model_id, submodel_id, info):

    info['ModelID'] = model_id
    info['SubModelID'] = submodel_id
    info['UpdateDate'] = tools.today()
    
    columns = [x for x in MODEL_INFO_COLUMNS if x in info.columns]
    info = info[columns]

    # merge to current info    
    curr_info = get_model_info()
    curr_info = pd.concat([curr_info, info], ignore_index=True)    
    curr_info = curr_info.drop_duplicates(subset=['SecurityID', 'ModelID'], keep='last')
    curr_info.to_csv(MODEL_INFO_FILEPATH, index=False)
    
    
####################################################################
def save_model(DATA, skip_index=['Securities', 'CoreFactors']):
    for name, df in DATA.items():
        index = name not in skip_index
        save_model_data(DATA, name, index)


    
def save_model_data(DATA, name, index=False):
    model_params = DATA['Parameters']
    data_dir = get_model_sub_dir(model_params)
    file_path = data_dir / f'{name}.csv'

    if name in ['Parameters', 'config']:
        df = tools.dict_to_df(model_params)
        df.to_csv(file_path, index=False)
        print('saved to:', file_path)
    else:
        if name in DATA:
            df = DATA[name]    
            if df is not None:
                df.to_csv(file_path, index=index)
                print('saved to:', file_path)

def read_model_data(name, model_id, submodel_id):
    data_dir = config['MODEL_DIR'] / model_id / submodel_id
    file_path = data_dir / f'{name}.csv'
    df = pd.read_csv(file_path)
    return df

def save_model_info_data(DATA):
    model_params = DATA['Parameters']
    model_id = model_params['Model ID']
    submodel_id = model_params['Submodel ID']
    securities= DATA['Securities']
    update_model_info(model_id, submodel_id, securities)
    
    
def save_dist(DATA, dist, category='PRICE'):
    
    dist = dist.reset_index(drop=True)
    
    params = DATA['Parameters']
    model_id = params['Model ID']
    var_utils.set_model_id(model_id)
    var_utils.save_dist(dist, category=category)
    
    # save log
    save_model_info_data(DATA)



def read_Model_Parameters(model_id):
    file_path = config['MODEL_DIR'] / model_id / 'Model' / 'Parameters.csv'
    model_params = tools.read_parameter_csv(file_path) 
    model_params['TS Start Date'] = model_params['TS Start Date']
    model_params['TS End Date'] = model_params['TS End Date']
    model_params['Number of Simulations'] = int(model_params['Number of Simulations'])
        
    return model_params

def load_corefactors(model_params):
    file_path = config['MODEL_DIR'] / model_params['Model ID'] / 'Model' / 'CoreFactors.csv'
    if file_path.exists():
        df = pd.read_csv(file_path)
        if 'Model Type' in model_params:
            df = df[df['Model']==model_params['Model Type']]
    else:
        df = pd.DataFrame(columns=['Model',	'SecurityID', 'Ticker', 'Name', 'Data Type'])
        
    return df
    
def save_corefactors(model_params, corefactors):
    if 'Model' not in corefactors:
        raise "save_corefactors: Missing Model column"
        
    file_path = config['MODEL_DIR'] / model_params['Model ID'] / 'Model' / 'CoreFactors.csv'
    
    # read existing corefactors
    df = load_corefactors(model_params)
    
    # concat to exsiting corefactors, overwrite if the model_type exists
    df = df[~df['Model'].isin(corefactors['Model'])] 
    df = pd.concat([df, corefactors])
    
    # save to file
    df.to_csv(file_path, index=False)
    print(f'save_corefactors: {file_path}')    
    
def read_cf_timeseries(model_params):
    file_path = config['MODEL_DIR'] / model_params['Model ID'] / 'Model' / 'core_factor_timeseries.csv'
    df = pd.read_csv(file_path, index_col=0)
    df.index = pd.to_datetime(df.index)
    return df

def save_cf_timeseries(model_params, hist_prices):
    file_path = config['MODEL_DIR'] / model_params['Model ID'] / 'Model' / 'core_factor_timeseries.csv'
    hist_prices.to_csv(file_path, index=True)
    print(f'save_cf_timeseries: {file_path}')    

def read_corefactor_dist(model_params):
    file_path = config['MODEL_DIR'] / model_params['Model ID'] / 'Model' / 'corefactor_dist.csv'
    df = pd.read_csv(file_path, index_col=0)
    df.index = pd.to_datetime(df.index)
    return df

def save_corefactor_dist(model_params, dist):
    file_path = config['MODEL_DIR'] / model_params['Model ID'] / 'Model' / 'corefactor_dist.csv'
    dist.to_csv(file_path, index=True)
    print(f'save_corefactor_dist: {file_path}')    


def save_model_model_data(DATA, name, index=False):
    model_params = DATA['Parameters']
    file_path = config['MODEL_DIR'] / model_params['Model ID'] / 'Model' / f'{name}.csv'

    # this function does not save Parameters
    if name == 'Parameters':
        print('save_model_model_data: the function does not save Parameters')
        return
    
    if name in DATA:
        df = DATA[name]    
        if df is not None:
            df.to_csv(file_path, index=index)
            print('saved to:', file_path)
                

####
# db
from database import db_utils
def get_db_model_id(model_id):
    
    sql = f"select * from risk_model where model_name = '{model_id}'"
    df = db_utils.get_sql_df(sql)
    if len(df) > 0:
        return df['model_id'].iloc[0]
    else:
        return 0 

#
# update risk_factor db table    
#
def update_risk_factor(DATA, wb=None):
    
    params = DATA['Parameters']
    dist = DATA['dist']
    model_id = params['Model ID']
    model_type = params['Model Type']

    if model_type in ['Equity', 'PrivateEquity', 'PrivateCredit', 'RealEstate', 'Cash', 'Benchmark', 'StructuredNote']:
        category = 'DELTA'
    elif model_type in ['Credit_Benchmark', 'SpreadGeneric', 'Spread']:
        category = 'SPREAD'
    elif model_type in ['UF_Model']:    
        category = 'YIELD'
    elif model_type in ['FX']:    
        category = 'DELTA'
    else:
        raise Exception(f'update_risk_factor: unknown model_type {model_type}')


    sec_ids = dist.columns.to_list()
    db_model_id = get_db_model_id(model_id)

    # delta risk factors
    delta_rf = pd.DataFrame()
    
    delta_rf['SecurityID'] = sec_ids
    delta_rf['Category']   = category
    delta_rf['RF_ID']   = sec_ids  
    delta_rf['Sensitivity']   = 1
    delta_rf['model_id']   = db_model_id
    
    risk_factors.db_insert(delta_rf)
    
    DATA['risk_factors'] = delta_rf
    
    # if 'wb' in DATA:
        # wb = DATA['wb']
        
    if wb:        
        xl_utils.add_df_to_excel(delta_rf, wb, 'risk_factors', index=False)


########################## obsolete ################################################
# def read_Parameters(model_id, submodel_id):
     
#     df = read_model_data('Parameters', model_id, submodel_id)
#     params = df.set_index(df.columns[0]).iloc[:,0] .to_dict()
#     for key in params:
#         if key[-4:] == 'Date':
#             params[key] = date_utils.parse_date(params[key])
#         if 'Number' in key:
#             params[key] = int(params[key])
        
#     return params



# def get_date_range():
#     df = read_model_data('parameters.csv')
#     params = df.set_index('Name')['Value'].to_dict()
#     return params['Start Date'], params['End Date']

# def save_model_data(df, filename, version, index=False):
#     if df is not None:
#         filename = config['MODEL_DIR'] / version / filename
#         df.to_csv(filename, index=index)
#         print('saved to:', filename)

# def read_model_data(filename, version):
#     filename = config['MODEL_DIR'] / version / filename
#     df = pd.read_csv(filename)
#     return df

    
# def save_parameters(wb, version):
#     df = xl.read_df_from_excel(wb, 'Parameters')
#     save_model_data(df, 'parameters.csv', version)    

# def get_market_data(sec_ids):
#     start_date, end_date = get_date_range()
#     prices = mkt_data.get_market_data(sec_ids, start_date, end_date)

#     return prices

# def get_securities(wb):
#     df = xl.read_df_from_excel(wb, 'Securities', index=True)
#     return df

# def save_core_factors(wb, version):
#     df = xl.read_df_from_excel(wb, 'CoreFactors')
#     save_model_data(df, 'core_factors.csv', version)    

# def get_equity_factors():
#     return read_model_data('equity.csv')

# def test():
#     wb = xw.Book('EquityModel.v1.xlsx')
#     save_core_factors(wb)

    
