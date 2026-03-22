# -*- coding: utf-8 -*-
"""
Created on Mon Mar  3 17:03:38 2025

@author: mgdin
"""
import pandas as pd
import numpy as np
import io
from api import app
from flask import request
from pathlib import Path

from trg_config import config
from security import security_info as sc
from models import equity_model, proxy_model, cash_model
from models import risk_factors
from utils import xl_utils, var_utils
from utils import mkt_data
from database import db_utils


def upload_security(file_path):

    results = {}
    
    # Read Excel file
    DATA = read_excel(file_path)
    
    params = DATA['params']

    # create new security
    if params['Security'] == 'Y':
        res = create_new_securities(DATA)
        results.update(res)
    
    # modeling risk for security
    if params['Model'] == 'Y':
        model, unmodeled_securities = model_securities(DATA)
        results['unmodeled_securities'] = unmodeled_securities
        results['model'] = model
    
    # upload security distribution
    if params['Distribution'] == 'Y':
        msg = save_distribution(DATA)
        results['dist'] = pd.DataFrame([{'Message': msg}])
    
    # upload security distribution
    if params['SecurityAttribute'] == 'Y':
        msg = save_security_attribute(DATA)
        results['security_attribute'] = pd.DataFrame([{'Message': msg}])

    # upload security distribution
    if params['RiskFactor'] == 'Y':
        msg = save_risk_factor(DATA)
        results['RiskFactor'] = pd.DataFrame([{'Message': msg}])
        
    book = construct_results(results)
    return book

def save_security_attribute(DATA):
    securities = DATA['securities']
    if 'SecurityID' not in securities:
        securities['SecurityID']=None
    idMap = securities.set_index('ID')['SecurityID'].to_dict()
    
    df = DATA['security_attribute']
    df['security_id'].fillna(df['ID'].map(idMap), inplace=True)

    if sum(df['security_id'].isna()) > 0:
        raise 'Missing SecurityID in security_attribute'
    
    db_utils.insert_df('security_attribute', df, key_column='security_id')
    return f"successfully upload security_attribute: {len(df)}"

def save_risk_factor(DATA):
    df = DATA['risk_factor']
    
    db_utils.delete_df('risk_factor', df)
    db_utils.insert_df('risk_factor', df)
    return f"successfully upload security_attribute: {len(df)}"

    
def save_distribution(DATA):
    df = DATA['dist']
    
    # save dist 
    var_utils.save_dist(df, category='PRICE')

    return f"successfully saved distribtion {df.shape}"
        
        
def create_new_securities(DATA):
    results = {}
    
    # add new securities to database
    securities = DATA['securities']
    new_securities, new_xref_df = sc.create_security_and_xref(securities)
    results['new_securities'] = new_securities
    results['new_xref_df'] = new_xref_df
    return results
    
def construct_results(results):
    book = {}
    for tab in ['new_securities', 'new_xref_df', 'unmodeled_securities', 'dist', 'security_attribute', 'RiskFactor']:
        if tab in results:
            df = results[tab]
            book[tab] = df

    
    stat = pd.DataFrame(columns=['Model', 'SecurityID'])
    securities = pd.DataFrame()
    rfactors = pd.DataFrame()
    sim_dist = pd.DataFrame()
    if 'model' in results:
        model_list = results['model']
        for name, model in model_list.items():
            # print(name)
            if 'Securities' in model:
                df = model['Securities']
                df['Model'] = name
                securities = pd.concat([securities, df])

            if 'stat_df' in model:
                df = model['stat_df']
                df['Model'] = name
                df = df.reset_index()
                stat = pd.concat([stat, df])
                
            if 'betas' in model:
                df = model['betas']
                df['Model'] = name
                df = df.reset_index()
                stat = pd.concat([stat, df])
                
            if 'risk_factors' in model:
                rf = model['risk_factors']
                rf['Model'] = name
                rfactors = pd.concat([rfactors, rf])
    
            if 'dist' in model:
                dist = model['dist']
                sim_dist = pd.concat([sim_dist, dist], axis=1)
            
        book['Securities'] = securities
        book['model_stats'] = stat
        book['risk_factors'] = rfactors
        book['sim_dist'] = sim_dist
    # xl_utils.write_book_to_xl(book, file_path)
    if not book:
        book['empty'] = pd.DataFrame([{'Message': 'Upload Security performed 0 tasks'}])
    return book

# read excel data
def read_excel(file_path):
    DATA = {}

    # read Parameters    
    params = read_params(file_path)
    DATA['params'] = params

    # read security
    if params['Security'] == 'Y':
        DATA['securities'] = read_security(file_path)

    # read hist
    if params['Model'] == 'Y':
        DATA['hist'] = read_hist(file_path)
    
        # read proxy
        proxy_securities = pd.read_excel(file_path, sheet_name='Proxy', dtype={'Correlation': float, 'Vol Multiple': float})
    
        # normalize column names
        proxy_securities.columns = [x.replace(' ', '') for x in proxy_securities.columns]
    
        DATA['proxy_securities'] = proxy_securities
    
    # read Distribution
    if params['Distribution'] == 'Y':
        DATA['dist'] = read_dist(file_path)

    # read Security Attribute
    if params['SecurityAttribute'] == 'Y':
        DATA['security_attribute'] = read_attribute(file_path)

    # read Security Attribute
    if params['RiskFactor'] == 'Y':
        DATA['risk_factor'] = read_risk_factor(file_path)
    
    return DATA
    
def read_security(file_path):
    
    # read new security data from tab 'Security'
    securities = pd.read_excel(file_path, sheet_name='Security', dtype={'ISIN': str, 'CUSIP': str, 'SEDOL': str} )

    # normalize column names
    securities.columns = [x.replace(' ', '') for x in securities.columns]

    # rename columns
    securities = securities.rename(columns={'Cusip': 'CUSIP'})
    
    if 'CUSIP' in securities:
        securities['CUSIP'] = securities['CUSIP'].apply(lambda x: x[-9:] if isinstance(x, str) else x) 
    
    return securities
    
def read_params(file_path):
    # read new security data from tab 'Security'
    df = pd.read_excel(file_path, sheet_name='Parameters')

    # only two columns
    df = df.iloc[:, :2]
    # drop blank value in the first column 
    params = df[~df.iloc[:,0].isna()].copy()   
    
    # convert params to dict
    params.index = params.iloc[:,0].apply(lambda x: x.replace(' ', ''))
    params = params.iloc[:,1].to_dict()
    
    return params

def read_hist(file_path):
    
    # read hist_prices, hist_yields from the file
    hist = {}
    
    
    prices = pd.read_excel(file_path, sheet_name='Prices', index_col=0)
    prices.index = pd.to_datetime(prices.index)
    prices = prices.dropna(axis=1, how='all')
    prices.replace(0, np.nan, inplace=True)
    hist['prices'] = prices
    

    yields = pd.read_excel(file_path, sheet_name='Yields', index_col=0)
    yields.index = pd.to_datetime(yields.index)
    yields = yields.dropna(axis=1, how='all')
    yields.replace(0, np.nan, inplace=True)
    hist['yields'] = yields
    
    return hist

def read_dist(file_path):
    
    # read dist from the file
    df = pd.read_excel(file_path, sheet_name='Distribution', index_col=0)
    
    return df

def read_attribute(file_path):
    df = pd.read_excel(file_path, sheet_name='SecurityAttribute', parse_dates=['maturity_date', ], 
                       dtype={'expected_return': 'float64', 'class': str, 'sc1': str})


    df['payment_frequency'] = df['payment_frequency'].fillna(0).astype('int64')

    return df
    
def read_risk_factor(file_path):
    df = pd.read_excel(file_path, sheet_name='RiskFactor')
    
    df = df.dropna(how='all') # drop rows with all NA
    
    return df

    
def model_securities(DATA):
    model = {}
    hist = DATA['hist']
    securities = DATA['securities']
    securities['SecurityID'] = sc.get_SecurityID_by_ref(securities)
    
    # track unmodeled securities
    missing = set(securities['SecurityID'])
    
    # Equity Model
    equities = filter_equity_model(securities, hist['prices'])
    if len(equities):
        missing = missing.difference(equities['SecurityID'])
        model['Equity'] = run_equity_model(equities, hist['prices'])

    # Bond Model
    bonds = filter_bond_model(securities, hist['yields'])
    if len(bonds):
        missing = missing.difference(bonds['SecurityID'])
        model['Bond'] = run_bond_model(bonds, hist['yields'])

    # Proxy Model
    proxy_equities = filter_proxy_model(securities, DATA['proxy_securities'])
    if len(proxy_equities):
        missing = missing.difference(proxy_equities['SecurityID'])
        model['Proxy'] = proxy_model.run_model(proxy_equities)

    # Cash Model
    cash_securities = securities[securities['Model']=='Cash']
    if len(cash_securities):
        missing = missing.difference(cash_securities['SecurityID'])
        model['Cash'] = cash_model.run_model(cash_securities)
    
    # unmodel    
    unmodeled_securities = securities[securities['SecurityID'].isin(missing)]

    # add riskfactor
    modeled_securities = securities[~securities['SecurityID'].isin(missing)]
    sec_ids = modeled_securities['SecurityID'].to_list()
    risk_factors.add_riskfactor_by_id(sec_ids)
    
    return model, unmodeled_securities

# hist_prices=hist['prices']
def run_equity_model(equities, hist_prices):
    
    # convert price ID to securityID
    id_map =  equities.set_index('ID')['SecurityID'].to_dict()
    hist_prices.rename(columns=id_map, inplace=True)

    # get equity prices
    eq_prices = hist_prices[equities['SecurityID']]
    
    # save prices to hdf
    save_hist_price_to_hdf(eq_prices)
    
    # run equity model
    DATA = equity_model.run_model(equities, eq_prices)    

    return DATA
    
def run_bond_model(bonds, hist_yields):
    # to be implemented
    pass
    

# hist_prices = hist['prices']
def filter_equity_model(securities, hist_prices):
    
    # idx1 = securities['AssetClass']=='Equity'
    # idx2 = (securities['AssetClass']=='Bond') & (securities['AssetType'].isin(['ETF', 'Fund']))
    # securities = securities[idx1 | idx2]
    securities =  securities[securities['Model']=='Equity']
    
    # securities that have hist_prices
    securities = securities[securities['ID'].isin(hist_prices.columns)]

    return securities


# hist_prices = hist['yields']
def filter_bond_model(securities, hist_yields):

    # idx = (securities['AssetClass']=='Bond') & (securities['AssetType'].isin(['Bond'])) & False
    # securities = securities[idx]
    securities =  securities[securities['Model']=='Bond']

    # securities that have hist_yields
    securities = securities[securities['ID'].isin(hist_yields.columns)]
    
    return securities


# hist_prices = hist['yields']
def filter_proxy_model(securities, proxy):

    # idx = (securities['AssetClass']=='Bond') & (securities['AssetType'].isin(['Bond'])) & False
    # securities = securities[idx]
    securities =  securities[securities['Model']=='Proxy']
    
    # common securities
    df = proxy.merge(securities[['ID', 'SecurityID']], on='ID', how='left')
    df = df[~df['SecurityID'].isna()]
    
    return df


def read_prices(file_path, securities):

    # check if there is any time series 
    df = pd.read_excel(file_path, sheet_name='Prices')
    if len(df.columns) == 1: # no time series provided
        return pd.DataFrame()

    # read prices
    # prices = pd.read_excel(file_path, sheet_name='Prices', header=[0,1], index_col=0)
    prices = pd.read_excel(file_path, sheet_name='Prices', index_col=0)
    prices.index = pd.to_datetime(prices.index)
    prices = prices.dropna(axis=1, how='all')
    prices.replace(0, np.nan, inplace=True)

    id_map =  securities.set_index('ID')['SecurityID'].to_dict()
    prices.rename(columns=id_map, inplace=True)
    
    return prices

def read_yields(file_path, securities):
    return pd.DataFrame()    

# read securities and hist prices from UploadSecurity excel file
# filename = 'ModelTest1.xlsx'
def read_upload_security(filename):
    
    file_path = config['MODEL_DIR'] / 'UploadFiles' / filename
    
    # read security
    secs = pd.read_excel(file_path, sheet_name='Security')
    secs['SecurityID'] = sc.get_SecurityID_by_ref(secs)
    if sum(secs['SecurityID'].isna()) > 0:
        missing = ';'.join(secs[secs['SecurityID'].isna()]['SecurityName'].to_list())
        raise Exception(f"Equity Model: Unknown Security: {missing}")

    # read prices
    prices = pd.read_excel(file_path, sheet_name='Prices', header=[0,1], index_col=0)
    prices.index = pd.to_datetime(prices.index)
    prices = prices.dropna(axis=1, how='all')
    prices.replace(0, np.nan, inplace=True)

    
    # create a ID map
    id_map = {}
    for col in ['ISIN', 'CUSIP', 'Ticker']:
        df = secs[~secs[col].isna()].set_index(col)
        df.index = [(col, x) for x in df.index]
        id_map.update(df['SecurityID'].to_dict())

    # map IDs in prices
    prices.columns = [id_map[col] for col in prices.columns]        	
    
    return secs, prices

def save_hist_price_to_hdf(df, source='YH', category='PRICE'):
    mkt_data.append_market_data(df, source, category)        


#######################################################################################################
#
# TEST  
#
import xlwings as xw    
def test():
    
    # file_path = Path.home() / 'Downloads' / 'ModelTest1.xlsx'
    file_path = config['TEST_DIR'] / 'upload_security' / 'UploadSecurity.7.xlsx'

    results = upload_security(file_path)

    # wb = xw.Book('Book2')
    # xl_utils.add_df_to_excel(new_securities, wb, 'new_securities', index=False)
