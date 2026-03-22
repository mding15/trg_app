# -*- coding: utf-8 -*-
"""
Created on Tue Jun 25 16:54:55 2024

@author: mgdin
"""

import pandas as pd
import xlwings as xw

from trg_config import config
from security import security_info
from models import bond_risk as br
from models import model_utils
from utils import var_utils
from database import db_utils


rf_columns = ['model_id', 'SecurityID', 'Category', 'RF_ID', 'Sensitivity']

def run():
    
    generate_delta_risk_factors()
    
    generate_spread_risk_factors()
    
    # view risk factors
    wb = xw.Book('Book1')
    df = read_risk_factor()
    xl_utils.add_df_to_excel(df, wb, 'riskfactors', index=True)
    
# based on security_info table, create entries for riskfactors and insert them into database    
def add_riskfactor_by_id(sec_list):
    securities = security_info.get_security_by_ID(sec_list)
    
    rf1 = gen_delta_riskfactors(securities)
    rf2 = gen_spread_riskfactors(securities)
    
    rf = pd.concat([rf1, rf2])
    rf = add_model_id(rf)
    
    db_insert(rf)
    

def gen_delta_riskfactors(securities):
    
    idx1 = securities['AssetClass'] == 'Equity'
    idx2 = (securities['AssetClass'] == 'Bond') & securities['AssetType'].isin(['Fund', 'ETF'])
    idx3 = securities['AssetClass'] == 'Alternative'
    idx4 = securities['AssetClass'] == 'Commodity'
    idx5 = securities['AssetClass'] == 'REIT'
    idx6 = securities['AssetClass'] == 'Cash'
    
    idx = idx1 | idx2 | idx3 | idx4 | idx5 | idx6

    sec_ids = securities.loc[idx, 'SecurityID'].to_list()
    
    # delta risk factors
    delta_rf = pd.DataFrame(columns=rf_columns)
    delta_rf['SecurityID'] = sec_ids
    delta_rf['Category']   = 'DELTA'    
    delta_rf['RF_ID']   = sec_ids  
    delta_rf['Sensitivity']   = 1
    
    return delta_rf

def gen_spread_riskfactors(securities):

    idx = (securities['AssetClass'] == 'Bond') & securities['AssetType'].isin(['Bond'])    
    
    sec_ids = securities.loc[idx, 'SecurityID'].to_list()
    
    # spread risk factors
    spread_rf = pd.DataFrame(columns=rf_columns)
    spread_rf['SecurityID'] = sec_ids
    spread_rf['Category']   = 'SPREAD'    
    spread_rf['RF_ID']   = sec_ids  
    spread_rf['Sensitivity']   = 1
    
    return spread_rf
    
# return risk factors for given securityIDs
def get_risk_factors(sec_ids):
    risk_factors = read_risk_factor()
    return risk_factors[risk_factors['SecurityID'].isin(sec_ids)]

# equity, bond bund/ETF, alternatives
def generate_delta_risk_factors():

    # equity
    sec_ids = security_info.get_ID_by_AssetClass('Equity')
    
    # Bond ETF/Fund
    ids = security_info.get_ID_by_AssetClass('Bond')
    securities = security_info.get_security_by_ID(ids)
    securities = securities[securities['AssetType'].isin(['ETF', 'Fund'])]
    sec_ids = set(sec_ids) | set(securities['SecurityID'])
    
    # 'Alternative'
    ids = security_info.get_ID_by_AssetClass('Alternative')
    sec_ids = set(sec_ids) | set(ids)

    # 'Commodity'
    ids = security_info.get_ID_by_AssetClass('Commodity')
    sec_ids = set(sec_ids) | set(ids)

    # 'REIT'
    ids = security_info.get_ID_by_AssetClass('REIT')
    sec_ids = set(sec_ids) | set(ids)

    # cash
    ids = security_info.get_ID_by_AssetClass('Cash')
    sec_ids = set(sec_ids) | set(ids)

    # convert to list
    sec_ids = list(sec_ids)
    
    # get dist
    dist_ids = var_utils.list_dist()
    dist_ids = dist_ids[dist_ids['Category']=='PRICE']['SecurityID'].to_list()
    
    # sec_ids that have distribution
    sec_ids = list(set(sec_ids) & set(dist_ids))


    # delta risk factors
    delta_rf = pd.DataFrame(columns=rf_columns)
    delta_rf['SecurityID'] = sec_ids
    delta_rf['Category']   = 'DELTA'    
    delta_rf['RF_ID']   = sec_ids  
    delta_rf['Sensitivity']   = 1
    
    
    return delta_rf
    

    
    
def generate_spread_risk_factors():
    sec_ids = security_info.get_ID_by_AssetClass('Bond')
    securities = security_info.get_security_by_ID(sec_ids)
    bonds = securities[securities['AssetType']=='Bond']

    dist_ids = var_utils.list_dist()
    dist_ids = dist_ids[dist_ids['Category']=='SPREAD']['SecurityID'].to_list()
    
    # bonds that have distribution
    sec_ids = list(set(bonds['SecurityID']) & set(dist_ids))
    
    # spread risk factors
    spread_rf = pd.DataFrame(columns=rf_columns)
    spread_rf['SecurityID'] = sec_ids
    spread_rf['Category']   = 'SPREAD'    
    spread_rf['RF_ID']   = sec_ids  
    spread_rf['Sensitivity']   = 1
    
    return spread_rf
    
#sec_ids = ['T10001063']
def spread_risk_factors(sec_ids):
    risk_factors = read_risk_factor()
    # spread_risk_factors
    risk_factors = risk_factors[risk_factors['Category'] == 'SPREAD']
    
    risk_factors = risk_factors[risk_factors['SecurityID'].isin(sec_ids)]
    
    return risk_factors



#######################################################################################
# riskfactor_df = ['SecurityID', 'Category', 'RF_ID', 'Sensitivity']
def add_model_id(riskfactor_df):
    model_id = var_utils.get_default_model_id()
    db_model_id = model_utils.get_db_model_id(model_id)
    riskfactor_df['model_id'] = db_model_id
    return riskfactor_df


# df = risk_factor['model_id', 'SecurityID', 'Category', 'RF_ID', 'Sensitivity']
def db_insert(df):

    # delete from db
    key_df = df[['model_id', 'SecurityID', 'Category']]
    db_utils.delete_df('risk_factor', key_df)    

    # insert into db
    db_utils.insert_df('risk_factor', df)


def db_test():
    # create a model
    df = pd.DataFrame([{'model_name': 'M_20240531', 'description': 'originally created in files'}])
    db_utils.insert_df('risk_model', df)
        
    # collect all DELTA risk-factors
    df = generate_delta_risk_factors()
    df['model_id']=1
    db_utils.insert_bulk_df('risk_factor', df) 
    
    # collect all DELTA risk-factors
    df = generate_spread_risk_factors()
    df['model_id']=1
    db_utils.insert_bulk_df('risk_factor', df) 

    
    wb = xw.Book()
    xl_utils.add_df_to_excel(df, wb, 'spread')

def read_risk_factor(model_id=None):
    
    if not model_id:
        model_id = var_utils.get_default_model_id()
    
    # risk factor from database    
    sql = """
    select rf.* from risk_factor rf, risk_model rm
    where rf.model_id = rm.model_id and rm.model_name=%(model_id)s
    """
    
    rf = db_utils.get_sql_df(sql, params={'model_id': model_id})
    return rf
    


def empty_risk_factors():
    return pd.DataFrame(columns=rf_columns)

# calculate on-demand    
# positions = df(index='SecurityID', columns=['IR_Tenor', 'Duration'])
# positions = pd.DataFrame({'SecurityID': ['ID1', 'ID2'], 'Currency':['USD', 'USD'], 'IR_Tenor':[1.5, 4.5], 'Duration': [1.1,3.8]})
# positions = bonds
def ir_risk_factors(positions):
    
    # only USD for now
    positions = positions[positions['Currency']=='USD']
    
    # exclude matured bonds
    positions = positions[positions['IR_Tenor']>0]
    
    if len(positions) == 0:
        return empty_risk_factors()

    rf = empty_risk_factors()
    rf['SecurityID'] = positions['SecurityID']
    rf['Category'] = 'IR'
    rf['Tenor'] = positions['IR_Tenor']

    bins   = [0] + br.ust_tenors['Tenor'].to_list()
    bins[-1] = 100

    labels = ['UST0M'] + br.ust_tenors.iloc[:-1]['SecurityID'].to_list()
    rf['T1'] = pd.cut(rf.Tenor, bins=bins, labels=labels)

    labels = br.ust_tenors['SecurityID'].to_list()
    rf['T2'] = pd.cut(rf.Tenor, bins=bins, labels=labels)

    rf['w1'] = rf[~rf.Tenor.isna()].apply(lambda x: br.calc_w1(x.Tenor, x.T1, x.T2), axis=1)
    rf['w2'] = 1 - rf['w1']

    rf1 = rf[['SecurityID', 'Category', 'T1', 'w1']].rename(columns={'T1':'RF_ID', 'w1': 'Sensitivity'})
    rf2 = rf[['SecurityID', 'Category', 'T2', 'w2']].rename(columns={'T2':'RF_ID', 'w2': 'Sensitivity'})
    
    risk_factors = pd.concat([rf1, rf2], ignore_index=True)


    # remove UST0M
    risk_factors = risk_factors[risk_factors.RF_ID != 'UST0M']

    # remove zero sensitivity
    risk_factors = risk_factors[risk_factors['Sensitivity'] > 0]
    
    return risk_factors


def fx_risk_factors(positions, base_ccy='USD'):
    positions = positions.drop_duplicates(subset=['SecurityID'], keep='first')
    risk_factors = empty_risk_factors()
    # first add fx risk factor for non USD currency
    positions_x = positions[positions['Currency'] != 'USD']

    if len(positions_x) > 0:
        rf_usd = positions_x[['SecurityID', 'Currency']].copy()
        rf_usd['Ticker'] = 'USD/' + rf_usd['Currency']
        rf_usd['RF_ID'] = security_info.get_SecurityID_by_ref(rf_usd)
        # check missing RF_ID
        if sum(rf_usd['RF_ID'].isna()) > 0:
            missing = rf_usd[rf_usd['RF_ID'].isna()]['Ticker'].unique().tolist()
            raise Exception(f'missing currency pairs: {missing}')
        rf_usd['Sensitivity'] = 1
        rf_usd['Category'] = 'FX'
        rf_usd['model_id'] = 0
        
        risk_factors = pd.concat([risk_factors, rf_usd[rf_columns]], ignore_index=True)
    
    # add base currency risk factors
    if base_ccy != 'USD':
        positions_x = positions[positions['Currency'] != base_ccy]
        if len(positions_x) > 0:
            rf_id = security_info.get_ID_by_Ticker(['USD/' + base_ccy])
            if len(rf_id) == 0:
                raise Exception(f'missing currency pairs: USD/{base_ccy}')

            rf_id = rf_id['SecurityID'].iloc[0]
            rf_base = positions_x[['SecurityID']].copy()
            rf_base['RF_ID'] = rf_id
            rf_base['Sensitivity'] = -1
            rf_base['Category'] = 'FX'
            rf_base['model_id'] = 0
            
            # delete USD/base_ccy from rf_usd
            risk_factors = risk_factors[risk_factors['RF_ID'] != rf_id]
            
            rf_base = rf_base[rf_columns]
            risk_factors = pd.concat([risk_factors, rf_base], ignore_index=True)
    
    return risk_factors
    
######################
# return all SecurityID that has been modeled
def get_modeled_securities():

    # risk factor from database    
    sql = """
    select rf.* from risk_factor rf, risk_model rm
    where rf.model_id = rm.model_id and rm.is_current=1
    """
    
    rf = db_utils.get_sql_df(sql)
    df = rf[['SecurityID']]

    # treasury
    sec = security_info.securities_by_asset_class_type(asset_class='Bond', asset_type='Treasury')
    df = pd.concat([df, sec[['SecurityID']]], ignore_index=True)
    
    # add fx risk factors
    # rf_id = security_info.get_ID_by_Ticker(['USD/' + base_ccy])
    
    # add equity option risk factors
    # option_factors = eq_option_var.risk_factors(positions)
    # rfactors = pd.concat([rfactors, option_factors])

    df = df.drop_duplicates(subset=['SecurityID'], keep='first')
    
    return df['SecurityID'].unique()
    
    
######################
# test
import xlwings as xw
from utils import xl_utils
def test():
    df = read_risk_factor()
    wb= xw.Book()
    xl_utils.add_df_to_excel(df, wb, 'risk_factors', index=False)
    
    modeled_secs = get_modeled_securities()
    len(modeled_secs)
