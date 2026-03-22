# -*- coding: utf-8 -*-
"""
Created on Sun Mar  3 16:16:25 2024

@author: mgdin
"""

import pandas as pd
import xlwings as xw

from trgapp import config
import trgapp.utils.xl_utils as xl
import trgapp.utils.handy as hd


SECURITY_ID_FILE    = config.SEC_DIR / 'SecurityID.csv'
SECURITY_XREF_FILE  = config.SEC_DIR / 'security_xref.csv'

security_ids  = pd.read_csv(SECURITY_ID_FILE)
security_xref = pd.read_csv(SECURITY_XREF_FILE)

# get all SecurityID
def get_SecurityID():
    global security_ids
    return security_ids

# security xref
def get_xref():
    global security_xref
    return security_xref

# internal use only
# overwrite the csv file with new_sec_ids
def _save_IDs(new_sec_ids):
    global security_ids
    security_ids = new_sec_ids
    security_ids.to_csv(SECURITY_ID_FILE, index=False)
    

# internal use only
# overwrite the csv file with new xref
def _save_xref(xref):
    global security_xref
    security_xref = xref
    xref.to_csv(SECURITY_XREF_FILE, index=False)

# internal use only
# generate n new securityID's
# caution: there is no lock here
def _gen_n_IDs(n):
    sec_ids = get_SecurityID()
    max_id = sec_ids['SecurityID'].max()
    max_int_id = int(max_id[1:])
    return [f'T{max_int_id+1+i}' for i in range(n)]
    
# USE THIS with caution!!!
# delete Security by ID
def _DELETE_SecurityIDs(secIDs):
    sec_ids = get_SecurityID()
    n = sec_ids['SecurityID'].isin(secIDs).sum()
    sec_ids = sec_ids[~sec_ids['SecurityID'].isin(secIDs)]
    _save_IDs(sec_ids)
    print('Deleted securities:', n)


# return securities with listed xref_ids
def get_securities(IDs, xrefs=['ISIN', 'CUSIP', 'Ticker', 'YF_ID']):
    securities = get_SecurityID()
    securities = securities[securities['SecurityID'].isin(IDs)]
    
    security_xref = get_xref()
    for ref_type in xrefs:
        idx = security_xref['REF_TYPE']==ref_type
        df = security_xref.loc[idx, ['SecurityID', 'REF_ID']].rename(columns={'REF_ID':ref_type})
        securities = securities.merge(df, on='SecurityID', how='left')

    return securities

# return all securities with a given ref_type = [ISIN, CUSIP, Ticker, YF_ID]
def securities_by_ref(ref_type):
    xref = get_xref()
    xref = xref[xref['REF_TYPE']==ref_type]
    xref = xref[['SecurityID', 'REF_ID']].rename(columns={'REF_ID':ref_type})
        
    securities = get_SecurityID()
    securities = xref.merge(securities, on='SecurityID', how='inner')
    
    return securities
        
# ref_type: str
# ref_ids: pd.Series
def get_IDs_by_ref(ref_type, ref_ids):
    xref = get_xref()
    xref = xref[xref['REF_TYPE']==ref_type]        
    xref = xref[xref['REF_ID'].isin(ref_ids)]
    if len(xref) == 0:
        return pd.Series()
    
    idx = xref['REF_TYPE']==ref_type
    xref = xref.loc[idx, ['SecurityID', 'REF_ID']]
    
    df = pd.DataFrame(ref_ids.rename('REF_ID'))
    df = df[~df['REF_ID'].isna()]
    df = df.reset_index().merge(xref, on='REF_ID', how='left').set_index('index')
    
    return df['SecurityID']

            

# positions is a DataFrame
def add_ID_to_position(positions):

    positions['SecurityID'] = None
    
    df = positions
    idx = df['SecurityID'].isna()
    if (len(idx)>0) & ('ISIN' in df):
        df.loc[idx, 'SecurityID'] = get_IDs_by_ref('ISIN', df['ISIN'])
    
    idx = df['SecurityID'].isna()
    if (len(idx)>0) & ('CUSIP' in df):
        df.loc[idx, 'SecurityID'] = get_IDs_by_ref('CUSIP', df['CUSIP'])
    
    idx = df['SecurityID'].isna()
    if (len(idx)>0) & ('Ticker' in df):
        df.loc[idx, 'SecurityID'] = get_IDs_by_ref('Ticker', df['Ticker'])

    idx = df['SecurityID'].isna()
    if (len(idx)>0) & ('YF_ID' in df):
        df.loc[idx, 'SecurityID'] = get_IDs_by_ref('YF_ID', df['YF_ID'])



def add_security(new_securities):
    security_ids = get_SecurityID()
    columns = security_ids.columns.to_list()
    
    n = len(new_securities)
    if n > 0:
        new_securities['SecurityID'] = _gen_n_IDs(n)
        new_securities['AddedDate'] = hd.timestamp()
    
        missing = set(columns).difference (new_securities)
        if len(missing) > 0:
            print('New Security: missing columns', list(missing))
        else:
            new_securities = new_securities[columns]
            security_ids = pd.concat([security_ids, new_securities])
            _save_IDs(security_ids)
            print('added new securities:', len(new_securities))
    
def add_xref(new_securities):
    xref = get_xref()
    n = len(xref)

    for ref_type in ['ISIN', 'CUSIP', 'Ticker', 'YF_ID']:
        if ref_type in new_securities:
            df = new_securities[['SecurityID', ref_type, 'Source']].rename(columns={ref_type:'REF_ID', 'Source':'SOURCE'})
            df = df[~df['REF_ID'].isna()].copy()
            if len(df) > 0:
                df['REF_TYPE'] = ref_type
                df['DATE_ADDED'] = hd.timestamp()
                xref = pd.concat([xref, df], ignore_index=True)

    diff = len(xref) - n
    if diff > 0:
        _save_xref(xref)
        print('added xref:', diff)

# work book functins        
def wb_search(wb):
    wb = xw.Book('SecurityManager.xlsm')
    positions = xl.read_df_from_excel(wb, 'Search')
    positions = positions.iloc[:,:5].copy()
    add_ID_to_position(positions)
    positions.columns = ['x'+c for c in positions]
    positions.rename(columns={'xSecurityID':'SecurityID'}, inplace=True)
    df = get_securities(positions['SecurityID'])
    positions = positions.merge(df, on='SecurityID', how='left')
    
    positions.columns = [c[1:] if c[0]=='x' else c for c in positions]
    xl.add_df_to_excel(positions, wb, 'Search', index=False)    

def wb_add_securities(wb):
    wb = xw.Book('SecurityManager.xlsm')
    securities = xl.read_df_from_excel(wb, 'Upload')
    add_ID_to_position(securities)
    new_securities = securities[securities['SecurityID'].isna()].copy()
    if len(new_securities) > 0:
        add_security(new_securities)
        
    xl.add_df_to_excel(new_securities, wb, 'New', index=False)
    
    # add new xref ...
    add_xref(new_securities)

    
#################################################################################
# generate yf_sec_list.csv file for YF data downloading process
def generate_yf_sec_list():

    securities = securities_by_ref('YF_ID')
    securities = securities[['YF_ID', 'SecurityID', 'SecurityName']]

    # save to csv file
    YF_SECURITY_FILE = config.yf_config['security_file']
    securities.to_csv(YF_SECURITY_FILE, index=False)
    print("saved file:", YF_SECURITY_FILE)
            
    
    
#################################################################################
def test():
    wb = xw.Book('Security.xlsx')
    df = get_SecurityID()
    xl.add_df_to_excel(df, wb, 'SecurityID', index=False)
    
    df = get_xref()
    xl.add_df_to_excel(df, wb, 'Xref', index=False)

    
    wb = xw.Book('Book1')
    generate_yf_sec_list()
    
    
    
    