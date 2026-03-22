# -*- coding: utf-8 -*-
"""
Created on Fri Jan 12 11:38:03 2024

@author: mgdin



WARNING  !!!!! OBSOLETE !!!!!

Replaced by security_info

"""
import pandas as pd
import xlwings as xw
    
from trg_config import config
from utils import tools, xl_utils as xl

SECURITY_FILE       = config['SEC_DIR']  / 'securities.csv'
SECURITY_XREF_FILE  = config['SEC_DIR']  / 'security_xref.csv'
REF_TYPE_LIST       = ['ISIN', 'CUSIP', 'Ticker', 'BB_UNIQUE', 'BB_GLOBAL','YF_ID']

if not SECURITY_FILE.exists():
    print('creating file:', SECURITY_FILE)
    
    # create one security 
    data = {'SecurityID': 'T10000001', 
            'SecurityName': 'S&P 500 Index', 
            'Currency': 'USD',
            'Source': 'Public',
            'AssetClass': 'Equity',
            'AssetType': 'Index',
            'DateAdded': tools.today()
            }
            
    pd.DataFrame([data]).to_csv(SECURITY_FILE, index=False)
    
if not SECURITY_XREF_FILE.exists():
    print('creating file:', SECURITY_XREF_FILE)
    
    # create one xref
    data = {'REF_ID':  'SPX', 
            'REF_TYPE': 'Ticker', 
            'SecurityID': 'T10000001', 
            'Source': 'Public',
            'DateAdded': tools.today()
            }
    
    pd.DataFrame([data]).to_csv(SECURITY_XREF_FILE, index=False)

    
    
Securities    = pd.read_csv(SECURITY_FILE)
Security_xref = pd.read_csv(SECURITY_XREF_FILE)

def load():
    global Securities, Security_xref
    Securities    = pd.read_csv(SECURITY_FILE)
    Security_xref = pd.read_csv(SECURITY_XREF_FILE)
    print('number of Securities:', len(Securities))    
    
    
#
# create new securities, and new xref
# 
# new_securities = df[securities.columns, 'ISIN', 'CUSIP', 'Ticker']
#
def create_security_and_xref(new_securities):
    new_securities, existing_securities = add_security(new_securities)
    
    all_securities = pd.concat([new_securities, existing_securities], ignore_index=True)
    xref = add_xref(all_securities)

    return new_securities, xref, existing_securities


# Search SecurityID based on ISIN, CUSIP, Ticker
# positions is a DataFrame
# positions = new_securities
def get_SecurityID_by_ref(positions):

    res = pd.DataFrame(index=positions.index, columns=['SecurityID'])

    for ref_type in REF_TYPE_LIST:
        # print(ref_type)
        if ref_type in positions:
            ref_ids = positions[ref_type]
            xref = Security_xref[(Security_xref['REF_TYPE']==ref_type) & Security_xref['REF_ID'].isin(ref_ids)]
            
            if len(xref) > 0:
                xref = xref[['SecurityID', 'REF_ID']]
                df = positions[[ref_type]].reset_index().merge(xref, left_on=ref_type, right_on='REF_ID', how='left')
                sec_ids = df[~df['SecurityID'].isna()].set_index('index')['SecurityID']
                idx = res['SecurityID'].isna()
                if len(idx)>0:
                    res.loc[idx, 'SecurityID'] = sec_ids
    return res

# return the union of securities and xref_ids
def get_securities_with_xref(sec_ids=None, xrefs=REF_TYPE_LIST):
    if sec_ids:
        secs = Securities[Securities['SecurityID'].isin(sec_ids)]
    else: # all securities
        secs = Securities
    
    for ref_type in xrefs:
        idx = Security_xref['REF_TYPE']==ref_type
        df = Security_xref.loc[idx, ['SecurityID', 'REF_ID']].rename(columns={'REF_ID':ref_type})
        secs = secs.merge(df, on='SecurityID', how='left')

    return secs

# add new securities
def add_security(new_securities):
    
    # remove existing securities
    new_securities['SecurityID'] = get_SecurityID_by_ref(new_securities)
    existing_securities = new_securities[~new_securities['SecurityID'].isna()]
    new_securities = new_securities[new_securities['SecurityID'].isna()].copy()
    new_securities['DateAdded'] = tools.timestamp()
    
    # check if missing columns
    columns = Securities.columns.to_list()
    missing = set(columns).difference (new_securities)
    if len(missing) > 0:
        print('New Security: missing columns', list(missing))
        raise Exception('New Security: missing columns', list(missing))

    n = len(new_securities)
    if n > 0:
        new_securities['SecurityID'] = _gen_n_IDs(n)
        securities = pd.concat([Securities, new_securities[columns]])
        _save_securites(securities)
        print('added new securities:', len(new_securities))

    return new_securities, existing_securities

def update_security(new_securities):
    # make sure the columns are the same
    new_securities = new_securities[Securities.columns]
    
    n = len(new_securities)
    if n > 0:
        securities = Securities[~Securities['SecurityID'].isin(new_securities['SecurityID'])]
        securities = pd.concat([securities, new_securities])
        _save_securites(securities)
        print('updated securities:', len(new_securities))
    
    
# add new xref's    
def add_xref(new_securities):
    xref = get_xref()
    
    new_xref = pd.DataFrame(columns=xref.columns)
    for ref_type in REF_TYPE_LIST:
        if ref_type in new_securities:
            df = new_securities[['SecurityID', ref_type, 'Source']].rename(columns={ref_type:'REF_ID'})
            df = df[~df['REF_ID'].isna()].copy()
            if len(df) > 0:
                df['REF_TYPE'] = ref_type
                df['DateAdded'] = tools.timestamp()
                new_xref = pd.concat([new_xref, df], ignore_index=True)

    if len(new_xref) > 0:
        xref = pd.concat([xref, new_xref], ignore_index=True)                
        xref = xref.drop_duplicates(subset=['REF_ID', 'REF_TYPE', 'SecurityID'], keep='first')   
        _save_xref(xref)
        print('added xref:', len(new_xref))
        
    return new_xref

############################################################################################################
# methods to get securities

#
# input:
#   sec_id_list = [('ISIN', 'LU0119620176'), ('CUSIP','74340XBN0'),('Ticker','GOOG'), ('TRG_ID', 'T10000425')]
# output: 
#   securities with xrefs
#

def get_security_by_sec_id_list(sec_id_list):
    sec_ids = get_SecurityIDs(sec_id_list)
    return get_securities_with_xref(sec_ids)

def get_security_by_ID(sec_ids):
    return Securities[Securities['SecurityID'].isin(sec_ids)]

def get_security_by_ISIN(isin_ids):
    return get_security_by_xref('ISIN', isin_ids)

def get_security_by_CUSIP(cusip_ids):
    return get_security_by_xref('CUSIP', cusip_ids)

def get_security_by_Ticker(tickers):
    return get_security_by_xref('Ticker', tickers)

def get_ID_by_Ticker(tickers):
    return get_xref_by_ref_ids('Ticker', tickers)

def get_ID_by_ISIN(isin_ids):
    return get_xref_by_ref_ids('ISIN', isin_ids)

def get_ID_by_CUSIP(cusip_ids):
    return get_xref_by_ref_ids('CUSIP', cusip_ids)

   
# ref_type: ISIN, CUSIP, Ticker
# return df = [ISIN, SecurityID, SecurityName, Currenty, ....]
def get_security_by_xref(ref_type, ref_ids):
    xref = get_xref_by_ref_ids(ref_type, ref_ids)
    secs = get_security_by_ID(xref['SecurityID'])
    return xref.merge(secs, on='SecurityID', how='left')

def get_xref_by_ref_ids(ref_type, ref_ids):
    xref = Security_xref[(Security_xref['REF_TYPE']==ref_type) & Security_xref['REF_ID'].isin(ref_ids)]
    return xref[['REF_ID', 'SecurityID']].rename(columns={'REF_ID': ref_type})

#
# input:
#   sec_id_list = [('ISIN', 'LU0119620176'), ('CUSIP','74340XBN0'),('Ticker','GOOG'), ('TRG_ID', 'T10000425')]
# output: 
#   ['T10000157', 'T10000425', 'T10000258', 'T10000875']
#
def get_SecurityIDs(sec_id_list):
    if sec_id_list is None:
        return None
    
    ids_by_type ={}
    for ref_type, ref_id in sec_id_list:
        if ref_type in ids_by_type:
            ids_by_type[ref_type].append(ref_id)
        else:
            ids_by_type[ref_type] = [ref_id]

    sec_ids = set()
    for ref_type, ref_ids in ids_by_type.items():
        # print(ref_type, ref_ids)
        if ref_type == 'TRG_ID':
            sec_ids.update(ref_ids)
        else:
            xref = get_xref_by_ref_ids(ref_type, ref_ids)
            sec_ids.update(xref['SecurityID'])

    return list(sec_ids)

##############################################################################
# basic security operation: add, delete, etc

# get all Securities
def get_securities():
    return Securities

# security xref
def get_xref():
    return Security_xref

# internal use only
# overwrite the csv file with new_sec_ids
def _save_securites(new_secs):
    global Securities
    Securities = new_secs
    Securities.to_csv(SECURITY_FILE, index=False)
    

# internal use only
# overwrite the csv file with new xref
def _save_xref(xref):
    global Security_xref
    Security_xref = xref
    Security_xref.to_csv(SECURITY_XREF_FILE, index=False)

# internal use only
# generate n new securityID's
# caution: there is no lock here
def _gen_n_IDs(n):
    secs = get_securities()
    max_id = secs['SecurityID'].max()
    max_int_id = int(max_id[1:])
    return [f'T{max_int_id+1+i}' for i in range(n)]
    
# USE THIS with caution!!!
# delete Security by ID
def _DELETE_securities(sec_ids):
    secs = get_securities()
    n = secs['SecurityID'].isin(sec_ids).sum()
    new_sec = secs[~secs['SecurityID'].isin(sec_ids)]
    _save_securites(new_sec)
    print('Deleted securities:', n)
    
    # delete xref
    xref = get_xref()
    idx = xref['SecurityID'].isin(sec_ids)
    _save_xref(xref[~idx])
    print('Deleted security_xref:', idx.sum())


##############################################################################
def drop_duplicate_xref():
    xref = get_xref()
    xref = xref.drop_duplicates(subset=['REF_ID', 'REF_TYPE', 'SecurityID'], keep='first')   
    _save_xref(xref)
    
    
def test():
    get_security_by_ID(['T10000009', 'T10000010'])
    get_security_by_ISIN(['LU1717117896', 'US91282CEM91'])
    get_securities_with_xref(['T10000009', 'T10000010'])


    wb = xw.Book('test1.xlsx')
    positions = xl.read_df_from_excel(wb, 'Positions')
    get_SecurityID_by_ref(positions)
