# -*- coding: utf-8 -*-
"""
Created on Sat Jun  1 17:07:11 2024

@author: mgdin
"""
import pandas as pd
import xlwings as xw

from utils import xl_utils
from utils import tools
from database.models import SecurityInfo, SecurityXref
from database import db


REF_TYPE_LIST = ['ISIN', 'CUSIP', 'BB_UNIQUE', 'BB_GLOBAL', 'Ticker', 'YF_ID']

#
# Search SecurityID based on ISIN, CUSIP, Ticker
# positions is a DataFrame df.columns = ['ISIN', 'CUSIP', 'Ticker', ...]
# 
def get_SecurityID_by_ref(positions):

    res = pd.DataFrame(index=positions.index, columns=['SecurityID'])

    # ref_type = 'ISIN'
    for ref_type in REF_TYPE_LIST:
        if ref_type in positions:
            idx = res['SecurityID'].isna()
            if len(idx)>0:
                res.loc[idx, 'SecurityID'] = get_pos_sec_id_by_ref_type(positions, ref_type)

    # cash
    # cash_pos = positions[positions['Class']=='Cash']
    # cash_securities = get_cash_securities(cash_pos['Currency'].to_list())
    # cash_securities = cash_securities.set_index('Currency')
    
    # res.loc[cash_pos.index, 'SecurityID'] = tools.df_series_merge(cash_pos, cash_securities['SecurityID'], key='Currency')

    # options, assign temp IDs ['X1', 'X2', ...]
    # options = positions['OptionType'].isin(['Call', 'Put'])
    # res.loc[options, 'SecurityID'] = [ 'X'+str(x) for x in range(sum(options))]
    
    # unknown securities    
    res.loc[res['SecurityID'].isna(), 'SecurityID'] = None
    
    return res                
                
# check if there are duplicates
def check_duplicates(new_securities):
    
    for col in REF_TYPE_LIST:
        if col in new_securities:
            if new_securities[col].dropna().duplicated().sum() > 0:
                dup_ids = new_securities[new_securities[col].duplicated()][col].to_list()
                ids_str = ', '.join(dup_ids)
                raise Exception(f'found duplicated values in {col}: {ids_str}')

#
# create new securities, and new xref
# 
# new_securities = df[securities.columns, 'ISIN', 'CUSIP', 'Ticker', 'Overwrite']
#
def create_security_and_xref(new_securities):
    
    # check if there are duplicates
    check_duplicates(new_securities)
        
    # find existing securities by adding securityID
    new_securities['SecurityID'] = get_SecurityID_by_ref(new_securities)
    
    # new securities    
    new_secs_df = new_securities[new_securities['SecurityID'].isna()]

    # update security    
    update_secs_df = new_securities[(new_securities['Overwrite']=="Y") & ~new_securities['SecurityID'].isna()]
    
    # insert new security to database
    db_insert_securities(new_secs_df)
    
    # assign SecurityID for the new ones
    new_securities.loc[new_secs_df.index, 'SecurityID'] = new_secs_df['SecurityID']

    # update database for update_securities
    db_update_security_info(update_secs_df)

    # save xref
    # xref_df = new_securities
    new_xref_list = db_insert_xref(new_securities)
    new_xref_df = SecurityXref_to_df(new_xref_list)
    
    # result to show what has been done
    new_securities['Result'] = 'Existing' # default case
    new_securities.loc[new_secs_df.index,    'Result'] = 'Created'
    new_securities.loc[update_secs_df.index, 'Result'] = 'Updated'

    return new_securities, new_xref_df
#
# Update new security
#
def update_security(new_securities):
    
    # check if there are duplicates
    check_duplicates(new_securities)
    
    # update security_info
    db_update_security_info(new_securities)
    
    # update security_xref
    db_insert_xref(new_securities)
    
 
def db_update_security_info(new_securities):
    
    # get db objects
    sec_ids = new_securities['SecurityID'].to_list()
    db_securities = security_by_sec_ids(sec_ids)
    db_objs = dict([[x.SecurityID, x] for x in db_securities])

    # compare values, if changed ,update
    nc = 0
    value_objs = df_to_SecurityInfo(new_securities)
    for value in value_objs:
        db_obj = db_objs.get(value.SecurityID)
        if db_obj != value:
            db_obj.assign(value)
            nc = nc+1
            
    db.session.commit()
    print(f'Updated SecurityInfo: {nc}')
    
    
#
# input:
#   sec_id_list = [('ISIN', 'LU0119620176'), ('CUSIP','74340XBN0'),('Ticker','GOOG'), ('TRG_ID', 'T10000425')]
# output: 
#   securities with xrefs
#
def get_security_by_sec_id_list(sec_id_list=[]):
    sec_ids = get_SecurityIDs(sec_id_list)
    return get_securities_with_xref(sec_ids)


# sec_ids = ['T10000009', 'T10000010']
def get_security_by_ID(sec_ids=None):
    securities = security_by_sec_ids(sec_ids)
    
    return SecurityInfo_to_df(securities)

def get_xref_by_ID(sec_ids=None):
    
    xrefs = xref_by_sec_ids(sec_ids)
    
    return SecurityXref_to_df(xrefs)
    
#
# return the xref columns of securities and xref_ids
# sec_ids = ['T10000009', 'T10000010']
def get_securities_with_xref(sec_ids=None, ref_types=REF_TYPE_LIST):
    
    secs = get_security_by_ID(sec_ids)
    
    if sec_ids is None:
        xrefs = get_xref()
    else:
        xrefs = get_xref_by_ID(sec_ids)
    
    for ref_type in ref_types:
        # print(ref_type)
        idx = xrefs['REF_TYPE']==ref_type
        df = xrefs.loc[idx, ['SecurityID', 'REF_ID']].rename(columns={'REF_ID':ref_type})
        if df['SecurityID'].duplicated().sum() > 0:
            df = df.groupby(by=['SecurityID']).agg({ref_type: lambda x: ', '.join(x)}).reset_index()
            #df.drop_duplicates(subset=['SecurityID'], keep='first', inplace=True)
        secs = secs.merge(df, on='SecurityID', how='left')

    return secs


# methods to get SecurityID
def get_ID_by_ISIN(isin_ids):
    return get_xref_by_ref_ids('ISIN', isin_ids)

def get_ID_by_CUSIP(cusip_ids):
    return get_xref_by_ref_ids('CUSIP', cusip_ids)

def get_ID_by_Ticker(tickers):
    return get_xref_by_ref_ids('Ticker', tickers)

def get_ID_by_YF(tickers):
    return get_xref_by_ref_ids('YF_ID', tickers)

def get_ID_by_AssetClass(asset_class):
    class_list = [asset_class]
    securities = security_by_asset_class(class_list)
    sec_ids = [x.SecurityID for x in securities]
    return sec_ids

# ref_type='Ticker'
# ref_ids = ['SPX', 'AGG']
def get_xref_by_ref_ids(ref_type, ref_ids):
    xrefs = xref_by_ref_ids(ref_type, ref_ids)
    df = SecurityXref_to_df(xrefs)
    return df[['REF_ID', 'SecurityID']].rename(columns={'REF_ID': ref_type})


#
# input:
#   sec_id_list = [('ISIN', 'LU0119620176'), ('CUSIP','74340XBN0'),('Ticker','GOOG'), ('TRG_ID', 'T10000425')]
# output: 
#   ['T10000157', 'T10000425', 'T10000258', 'T10000875']
#
def get_SecurityIDs(sec_id_list):
    
    if len(sec_id_list) == 0:
        return []
    
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
            xref = xref_by_ref_ids(ref_type, ref_ids)
            id_list = [x.SecurityID for x in xref]
            sec_ids.update(id_list)

    return list(sec_ids)

#====================== PRIVATE FUNCTIONS =====================================


    
def get_pos_sec_id_by_ref_type(positions, ref_type):
    ref_ids = positions[~positions[ref_type].isna()][ref_type].to_list()
    xref_list = xref_by_ref_ids(ref_type, ref_ids)
    if len(xref_list) > 0:
        data = [[x.REF_ID, x.SecurityID] for x in xref_list]
        xdf = pd.DataFrame(data, columns=[ref_type, 'SecurityID'])

        # check duplicates
        if xdf[ref_type].duplicated().sum() > 0:
            dup_ids = xdf[xdf[ref_type].duplicated()][ref_type].to_list()
            dup_str = ', '.join(dup_ids)
            raise Exception(f'duplicated REF_ID: {dup_str}')
        
        df  = positions[[ref_type]].reset_index().merge(xdf, on=ref_type, how='left')
        return df[~df['SecurityID'].isna()].set_index('index')['SecurityID']
    else:
        return pd.Series()        
    
    


#
# sec_id_list = [('ISIN', 'LU0119620176'), ('CUSIP','74340XBN0'),('Ticker','GOOG'), ('TRG_ID', 'T10000425')]
# positions = df['ISIN', 'CUSIP', 'Ticker', 'SecurityID', ...]
# convert sec_id_list to postions
def xref_to_postions(sec_id_list):
    positions = pd.DataFrame()
    for r_type, r_id in sec_id_list:
        if r_type == 'TRG_ID':
            r_type = 'SecurityID'
        df = pd.DataFrame({r_type: [r_id]})
        positions = pd.concat([positions, df], ignore_index=True)
    return positions


############################################################################################################
# Database layer


# sec_id_list = ['T10000009', 'T10000010']
def security_by_sec_ids(sec_ids=None):
    if sec_ids is None:
        securities = db.session.query(SecurityInfo).all()
    else:
        securities = db.session.query(SecurityInfo).filter(SecurityInfo.SecurityID.in_(sec_ids)).all()
    
    return securities

# ref_type: ISIN, CUSIP, Ticker
# return list of SecurityInfo
def security_by_ref_ids(ref_type, ref_ids):

    xrefs = xref_by_ref_ids(ref_type, ref_ids)
    sec_id_list = [x.SecurityID for x in xrefs]
    
    securities = db.session.query(SecurityInfo).filter(SecurityInfo.SecurityID.in_(sec_id_list)).all()
    return securities

# return list of SecurityXref
def xref_by_ref_ids(ref_type, ref_ids=None):
    if ref_ids is None:
        xrefs = db.session.query(SecurityXref).filter(SecurityXref.REF_TYPE==ref_type).all()
    else:
        xrefs = db.session.query(SecurityXref).filter(SecurityXref.REF_ID.in_(ref_ids), 
                                                      SecurityXref.REF_TYPE==ref_type).all()
    return xrefs

# return list of SecurityXref
def xref_by_ref_ids_df(ref_type, ref_ids):
    xrefs = xref_by_ref_ids(ref_type, ref_ids)
            
    return SecurityXref_to_df(xrefs)

def xref_by_sec_ids(sec_ids=None):
    if sec_ids is None :
        xrefs = db.session.query(SecurityXref).all()
    else:
        xrefs = db.session.query(SecurityXref).filter(SecurityXref.SecurityID.in_(sec_ids)).all()
    return xrefs    
    
def security_by_asset_class(class_list):
    securities = db.session.query(SecurityInfo).filter(SecurityInfo.AssetClass.in_(class_list)).all()
    return securities

# asset_class = 'Bond'
# asset_type = 'Treasury'
def securities_by_asset_class_type(asset_class, asset_type):
    sql = f""" 
    select * from security_info si where si."AssetClass" = '{asset_class}' and si."AssetType" = '{asset_type}'
    """
    securities = pd.read_sql(sql, con=db.engine)
    return securities
    
# return dataframe
def get_cash_securities(currencies=[]):
        
    cash = security_by_asset_class(['Cash'])
    cash = SecurityInfo_to_df(cash)
    if currencies:
        cash = cash[cash['Currency'].isin(currencies)]
    
    return cash


#
# basic security operation: add, delete, etc
#

# get all Securities
def get_securities():
    Securities = pd.read_sql('select * from security_info', con=db.engine)
    return Securities


    
    
# security xref
def get_xref():
    Security_xref = pd.read_sql('select * from security_xref', con=db.engine)
    return Security_xref

# internal use only
# convert new_secs_df to db objects and insert into database, also update SecurityID
def db_insert_securities(new_secs_df):

    sec_list = create_security_list(new_secs_df)
    for sec in sec_list:
        db.session.add(sec)
    db.session.commit()

    # update SecurityID based on id        
    for sec in sec_list:
        sec_id = sec.id
        sec.SecurityID = f'T{10000000+sec_id}'
    db.session.commit()
    
    sec_id_list = [x.SecurityID for x in sec_list]
    new_secs_df['SecurityID'] = sec_id_list
    
    return sec_id_list


# internal use only
# insert xref into database
# xref_df = new_securities
def db_insert_xref(xref_df):
    new_xref_list = create_xref_list(xref_df)
    
    for xref in new_xref_list:
        db.session.add(xref)
    db.session.commit()
    
    print('Inserting SecurityXref to database: ', len(new_xref_list))    
    return new_xref_list


#################################

# designed for function db_insert_securities()
#
# df = pd.DataFrame(columns=['SecurityName', 'Currency', 'DataSource', 'AssetClass', 'AssetType'])
# create securities from dataframe
#
def create_security_list(new_secs_df):
    columns = ['SecurityName', 'Currency', 'DataSource', 'AssetClass', 'AssetType']
    
    # check if df has all required columns
    missing = set(columns).difference(new_secs_df.columns)
    if len(missing) > 0:
        missing_cols = ', '.join(missing)
        raise Exception(f'found missing security_info columns: [{missing_cols}]')

    # convert to database objects
    df = new_secs_df[columns]

    sec_list = []    
    for i in range(len(df)):
        SecurityName, Currency, DataSource, AssetClass, AssetType = df.iloc[i]
        
        security = SecurityInfo(                                
                                SecurityName = SecurityName,
                                Currency = Currency,
                                AssetClass = AssetClass,
                                AssetType = AssetType,
                                DataSource = DataSource
                               )
        sec_list.append(security)

    return sec_list

# designed for function db_insert_xref()
#
# df.columns = [SecurityID, DataSource, ISIN, CUSIP ...]
# REF_TYPE_LIST = ['ISIN', 'CUSIP', 'Ticker', 'BB_UNIQUE', 'BB_GLOBAL','YF_ID']
# create Xref from dataframe
#
def create_xref_list(xref_df):
    
    new_xref_list = []
    for ref_type in REF_TYPE_LIST:
        xref_list = create_xref_by_type(xref_df, ref_type)
        new_xref_list.extend(xref_list)

    return new_xref_list
    
# create xref from dataframe for one ref_type (ISIN)
def create_xref_by_type(xref_df, ref_type):
    # ref_type = 'Ticker'

    new_xref_list = []
    if ref_type in xref_df:
        # find new xref that are not in the database                
        df = find_new_xref(xref_df, ref_type)

        for i in range(len(df)):
            REF_ID, SecurityID, DataSource = df.iloc[i][[ref_type, 'SecurityID', 'DataSource']]
                        
            sec_xref = SecurityXref(
                           REF_ID = REF_ID,
                           REF_TYPE = ref_type,
                           SecurityID = SecurityID,
                           DataSource = DataSource
                           )
            
            new_xref_list.append(sec_xref)
    
    return new_xref_list            

# auxiliary function for create_xref_by_type()
# find new xref that are not in the database                
def find_new_xref(xref_df, ref_type):
    df = xref_df[[ref_type, 'SecurityID']]
    df = df[~df[ref_type].isna()] # remove nulls
    df['SecurityID_DB'] = get_SecurityID_by_ref(df)

    # check mis-match
    df_db = df[~df['SecurityID_DB'].isna()]
    mismatch = df_db[ df_db['SecurityID'] != df_db['SecurityID_DB'] ]
    if len(mismatch) > 0:
        msg = mismatch.to_csv(index=False)
        raise Exception(f'Found mis-match in input {ref_type}: {msg}')
    
    new_xref_ids = df[df['SecurityID_DB'].isna()][ref_type].to_list()
    return xref_df[xref_df[ref_type].isin(new_xref_ids)]
    
    
    
##############################################################################

# Function to convert list of SQLAlchemy objects to a Pandas DataFrame
def SecurityInfo_to_df(results):
    # Extract column names from the table
    column_names = [column.name for column in SecurityInfo.__table__.columns]
    # Extract data as a list of dictionaries
    data = [{col: getattr(user, col) for col in column_names} for user in results]
    # Create DataFrame
    df = pd.DataFrame(data, columns=column_names)
    return df

# Function to convert list of SQLAlchemy objects to a Pandas DataFrame
def SecurityXref_to_df(results):
    # Extract column names from the table
    column_names = [column.name for column in SecurityXref.__table__.columns]
    # Extract data as a list of dictionaries
    data = [{col: getattr(user, col) for col in column_names} for user in results]
    # Create DataFrame
    df = pd.DataFrame(data, columns=column_names)
    return df

# Function to convert list of SQLAlchemy objects to a Pandas DataFrame
def df_to_SecurityInfo(df):

    columns = ['SecurityID', 'SecurityName', 'Currency', 'AssetClass', 'AssetType', 'DataSource']
    df = df[columns]
    
    obj_list = []
    for i in range(len(df)):
        row = df.iloc[i].to_dict()
        obj = SecurityInfo(**row)
        obj_list.append(obj)
    
    return obj_list

##############################################################################
    
    
def test():
    # by SecurityID
    get_security_by_ID(['T10000009', 'T10000010'])

    # by ISIN
    sec_ids = get_ID_by_ISIN(['LU1717117896', 'US91282CEM91'])['SecurityID']
    get_security_by_ID(sec_ids)

    # by list of (ref_type, ref_id)
    sec_id_list = [('ISIN', 'LU0119620176'), ('CUSIP','74340XBN0'),('Ticker','GOOG'), ('TRG_ID', 'T10000033')]
    get_security_by_sec_id_list(sec_id_list)

    # search SecurityID    
    positions = xref_to_postions(sec_id_list)
    get_SecurityID_by_ref(positions)

    # security info and xref
    get_securities_with_xref(['T10000009', 'T10000010'])

    
def test_add_security():
    
    new_securities = pd.DataFrame({'SecurityName': ['test security'], 
                                 'Currency': ['USD'],
                                 'AssetClass': ['Equity'],
                                 'AssetType': ['Stock'],
                                 'DataSource': ['Test'],
                                 'ISIN': ['TEST']
                                 })
    
    create_security_and_xref(new_securities)

    # TEST DATA from Excel
    wb = xw.Book('Security.xlsx')
    new_securities = tools.read_positions(wb, 'Security')
    
    new_securities, new_xref_df = create_security_and_xref(new_securities)
    xl_utils.add_df_to_excel(new_securities, wb, 'new_securities')
    xl_utils.add_df_to_excel(new_xref_df,    wb, 'new_xref_df')




