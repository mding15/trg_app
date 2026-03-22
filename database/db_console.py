# -*- coding: utf-8 -*-
"""
Created on Wed May 14 16:15:38 2025

@author: mgdin
"""
import pandas as pd
import xlwings as xw
from utils import xl_utils
from database import db, ms_sql_server
from database import db_utils
from utils import df_utils

def console():
    db_source = 'pg' # pg, ms

    wb = xw.Book('Book1')

    
    sql = """
    select * from mkt_data_source where "Source" ='YH'

    """

    df = get_sql_df(sql, source=db_source)

    xl_utils.add_df_to_excel(df, wb, 'db', index=False)



    
POSTGRES_BOOK=r'C:\Users\mgdin\dev\TRG_App\Documents\Postgres.xlsx'
def xl_upload_data():
    wb_db = xw.Book(POSTGRES_BOOK)

    copy_xl_to_db(wb_db, 'security_xref')

    copy_xl_to_db(wb_db, 'account')
    
    copy_xl_to_db(wb_db, 'account_positions')
    
    copy_xl_to_db(wb_db, 'account_run_parameters')
    
    copy_xl_to_db(wb_db, 'mkt_data_source')
    
    copy_xl_to_db(wb_db, 'class_expect_return')
    
    copy_xl_to_db(wb_db, 'risk_limit_level')
    

MSSS_BOOK=r'C:\Users\mgdin\dev\TRG_App\Documents\MSSS.xlsx'
def xl_upload_data():
    wb_db = xw.Book(MSSS_BOOK)

    copy_xl_to_msdb(wb_db, 'dm_dim_d_Broker')

    copy_xl_to_msdb(wb_db, 'dm_port_consolidated')

    copy_xl_to_msdb(wb_db, 'dm_fact_d_Positions')

     
def adhoc_temp(wb):
    df = xl_utils.read_df_from_excel(wb, 'adhoc_temp')
    df.columns=['char_value']
    df['batch']='batch1'
    db_utils.insert_df('adhoc_temp', df, 'batch')


def select_from_table(wb):
    sql = """select * from security_info_view where "Ticker" in ('BBR75')"""
    
    df = db_utils.get_sql_df(sql)
    xl_utils.add_df_to_excel(df, wb, 'db', index=False)
    

########################################################################################################
# table_name='security_xref'
# wb=wb_db
def copy_xl_to_db(wb, table_name):
    df = xl_utils.read_df_from_excel(wb, tab=table_name)   
    df = df_utils.drop_columns(df, ['id'])

    db_utils.insert_df(table_name, df)
    
# wb = wb_db
# table_name = 'dm_port_consolidated'
def copy_xl_to_msdb(wb, table_name):
    df = xl_utils.read_df_from_excel(wb, tab=table_name)   
    df = df_utils.drop_columns(df, ['id', 'insert_time', 'bdp_id'])
        
    ms_sql_server.insert_df(table_name, df, key_column='report_id')

# columns=['id', 'insert_time', 'bdp_id']
def drop_columns(df, columns):
    columns = set(df.columns) & set(columns)
    return df.drop(columns=columns)

#########################################
from sqlalchemy import create_engine, text
def get_sql_df(query: str, params: dict | None = None, source: str ='pg', chunksize: int | None = None):
    
    engine = get_engine(source)
    
    # Use SQLAlchemy `text()` for safe bound parameters
    stmt = text(query)
    
    
    if chunksize:
        # Stream in chunks to avoid high memory usage, then concatenate
        chunks = pd.read_sql_query(stmt, engine, params=params, chunksize=chunksize)
        df = pd.concat(chunks, ignore_index=True) if chunks is not None else pd.DataFrame()
    else:
        df = pd.read_sql_query(stmt, engine, params=params)

    return df    
    

def get_engine(source: str):
    if source == 'pg':
        engine = db.engine
    elif source == 'ms':
        engine = ms_sql_server.get_engine()
    else:
        raise Exception(f"get_sql_df: unknown source: {source}")
        
    return engine
