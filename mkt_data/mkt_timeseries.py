# -*- coding: utf-8 -*-
"""
Created on Sat Jun  8 13:04:56 2024

@author: mgdin
"""

import pandas as pd
import numpy as np
import xlwings as xw
from sqlalchemy import text
from sqlalchemy import bindparam

from trg_config import config
from utils import hdf_utils as hdf
from utils import xl_utils as xl
from utils import tools, date_utils, xl_utils
from utils import mkt_data
from security import security_info
from database.models import MktDataInfo
from database import db_utils
from database import pg_create_connection
from psycopg2.extras import execute_values
from api import db
from mkt_data import mkt_data_info
from utils.date_utils import time_it


def migration(wb):
    sec_list = mkt_data.get_mkt_data_sec_list()
    sec_ids = sec_list[sec_list['Category']=='PRICE']['SecurityID'].unique()
    df = db_utils.get_sql_df('select  * from security_info')
    sec_info_ids = df['SecurityID'].unique()
    sec_ids = set(sec_ids) & set(sec_info_ids)
    sec_ids = list(sec_ids)
    
    len(sec_ids)
    
    df_data = []
    sec_id2 = sec_ids[1000:2000]
    data = mkt_data.get_market_data(sec_id2)
    for sec_id in sec_id2:
        df = data[[sec_id]]
        df = df.dropna()
        df = df.reset_index()    
        df.columns = ['price_date', 'price']
        df = df.drop_duplicates()
        df['security_id'] = sec_id

        df_data.append(df)    
    
    df = pd.concat(df_data)    
    print(len(df))
    db_utils.insert_bulk_df('mkt_data_price', df)
    
    
    df = sec_list
    xl_utils.add_df_to_excel(df, wb, 'hist')
    df.columns
    
    
# return df['Category', 'SecurityID']
def get_mkt_data_sec_list():
    return mkt_data.get_mkt_data_sec_list()


# IDs = positions['SecurityID'].unique()
# to_date = end_date
def get_hist(IDs, from_date=None, to_date=None, category='PRICE'):
    # IDs = ['T10000001', 'T10000002']
    sec_ids = [x for x in IDs]

    # Create SQL query with placeholders (%s repeated for each item in the list)
    query = text("SELECT * FROM mkt_data_price WHERE security_id IN :ids").bindparams(bindparam("ids", expanding=True))
    df = db_utils.get_sql_df(query, params={'ids': sec_ids})    

    # convert price_date to datetime    
    df['price_date']=pd.to_datetime(df['price_date'])
    
    # from_date=datetime.datetime(2024,1,1)
    if from_date:
        df = df[df['price_date'] >= from_date]
    
    if to_date:
        df = df[df['price_date'] <= to_date]

    pivot = pd.pivot_table(
        df,
        values='price',
        index='price_date',
        columns='security_id',
        aggfunc='mean',
        fill_value=np.nan  # replace NaN with 0
    )
            
    return pivot

def get_hist2(IDs, from_date=None, to_date=None, category='PRICE'):

    if from_date:
        from_date = date_utils.parse_date(from_date).strftime('%Y-%m-%d')
    else:
        from_date = '2020-01-01'
    
    if to_date:
        to_date = date_utils.parse_date(to_date).strftime('%Y-%m-%d')
    else:
        to_date = date_utils.today()
    
    # IDs = ['T10000001', 'T10000002']
    sec_ids = [x for x in IDs]

    # Create SQL query with placeholders (%s repeated for each item in the list)
    query = text(f"SELECT * FROM mkt_data_price WHERE security_id IN :ids and price_date > '{from_date}' and price_date < '{to_date}'").bindparams(bindparam("ids", expanding=True))
    df = db_utils.get_sql_df(query, params={'ids': sec_ids})    

    # convert price_date to datetime    
    df['price_date']=pd.to_datetime(df['price_date'])

    pivot = pd.pivot_table(
        df,
        values='price',
        index='price_date',
        columns='security_id',
        aggfunc='mean',
        fill_value=np.nan  # replace NaN with 0
    )
            
    return pivot

@time_it
def get_hist3(IDs, from_date=None, to_date=None, category='PRICE'):
    BATCH_SIZE=50
    results = []
    for i in range(0, len(IDs), BATCH_SIZE):
        batch = IDs[i:i+BATCH_SIZE]
        print(f'batch: {i}')
        df = get_hist2(batch, from_date, to_date, category)
        results.append(df)
        
    df = pd.concat(results, axis=1)
    return df
    
def get_hist1(IDs, from_date=None, to_date=None, category='PRICE'):
    
    # IDs = ['T10000001', 'T10000002']
    sec_ids = [x for x in IDs]
    
    
    # with pg_connection() as conn:
    conn = pg_create_connection()
    cur = conn.cursor()    
    
    # Step 1: Create a temporary table
    cur.execute("""
        CREATE TEMP TABLE temp_ids (
            security_id varchar(20)
        ) ON COMMIT DROP;  -- optional: drop table after commit
    """)

    # Step 2: Bulk insert IDs into the temp table
    execute_values(cur,
        "INSERT INTO temp_ids (security_id) VALUES %s",
        [(i,) for i in sec_ids]
    )

    # Step 3: Run the optimized join query
    cur.execute("""
        SELECT md.*
        FROM mkt_data_price md
        JOIN temp_ids t ON md.security_id = t.security_id;
    """)


    # Step 4: Fetch results
    results = cur.fetchall()

    # get column names
    colnames = [desc[0] for desc in cur.description]

    # Close connection
    cur.close()
    conn.close()

    
    df = pd.DataFrame(results, columns=colnames)
    df['price_date'] = pd.to_datetime(df['price_date'])
    df['price'] = pd.to_numeric(df['price'])

    # from_date=datetime.datetime(2024,1,1)
    if from_date:
        df = df[df['price_date'] >= from_date]
    
    if to_date:
        df = df[df['price_date'] <= to_date]
    
    pivot = pd.pivot_table(
        df,
        values='price',
        index='price_date',
        columns='security_id',
        aggfunc='mean',
        fill_value=np.nan  # replace NaN with 0
    )

    return pivot

# IDs = sec_list
@time_it
def get(IDs, from_date=None, to_date=None, category='PRICE'):
    return mkt_data.get_market_data(IDs, from_date, to_date, category)


# get last avaliable price for a given sec_id_list
def get_last_prices(sec_ids, to_date):
    df = get(sec_ids, to_date=to_date)

    cols = []
    dates = []
    values = []
    for col, data in df.items():
        cols.append(col)
        idx = data.last_valid_index()
        dates.append(idx)
        if idx:
            values.append(data[idx])
        else:
            values.append(None)
    
    return pd.DataFrame({'SecurityID': cols, 'Price': values, 'PriceDate': dates})


# df = prices
# source='YF'
# source='FED'
# category='MACRO'
# df = data
# df = df[existing_ids]
def save(df, source, category='PRICE'):
    sec_ids = df.columns.to_list()

    new_ids, existing_ids = split_new_existing(sec_ids, category)
    
    save_new(df[new_ids], source, category)
    
    update_existing(df[existing_ids], source, category)

    
# df.index=datetime, columns=[sec_id, sec_id, ....], value=price
# df=df[new_ids]
def save_new(df, source, category):
    if len(df)==0 or len(df.columns) == 0:
        return

    # save to mkt_data
    mkt_data.save_market_data(df, source, category)
    
    # update stats
    mkt_data_info.update_stat(df, source, category)

# append df to the existing timeseries
# df = prices
# source = 'YF'
# category = 'PRICE'
def update_existing(df, source, category):
    if len(df)==0 or len(df.columns) == 0:
        return

    mkt_data.append_market_data(df, source, category)
    # update stats
    sec_ids = df.columns.tolist()
    mkt_data_info.update_stat_by_sec_id(sec_ids, source, category)
    
def split_new_existing(sec_ids, category='PRICE'):
    
    # get mkt_data sec_ids
    sec_list = mkt_data.get_mkt_data_sec_list()
    mkt_data_ids = sec_list[sec_list['Category'] == category]['SecurityID'].to_list()
    
    # split existing and new
    existing_ids = [x for x in sec_ids if x in mkt_data_ids]
    new_ids = [x for x in sec_ids if x not in mkt_data_ids]
    
    return new_ids, existing_ids


# Translate tickers to secIDs and retrieve the market data, and use tickers to label the securities
def get_by_tickers(tickers, from_date, to_date, category='PRICE'):
    ticker_ids = security_info.get_ID_by_Ticker(tickers)
    prices = mkt_data.get_market_data(ticker_ids['SecurityID'], from_date, to_date, category)
    prices.columns = ticker_ids['Ticker'].to_list()
    prices.index.name = 'Date'
    return prices

#
# get total return time series, which includes dividends
# sec_ids = ['T10000921', 'T10000001']
def get_total_return(sec_ids, from_date, to_date):
    
    prices    = mkt_data.get_market_data(sec_ids, from_date, to_date, category='PRICE')
    dividends = mkt_data.get_market_data(sec_ids, from_date, to_date, category='DIVIDEND')

    # xl_utils.add_df_to_excel(prices, wb, tab='hist_prices')    
    # xl_utils.add_df_to_excel(dividends, wb, tab='hist_prices', addr='E1')    

    # calc price percentage returns
    prices = pd.DataFrame(prices)
    prices = prices.fillna(method='ffill')
    pct_changes =  prices.pct_change(1).fillna(0)

    for sec_id in prices:
        if sec_id in dividends:
            div = prices[[sec_id]]
            div['Dividend'] = dividends[sec_id]
            div['Dividend'].fillna(0, inplace=True)
    
            div['Dividend_pct'] = div['Dividend'] / div[sec_id].shift(1).fillna(method='bfill')
    
            pct_changes[sec_id] = (1+pct_changes[sec_id]) * (1+div['Dividend_pct'])-1

    return pct_changes

# In[]
def test():
    df = get_mkt_data_sec_list()
    wb = xw.Book('Book1')
    xl.add_df_to_excel(df, wb, 'mkt_list')
    
    tik = security_info.get_ID_by_Ticker(['VIX'])
    sec_ids = ['T10000011']
    df = get(sec_ids)
    xl.add_df_to_excel(df, wb, 'ts')

    df = db_utils.get_sql_df("""select "SecurityID" from current_security""")
    sec_ids = df['SecurityID'].to_list()

    df1 = get_hist3(sec_ids)        
    df1.shape

    df2 = get(sec_ids)
