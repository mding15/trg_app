# -*- coding: utf-8 -*-
"""
Created on Sat May 17 21:16:37 2025

@author: mgdin
"""
import sys
from pathlib import Path
# sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from database2 import pg_connection
from utils import mkt_data
from detl import yh_extract
from mkt_data import mkt_data_info, mkt_timeseries


def _pg_df(sql, params=None):
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)


    
def extract_yh_price(security_ids=None, tickers=None):
    """
    Pull historical daily prices from Yahoo Finance into Postgres, then sync
    Postgres -> HDF, for YH-sourced securities (all of them if security_ids
    and tickers are both omitted).

    Safe to re-run: nothing is duplicated or overwritten.
      - yh_extract.update_hist_price() skips tickers already downloaded today
        (file-based, from the day's history/ archive), then for the rest,
        inserts into yh_stock_price only rows newer than that ticker's
        existing MAX(date) in the table. Today's date is excluded unless run
        after 6pm ET, to avoid storing a partial/live quote as a final close.
      - copy_yh_from_db() then reads each security's existing HDF series,
        appends only DB rows newer than the HDF's current max date, and
        rewrites that security's HDF dataset (each security is its own HDF
        dataset, so the write is a full overwrite of just that series).
      - mkt_data_info is refreshed for the pulled securities at the end.
    """
    # get SourceID and SecurityID
    df = get_yh_source_id(security_ids, tickers)
    
    # pull YH historical prices and save to db
    yh_extract.update_hist_price(df['SourceID'].to_list() )
                      
    # copy data from db to hdf
    for _, row in df.iterrows():
        ticker, sec_id = row[['SourceID', 'SecurityID']]
        # print(ticker, sec_id)
        copy_yh_from_db(ticker, sec_id)
        
    # update table mkt_data_info
    mkt_data_info.update_stat_by_sec_id(df['SecurityID'].to_list(), 'YH', 'PRICE')


def update_yh_price(security_ids=None, tickers=None):
    """
    Fix bad existing price data for specific YH securities: delete their
    yh_stock_price rows and refetch full history from Yahoo Finance, then
    fully overwrite (not merge) their HDF series with the fresh data.

    Unlike extract_yh_price(), which only appends rows newer than what's
    already stored (and would leave a bad historical value untouched),
    this wipes each ticker's existing data first so the rewrite actually
    replaces it. Also bypasses the same-day "already downloaded" file
    check that update_hist_price() uses, so it always re-pulls from the API.

    security_ids/tickers: required (at least one), to avoid an accidental
    full wipe of every YH security.
    """
    if not security_ids and not tickers:
        raise ValueError(
            'update_yh_price requires security_ids or tickers -- refusing to run for all YH securities'
        )

    df = get_yh_source_id(security_ids, tickers)

    # wipe each ticker's existing price history in postgres
    for ticker in df['SourceID'].to_list():
        yh_extract.delete_stock_price(ticker)

    # refetch full history from YH and insert (no existing rows left, so
    # insert_stock_price's newer-than-max-date filter keeps everything)
    yh_extract.extract_hist_prices(df['SourceID'].to_list())

    # fully overwrite (not merge) each security's HDF series with the fresh data
    for _, row in df.iterrows():
        ticker, sec_id = row[['SourceID', 'SecurityID']]
        replace_yh_in_hdf(ticker, sec_id)

    # update table mkt_data_info
    mkt_data_info.update_stat_by_sec_id(df['SecurityID'].to_list(), 'YH', 'PRICE')


def get_yh_source_id(security_ids=None, tickers=None):
    from database2 import pg_connection
    if security_ids and tickers:
        sql = """
            SELECT * FROM mkt_data_source
            WHERE "Source" = 'YH'
              AND ("SecurityID" = ANY(%s) OR "SourceID" = ANY(%s))
        """
        params = (security_ids, tickers)
    elif security_ids:
        sql = 'SELECT * FROM mkt_data_source WHERE "Source" = \'YH\' AND "SecurityID" = ANY(%s)'
        params = (security_ids,)
    elif tickers:
        sql = 'SELECT * FROM mkt_data_source WHERE "Source" = \'YH\' AND "SourceID" = ANY(%s)'
        params = (tickers,)
    else:
        sql = 'SELECT * FROM mkt_data_source WHERE "Source" = \'YH\''
        params = None

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)

# copy yh from  db to hdf
def yh_db_2_hdf():
    df = _pg_df('SELECT * FROM mkt_data_source WHERE "Source" = \'YH\'')
    for _, row in df.iterrows():
        ticker, sec_id = row[['SourceID', 'SecurityID']]
        copy_yh_from_db(ticker, sec_id)
    
def yh_stat():
    sec_list = get_yh_sec_list()
    prices = mkt_timeseries.get(sec_list)
    stat = mkt_data_info.calc_stat(prices)

    file_path = yh_extract.get_stat_file() 
    stat.to_csv(file_path, index=False)
    print(f'saved file: {file_path}')

def curr_sec_stat():
    sec_list = get_current_sec_list()
    prices = mkt_timeseries.get(sec_list)
    stat = mkt_data_info.calc_stat(prices)

    file_path = yh_extract.get_stat_file() 
    stat.to_csv(file_path, index=False)
    print(f'saved file: {file_path}')

########################################################################################    
def copy_yh_from_db(ticker, sec_id):
    
    # ticker = 'SPY'
    # sec_id = 'T10000108'
    print(f'copy yh stock price: ticker={ticker}, security_id={sec_id}')
    
    # get hdf data
    hdf_ts = mkt_data.get_market_data([sec_id]) 
    end_date = hdf_ts.index.max()
    
    # get data from db
    df = _pg_df('SELECT * FROM yh_stock_price WHERE ticker = %s', (ticker,))
    if len(df) == 0:
        return
    
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    df = df[['close']].rename(columns={'close': sec_id}).astype(float)
    if len(hdf_ts) > 0:
        df = df[df.index > end_date]

    if len(df) > 0:    
        ts = pd.concat([hdf_ts, df])

        # save to hdf
        mkt_data.save_market_data(ts, source='YH', category='PRICE')    
    
def test_copy_yh_from_db_hdf():
    ticker, sec_id = 'COIN', 'T10001583'
    copy_yh_from_db(ticker, sec_id)

# fully overwrite a security's HDF series with its current db data
# (unlike copy_yh_from_db, does not merge with what's already in hdf)
def replace_yh_in_hdf(ticker, sec_id):

    print(f'replace yh stock price in hdf: ticker={ticker}, security_id={sec_id}')

    # get all data from db
    df = _pg_df('SELECT * FROM yh_stock_price WHERE ticker = %s', (ticker,))
    if len(df) == 0:
        return

    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    df = df[['close']].rename(columns={'close': sec_id}).astype(float)

    # save to hdf
    mkt_data.save_market_data(df, source='YH', category='PRICE')
    
#######################################
# auxilary
def get_current_sec_list():
    df = _pg_df('SELECT "SecurityID" FROM current_security')
    return df['SecurityID'].to_list()

def get_yh_sec_list():
    df = _pg_df('SELECT "SecurityID" FROM security_xref WHERE "REF_TYPE" = \'YH\'')
    return df['SecurityID'].to_list()


def test():
    #extract_yh_price(tickers=['AAPL'])
    update_yh_price(security_ids=['T10000880'])

if __name__ == '__main__':
    test()
