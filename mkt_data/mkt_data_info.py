# -*- coding: utf-8 -*-
"""
Purpose:
    Provide information on all entities for market data

Created on Mon Jun  3 09:40:21 2024

@author: mgdin
"""
import datetime

import numpy as np
import pandas as pd

from database2 import pg_connection
from utils import mkt_data, date_utils
from mkt_data import mkt_timeseries


# ── SQL ────────────────────────────────────────────────────────────────────────

_UPDATE_SQL = """
UPDATE mkt_data_info SET
    "SecurityName" = %(SecurityName)s,
    "AssetClass"   = %(AssetClass)s,
    "AssetType"    = %(AssetType)s,
    "DataSource"   = %(DataSource)s,
    "StartDate"    = %(StartDate)s,
    "EndDate"      = %(EndDate)s,
    "Length"       = %(Length)s,
    "MaxValue"     = %(MaxValue)s,
    "MinValue"     = %(MinValue)s,
    "AverageValue" = %(AverageValue)s,
    "StdValue"     = %(StdValue)s,
    "LastUpdate"   = NOW()
WHERE "SecurityID" = %(SecurityID)s AND "Category" = %(Category)s
"""

_INSERT_SQL = """
INSERT INTO mkt_data_info
    ("SecurityID", "Category", "SecurityName", "AssetClass", "AssetType",
     "DataSource", "StartDate", "EndDate", "Length", "MaxValue", "MinValue",
     "AverageValue", "StdValue", "LastUpdate")
VALUES
    (%(SecurityID)s, %(Category)s, %(SecurityName)s, %(AssetClass)s, %(AssetType)s,
     %(DataSource)s, %(StartDate)s, %(EndDate)s, %(Length)s, %(MaxValue)s, %(MinValue)s,
     %(AverageValue)s, %(StdValue)s, NOW())
"""


# ── Helpers ────────────────────────────────────────────────────────────────────

def _pg_df(sql, params=None):
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)


# ── Public API ─────────────────────────────────────────────────────────────────

def update_curr_sec():
    df = _pg_df('SELECT "SecurityID" FROM current_security')
    update_stat_by_sec_id(df['SecurityID'].to_list())


# update mkt_data_info by calc stats for the timeseries in mkt_file
# input_sec_ids = prices.columns.to_list()
# input_sec_ids = None means all
def update_stat_by_sec_id(input_sec_ids=None, source=None, category='PRICE'):

    sec_list = mkt_timeseries.get_mkt_data_sec_list()

    if input_sec_ids is None:
        input_sec_ids = sec_list['SecurityID'].to_list()
    else:
        sec_list = sec_list[sec_list['SecurityID'].isin(input_sec_ids)]

    categories = set(sec_list['Category'].unique()).difference(['TEST'])

    for category in categories:
        print(category)
        sec_ids = sec_list[sec_list['Category'] == category]['SecurityID'].to_list()

        BATCH_SIZE = 50
        for i in range(0, len(sec_ids), BATCH_SIZE):
            print(f'running batch {i}')
            batch = sec_ids[i: i + BATCH_SIZE]
            prices = mkt_timeseries.get(batch, category=category)
            update_stat(prices, source, category)


def update_stat(prices, source, category='PRICE'):
    stat = calc_stat(prices)
    stat['Category'] = category
    if source:
        stat['DataSource'] = source

    try:
        insert_db_stat(stat)
    except Exception as e:
        print(f'insert_db_stat failed: {e}')


def calc_stat(prices):
    sec_ids = prices.columns.tolist()

    df = _pg_df(
        'SELECT "SecurityID", "SecurityName", "AssetClass", "AssetType" FROM security_info WHERE "SecurityID" = ANY(%s)',
        (sec_ids,),
    )
    df = df.set_index('SecurityID')[['SecurityName', 'AssetClass', 'AssetType']].copy()

    df['StartDate']    = date_utils.get_first_date(prices)
    df['EndDate']      = date_utils.get_last_date(prices)
    df['Length']       = prices.count()
    df['MaxValue']     = prices.max()
    df['MinValue']     = prices.min()
    df['AverageValue'] = prices.mean()
    df['StdValue']     = prices.std()

    return df.reset_index()


def insert_db_stat(stat):
    stat = stat[stat['Length'] != 0].copy()
    stat.replace([np.nan, np.inf, -np.inf], None, inplace=True)
    records = stat.to_dict(orient='records')

    nnew, nupdate = 0, 0
    with pg_connection() as conn:
        with conn.cursor() as cur:
            for record in records:
                cur.execute(_UPDATE_SQL, record)
                if cur.rowcount == 0:
                    cur.execute(_INSERT_SQL, record)
                    nnew += 1
                else:
                    nupdate += 1
        conn.commit()

    print(f'Insert into database mkt_data_info: new: {nnew}, update: {nupdate}')


def get_mkt_data_info(sec_ids=None):
    if sec_ids is None:
        return _pg_df('SELECT * FROM mkt_data_info')
    return _pg_df('SELECT * FROM mkt_data_info WHERE "SecurityID" = ANY(%s)', (sec_ids,))


def get_mkt_data_info_df(sec_ids=None):
    return get_mkt_data_info(sec_ids)


def get_info_by_source(source):
    return _pg_df('SELECT * FROM mkt_data_info WHERE "DataSource" = %s', (source,))


def get_sec_ids(source=None):
    if source is None:
        df = _pg_df('SELECT "SecurityID" FROM mkt_data_info')
    else:
        df = _pg_df('SELECT "SecurityID" FROM mkt_data_info WHERE "DataSource" = %s', (source,))
    return df['SecurityID'].to_list()


def get_last_date(source):
    df = _pg_df('SELECT MAX("EndDate") AS max_date FROM mkt_data_info WHERE "DataSource" = %s', (source,))
    return df['max_date'].iloc[0]


def update_cash_securities(sec_ids=None):
    if sec_ids is None:
        df = _pg_df('SELECT "SecurityID" FROM security_info WHERE "AssetClass" = \'Cash\'')
        sec_ids = df['SecurityID'].to_list()

    last_date  = get_last_date('YF')
    start_date = '2010-01-01'
    end_date   = last_date.strftime('%Y-%m-%d')
    dates      = date_utils.get_bus_dates(start_date, end_date)

    hist_price = pd.DataFrame(index=dates)
    hist_price[sec_ids] = 1.0
    mkt_data.save_market_data(hist_price, 'Calculate')

    update_stat_by_sec_id(sec_ids, source='Calculate', category='PRICE')


def test():
    sec_ids = ['T10000001']
    update_stat_by_sec_id(sec_ids)
