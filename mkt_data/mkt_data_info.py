# -*- coding: utf-8 -*-
"""
Purpose:
    Provide information on all entities for market data

Created on Mon Jun  3 09:40:21 2024

@author: mgdin
"""
import pandas as pd
import numpy as np
import xlwings as xw
import datetime

from api import db
from security import security_info as si
from utils import mkt_data, xl_utils, date_utils
from database.models import MktDataInfo
from database import db_utils
from mkt_data import mkt_timeseries


def update_curr_sec():
    # get current securities
    df =db_utils.get_sql_df('select * from current_security')
    sec_list = df['SecurityID'].to_list()

    update_stat_by_sec_id(sec_list)
    
    
# update mkt_data_info by calc stats for the timeseries in mkt_file
# input_sec_ids = prices.columns.to_list()
# input_sec_ids = None means all
# input_sec_ids=sec_ids
def update_stat_by_sec_id(input_sec_ids=None, source=None, category='PRICE'):
    
    sec_list = mkt_timeseries.get_mkt_data_sec_list()    

    if input_sec_ids is None:
        input_sec_ids = sec_list['SecurityID'].to_list()
    else:
        sec_list = sec_list[sec_list['SecurityID'].isin(input_sec_ids)]
    
    categories = sec_list['Category'].unique()
    categories = set(categories).difference(['TEST'])
    
    for category in categories:
        # categories = list(categories)
        # category = categories[1]
        print(category)
    
        sec_ids = sec_list[sec_list['Category']==category]['SecurityID'].to_list()    

        BATCH_SIZE = 50
        for i in range(0, len(sec_ids), BATCH_SIZE):
            print(f'running batch {i}')
            batch = sec_ids[i: i+BATCH_SIZE]
        
            # get prices
            # prices = mkt_data.get_market_data(sec_ids, category=category)
            prices = mkt_timeseries.get(batch, category=category)
    
            # update stat
            update_stat(prices, source, category)

# source="YF"
def update_stat(prices, source, category='PRICE'):
    stat = calc_stat(prices)
    stat['Category'] = category
    if source:
        stat['DataSource'] = source
        
    try:
        insert_db_stat(stat)
    except Exception as e:
        db.session.rollback()
        print(e)
    
def calc_stat(prices):

    sec_ids = prices.columns.tolist()

    # get securities
    df = si.get_security_by_ID(sec_ids)
    df = df.set_index('SecurityID')[['SecurityName', 'AssetClass', 'AssetType']].copy()

    # stats
    
    df['StartDate'] = date_utils.get_first_date(prices)
    df['EndDate']   = date_utils.get_last_date(prices)
    df['Length']    = prices.count()
    df['MaxValue']  = prices.max()
    df['MinValue']  = prices.min()
    df['AverageValue'] = prices.mean()
    df['StdValue'] = prices.std()
    
    df = df.reset_index()
    
    return df
    

# update stats for the given list of sec_ids
#def insert_db_stat(sec_ids=None, source=''):
def insert_db_stat(stat):

    # remove length = 0
    stat = stat[stat['Length']!=0].copy()

    # get all SecurityID    
    sec_ids = stat['SecurityID'].to_list()
    
    # get mkt_data_info from db
    md_info = get_mkt_data_info(sec_ids)
    info_sec_id_lookup = dict([((x.SecurityID, x.Category), x) for x in md_info])

    # convert nan to None
    stat.replace([np.nan, np.inf, -np.inf], None, inplace=True)
    
    # convert each row into records
    records = stat.to_dict(orient='records')
    nnew, nupdate = 0, 0
    for record in records:    
        mkt_info = MktDataInfo(**record)
        mkt_info.LastUpdate = datetime.datetime.now()
        
        key = (mkt_info.SecurityID, mkt_info.Category)
        if key in info_sec_id_lookup: # update obj
            db_info = info_sec_id_lookup[key]
            copy_sql_obj(mkt_info, db_info)
            nupdate = nupdate + 1
        else:  # add new  
            db.session.add(mkt_info)   
            nnew = nnew + 1
            
    db.session.commit()
    print(f'Insert into database mkt_data_info: new: {nnew}, update: {nupdate}')

    # db.session.rollback()
    # db.session.flush()

#
# mkt_data stat info
#    
def get_mkt_data_info_df(sec_ids=None):
    info = get_mkt_data_info(sec_ids)
    df = MktDataInfo_to_df(info)
    return df
    
def get_mkt_data_info(sec_ids=None):
    if sec_ids is None:
        info_list = db.session.query(MktDataInfo).all()
    else:
        info_list = db.session.query(MktDataInfo).filter(MktDataInfo.SecurityID.in_(sec_ids)).all()
    return info_list

def get_info_by_source(source):
    info_list = db.session.query(MktDataInfo).filter(MktDataInfo.DataSource == source).all()
    #df = MktDataInfo_to_df(info_list)
    return info_list

# get all SecurityIDs for the given source
def get_sec_ids(source=None):
    if source is None:
        info_list = db.session.query(MktDataInfo).all()
    else:
        info_list = get_info_by_source(source)
    return [x.SecurityID for x in info_list]    

def get_last_date(source):
    info_list = get_info_by_source(source)
    return max([x.EndDate for x in info_list])
    
# make a timeseries for cash securities with constant value of 1    
def update_cash_securities(sec_ids=None):
    # sec_ids = ['T10000885']
    if sec_ids is None:
        sec_ids = si.get_ID_by_AssetClass('Cash')
    
    last_date = get_last_date('YF')
    start_date = '2010-01-01'
    end_date  = last_date.strftime('%Y-%m-%d') 
    dates = date_utils.get_bus_dates(start_date, end_date)
    
    hist_price = pd.DataFrame(index=dates)
    hist_price[sec_ids] = 1.0
    mkt_data.save_market_data(hist_price, 'Calculate')

    # calc stats and insert to database
    update_stat_by_sec_id(sec_ids, source='Calculate', category='PRICE')


# copy obj1 to obj2
def copy_sql_obj(obj1, obj2):
    for key, value in vars(obj1).items():
        if key != '_sa_instance_state':
            setattr(obj2, key, value)
    
    
##################################################################################
# Function to convert list of SQLAlchemy objects to a Pandas DataFrame
def MktDataInfo_to_df(results):
    # Extract column names from the table
    column_names = [column.name for column in MktDataInfo.__table__.columns]
    # Extract data as a list of dictionaries
    data = [{col: getattr(user, col) for col in column_names} for user in results]
    # Create DataFrame
    df = pd.DataFrame(data, columns=column_names)
    return df

##################################################################################

def test():
    
    sec_ids = ['T10000001']
    update_stat_by_sec_id(sec_ids)

