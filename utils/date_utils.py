# -*- coding: utf-8 -*-
"""
Utilities for Python date functions

Created on Wed Feb 28 17:06:22 2024

@author: mgding
"""

import time
import datetime
import calendar
from dateutil import parser
import pandas as pd
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import BDay
from zoneinfo import ZoneInfo
import pytz




# return string 2024-03-15 3:10:40    
def timestamp():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def today():
    return datetime.datetime.now().strftime('%Y-%m-%d')

def month_end(year, month):
    last_day = calendar.monthrange(year, month)[1]
    return datetime.date(year, month, last_day)

# return string 2024-03-15 3:10:40    
def filename_timestamp():
    return datetime.datetime.now().strftime('%Y%m%d.%H.%M.%S')


# convert timestamp to New York Time
def timestamp_to_datetime(timestamp):

    # Convert to UTC-aware datetime
    utc_time = datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)
    # print("UTC Time:", utc_time.strftime('%Y-%m-%d %H:%M:%S %Z'))

    # Convert to New York Time (ET)
    ny_time = utc_time.astimezone(ZoneInfo("America/New_York"))
    # print("New York Time:", ny_time.strftime('%Y-%m-%d %H:%M:%S %Z'))
    return ny_time

def parse_date(date_str):
    # date_str = '04/15/2025'
    # date_str = '15/04/2025'
    # date_str = '2025-04-15'
    if isinstance(date_str, str):
        date_obj = parser.parse(date_str)
    else:
        date_obj = date_str
    # print(date_obj)
    return date_obj


def add_years(from_date, years):
    return from_date + relativedelta(years=years)

# return today
def get_cob():
    '''
    Returns: close of business day as of today
             if today is week day, return today, else return last friday
    '''
    
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    today = utc_now.astimezone(pytz.timezone("America/New_York"))

    # if the time is before 4pm close time, use yesterday
    if today.hour < 16:
        today = today + datetime.timedelta(days=-1)

    # if weekend days, adjust to Friday
    if today.weekday() > 4:
        wd = today.weekday() - 4
        today = today + datetime.timedelta(days=-wd)

    return today.date()

# business date
def bus_date(d):
    # If it's Saturday, move back 1 day; if Sunday, move back 2 days
    if d.weekday() == 5:  # Saturday
        d -= datetime.timedelta(days=1)
    elif d.weekday() == 6:  # Sunday
        d -= datetime.timedelta(days=2)    
    
    return d
    
# is week day?
def is_weekday():
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    today = utc_now.astimezone(pytz.timezone("America/New_York"))
    return today.weekday() < 5

# last business date
def previous_bus_date(cob, days=1):
    pre_bdate = (cob - BDay(days)).date()
    return pd.to_datetime(pre_bdate)

# business dates    
def get_bus_dates(start_date, end_date):
    date_range = pd.date_range(start=start_date, end=end_date, freq='B')
    return [d.to_pydatetime() for d in date_range]


# get first date in a time series
def get_first_date(price_hist):
    cols = []
    dates = []
    for col, data in price_hist.items():
        cols.append(col)
        dates.append(data.first_valid_index())
    
    return pd.Series(dates, index=cols).dt.date

def get_last_date(price_hist):
    cols = []
    dates = []
    for col, data in price_hist.items():
        cols.append(col)
        dates.append(data.last_valid_index())
    
    return pd.Series(dates, index=cols).dt.date

# measure excution time of func
def time_it(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"{func.__name__} executed in {end - start:.6f} seconds")
        return result
    return wrapper


# example of using decorator time_it
# @time_it
# def my_function():
#     total = sum(range(1000000))
#     return total

# # Call the function
# result = my_function()
