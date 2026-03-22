# -*- coding: utf-8 -*-
"""
Created on Thu Feb 27 21:21:54 2025

@author: mgdin
"""

import pandas as pd
import time
import datetime
import uuid
import concurrent.futures

from trg_config import config
from detl import YH_API
from utils import tools, date_utils
from database import db_utils, pg_connection

from utils import xl_utils


max_workers = 10

def test_API():
    tickers = ['AXON', 'CCJ']

    # main function: for all yh_tickers, extract hist_prices from YH and insert data into yh_stock_price table
    update_hist_price()
    
    # extract prices for the new tickers in current_security table
    update_current_price()
    
    # extract profiles and save to db
    extract_stock_profiles(tickers)
    
    # extract historical prices and insert into yh_stock_price
    extract_hist_prices(tickers)
    
    # etract end of day prices
    extract_eod()

# for all yh_tickers, extract hist_prices and insert yh_stock_price table
def update_hist_price(tickers=None):
    if not tickers:
        tickers = get_yh_tickers()

    #today = datetime.datetime(2025,5,27) 
    downloaded_tickers = get_downloaded_tickers(today=None)

    # exclude downloaded tickers
    tickers = list(set(tickers).difference(downloaded_tickers))
    
    BATCH_SIZE = 10
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i: i+BATCH_SIZE]
        extract_hist_prices(batch)
        time.sleep(1)
    
def test_update_hist_price():
    tickers = ['SPY', 'QQQ', 'AAPL']
    update_hist_price(tickers)
    
def get_yh_tickers():
    # select * from security_xref where "REF_TYPE" ='YH'
    query = """
    select * from mkt_data_source where "Source" ='YH'
    """
    df =db_utils.get_sql_df(query)
    tickers = df['SourceID'].unique().tolist()
    return tickers

def test_get_yh_tickers():
    tickers = get_yh_tickers()

#
# extract Stock Profile from YH API and save to database
#
# tickers = ['AXON', 'CCJ']
def extract_stock_profiles(tickers):

    # get stock profiles through API    
    df = api_stock_profiles(tickers)

    tickers1 = df['ticker'].to_list()    
    missing = set(tickers).difference(tickers1)

    # add to database
    db_utils.insert_df('yh_stock_profile', df, 'ticker')

    if missing:
        missing_str = ", ".join(list(missing))
        print(f"extract_stock_profile: no data extract for tickers: {missing_str}")    
        
    print(f'extract_stock_profile: extracted profiles for {len(tickers1)} tickers')

#
# extract historical prices and dividends, then save to file and db
#
def extract_hist_prices(tickers):
    
    if len(tickers)==0:
        return
    
    # get historical prices for tickers
    price_df, div_df = api_hist_price(tickers)
    
    if not div_df.empty:
        # save to file and db
        save_hist_dividend(div_df)
    
    if not price_df.empty:
        # save to file and db
        save_hist_prices(price_df)
    
    
def test_extract_hist_prices(tickers):
    tickers = ['SPY', 'AAPL']
    extract_hist_prices(tickers)
    

def save_hist_prices(df):
    folder = get_hist_folder()
    name = str(uuid.uuid4())[:8]
    file_path = folder / f'price.{name}.csv'
    df.to_csv(file_path, index=False)
    print(f'write to file: {file_path}')    

    insert_stock_price(df)
    
# df = div_df
def save_hist_dividend(df):
    folder = get_hist_folder()
    name = str(uuid.uuid4())[:8]
    file_path = folder / f'dividend.{name}.csv'
    df.to_csv(file_path, index=False)
    print(f'write to file: {file_path}')    
    insert_dividend(df)
    
#
# extract stock dividends and save to db
# cob = '2025-03-21'
def extract_stock_dividend(cob):
    
    if isinstance(cob, datetime.datetime):
        cob = cob.strftime('%Y-%m-%d')
    
    # get historical prices for tickers
    df = YH_API.STOCK_DIVIDEND(cob)

    # copy data to db
    insert_stock_dividend(df)
    
def insert_stock_dividend(df):

    # make sure name is less than 100
    df['companyName'] = df['companyName'].apply(lambda x: x[:100])    

    # drop dups    
    df.drop_duplicates(subset=['symbol', 'dividend_Ex_Date'], keep='first', inplace=True)
    
    # insert data to db
    db_utils.insert_df('yh_stock_dividend', df, key_column='dividend_Ex_Date')
    
    
def insert_stock_price(df):

    tickers = df['ticker'].unique()
    ticker_list = ','.join([f"'{s}'" for s in tickers])
    
    query = f"""
    select ticker, max(date) as max_date from yh_stock_price where ticker in ({ticker_list}) group by ticker
    """        
    max_dates_df = db_utils.get_sql_df(query) 
    
    # convert to date type
    max_dates_df['max_date'] = pd.to_datetime(max_dates_df['max_date'])
    df['date'] = pd.to_datetime(df['date'])

    # delete rows that have dates <= max_date    
    df_filter = df.merge(max_dates_df, on='ticker', how='left')
    df_filter = df_filter[df_filter['max_date'].isna() | (df_filter['date']>df_filter['max_date']) ]
    df_filter = df_filter.drop(columns=['max_date'])    
    
    # delete rows from stock_price if exist for the same ticker and date range (min, max)
    # result = df.groupby('ticker')['date'].agg(['min', 'max'])
    # for ticker, row in result.iterrows():
    #     # min_date = row['min'].strftime('%Y-%m-%d')
    #     # max_date = row['max'].strftime('%Y-%m-%d')
    #     min_date = row['min']
    #     max_date = row['max']
    #     # print(f'delete {ticker}, {min_date}, {max_date}')
    #     delete_stock_price(ticker, min_date, max_date)

    # insert df into table stock_price
    db_utils.insert_bulk_df('yh_stock_price', df_filter)
    

def insert_dividend(df):

    tickers = df['ticker'].unique()
    ticker_list = ','.join([f"'{s}'" for s in tickers])
    
    query = f"""
    select ticker, max(ex_date) as max_date from dividend where ticker in ({ticker_list}) group by ticker
    """        
    max_dates_df = db_utils.get_sql_df(query) 
    
    # convert to date type
    max_dates_df['max_date'] = pd.to_datetime(max_dates_df['max_date'])
    df['ex_date'] = pd.to_datetime(df['ex_date'])
    df['ex_date'] = df['ex_date'].dt.tz_localize(None)

    # delete rows that have dates <= max_date    
    df_filter = df.merge(max_dates_df, on='ticker', how='left')
    df_filter = df_filter[df_filter['max_date'].isna() | (df_filter['ex_date']>df_filter['max_date']) ]
    df_filter = df_filter.drop(columns=['max_date'])    

    # insert df into table stock_price
    db_utils.insert_bulk_df('dividend', df_filter)
    
# min_date = date.strftime('%Y-%m-%d')
# max_date = date.strftime('%Y-%m-%d')
def delete_stock_price(ticker, min_date, max_date):
    
    query = """DELETE FROM yh_stock_price WHERE ticker = %s
    AND "date" BETWEEN %s AND %s
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (ticker, min_date, max_date))
            conn.commit()
            print(f"deleted {cur.rowcount} row(s) from yh_stock_price")

        
#
# extract End of Day Price and save db
# today = datetime.datetime.now()
def extract_eod(tickers=None):

    # return Monday to Friday
    today = get_eod_date()

    # get tickers from table <current_security>
    df = get_eod_tickers(tickers)
    tickers = df['Ticker'].to_list()
    id_map = df.set_index('Ticker')['SecurityID'].to_dict()

    # call API    
    prices = api_eod_price(tickers, today)
    
    # add SecurityID
    prices['SecurityID'] = prices['Ticker'].apply(lambda x: id_map[x])

    # delete rows in the table in case of re-run
    db_utils.delete('current_price', "Date", [today])
    
    # insert prices to db
    db_utils.insert_bulk_df('current_price', prices)

    print(f'Successfully extracted prices for {len(prices)} tickers')
    

def test_extract_eod():
    extract_eod(['SPY', 'QQQ'])
    
#
# call this function when add new tickers into the current_security table
# 1) find the new tickers in the current_security
# 2) extract prices from YH API
# 3) insert data into stock_price
# 4) copy data into current_price table
#
def update_current_price():

    # get current security list that are not in the stock_price
    query = """select distinct cs."Ticker" from current_security cs
    left join current_price cp on cp."SecurityID" = cs."SecurityID" 
    where cp."SecurityID" is NULL and cs."DataSource" = 'YH'
    """

    df = db_utils.get_sql_df(query)
    tickers = df['Ticker'].to_list()
    
    if not tickers:
        print("found 0 new tickers")
        return
    
    # extract historical prices from API and save to table stock_price
    extract_hist_prices(tickers)

    # insert into current_price using stock_price
    copy_stock_price_to_current_price(tickers)
    
# insert into current_price using stock_price
def copy_stock_price_to_current_price(tickers):
    
    # last date is the starting date in current_price
    last_date = db_utils.get_sql_df("select date_value from parameters where param_name='hist_price_last_date'")
    last_date = pd.to_datetime(last_date['date_value']).iloc[0]

    tickers_str = ", ".join(["'" + x + "'" for x in tickers])
    query = f"select * from yh_stock_price where ticker in ({tickers_str})"     
    df = db_utils.get_sql_df(query)
    df['date'] = pd.to_datetime(df['date'])
    df = df[df['date']>last_date]

    # rename    
    df = df.rename(columns={'ticker': 'Ticker', 'date':'Date', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volumne': 'Volume'})

    # get SecurityID
    query = f"""select * from current_security where "Ticker" in ({tickers_str})"""
    df2 = db_utils.get_sql_df(query)
    df['SecurityID'] = df['Ticker'].map(df2.set_index('Ticker')['SecurityID'].to_dict())
    df['PriceTime'] = df['Date']

    db_utils.insert_df('current_price', df, key_column='SecurityID')    
    
########################################################################################
# 
def get_downloaded_tickers(today=None):
    # today = datetime.datetime(2025,5,26) #'2025-05-26'
    if today:
        folder = config['YH_DIR'] / 'history' / f"{today.strftime('%Y-%m-%d')}"
    else:
        folder = get_hist_folder()
        
    files = folder.glob('*.csv')
    tickers=[]
    for file in files:
        # print(file.name)
        df = pd.read_csv(file)
        tickers.extend(df['ticker'].unique())

    return tickers    
    
########################################################################################
# augment API functions

# extract hist price concurently for a list of tickers
# also return dividend history
def api_hist_price(tickers):
    price_list = []
    div_list = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {executor.submit(YH_API.GET_HISTORY, ticker): ticker for ticker in tickers}
    
        for future in concurrent.futures.as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                price_df, div_df = future.result()
                price_list.append(price_df)
                div_list.append(div_df)
                
            except Exception as e:
                print(f"Error processing {ticker}: {str(e)}")

    price_history = pd.concat(price_list, ignore_index=True)
    div_history = pd.concat(div_list, ignore_index=True)
    return price_history, div_history

def test_api_hist_price(wb):
    tickers = ['SPY', 'AAPL']
    price_df, div_dif = api_hist_price(tickers)
    xl_utils.add_df_to_excel(price_df, wb, 'hist_price')
    xl_utils.add_df_to_excel(div_dif, wb, 'hist_dividend')

# extract stock prices for a list of tickers
def api_stock_profiles(tickers):
    profile_dfs = []
    
    for ticker in tickers:
        try:
            profile_df = YH_API.STOCK_PROFILE(ticker)
            profile_dfs.append(profile_df)
        except Exception as e:
            print(f"Failed to fetch profile for {ticker}: {str(e)}")
    
    all_profiles = pd.concat(profile_dfs, ignore_index=True)
    
    return all_profiles

# call API GET_QUOTES
def api_eod_price(tickers, today):
    df = YH_API.GET_QUOTES(tickers)
    prices = extract_eod_price(df)
    save_eod_price(prices, today)

    # file_path = get_eod_file_path(today)
    # prices = pd.read_csv(file_path)
    # prices['PriceTime'] = pd.to_datetime(prices['PriceTime'])
    
    # rename the columns and save to db
    prices = prices.rename(columns={
        'symbol':	'Ticker',
        'regularMarketPrice':	'Close',
        'regularMarketOpen':	'Open',
        'regularMarketDayHigh':	'High',
        'regularMarketDayLow':	'Low',
        'regularMarketVolume':  'Volume',
        })

    prices['Date'] = today

    return prices


#########################################################################################
def get_tickers_folder():
    return tools.get_folder(config['YH_DIR'] / 'tickers')

def extract_tickers():

    folder = get_tickers_folder()

    for page in range(2,10):
        print(page)
        df = YH_API.GET_TICKERS('INDEX', page)
        file_path = folder / f'index.{page}.csv'
        df.to_csv(file_path, index=False)
        print(f'saved to: {file_path}')
        if page % 5 == 0:
            print('sleep 2 seconds ...')
            time.sleep(2)

    # read all tickers
    file_list = folder.glob("index.*.csv")
    df_list = [pd.read_csv(file) for file in file_list]
    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df.to_csv(folder/'tickers.index.csv', index=False)
    
    df = pd.read_csv(folder/'YH_Tickers.csv')
    df = df.drop_duplicates(subset=['ticker'], keep='first')
    df.to_csv(folder/'YH_Tickers.csv', index=False)    

########################################################################################
# End of Day Price
# today = datetime.datetime(2025,3,10)
def get_eod_date():
    return date_utils.get_cob()

def get_eod_folder():
    folder = config['YH_DIR'] / 'EOD'
    return tools.get_folder(folder)

def get_hist_folder():
    today = get_eod_date()
    folder = config['YH_DIR'] / 'history' / f"{today.strftime('%Y-%m-%d')}"
    return tools.get_folder(folder)

def get_stat_file():
    today = get_eod_date()
    folder = config['YH_DIR'] / 'stat'
    folder = tools.get_folder(folder)
    file_path = folder / f"stat.{today.strftime('%Y-%m-%d')}.csv"
    return file_path
    
def get_eod_file_path(today):
    file_folder = get_eod_folder() / f'{today.year}'
    today_str = today.strftime('%Y%m%d')
    file_path = tools.get_folder(file_folder) / f'price.{today_str}.csv'
    return file_path    
    
# tickers=['SPY', 'QQQ']
def get_eod_tickers(tickers):
        
    sql = """
    SELECT "SecurityID", "Ticker" FROM current_security where "DataSource" = 'YH'
    """
    
    df = db_utils.get_sql_df(sql)

    if tickers:
        df = df[df['Ticker'].isin(tickers)]
    
    return df

def save_eod_price(prices, today):

    file_path = get_eod_file_path(today)
    prices.to_csv(file_path, index=False)
    print(f'save file: {file_path}')
    
def extract_eod_price(df):
    COLUMNS = ['symbol', 'longName', 'exchange', 'financialCurrency',
       'regularMarketPrice', 'regularMarketTime', 'regularMarketOpen',
       'regularMarketDayHigh', 'regularMarketDayLow',
       'regularMarketVolume', 'regularMarketPreviousClose']
    df = df[COLUMNS].copy()
    df = df[~df['regularMarketTime'].isna()]
    df['PriceTime'] = df['regularMarketTime'].apply(date_utils.timestamp_to_datetime)

    return df    

# for securities in the current_security table, extract hist_price and insert into yh_stock_prices
def update_curr_sec_hist_prices():
    
    query = """
    select distinct yt.ticker 
    from current_security cs, yh_tickers yt  where cs."Ticker"  = yt.ticker 

    """
    df = db_utils.get_sql_df(query)
    tickers = df['ticker'].to_list()
    
    extract_hist_prices(tickers)
    
########################################################################################
def test(wb):
    tickers = ['AAPL', 'BBBI']
    df = api_stock_profiles(tickers)
    xl_utils.add_df_to_excel(df, wb, 'df')    

    db_utils.insert_df('yh_stock_profile', df, key_column='ticker')

    df = xl_utils.read_df_from_excel(wb, 'Ticker')    
    tickers = df['Ticker'].to_list()

    tickers = ['IMID.L', 'SWDA.L']
    df = YH_API.GET_QUOTES(tickers)

    tickers = ['DAY', 'KVUE', 'KKR', 'VST', 'JBL', 'GEV', 'COR', 'SMCI', 'DECK', 'SOLV', 'BLDR', 'CPAY', 'HUBB', 'NWSA', 'SW', 'CRWD', 'GDDY']
    xl_utils.add_df_to_excel(df, wb, 'df')    
