# -*- coding: utf-8 -*-
"""
Created on Fri Nov 14 09:54:45 2025

@author: mgdin
"""
import xlwings as xw
import pandas as pd
from preprocess import upload_security
from database import db_utils
from utils import xl_utils, tools
from detl import YH_API, yh_extract

wb_path = r'C:\Users\mgdin\dev\TRG_App\Workbooks\Upload_Security_WB.xlsx'

def upload():

    wb = xw.Book(wb_path)
   
    port_id = 5335

    # reference data
    df = xl_utils.read_df_from_excel(wb, 'theMap', addr='A1')
    asset_class_map = df.set_index('userAssetClass')['AssetClass'].to_dict()

    # get unknown security
    sql = f'select * from port_positions where port_id = {port_id} and  unknown_security = True'
    unknown_security = db_utils.get_sql_df(sql)    
    tools.write_positions(unknown_security, wb, 'unknown_security')

    # get tickers
    tickers = unknown_security['Ticker'].to_list()
    ticker_df = unknown_security[['Ticker']]
    xl_utils.add_df_to_excel(ticker_df, wb, 'Ticker', index=False)
    
    # get today's price from YH
    quote = YH_API.GET_QUOTES(tickers)    
    xl_utils.add_df_to_excel(quote, wb, 'YH', index=False)

    # missing
    missing_tickers = list(set(tickers).difference(quote['symbol']))
    print(missing_tickers)
    ticker_df['Found'] = "Y"
    ticker_df.loc[ticker_df['Ticker'].isin(missing_tickers), 'Found'] = "N"
    ticker_df.loc[ticker_df['Found']=='Y', 'TickerFixed'] = ticker_df['Ticker']
    xl_utils.add_df_to_excel(ticker_df, wb, 'Ticker', index=False)

    # fix ticker
    df1 = xl_utils.read_df_from_excel(wb, 'Ticker')
    df1 = df1[~df1['TickerFixed'].isna()]
    tickers = df1['TickerFixed'].to_list()
    ticker_fixed = df1.loc[df1['Found']=='N','TickerFixed'].to_list()
    
    # map the ticker
    theMap = df1.set_index('Ticker')['TickerFixed'].to_dict()
    unknown_security['Ticker'] = unknown_security['Ticker'].map(theMap)
    
    # get quote
    quote1 = YH_API.GET_QUOTES(ticker_fixed)   
    if not quote1.empty:
        print('found new quote')
        quote = pd.concat([quote, quote1])    
        xl_utils.add_df_to_excel(quote, wb, 'YH', index=False)

    # missing
    missing_tickers = list(set(tickers).difference(quote['symbol']))
    if missing_tickers:
        print(missing_tickers)


    # new securities
    unknown_security = unknown_security.set_index('Ticker')
    new_security = pd.DataFrame(index=unknown_security.index)
    df = quote.set_index('symbol')
    
    new_security['ID']=unknown_security.index
    new_security['Security Name'] = unknown_security['SecurityName']
    new_security['ISIN'] = unknown_security['ISIN']
    new_security['SEDOL'] = None
    new_security['CUSIP'] = unknown_security['CUSIP']
    new_security['Ticker'] = unknown_security.index
    new_security['Currency'] = df['currency']
    new_security['Asset Class'] = unknown_security['userAssetClass'].map(asset_class_map)
    new_security['Asset Type'] = df['typeDisp'].map({'Equity': 'Stock', 'ETF':'ETF'})
    new_security['Overwrite'] = 'N'
    new_security['Data Source'] = 'YH'
    new_security['Model'] = None

    tools.write_positions(new_security, wb, 'new_security')

    # get hist prices from YH
    df = xl_utils.read_df_from_excel(wb, 'Ticker')
    tickers = df['TickerFixed'].to_list()
    
    # get historical prices for tickers
    price_df, div_df = yh_extract.api_hist_price(tickers)

    price_data =[]
    if not price_df.empty:
        xl_utils.add_df_to_excel(price_df, wb, 'price_hist', index=False)
        # price_df = price_df[['date', 'ticker', 'close']]
        for tic in tickers:
            print(tic)
            df = price_df[price_df['ticker']==tic]
            df = df.set_index('date')[['close']].rename(columns={'close':tic})
            price_data.append(df)

    price_hist = pd.concat(price_data, axis=1)        
    price_hist.index.name = 'Date'
    xl_utils.add_df_to_excel(price_hist, wb, 'price_hist')

    if not div_df.empty:
        xl_utils.add_df_to_excel(div_df, wb, 'div_hist', index=False)

    # get security info
    df = yh_extract.api_stock_profiles(tickers)
    profile = df.set_index('ticker')
    security_info = pd.DataFrame(index=new_security.index)
    security_info['ID'] = new_security['ID']
    security_info['security_id'] = None
    security_info['security_name'] = new_security['Security Name']
    security_info['expected_return'] = 0.1
    security_info['currency'] = new_security['Currency']
    security_info['class'] = unknown_security['userAssetClass']
    security_info['sc1'] = None
    security_info['sc2'] = None
    security_info['country'] = profile['country']
    security_info['region'] = None
    security_info['sector'] = profile['sector']
    security_info['industry'] = profile['industry']
    security_info['option_type'] = None
    security_info['payment_frequency'] = None
    security_info['maturity_date'] = None
    security_info['option_strike'] = None
    security_info['underlying_security_id'] = None
    security_info['coupon_rate'] = None
    security_info['isin'] = new_security['ISIN']
    security_info['cusip'] = new_security['CUSIP']
    security_info['ticker'] = new_security['Ticker']
    
    
    tools.write_positions(security_info, wb, 'SecurityAttribute')
    
    xl_utils.add_df_to_excel(df, wb, 'profile', index=False)
    
