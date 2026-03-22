# -*- coding: utf-8 -*-
"""
Created on Mon Jul 22 17:42:37 2024

@author: mgdin
"""

import alpha_vantage
from alpha_vantage.fundamentaldata import FundamentalData

import pandas as pd



key = 'M3SBP18NAABBECY4' # Replace with your API key

fd = FundamentalData(key, output_format='pandas')

ticker = 'GOOGL'
data, _ = fd.get_company_overview(ticker)
    
    
data = pd.read_csv('updated_equity_yf_7.csv')
def get_sector_industry(ticker):
  try:
    data, _ = fd.get_company_overview(ticker)
    sector = data['Sector'].iloc[0]
    industry = data['Industry'].iloc[0]
  except Exception as e:
    print(f"Failed to fetch data for {ticker}: {e}")
    sector, industry = 'N/A', 'N/A'
  return sector, industry
data['Sector'], data['Industry'] = zip(*data['Ticker'].map(get_sector_industry))
data.to_csv('updated_equity_yf_8.csv', index=False)




###################################################################
"Allen's code from Alpha vantage official website"
import requests
import pandas as pd
import concurrent.futures
import time
from trg_config import config
import psycopg2
from psycopg2 import sql
from io import StringIO
import os


api_key = 'NSCB52YSRNYW84VM'

DB_CONFIG = {
"database" : 'postgres',   
"user" : os.environ["PRS_USERNAME"],    
"password" : os.environ["PRS_PASSWORD"],
"host" : 'trg-input-database.c9sm826ie6uy.us-east-1.rds.amazonaws.com',
"port" : '5432'
}

def fetch_stock_data(ticker, api_key):
    """
    Fetch stock data for a single ticker from Alpha Vantage.
    
    :param ticker: Stock ticker (e.g., "AAPL")
    :param api_key: Alpha Vantage API key
    :return: DataFrame containing stock data for the given ticker
    """
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={api_key}&outputsize=full"
    response = requests.get(url)
    data = response.json()

    # 检查是否返回有效数据
    if "Meta Data" not in data or "Time Series (Daily)" not in data:
        print(f"Failed to fetch data for {ticker}")
        return None

    # 提取数据
    meta_data = data["Meta Data"]
    time_series = data["Time Series (Daily)"]

    # 创建 DataFrame
    df = pd.DataFrame.from_dict(time_series, orient="index")
    df.columns = ["open", "high", "low", "close", "volume"]
    df.insert(0, "ticker", meta_data["2. Symbol"])  # 第一列是 ticker
    df.insert(1, "date", df.index)  # 第二列是日期
    df.reset_index(drop=True, inplace=True)

    # 确保数值列转换为 float
    df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)

    # **步骤 1：删除 2020-03-06 及之后的数据**
    df["date"] = pd.to_datetime(df["date"])  # 转换为日期格式
    df = df[df["date"] < "2020-03-06"]  # 只保留 2020-03-06 之前的数据

    return df


def fetch_all_stocks(tickers, api_key, max_workers=10):
    """
    Fetch stock data for multiple tickers in parallel using ThreadPoolExecutor.
    
    :param tickers: List of stock tickers
    :param api_key: Alpha Vantage API key
    :param max_workers: Number of threads for parallel requests
    :return: Combined pandas DataFrame with stock data for all tickers
    """
    all_data = []
    start_time = time.time()

    # **步骤 2：使用多线程加速 API 请求**
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_stock_data, ticker, api_key): ticker for ticker in tickers}
        
        for future in concurrent.futures.as_completed(futures):
            try:
                df = future.result()
                if df is not None:
                    all_data.append(df)
            except Exception as e:
                print(f"Error fetching data for {futures[future]}: {e}")

    # 合并所有 DataFrame
    final_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

    # 计算运行时间
    elapsed_time = time.time() - start_time
    print(f"Fetched {len(tickers)} stocks in {elapsed_time:.2f} seconds.")

    return final_df


def GET_TICKERS_FROM_DB():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        query = "SELECT ticker FROM yh_tickers"  
        cursor.execute(query)

        tickers = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return tickers
    except Exception as e:
        print(f"fail to get ticker: {str(e)}")
        return []