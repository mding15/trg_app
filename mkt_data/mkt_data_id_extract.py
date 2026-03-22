# -*- coding: utf-8 -*-
"""
Created on Wed Jun 11 13:29:34 2025

@author: 37365
"""

import os
import time
from datetime import datetime

import pandas as pd
import requests

from database import db, pg_connection
from database import db_utils
  # 假设这些在 database_helpers.py；若同一文件，直接 import *

FMP_API_KEY = "t8eFmOJgqdkTkBfVDBgM9LXOLRludDuE"  # 可直接硬编码
search_symbol = "https://financialmodelingprep.com/stable/search-symbol"
search_cik = "https://financialmodelingprep.com/stable/search-cik"
search_ev = "https://financialmodelingprep.com/stable/search-exchange-variants"

# ────────────────────────────────────────────────────
# def ensure_target_table():
#     create_sql = """
#     CREATE TABLE IF NOT EXISTS yh_tickers_ids (
#         ticker      TEXT PRIMARY KEY,
#         cusip       TEXT,
#         isin        TEXT,
#         updated_at  TIMESTAMPTZ DEFAULT now()
#     );
#     """
#     with pg_connection() as conn:
#         cur = conn.cursor()
#         cur.execute(create_sql)
#         conn.commit()

def search_one_symbol(keyword: str, exchange: str | None = None) -> dict | None:
    params = {
        "query": keyword,
        "apikey": FMP_API_KEY
    }
    if exchange:                # 可选：按交易所过滤
        params["exchange"] = exchange

    resp = requests.get(search_symbol, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return data[0] if data else None

def search_one_cik(keyword: str, exchange: str | None = None) -> dict | None:
    params = {
        "cik": keyword,
        "apikey": FMP_API_KEY
    }


    resp = requests.get(search_cik, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return data[0] if data else None

def search_one_ev(keyword: str, exchange: str | None = None) -> dict | None:
    params = {
        "symbol": keyword,
        "apikey": FMP_API_KEY
    }

    resp = requests.get(search_ev, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return data[0] if data else None


search_one_symbol("AAPL")
search_one_cik("320193")
search_one_ev("AAPL")

# def main() -> None:
#     ensure_target_table()

#     # ① 取源 ticker 列表
#     src_tickers = db_utils.get_sql_df(
#         "SELECT DISTINCT ticker FROM yh_tickers WHERE ticker IS NOT NULL"
#     )["ticker"].str.upper().tolist()

#     rows = []
#     for idx, tk in enumerate(src_tickers, 1):
#         cusip, isin = fetch_profile_fmp(tk)
#         if cusip or isin:
#             rows.append(
#                 dict(
#                     ticker=tk,
#                     cusip=cusip,
#                     isin=isin,
#                     updated_at=datetime.utcnow(),
#                 )
#             )
#             print(f"[{idx}/{len(src_tickers)}] {tk}: CUSIP={cusip}  ISIN={isin}")
#         else:
#             print(f"[{idx}/{len(src_tickers)}] {tk}: 未找到 CUSIP/ISIN")
#         time.sleep(REQ_SLEEP)

#     # ② 写回数据库（先删后 COPY）
#     if rows:
#         df_out = pd.DataFrame(rows)
#         db_utils.delete("yh_tickers_ids", "ticker", df_out["ticker"].unique())
#         db_utils.insert_bulk_df("yh_tickers_ids", df_out)

#     print("✅ FMP 批量补全完成")

# ── 单股调试函数 ─────────────────────────────────────
def test_single_ticker_fmp(ticker: str) -> None:
    """
    快速测试一只股票能否从 FMP 拿到 CUSIP / ISIN
    """
    cusip, isin = fetch_profile_fmp(ticker)
    print("=== FMP 返回结果 ===")
    print(f"Ticker: {ticker.upper()}")
    print(f"CUSIP : {cusip or '未找到'}")
    print(f"ISIN  : {isin  or '未找到'}")

    
# result = test_single_ticker_fmp("AAPL")



# ——————————————EODHD——————————————————————————————————

eodhd_api_token = '6851b747cf6dd5.19124352'
ticker = 'AAPL.US'
eodhd_url_ETF = f'https://eodhd.com/api/fundamentals/VTI.US?api_token=demo&fmt=json'
eodhd_url_Fund = f'https://eodhd.com/api/fundamentals/SWPPX.US?api_token=demo&fmt=json'



eodhd_bond_url = f'https://eodhd.com/api/bond-fundamentals/910047AG4?api_token=demo&fmt=json'

etf_result = requests.get(eodhd_url_ETF).json()
print(etf_result)

fund_result = requests.get(eodhd_url_Fund).json()
print(fund_result)

data = requests.get(eodhd_bond_url).json()













