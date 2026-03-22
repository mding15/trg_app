# -*- coding: utf-8 -*-
"""
Created on Sat Feb 15 11:22:22 2025

@author: mgdin
"""
import urllib.parse
from urllib.parse import urlparse, parse_qs
from pathlib import Path
import io
from api import app
from flask import request

# read xl into Bytes IO stream
def xl_ioBytes(filename):
    with open(filename, "rb") as f:
        excel_data = f.read()

    return io.BytesIO(excel_data)

    # username = 'test1@trg.com'
    # port_file_path = Path.home() / 'Downloads' / 'Demo.xlsx'

def upload_request_ctx(file_path):
        
    ctx = app.test_request_context('/upload_file', method='POST', 
                                   data={'file': (xl_ioBytes(file_path), file_path.name)}, 
                                   content_type='multipart/form-data')
    ctx.push()
    
    return ctx

def test_upload_request_ctx():
    file_path = Path.home() / 'Downloads' / 'Demo.xlsx'
    savefile_path = Path.home() / 'Downloads' / 'Demo.save.xlsx'
    
    ctx = upload_request_ctx(file_path)
    try:
        file = request.files['file']
        if file:
        	file.save(savefile_path)
    finally:
        # close the context
        ctx.pop()



def parse_url_parameters(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    
    # Decoding the symbols parameter
    if 'symbol' in query_params:
        symbols = query_params['symbol'][0].split('%2C')  # Splitting encoded commas (%2C)
    elif 'ticker' in query_params:
        symbols = query_params['symbol'][0].split('%2C')  # Splitting encoded commas (%2C)

    else:
        symbols = []
    
    return symbols

# Example URL
def example_parse_url_parameters():
    url = "/symbol/composite?symbol=goto.jk%2Caapl%2CMSFT"
    url = '/api/v1/markets/stock/quotes?ticker=AAPL%2CMSFT%2C%5ESPX%2C%5ENYA%2CGAZP.ME%2CSIBN.ME%2CGEECEE.NS'
    
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    print("params:", query_params)

def example_encode_tickers():
    """
    Encodes a list of tickers into a query string for the given API URL.
    
    :param base_url: The base API URL before the ticker parameter
    :param tickers: List of ticker symbols
    :return: Fully formatted API URL with encoded tickers
    """

    base_url = "/api/v1/markets/stock/quotes"
    tickers = ['SPY', 'QQQ', "AAPL", "MSFT", "^SPX", "^NYA", "GAZP.ME"]

    # Join tickers with a comma and encode them
    encoded_tickers = urllib.parse.quote(",".join(tickers))
    
    # Construct the full API URL
    full_url = f"{base_url}?ticker={encoded_tickers}"
    
    print(full_url)
