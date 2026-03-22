# -*- coding: utf-8 -*-
"""
Created on Wed Apr 30 17:30:18 2025

@author: mgdin
"""

import time
from trg_config import config
from api import app
app.app_context().push()

import yfinance as yf
t = yf.Ticker("BILS")
print(t)
div = t.dividends

from api import portfolios
from mkt_data import mkt_timeseries

def test_mkt_timeseries():
    port_id = 4785
    # read input file
    params, positions, unknown_positions = portfolios.load_portfolio_by_port_id(port_id)

    # get timeseries for sec_ids
    sec_ids = positions['SecurityID'].unique()
    print(len(sec_ids))
    start = time.time()
    print(f'start time: {start}')
    price_hist = mkt_timeseries.get_hist(sec_ids)
    # price_hist = mkt_timeseries.get(sec_ids)

    end = time.time()
    print(f'end time: {end}')
    print(f'elapsed time: {end - start:.4f} seconds')
    print(price_hist.shape)
