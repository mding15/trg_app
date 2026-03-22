import os
import sys

import pandas as pd
from psycopg2.extras import execute_batch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database2 import pg_connection
from mkt_data import mkt_timeseries


def update_position_price(positions, as_of_date):
    positions = positions.copy()
    positions['Quantity'] = pd.to_numeric(positions['Quantity'], errors='coerce')
    positions['LastPrice'] = pd.to_numeric(positions['LastPrice'], errors='coerce')
    positions['MarketValue'] = pd.to_numeric(positions['MarketValue'], errors='coerce')
    sec_ids = positions['SecurityID'].unique().tolist()
    prices = mkt_timeseries.get_last_prices(sec_ids, as_of_date)

    # update position price from market DB
    prices = prices.rename(columns={'Price': 'xPrice', 'PriceDate': 'xPriceDate'})
    positions['LastPriceDate'] = pd.to_datetime(positions['LastPriceDate'], errors='coerce')
    pos = positions.merge(prices, on='SecurityID', how='left')
    mask = ~pos['xPrice'].isna() & (pos['LastPrice'].isna() | (pos['LastPriceDate'] < pos['xPriceDate']))
    pos.loc[mask, 'LastPrice']     = pos.loc[mask, 'xPrice']
    pos.loc[mask, 'LastPriceDate'] = pos.loc[mask, 'xPriceDate']
    positions = pos.drop(columns=['xPrice', 'xPriceDate'])

    # Set cash price to 1
    mask = positions['AssetClass'] == 'Cash'
    positions.loc[mask, 'LastPrice'] = 1
    positions.loc[mask, 'LastPriceDate'] = as_of_date

    # implied price
    mask = positions['LastPrice'].isna() & ~positions['Quantity'].isna()
    positions.loc[mask, 'LastPrice'] = positions.loc[mask, 'MarketValue'] / positions.loc[mask, 'Quantity']
    positions.loc[mask, 'LastPriceDate'] = as_of_date

    # fallback price to 1
    mask = positions['LastPrice'].isna()
    positions.loc[mask, 'LastPrice'] = 1
    positions.loc[mask, 'LastPriceDate'] = as_of_date

    # update market value
    # mask = ~positions['Quantity'].isna()
    # positions.loc[mask, 'MarketValue'] = positions.loc[mask, 'Quantity'] * positions.loc[mask, 'LastPrice']

    return positions



def test_data():
    data = {'SecurityID': 'T10000108',
            'SecurityName': 'SPDR S&P 500 ETF TRUST',
            'Quantity': 100,
            'MarketValue': 66900,
            'ExpectedReturn': 0.15,
            'Class': 'Equity',
            'SC1': 'EQ ETF',
            'SC2': None,
            'Country': 'United States',
            'Region': 'North America',
            'Sector': 'Basket',
            'Industry': 'Basket',
            'OptionType': None,
            'PaymentFrequency': None,
            'MaturityDate': None,
            'OptionStrike': None,
            'UnderlyingSecurityID': None,
            'CouponRate': None,
            'is_option': False,
            'UnderlyingID': None,
            'LastPrice': None,
            'LastPriceDate': None,
            'AssetClass': 'Equity',
            'AssetType': 'ETF',
            'Currency': 'USD',
            'ISIN': 'US78462F1030',
            'CUSIP': '78462F103',
            'Ticker': 'SPY',
            }

    positions = pd.DataFrame([data])
    return positions

def test():
    as_of_date = '2025-09-30'
    positions = test_data()

    result = update_position_price(positions, as_of_date)
    print(result.T)


if __name__ == '__main__':
    test()
