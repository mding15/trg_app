# -*- coding: utf-8 -*-
"""
Created on Mon May 19 16:15:40 2025

@author: mgdin
"""
import pandas as pd
import xlwings as xw

from utils import tools, date_utils, xl_utils
from database import db_utils, model_aux, pg_connection
from mkt_data import mkt_timeseries
from security import security_info

# as_of_date = '2025-05-15'
# account_id = 1001
def get_account_portfolio(account_id, as_of_date):
    
    if isinstance(as_of_date, str):
        as_of_date = date_utils.parse_date(as_of_date)

    params = get_params(account_id, as_of_date)
    positions = get_positions(account_id, as_of_date)
    positions = update_price(positions, as_of_date)
    limit = get_account_limit(account_id)
    
    return params, positions, limit

def get_account_limit(account_id):
    """Get limit data from account_limit table"""
    df = db_utils.get_sql_df(f'select * from account_limit where account_id = {account_id}')
    if df.empty:
        return {}
    
    # Convert DataFrame to dict format
    limit_dict = {}
    for _, row in df.iterrows():
        limit_dict[row['limit_category']] = row['limit_value']
    
    return limit_dict

def save_account_limit(account_id, limit):
    """Save limit data to account_limit table"""
    # Delete existing limit data for this account
    try:
        sql = "DELETE FROM account_limit WHERE account_id = %s"
        with pg_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (account_id,))
                conn.commit()
        print(f'Deleted existing limit data for account_id={account_id}')
    except Exception as e:
        print(f'Failed to delete existing limit data: {str(e)}')
        raise Exception(f'Failed to delete existing limit data: {str(e)}')
    
    # Convert limit dict to DataFrame
    limit_data = []
    for limit_category, limit_value in limit.items():
        if limit_category != 'port_id':  # 跳过port_id字段
            limit_data.append({
                'account_id': account_id,
                'limit_category': limit_category,
                'limit_value': limit_value
            })
    
    if limit_data:
        df = pd.DataFrame(limit_data)
        db_utils.insert_df('account_limit', df, 'account_id')
        print(f'Saved limit data for account_id={account_id}')

def get_params(account_id, as_of_date):
    df = db_utils.get_sql_df(f'select * from account_run_parameters where account_id = {account_id}')    
    if df.empty:
        raise Exception(f'missing account_run_parameters for account_id: {account_id}')
        
    params = df.iloc[0].T.to_dict()
    params['AsofDate'] = as_of_date
    params['ReportDate'] = as_of_date
    params['TrackedOvertime'] = 'Y'
    return params

def get_positions(account_id, as_of_date):
    """Get positions for account, selecting the most recent upload up to the given date"""
    if isinstance(as_of_date, str):
        as_of_date = date_utils.parse_date(as_of_date)
    
    # Get the most recent upload up to the given date
    # First find the most recent insert_time for this account
    sql_latest = """
    SELECT MAX(insert_time) as latest_insert_time
    FROM account_positions 
    WHERE account_id = %s 
    AND insert_time <= %s
    """
    
    latest_df = db_utils.get_sql_df(sql_latest, (account_id, as_of_date))
    
    if latest_df.empty or latest_df.iloc[0]['latest_insert_time'] is None:
        raise Exception(f'No positions found for account_id {account_id} up to {as_of_date}')
    
    latest_insert_time = latest_df.iloc[0]['latest_insert_time']
    
    # Get all positions from the most recent upload
    sql = """
    SELECT * FROM account_positions 
    WHERE account_id = %s 
    AND insert_time = %s
    ORDER BY position_id
    """
    
    df = db_utils.get_sql_df(sql, (account_id, latest_insert_time))
    
    if df.empty:
        raise Exception(f'No positions found for account_id {account_id} up to {as_of_date}')
    
    # Rename columns to match expected format
    positions = df.rename(columns={'position_id': 'ID', 'security_name': 'SecurityName', 
                                   'ticker': 'Ticker', 'quantity': 'Quantity', 'market_value': 'MarketValue',
                                   'isin': 'ISIN', 'cusip': 'CUSIP', 'security_id': 'SecurityID'})
    
    # Get price information using update_price function
    positions['Price'] = positions['MarketValue'] / positions['Quantity']
    
    return positions

def update_price(positions, as_of_date):
    # as_of_date is always a business day (handled by get_cob() in scheduler)
    sec_ids = positions['SecurityID'].to_list()
    prices = mkt_timeseries.get_last_prices(sec_ids, as_of_date)
    
    # update position price
    prices = prices.rename(columns={'Price': 'xPrice', 'PriceDate': 'xPriceDate'})
    pos = positions.merge(prices, on='SecurityID', how='left')
    # mask = (~pos['xPrice'].isna()) & (pos['PriceDate'] < pos['xPriceDate'])
    mask = ~pos['xPrice'].isna() 
    
    pos.loc[mask, 'Price'] = pos.loc[mask, 'xPrice']
    pos.loc[mask, 'PriceDate'] = pos.loc[mask, 'xPriceDate']
    positions = pos.drop(['xPrice', 'xPriceDate'], axis=1)
    
    # Check for missing or zero prices
    missing_prices = positions[(positions['Price'].isna()) | (positions['Price'] == 0)]
    if not missing_prices.empty:
        missing_securities = missing_prices[['SecurityID', 'Ticker', 'Price']].to_dict('records')
        error_msg = f"No latest price for securities: {missing_securities}"
        raise Exception(error_msg)
    
    # update market value    
    positions['MarketValue'] =  positions['Quantity'] * positions['Price']
    
    # Check for invalid market values
    invalid_market_values = positions[(positions['MarketValue'] == 0) | (positions['MarketValue'].isna())]
    if not invalid_market_values.empty:
        invalid_securities = invalid_market_values[['SecurityID', 'Ticker', 'Quantity', 'Price', 'MarketValue']].to_dict('records')
        error_msg = f"Invalid market values found for securities: {invalid_securities}"
        raise Exception(error_msg)

    return positions

    # wb = xw.Book('Book1')    
    # xl_utils.add_df_to_excel(df, wb, 'df')
    # xl_utils.add_df_to_excel(prices, wb, 'prices')
    # xl_utils.add_df_to_excel(positions, wb, 'positions3')
    
def get_prices(sec_ids, as_of_date):
    df = mkt_timeseries.get(sec_ids, to_date=as_of_date)

    cols = []
    dates = []
    values = []
    for col, data in df.items():
        cols.append(col)
        idx = data.last_valid_index()
        dates.append(idx)
        values.append(data[idx])
    
    return pd.DataFrame({'SecurityID': cols, 'Price': values, 'PriceDate': dates})

def create_account_for_tracking(user, portfolio_name, client_id):
    """Create new account for track over time functionality"""
    try:
        account = model_aux.add_account(user, portfolio_name, client_id)
        print(f'Created new account for tracking: account_id={account.account_id}, portfolio_name={portfolio_name}')
        return account
    except Exception as e:
        print(f'Failed to create account for tracking: {str(e)}')
        raise Exception(f'Failed to create account: {str(e)}')

def save_account_positions(account_id, positions_df):
    """Save positions data to account_positions table"""
    try:
        # Delete existing positions for this account on the same day
        delete_account_positions_for_today(account_id)
        
        # Prepare data for insertion
        positions_df = positions_df.copy()
        positions_df['account_id'] = account_id
        positions_df['insert_time'] = pd.Timestamp.now().date()
        positions_df['security_id'] = security_info.get_SecurityID_by_ref(positions_df)
        
        # Map column names to match database schema
        column_mapping = {
            'ID': 'position_id',
            'SecurityName': 'security_name', 
            'ISIN': 'isin',
            'CUSIP': 'cusip',
            'Ticker': 'ticker',
            'Quantity': 'quantity',
            'MarketValue': 'market_value',
            'userAssetClass': 'asset_class',
            'userCurrency': 'currency'
        }
        
        # Rename columns to match database
        positions_df = positions_df.rename(columns=column_mapping)
        
        # Select only the columns that exist in the database
        db_columns = ['account_id', 'position_id', 'security_name', 'isin', 'cusip', 
                     'ticker', 'quantity', 'market_value', 'asset_class', 'currency', 'insert_time', 'security_id']
        
        # Filter to only include columns that exist in both dataframe and database
        available_columns = [col for col in db_columns if col in positions_df.columns]
        positions_df = positions_df[available_columns]
        
        # Insert data into database
        db_utils.insert_df('account_positions', positions_df, 'account_id')
        print(f'Saved {len(positions_df)} positions for account_id={account_id}')
        
    except Exception as e:
        print(f'Failed to save account positions: {str(e)}')
        raise Exception(f'Failed to save positions: {str(e)}')

def delete_account_positions_for_today(account_id):
    """Delete all positions for an account that were inserted today"""
    try:
        today = pd.Timestamp.now().date()
        sql = "DELETE FROM account_positions WHERE account_id = %s AND insert_time = %s"
        with pg_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (account_id, today))
                conn.commit()
        print(f'Deleted existing positions for account_id={account_id} on {today}')
    except Exception as e:
        print(f'Failed to delete existing positions: {str(e)}')
        raise Exception(f'Failed to delete existing positions: {str(e)}')

def save_account_run_parameters(account_id, params_dict, is_new_account=True):
    """Save parameters data to account_run_parameters table"""
    try:
        # Only insert parameters for new accounts, ignore for existing accounts
        if not is_new_account:
            print(f'Skipping parameters update for existing account_id={account_id}')
            return
        
        # Delete existing parameters for this account (should not happen for new accounts, but safety check)
        delete_account_run_parameters(account_id)
        
        # Prepare data for insertion
        params_dict['account_id'] = account_id
        params_dict['insert_date'] = pd.Timestamp.now().date()
        
        # Ensure PortfolioName is set (use account_name if not present in params)
        if 'PortfolioName' not in params_dict or pd.isna(params_dict.get('PortfolioName')):
            # Get account name from database
            account_info = db_utils.get_sql_df(f"SELECT account_name FROM account WHERE account_id = {account_id}")
            if not account_info.empty:
                params_dict['PortfolioName'] = account_info.iloc[0]['account_name']
            else:
                params_dict['PortfolioName'] = 'Unknown Portfolio'
        
        # Map parameter names to database column names
        column_mapping = {
            'PortfolioName': 'PortfolioName',
            'AsofDate': 'AsofDate', 
            'ReportDate': 'ReportDate',
            'RiskHorizon': 'RiskHorizon',
            'TailMeasure': 'TailMeasure',
            'ReturnFrequency': 'ReturnFrequency',
            'Benchmark': 'Benchmark',
            'ExpectedReturn': 'ExpectedReturn',
            'BaseCurrency': 'BaseCurrency'
        }
        
        # Create DataFrame with only the parameters that exist
        params_data = {}
        for param_name, db_column in column_mapping.items():
            if param_name in params_dict:
                params_data[db_column] = [params_dict[param_name]]
        
        # Add account_id and insert_date
        params_data['account_id'] = [account_id]
        params_data['insert_date'] = [pd.Timestamp.now().date()]
        
        params_df = pd.DataFrame(params_data)
        
        # Insert data into database
        db_utils.insert_df('account_run_parameters', params_df, 'account_id')
        print(f'Saved parameters for new account_id={account_id}')
        
    except Exception as e:
        print(f'Failed to save account run parameters: {str(e)}')
        raise Exception(f'Failed to save parameters: {str(e)}')

def delete_account_run_parameters(account_id):
    """Delete existing parameters for an account"""
    try:
        sql = "DELETE FROM account_run_parameters WHERE account_id = %s"
        with pg_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (account_id,))
                conn.commit()
        print(f'Deleted existing parameters for account_id={account_id}')
    except Exception as e:
        print(f'Failed to delete existing parameters: {str(e)}')
        raise Exception(f'Failed to delete existing parameters: {str(e)}')

def find_account_by_name(user_id, portfolio_name):
    """Find existing account by portfolio name for a user"""
    try:
        sql = """
        SELECT a.account_id, a.account_name
        FROM account a
        WHERE a.owner_id = %s AND a.account_name = %s
        ORDER BY a.create_time DESC
        LIMIT 1
        """
        with pg_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (user_id, portfolio_name))
                result = cursor.fetchone()
                if result:
                    columns = [x.name for x in cursor.description]
                    return dict(zip(columns, result))
                return None
    except Exception as e:
        print(f'Failed to find account by name: {str(e)}')
        return None
