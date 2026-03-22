# -*- coding: utf-8 -*-
"""
Created on Sat Mar 23 17:06:02 2024

@author: mgdin

params:
    - ClientID optional: if exists, use different var model
    



"""
import datetime
import pandas as pd
import numpy as np
import xlwings as xw

from security import security_info as sc
from security import bond_info
from models import risk_factors
from models import bond_risk as br
from utils import xl_utils as xl
from utils import data_utils, var_utils, date_utils, mkt_data, tools
from engine import validate_positions
from engine import eq_option_var
from database import db_utils

def load_static_data():
    static_data = data_utils.load_stat('static_data')
    if len(static_data)==0:
        print('missing static_data. set Riskfree Rate to 2%')
        static_data.loc['Riskfree Rate', 'Value'] = 0.02 # default risk free rate

    return static_data


#####################################################################
# main function
def create_DATA(positions, params):
    DATA = {}
    DATA['InputPositions'] = positions
    DATA['Parameters'] = params
    DATA['Static'] = load_static_data()
    DATA['Log'] = pd.DataFrame(columns=['SecurityID', 'Message'])

    return DATA

def calc_VaR(positions, params):
    DATA = create_DATA(positions, params)
    try:
        # pre process
        pre_process(DATA)      

        # calculate VaR
        position_VaR(DATA)

    except Exception as e:
        print(e)
        DATA['Error'] = str(e)
        

    return DATA

# s = user_positions[col]
def col_to_str(s):
    return s.apply(lambda x: f"{x}" if x is not None and isinstance(x, str) == False else x)

    
def pre_process(DATA):
    user_positions = DATA['InputPositions']
    params         = DATA['Parameters']
    
    # rename
    user_positions = user_positions.rename(columns={'asset_class': 'AssetClass', 'asset_type': 'AssetType'})
    
    check_params(params)
    check_positions(user_positions)
    cob = pd.to_datetime(params['AsofDate'])

    # additional parameters
    amend_params(params)

    # copy a new positions
    positions = user_positions[POSITION_COLUMNS].copy()
    positions['pos_id'] = user_positions['pos_id'].values if 'pos_id' in user_positions.columns else positions.index.astype(str)

    # ID columns
    for col in ID_COLUMNS:
        if col in user_positions:
            positions[col] = col_to_str(user_positions[col])

    # optional columns
    # for col in ['LastPrice', 'LastPriceDate', 'AssetClass', 'AssetType', 'Currency']:
    #     if col in user_positions:
    #         positions[f'user_{col}'] = user_positions[col]

    # add SecurityID, moved to position scrubbing
    # positions['SecurityID'] = sc.get_SecurityID_by_ref(positions)

    # if missing securities
    # missing = positions[positions['SecurityID'].isna()]
    # DATA['Unknwon Securities'] = missing

    # if len(missing) > 0:
    #     columns = ['SecurityName'] + list(set(ID_COLUMNS) & set(missing))
    #     error = {'Unknown Securities': missing[columns]}
    #     error_msg = tools.convert_to_json_str(error)
    #     raise Exception(f'Unknown Securities: {error_msg}')

    # get security info
    sec_info = get_security_info(positions)
    DATA['Security_info'] = sec_info

    # add SecurityType
    # positions = positions.merge(sec_info[['SecurityID', 'Currency', 'AssetClass', 'AssetType']], on='SecurityID', how='left')

    # get price
    last_prices = get_last_prices(DATA, positions)
    # positions = positions.merge(last_prices, on='SecurityID', how='left')
    DATA['LastPrices'] = last_prices

    # get underlying price
    prices = get_underlying_price(positions, cob)
    positions['UnderlyingPrice'] = tools.df_series_merge(positions, prices, 'UnderlyingID') 
    
    if 'LastPriceDate' in positions:
        positions['LastPriceDate'] = pd.to_datetime(positions['LastPriceDate'])
    if 'MaturityDate' in positions:
        positions['MaturityDate'] = pd.to_datetime(positions['MaturityDate'])

    
    # Riskfree Rate
    static_data = DATA['Static']
    positions['RiskFreeRate'] = static_data.loc['Riskfree Rate', 'Value']
    
    # Tenor
    cob = pd.to_datetime(cob)
    positions['Tenor'] = positions.apply(lambda x: (x.MaturityDate - cob).days/365, axis=1)
    
    
    # get bond info
    DATA['Bonds'] = get_bond_info(sec_info)
 
    # assign NA for missing Currency, country, Region, etc..
    for col in ['Class', 'SC1', 'SC2', 'Currency', 'Country', 'Region', 'Sector', 'Industry']:
        positions[col] = positions[col].fillna('Unknown')

    DATA['Positions'] = positions
    
    # set var model
    # var_utils.set_client_id(params['ClientID'])
    if 'ModelID' in params:
        var_utils.set_model_id(params['ModelID'])
    else: # default model
        var_utils.set_model_id() 
        
def get_security_info(positions):
    df = positions[['SecurityID', 'Currency', 'AssetClass', 'AssetType']]
    return df

    # # security_info for non-derivatives
    # sec_info = sc.get_security_by_ID(positions['SecurityID'])
    # sec_info = sec_info[['SecurityID', 'Currency', 'AssetClass', 'AssetType']]
    
    # # Equity options
    # idx = positions['OptionType'].isin(['Call', 'Put'])
    # options = positions.loc[idx][['SecurityID', 'user_Currency']]
    # options.columns = ['SecurityID', 'Currency']
    # options[['AssetClass', 'AssetType']] = ['Derivative', 'Option']
    
    # sec_info = pd.concat([sec_info, options], ignore_index=True)
    
    # return sec_info

def position_VaR(DATA):

    # Calculate sensitivities, such as: duration, convexity 
    print('Calculating sensitivities...')
    calc_sensitivities(DATA)
    
    # calc risk factors
    print('Generating risk factors ...')
    DATA['RiskFactor'] = calc_risk_factors(DATA)

    # calc risk factor PnL
    print('Calculating risk factor distribution ...')
    DATA['RF_PnL'] = calc_RF_PnL(DATA)
    
    # calc position VaR
    print('Calculating VaR ...')
    DATA['VaR'] = calc_position_VaR(DATA)
    
    print('Calculating RF VaR ...')
    DATA['RF_VaR'] = calc_rf_VaR(DATA)

###############################################################################################
# collect input data for VaR Calculation
def check_params(params):
    expected_keys = set(['AsofDate', 'Benchmark',  'RiskHorizon'])

    diff = expected_keys.difference(params.keys())
    if diff:
        raise Exception(f'missing parameters: {diff}')

    

def amend_params(params):
        
    rpt_date = params['AsofDate'] 
    if isinstance(rpt_date, datetime.datetime) == False:
        params['AsofDate'] = pd.to_datetime(rpt_date)

    horizon = params['RiskHorizon']
    params['HorizonDays'] = get_horizon_days(horizon)
    
    if 'BenchmarkExpectedReturn' not in params:
        bm_ticker = params['Benchmark']
        exp_ret = get_bm_return(bm_ticker)
        params['BenchmarkExpectedReturn'] = exp_ret
    
    # default base ccy to USD
    if 'BaseCurrency' not in params:
        print('set base currency to USD')
        params['BaseCurrency'] = 'USD'
        
    
def get_bm_return(bm_ticker):
    # bm_ticker = 'BM_60_40'
    
    sql = f"""
    select "ExpectedReturn" from class_expect_return where "Class" = 'Benchmark' and "SC1" = '{bm_ticker}'
    """
    df = db_utils.get_sql_df(sql)
    if df.empty:
        exp_ret = 0.05
    else:
        exp_ret = df['ExpectedReturn'].iloc[0]
    return exp_ret

POSITION_COLUMNS = ['SecurityID', 'SecurityName', 'Quantity', 'MarketValue', 'ExpectedReturn', 
                    'Class', 'SC1', 'SC2', 'Country', 'Region', 'Sector', 'Industry',
                    'OptionType', 'PaymentFrequency', 'MaturityDate', 'OptionStrike', 'UnderlyingSecurityID', 'CouponRate',
                    'is_option', 'UnderlyingID', 'LastPrice', 'LastPriceDate', 'AssetClass', 'AssetType', 'Currency'
                    ]
ID_COLUMNS = ['ISIN', 'CUSIP', 'Ticker']
def check_positions(positions):

    if 'SecurityID' not in positions.columns:
        positions['SecurityID'] = sc.get_SecurityID_by_ref(positions)

    diff = set(POSITION_COLUMNS).difference(positions.columns)
    if len(diff) > 0:
        diff_str = str(diff)
        raise Exception('missing columns:' + f'missing columns in Position: {diff_str}')
    
    # check ID columns
    if len(set(positions.columns) & set(ID_COLUMNS)) == 0:
        col_str = ', '.join(ID_COLUMNS)
        raise Exception('missing columns: ' + f'Engine requires at least one ID columns: {col_str}')
        
    
def add_sec_type(positions):
    #security = sc1.get_securities_by_ID(positions['SecurityID'])    
    security = sc.get_security_by_ID(positions['SecurityID'])    
    #xl.add_df_to_excel(security, wb, 'security', index=False)    
    positions = positions.merge(security[['SecurityID', 'Currency', 'AssetClass', 'AssetType']], on='SecurityID', how='left')
    return positions


# get last available prices upto rpt_date
def get_last_prices(DATA, positions):
    df = positions.set_index('SecurityID')[['LastPrice', 'LastPriceDate']]
    df = df[~df.index.duplicated(keep='first')]
    return df
    
    # sec_ids = positions['SecurityID'].unique()
    # rpt_date = DATA['Parameters']['AsofDate']

    # from_date = date_utils.add_years(rpt_date, -1)
    # prices = mkt_data.get_market_data(sec_ids, from_date, rpt_date)
    # prices = prices.dropna(axis=1, how='all') # drop columns that do not have any value
    
    # last_prices = pd.DataFrame(columns=['LastPrice', 'LastPriceDate'])
    # for sec_id, data in prices.items():
    #     date = data.last_valid_index()
    #     price = data[date]
    #     last_prices.loc[sec_id] = [price, date]

    # last_prices.index.name = 'SecurityID'
        
    # # missing prices
    # missing = set(sec_ids).difference(last_prices.index)
    
    # # for missing prices, try user input
    # if len(missing) > 0:
    #     if ('user_LastPrice' in positions) and ('user_LastPriceDate' in positions):
    #         user_prices = (positions[['SecurityID', 'user_LastPrice', 'user_LastPriceDate']]
    #                        .set_index('SecurityID')
    #                        .rename(columns={'user_LastPrice': 'LastPrice', 'user_LastPriceDate': 'LastPriceDate'}))
        
    #         user_prices = user_prices[user_prices['LastPrice']>0]
    #         user_prices = user_prices[~user_prices.index.duplicated(keep='first')]

    #         for sec_id in missing & set(user_prices.index):
    #             print(f'use user price: {sec_id}')
    #             last_prices.loc[sec_id] = user_prices.loc[sec_id]

    # missing = set(sec_ids).difference(last_prices.index)
    # if len(missing) > 0:
    #     msg = ', '.join(list(missing))
    #     raise Exception(f'Missing Price: {msg}')
    
    # return last_prices

# underlying security price
# price_date=cob
def get_underlying_price(positions, price_date):
    # price_date = datetime.datetime(2024, 9, 10)
    from_date = price_date + datetime.timedelta(-10)
    
    # sec_ids = ['T10000108', 'T10000368']
    sec_ids = positions.loc[~positions['UnderlyingID'].isna(), 'UnderlyingID'].unique()
    if len(sec_ids) == 0: # no underlying sec_ids
        return pd.Series() # return an empty Series
    
    prices = mkt_data.get_market_data(sec_ids, from_date, price_date)
    if prices.empty:
        return pd.Series()

    return prices.ffill().iloc[-1]

    
#####################################################################
def calc_sensitivities_equity(DATA):
    positions = DATA['Positions']
    
    idx1 = positions['AssetClass'] == 'Equity'
    idx2 = (positions['AssetClass'] == 'Bond') & positions['AssetType'].isin(['Fund', 'ETF'])
    idx3 = positions['AssetClass'] == 'Alternative'
    idx4 = positions['AssetClass'] == 'Commodity'
    idx5 = positions['AssetClass'] == 'REIT'
    
    idx = idx1 | idx2 | idx3 | idx4 | idx5 
    positions.loc[idx, 'DELTA'] = positions['MarketValue']
    
    DATA['Positions'] = positions 
    
# xl.add_df_to_excel(positions, wb, 'test_pos', index=False)    

# calculate IR_PV01 and SP_PV01
# xl_utils.add_df_to_excel(bonds, wb, 'Bonds', index=True)
def calc_sensitivities_bond(DATA):
    positions = DATA['Positions']

    bonds = DATA['Bonds']    
    if len(bonds) == 0:
        DATA['Positions'] = positions
        return
    
    rpt_date = DATA['Parameters']['AsofDate']
    positions = DATA['Positions']

    last_prices = DATA['LastPrices']
    bonds['Price'] = last_prices['LastPrice'] / 100 # convert bond price to par price=1
    # bad bond prices    
    bad_price_bonds = bonds[(bonds['Price'] < 0.75) | (bonds['Price']>1.5) ].copy()
    if len(bad_price_bonds) > 0:
        bad_price_bonds['Message'] = bad_price_bonds['Price'].apply(lambda x: f'bad bond price: {x*100}')
        add_log(DATA, bad_price_bonds.reset_index()[['SecurityID', 'Message']])

    bonds['Price'] = np.where((bonds['Price']<0.75) | (bonds['Price']>1.5) , 1, bonds['Price']) # cap 1.5, floor=0.75
    sec_ids = bonds.index.tolist()

    # bond tenor
    bonds['Tenor']    = bonds.apply(lambda x: (x['MaturityDate'] - rpt_date).days/365.25, axis=1)
    bonds['IR_Tenor'] = bonds.apply(lambda x: (x['MaturityDate'] - rpt_date).days/365.25, axis=1)
    # assign negative tenor to 0
    bonds.loc[bonds['Tenor']<0, 'Tenor'] = 0
    bonds.loc[bonds['IR_Tenor']<0, 'IR_Tenor'] = 0
    
    # calculate sensitivities
    bonds['Yield']     = bonds.apply(lambda x: br.bond_yield(x['CouponRate'], x['Tenor'], x['PaymentFrequency'] , x['Price']), axis=1)
    bonds['Duration']  = bonds.apply(lambda x: br.bond_duration(x.Yield, x.CouponRate, x.Tenor, x['PaymentFrequency']), axis=1)
    bonds['Convexity'] = bonds.apply(lambda x: br.bond_convexity(x.Yield, x.CouponRate, x.Tenor, x['PaymentFrequency']), axis=1)

    # copy spread duration/convexity
    bonds['SpreadDuration'] = bonds['Duration']
    bonds['SpreadConvexity'] = bonds['Convexity']

    # for floating bonds, override duration
    idx = bonds['CouponType']=='Variable'
    bonds.loc[idx, 'IR_Tenor'] = 0.5
    bonds.loc[idx, 'Duration'] = bonds.loc[idx].apply(lambda x: br.bond_duration(x.Yield, x.CouponRate, x.IR_Tenor), axis=1)
    bonds.loc[idx, 'Convexity'] = bonds.loc[idx].apply(lambda x: br.bond_convexity(x.Yield, x.CouponRate, x.IR_Tenor), axis=1)
    
    # position sensitivity
    idx = positions['SecurityID'].isin(sec_ids)
    positions.loc[idx, 'Tenor'] = tools.df_series_merge(positions, bonds['Tenor'], 'SecurityID')
    positions.loc[idx, 'IR_Tenor'] = tools.df_series_merge(positions, bonds['IR_Tenor'], 'SecurityID')
    
    positions.loc[idx, 'Yield']    = tools.df_series_merge(positions, bonds['Yield'], 'SecurityID')
    positions.loc[idx, 'Duration'] = tools.df_series_merge(positions, bonds['Duration'], 'SecurityID')
    positions.loc[idx, 'Convexity'] = tools.df_series_merge(positions, bonds['Convexity'], 'SecurityID')

    # corp bond only 
    sp_idx = idx & (positions['AssetType'] != 'Treasury')
    positions.loc[sp_idx, 'SpreadDuration'] = tools.df_series_merge(positions, bonds['SpreadDuration'], 'SecurityID')
    positions.loc[sp_idx, 'SpreadConvexity'] = tools.df_series_merge(positions, bonds['SpreadConvexity'], 'SecurityID')

    positions.loc[idx, 'IR_PV01'] = -positions['MarketValue'] * positions['Duration'] * 0.0001
    positions.loc[idx, 'SP_PV01'] = -positions['MarketValue'] * positions['SpreadDuration'] * 0.0001
    
    # Add DELTA for FX risk
    positions.loc[idx, 'DELTA'] = positions['MarketValue']
    
    DATA['Positions'] = positions


# wb = xw.Book('Book1')
# xl.add_df_to_excel(positions, wb, 'pos')
# xl.add_df_to_excel(greeks, wb, 'Greeks')

def calc_sensitivities_derivative(DATA):
    calc_sensitivities_structured_notes(DATA)
    calc_sensitivities_options(DATA)
    
def calc_sensitivities_structured_notes(DATA):

    positions = DATA['Positions']
    
    idx = (positions['AssetClass'] == 'Derivative') & (positions['AssetType']== 'Structured Note')
    
    positions.loc[idx, 'DELTA'] = positions.loc[idx, 'MarketValue']
    
    DATA['Positions'] = positions 
    
def calc_sensitivities_options(DATA):

    positions = DATA['Positions']
    
    # equity options
    greeks = eq_option_var.calc_option_greeks(positions)
    
    # options
    idx = positions.is_option
    if idx.any():

        positions.loc[idx, 'DELTA'] = tools.df_series_merge(positions, greeks['DELTA'], 'SecurityID')
        positions.loc[idx, 'GAMMA'] = tools.df_series_merge(positions, greeks['GAMMA'], 'SecurityID')
        positions.loc[idx, 'VEGA'] = tools.df_series_merge(positions, greeks['VEGA'], 'SecurityID')
        positions.loc[idx, 'IV'] = tools.df_series_merge(positions, greeks['IV'], 'SecurityID')
        
        # calculate position greeks
        df = positions.loc[idx]
        contract_size = 100 # 100 shares per contract
        
        # position_delta = delta * S * q * 100
        positions.loc[idx, 'DELTA'] = df['DELTA'] * df['UnderlyingPrice'] * df['Quantity'] * contract_size

        # position_gamma = gamm * S^2 * q * 100
        positions.loc[idx, 'GAMMA'] = df['GAMMA'] * (df['UnderlyingPrice']**2) * df['Quantity'] * contract_size
        
        # position_vega = vega * q * 100
        positions.loc[idx, 'VEGA'] = df['VEGA'] * df['Quantity'] * contract_size
        
    DATA['Positions'] = positions

def calc_sensitivities(DATA):
    positions = DATA['Positions']
    
    
    positions[['DELTA', 'GAMMA', 'VEGA', 'IV',
               'IR_Tenor', 'Yield', 'Duration', 'Convexity', 'IR_PV01', 'SP_PV01',
               'Duration', 'Convexity', 'SpreadDuration', 'SpreadConvexity']] = 0.0
    
    DATA['Positions'] = positions 
    
    calc_sensitivities_equity(DATA)
    calc_sensitivities_bond(DATA)
    calc_sensitivities_derivative(DATA)
    calc_sensitivities_cash(DATA)
    
def calc_sensitivities_cash(DATA):
    positions = DATA['Positions']
    
    idx = positions['AssetClass'] == 'Cash'
    positions.loc[idx, 'DELTA'] = positions['MarketValue']
    
    DATA['Positions'] = positions

#######################################################################
def get_ir_risk_factors(positions):
    bonds = positions[(positions['AssetClass'] == 'Bond') & (positions['AssetType'].isin(['Bond', 'Treasury']))]
    return risk_factors.ir_risk_factors(bonds)
    
# xl.add_df_to_excel(rfactors, wb, 'riskfactors')
def calc_risk_factors(DATA):
    
    positions = DATA['Positions']
    
    # get static risk factors
    sec_ids = positions['SecurityID'].unique()
    rfactors = risk_factors.get_risk_factors(sec_ids)
    
    # add ir risk factors
    ir_factors = get_ir_risk_factors(positions)
    rfactors = pd.concat([rfactors, ir_factors])

    # add fx risk factors
    base_ccy = DATA['Parameters']['BaseCurrency']
    fx_factors = risk_factors.fx_risk_factors(positions, base_ccy)
    rfactors = pd.concat([rfactors, fx_factors])
    
    # add equity option risk factors
    option_factors = eq_option_var.risk_factors(positions)
    rfactors = pd.concat([rfactors, option_factors])
    
    # check if missing
    missing = list(set(sec_ids).difference(rfactors['SecurityID']))
    if len(missing) > 0:
        print('Missing risk_factors: ' + ','.join(missing))
        raise Exception('Missing risk_factors: ' + ','.join(missing))
    
    # join to position
    rfactors = positions[['pos_id', 'SecurityID']].merge(rfactors, on='SecurityID', how='outer')
    
    # Add position's sensitivity
    rfactors = rfactors.merge(positions[['pos_id', 'DELTA', 'IR_PV01', 'SP_PV01', 'GAMMA', 'VEGA']], on='pos_id', how='left')    
    
    rfactors.loc[rfactors['Category']=='DELTA' , 'Exposure'] = rfactors['Sensitivity'] * rfactors['DELTA']
    rfactors.loc[rfactors['Category']=='FX' ,    'Exposure'] = rfactors['Sensitivity'] * rfactors['DELTA']
    rfactors.loc[rfactors['Category']=='IR'    , 'Exposure'] = rfactors['Sensitivity'] * rfactors['IR_PV01'] * 10000   # ir timesereis is in decimal
    rfactors.loc[rfactors['Category']=='SPREAD', 'Exposure'] = rfactors['Sensitivity'] * rfactors['SP_PV01'] * 100     # spread timesereis is in percentage point
    rfactors.loc[rfactors['Category']=='GAMMA' , 'Exposure'] = rfactors['Sensitivity'] * rfactors['GAMMA']
    rfactors.loc[rfactors['Category']=='VEGA'   , 'Exposure'] = rfactors['Sensitivity'] * rfactors['VEGA']
    
    return rfactors

def calc_RF_PnL(DATA):
    rfactors = DATA['RiskFactor']
    log = DATA['Log']
    
    #xl.add_df_to_excel(rfactors, wb, 'rfactors')    
    
    PnL = {}
    
    nonlinear_cats = ['GAMMA']
    linear_cats = list(set(rfactors.Category).difference(nonlinear_cats))
    
    for cat in linear_cats:
        # print(cat)
        # cat = 'VEGA'
        rf = rfactors[rfactors.Category == cat]
        for ID in rf[rf.RF_ID.isna()]['pos_id']:
            log.loc[len(log)] = [ID, f'missing riskfactor ID for category: {cat}']
        
        rf = rf[~rf.RF_ID.isna()]
        rf_ids = rf['RF_ID'].unique()

        # get distribution
        ts = var_utils.get_dist(rf_ids, cat)
        missing = set(rf_ids).difference(set(ts.columns))
        if len(missing) > 0:
            raise Exception('missing ditribution', 'missing distribution for: ' + ", ".join(missing))
        #xl.add_df_to_excel(ts, wb, 'ts')    
        
        # remove RF that do not have time series
        rf = rf[rf.RF_ID.isin(ts)]

        # calculate P/L
        pl = pd.DataFrame(rf.Exposure.values * ts[rf.RF_ID].values, columns=rf.pos_id, index=ts.index)
        #xl.add_df_to_excel(pl, wb, 'pl')    
        
        # aggregate by pos_id
        pl = pl.T.groupby(level=0).sum().T

        PnL[cat] = pl
        
    # GAMMA
    PnL['GAMMA'] = calc_gamma_PnL(rfactors)

    # for debug purpose
    #count_pnl_na(PnL)
    
    return PnL

def calc_pnl_VaR_metrics(pnl):
    # result VaR data frame
    VaR = pd.DataFrame(index = pnl.columns)

    # volatility daily in Dollar
    VaR['STD'] = pnl.std()

    # marginal vol daily in Dollar
    VaR['Marginal_STD'] = var_utils.calc_marginal_vol(pnl)

    # VaR
    VaR['VaR']  = var_utils.calc_VaR(pnl)
    VaR['tVaR']  = var_utils.calc_tVaR(pnl)

    # marginal VaR
    VaR['Marginal_VaR'] = var_utils.calc_marginal_VaR(pnl) 
    VaR['Marginal_tVaR'] = var_utils.calc_marginal_tVaR(pnl)

    # xl.add_df_to_excel(VaR, wb, 'VaR')
    return VaR
    

def calc_gamma_PnL(rfactors):
    rf = rfactors[rfactors.Category == 'GAMMA']
    rf = rf[~rf.RF_ID.isna()]
    rf_ids = rf['RF_ID'].unique()

    # get PRICE ts and drop RFs that do not have ts
    ts = var_utils.get_dist(rf_ids, 'PRICE')
    
    missing = set(rf_ids).difference(set(ts.columns))
    if missing:
        raise Exception('missing distribution for: ' + ", ".join(missing))
    
    # remove RFs that do not have time series
    rf = rf[rf.RF_ID.isin(ts)]
    
    # PL = 0.5 * exposure * (dS/S)^2
    pl = pd.DataFrame(0.5 * rf.Exposure.values * (ts[rf.RF_ID].values**2), columns=rf.pos_id, index=ts.index)
    # xl.add_df_to_excel(rf.Exposure, wb, 'calc')
    # xl.add_df_to_excel(ts[rf.RF_ID], wb, 'calc', addr='D1')
    # xl.add_df_to_excel(pl, wb, 'calc', addr='H1')
    
    # aggregate by pos_id
    pl = pl.T.groupby(level=0).sum().T

    return pl

# xl_utils.add_df_to_excel(positions, wb, 'pos', index=False)
# xl_utils.add_df_to_excel(pl, wb, 'PL', index=False)
# len(positions)
# len(positions['pos_id'].unique())
# len(VaR)
def calc_position_VaR(DATA):
    PnL = DATA['RF_PnL']
    positions = DATA['Positions']
    T = DATA['Parameters']['HorizonDays']

    # aggregate rf_pnl to pos_pnl
    tot_pnl = pd.DataFrame()    
    for pl in PnL.values():
        tot_pnl = tot_pnl.add(pl, fill_value=0)

    DATA['PnL'] = tot_pnl

    # calculate VaR
    VaR = pd.DataFrame(index = positions['pos_id'])
    for cat, pl in PnL.items():
        # print(cat)
        # cat = 'IR'
        pl = PnL[cat]
        VaR[cat + ' VaR'] = var_utils.calc_VaR(pl) * np.sqrt(T)

    posVaR = calc_pnl_VaR_metrics(tot_pnl) * np.sqrt(T)
    VaR = pd.concat([VaR, posVaR], axis=1)

    VaR = VaR.reset_index()    
    
    # for cat, pl in PnL.items():
    #     xl.add_df_to_excel(pl, wb, f'{cat}_PnL')
    # xl.add_df_to_excel(tot_pnl, wb, 'tot_pnl')
    
    # xl.add_df_to_excel(VaR, wb, 'VaR')
    return VaR

# calc VaR by risk factors: IR, SPREAD, VOL, etc
def calc_rf_VaR(DATA):
    PnL = DATA['RF_PnL']
    T = DATA['Parameters']['HorizonDays']

    # calculate pnl
    tot_pnl = pd.DataFrame()
    for cat, pl in PnL.items():
        pl = pl.sum(axis=1).rename(cat)
        tot_pnl = pd.concat([tot_pnl, pl], axis=1)

    VaR = calc_pnl_VaR_metrics(tot_pnl) * np.sqrt(T)
    VaR.index.name = 'RF_Type'
    VaR = VaR.reset_index()
    
    return VaR
    

#####################################################################
# auxilary functions
def add_log(DATA, df):
    log = DATA['Log']
    DATA['Log'] = pd.concat([log, df])


def get_horizon_days(horizon):
    
    if horizon in validate_positions.RISK_HORIZON_DAYS:
        horizon_days = validate_positions.RISK_HORIZON_DAYS[horizon]
    else:
        raise Exception(f'Unknown Risk Horison: {horizon}')
        
    return horizon_days
    
def get_bond_info(sec_info):
    sec_ids = sec_info[(sec_info['AssetClass'] == 'Bond') & (sec_info['AssetType'].isin(['Bond', 'Treasury']))]['SecurityID']
    bonds = bond_info.get(sec_ids)
    missing = list(set(sec_ids).difference(bonds['SecurityID']))
    if len(missing) > 0:
        raise Exception('Missing Bond Info: ' + ', '.join(missing))

    # PaymentFrequency default to 2
    bonds['PaymentFrequency'] = bonds['PaymentFrequency'].fillna(2)
    
    return bonds.set_index('SecurityID')

    
###########################################################################
# def output_positions(DATA):

#     in_pos = DATA['InputPositions']
#     pos = DATA['Positions']
    
#     # rename columns
#     pos = pos.rename(columns={'AssetClass': 'EngineAssetClass', 'AssetType': 'EngineAssetType', 
#                               'LastPrice':'EngineLastPrice', 'LastPriceDate':'EngineLastPriceDate'})
    
#     # drop columns in input_positions
#     common_columns = list(set.intersection(set(pos), set(in_pos)))
#     pos = pos.drop(common_columns, axis=1)
#     pos['Error'] = 'Success'
    
#     # merge
#     out_pos = in_pos.merge(pos, left_index=True, right_on='pos_id', how='left').drop(['pos_id'], axis=1)
#     out_pos.loc[out_pos['SecurityID'].isna(), 'Error'] = 'Unknown Security'

#     return out_pos    
    

############################################################################
# Test auxiliary functions

def read_template_portfolio():
    file = r'test_data\engine.test.xlsx'
    positions = pd.read_excel(file, sheet_name='Positions')
    parameters = pd.read_excel(file, sheet_name='Parameters')
    
    return positions, parameters

def open_template():
    positions, parameters = read_template_portfolio()
    wb = xw.Book()
    xl.add_df_to_excel(positions, wb, 'Positions', index=False)
    xl.add_df_to_excel(parameters, wb, 'Parameters', index=False)
    return wb

def read_params(wb):
    params = xl.read_df_from_excel(wb, 'Parameters', index=True)
    params.index = [x.replace(' ', '')  for x in params.index]
    params = params[params.columns[0]].to_dict()
    
    return params

def read_positions(wb):
    positions = tools.read_positions(wb)
    return positions

# write to excel                
def write_to_excel(wb, DATA):
    # DATA.keys()

    # write to excel        
    for k, df in DATA.items():
        if k[-3:] == 'PnL': # skip PnL
            continue
        if k == 'Parameters': # skip
            continue
        
        if k == 'Positions': # rename 
            k = 'PosVaR'

        xl.add_df_to_excel(df, wb, k)
############################################################################
# TEST

def test():
    
    # wb = open_template()
    # wb = xw.Book('VaRCalculator.xlsm')
    # params = read_params(wb)
    # positions = read_positions(wb)

    params, positions = tools.load_test_portfolio()
    DATA = create_DATA(positions, params)
    pre_process(DATA)      
    
    # position_VaR(DATA)
    calc_sensitivities(DATA)
    
    positions = DATA['Positions']
    
    DATA.keys()
    # write to excel
    wb = xw.Book('Book1')
    write_to_excel(wb, DATA)

    xl.add_df_to_excel(positions, wb, 'pos')    
    
    
    # addhoc
    rf_pnl = DATA['RF_PnL']
    df = rf_pnl['IR']
    xl.add_df_to_excel(df, wb, 'pnl')
    
    
