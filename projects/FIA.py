# -*- coding: utf-8 -*-
"""
Created on Wed Aug  7 12:23:18 2024

@author: mgdin
"""
import pandas as pd
import numpy as np
import xlwings as xw
from pathlib import Path
from security import security_info, security_sector, country_region, sector_proxy
from security import bond_info, fund_sectors, fund_region, bond_fund_maturity, bond_fund_rating
from mkt_data import mkt_timeseries
from engine import VaR_engine as engine
from preprocess import read_portfolio
from engine.var_aggregation import calc_hierarchy_var
from report import powerbi as pbi
from utils import tools, xl_utils, data_utils, var_utils
from trg_config import config

PROJ_HOME = Path(r'C:\Users\mgdin\OneDrive - tailriskglobal.com\TRG\Clients\FIA')
WORK_DIR  = PROJ_HOME /  'Work'
WORK_BOOK = WORK_DIR / 'FIAE Report.v5.xlsx'
portfolio_file_path = PROJ_HOME / 'Portfolios' / 'FIAE Portfolio.xlsx'

def preprocess():
    wb = xw.Book(WORK_BOOK)
    
    positions = tools.read_positions(wb)
    
    # add SecurityID
    # positions['SecurityID2'] = security_info.get_SecurityID_by_ref(positions)
    # tools.write_positions(positions, wb)

    # add prices    
    sec_ids = positions['SecurityID'].to_list()
    df = mkt_timeseries.get(sec_ids)
    # xl_utils.add_df_to_excel(df, wb, 'Prices')
    cob = '2024-6-11'
    positions['LastPrice'] = tools.df_series_merge(positions, df.loc[cob], 'SecurityID')
    positions['LastPriceDate'] = cob
    xl_utils.add_df_to_excel(positions, wb, 'Positions', index=False)
    
    # ExpectedReturn
    if 'ExpectedReturn' not in positions.columns:
        df = data_utils.load_stat('ExpectedReturn')
        positions['ExpectedReturn'] = tools.df_series_merge(positions, df['ExpectedReturn'], key='SecurityID')        
        tools.write_positions(positions, wb)

    # Sector, industry, country
    sectors = security_sector.get(sec_ids).set_index('SecurityID')
    region =  country_region.get().set_index('Country')
    positions['Sector'] = tools.df_series_merge(positions, sectors['Sector'], key='SecurityID')        
    positions['Industry'] = tools.df_series_merge(positions, sectors['Industry'], key='SecurityID')        
    positions['Country'] = tools.df_series_merge(positions, sectors['Country'], key='SecurityID')        
    positions['Region'] = tools.df_series_merge(positions, region['Region'], key='Country')        
        

    for col in engine.POSITION_COLUMNS:
        if col not in positions.columns:
            positions[col] = None
    tools.write_positions(positions, wb)

    # add ticker
    wb = xw.Book('Securities.xlsx')    
    positions = tools.read_positions(wb, 'Securities2')
    sec_ids = positions['SecurityID'].to_list()
    df = security_info.get_xref_by_ID(sec_ids)        
    df = df[df['REF_TYPE']=='Ticker'][['SecurityID', 'REF_ID']]
    positions = positions.merge(df, on='SecurityID', how='left')
    tools.write_positions(positions, wb, 'sec4')
        
    
def report():
    # wb = xw.Book()
    # wb.save(WORK_BOOK)
    wb = xw.Book(WORK_BOOK)    

    # read positions
    params, positions = read_portfolio.read_input_file(portfolio_file_path)

    # write positions to workbook
    xl_utils.add_df_to_excel(positions, wb, tab='Positions', index=False)    
    
    params_df = read_portfolio.params_to_df(params)
    xl_utils.add_df_to_excel(params_df, wb, tab='Parameters', index=False)    
    
    # Asset allocation
    asset_allocation = positions.groupby(by=['Class'])['MarketValue'].sum()
    xl_utils.add_df_to_excel(asset_allocation, wb, 'allocation', index=True)

    # Calculate VaR
    DATA = calc_VaR(params, positions)
    
    # generate PBI Report
    results = pbi.generate_report2(DATA)
    # write results to the input file
    # wb1 = xw.Book()
    # write_pbi_to_excel(results, wb1)

    # sharpe ratio
    df = results['Fact_Agg Table']
    xl_utils.add_df_to_excel(df, wb, 'SharpeRatio', index=False)

    # Position VaR
    pos_var = get_pos_var(DATA)
    xl_utils.add_df_to_excel(pos_var, wb, 'PosVaR', index=False)

    # by Class
    df = agg_var_by(DATA, ['Class', 'SC1', 'SC2'])
    xl_utils.add_df_to_excel(df, wb, 'Class')

    # top 10 risk
    df = pos_var.sort_values(by=['tVaR'], ascending=False).head(10)
    xl_utils.add_df_to_excel(df, wb, 'TopRisk', index=False)

    # by broker
    df = agg_var_by(DATA, ['Broker'])
    xl_utils.add_df_to_excel(df, wb, 'Broker')

    # by country
    df = agg_var_by(DATA, ['Country'])
    xl_utils.add_df_to_excel(df, wb, 'Country')

    df = agg_region_decomp_VaR(DATA)
    xl_utils.add_df_to_excel(df, wb, 'CountryDecomp')

    # by sector
    df = agg_var_by(DATA, ['Sector'])
    xl_utils.add_df_to_excel(df, wb, 'Sector')

    df = agg_sector_decomp_VaR(DATA)
    xl_utils.add_df_to_excel(df, wb, 'SectorDecomp')

    # by rating
    df = calc_rating_VaR(DATA, BaseCurrency='USD')
    xl_utils.add_df_to_excel(df, wb, 'Rating-USD')

    df = calc_rating_VaR(DATA, BaseCurrency='CLP')
    xl_utils.add_df_to_excel(df, wb, 'Rating-CLP')

    # by tenor
    df = calc_tenor_VaR(DATA, 'USD')
    xl_utils.add_df_to_excel(df, wb, 'Maturity')

    df = calc_tenor_VaR(DATA, 'CLP')
    xl_utils.add_df_to_excel(df, wb, 'Maturity-CLP')



    
#########################################################################################
def get_pos_var(DATA):
    input_pos = DATA['InputPositions']
    var = DATA['VaR']
    pos_var = input_pos.merge(var, left_index=True, right_on='pos_id', how='left')
    return pos_var


def agg_sector_decomp_VaR(DATA, root='H0'):
    positions = DATA['InputPositions']
    params = DATA['Parameters']

    # equity position only
    positions = positions[positions['Class']=='Equity']

    # decompose basket    
    pos_basket = positions[ positions['Sector']=='Basket']
    positions  = positions[~positions['SecurityID'].isin(pos_basket['SecurityID'])]

    pos_decomp = sector_decomposition(pos_basket)
    positions = pd.concat([positions, pos_decomp], ignore_index=True)

    DATA = calc_VaR(params, positions)
    df = agg_var_by(DATA, ['Sector'])

    return df

def agg_region_decomp_VaR(DATA, root='H0'):
    positions = DATA['InputPositions']
    params = DATA['Parameters']
    
    print('total positions:', len(positions))

    # decompose basket    
    pos_basket = positions[positions['Country']=='Basket']
    positions = positions[~positions['SecurityID'].isin(pos_basket['SecurityID'])]
    print('split basket:non-basket ', len(pos_basket), len(positions))
    
    pos_decomp = region_decomposition(pos_basket)
    positions = pd.concat([positions, pos_decomp], ignore_index=True)

    DATA = calc_VaR(params, positions)
    df = agg_var_by(DATA, ['Region', 'Country'])

    return df
    
    
def calc_rating_VaR(DATA, BaseCurrency='USD'):
    positions = DATA['InputPositions']
    params = DATA['Parameters']
    pos_calc = DATA['Positions']

    # Change base currency in params
    BaseCurrency_save = params['BaseCurrency']
    params['BaseCurrency'] = BaseCurrency
    
    
    # fixed income positions
    positions = positions[positions['Class']=='Fixed Income'].copy()
    print('total positions:', len(positions))
    
    # add rating
    binfo = bond_info.get(positions['SecurityID']).set_index('SecurityID')
    positions['Rating'] = tools.df_series_merge(positions, binfo['Rating'], key='SecurityID')
    
    # override treasury to AAA
    positions.loc[positions['SC2']=='Treasury', 'Rating'] = 'Treasury'
    
    # add currency
    pos_currency = pos_calc.drop_duplicates(subset=['SecurityID']).set_index('SecurityID')['Currency']
    positions['Currency'] = tools.df_series_merge(positions, pos_currency, key='SecurityID')

    # decompose basket    
    pos_basket = positions[positions['SC2'].isin(['Bond Fund', 'Bond ETF', 'Local Bond Fund'])]
    positions = positions[~positions['SecurityID'].isin(pos_basket['SecurityID'])]
    print('split basket:non-basket ', len(pos_basket), len(positions))
    
    # assign BBB to the missing bonds
    positions.loc[positions['Rating'].isna(), 'Rating'] = 'BBB'

    # break down the basket
    pos_decomp = rating_decomposition(pos_basket)
    positions = pd.concat([positions, pos_decomp], ignore_index=True)

    # normalize rating    
    rating_map = get_rating_map()
    positions = positions.merge(rating_map, on='Rating', how='left')

    # calc VaR
    # positions = positions[positions['SecurityID']=='T10001167']
    positions['Class'] = positions['Rating2']
    positions['SC1'] = positions['Currency'].apply(lambda x: 'USD' if x=='USD' else 'NonUSD')
    DATA = calc_VaR(params, positions)
    df = agg_var_by(DATA, ['Class', 'SC1'])

    # put back the original base currency    
    params['BaseCurrency'] = BaseCurrency_save
    
    return df

    

    # Rating VaR
    # agg_var = calc_hierarchy_var(DATA, ['Class'], 'H0')
    # agg_var.index = [x[3:] if x!='H0' else 'Total' for x in agg_var.index]
    # agg_var['MarketValue'] = positions.groupby(by=['Class'])['MarketValue'].sum()
    # agg_var.loc['Total', 'MarketValue'] = positions['MarketValue'].sum()
    # agg_var['VaR%'] = agg_var['tVaR'] / agg_var['MarketValue']
    # agg_var['weight'] = agg_var['MarketValue'] / agg_var.loc['Total', 'MarketValue']
    # agg_var['risk'] = agg_var['MarginalVaR'] / agg_var.loc['Total', 'MarginalVaR']
    # agg_var.index.name = 'Rating'

    # re-order
    # df = xl_utils.read_df_from_excel(wb, 'map', addr='F1')
    # index = df[df['rating2'].isin(agg_var.index)]['rating2'].to_list()
    # agg_var = agg_var.loc[index]
    # xl_utils.add_df_to_excel(agg_var, wb, 'rating_var')    
    
    # risk factor
    # df = DATA['RiskFactor']
    # xl_utils.add_df_to_excel(df, wb, 'rf', index=False)    
    
# BaseCurrency='CLP'
def calc_tenor_VaR(DATA, BaseCurrency='USD'):
    positions = DATA['InputPositions']
    params = DATA['Parameters']
    pos_calc = DATA['Positions']
    
    # Change base currency in params
    BaseCurrency_save = params['BaseCurrency']
    params['BaseCurrency'] = BaseCurrency

    # fixed-income positions
    positions = positions[positions['Class']=='Fixed Income'].copy()
    print('total positions:', len(positions))
    
    # add maturity
    binfo = bond_info.get(positions['SecurityID']).set_index('SecurityID')
    positions['Maturity'] = tools.df_series_merge(positions, binfo['MaturityDate'], key='SecurityID')
    
    # add tenor bucket
    rpt_date = params['AsofDate']
    positions['Tenor'] = positions['Maturity'].apply(lambda x: (x - rpt_date).days/365.25)
    positions['MaturityBucket'] = pd.cut(positions['Tenor'], bins=[0,1,3,5,10,20,100], 
                                         labels=['0 - 1 Year', '1 - 3 Year', '3 - 5 Year', '5 - 10 Year', '10 - 20 Year', '20+ Year'])
    
    # add currency
    pos_currency = pos_calc.drop_duplicates(subset=['SecurityID']).set_index('SecurityID')['Currency']
    positions['Currency'] = tools.df_series_merge(positions, pos_currency, key='SecurityID')
    
    # decompose basket    
    pos_basket = positions[positions['SC2'].isin(['Bond Fund', 'Bond ETF', 'Local Bond Fund'])]
    positions = positions[~positions['SecurityID'].isin(pos_basket['SecurityID'])]
    print('split basket:non-basket ', len(pos_basket), len(positions))

    # break down the basket
    pos_decomp = tenor_decomposition(pos_basket)
    positions = pd.concat([positions, pos_decomp], ignore_index=True)
    
    # missing maturity 
    missing = positions.loc[positions['MaturityBucket'].isna()] 
    if len(missing):
        print('missing maturity', missing['SecurityID'].to_list())
    
    # override 'Cash and Derivatives' to Others
    positions.loc[positions['MaturityBucket']=='Cash and Derivatives', 'MaturityBucket'] = 'Others'
    
    # calc VaR
    positions['Class'] = positions['MaturityBucket']
    positions['SC1'] = (positions['Currency'].apply(lambda x:  'CLP' if x=='UF' else x)
          .apply(lambda x: BaseCurrency if x==BaseCurrency else 'Other'))
    
    DATA = calc_VaR(params, positions)
    df = agg_var_by(DATA, ['Class', 'SC1'])

    # put back the original base currency    
    params['BaseCurrency'] = BaseCurrency_save

    return df
    
    # wb = xw.Book(WORK_BOOK)    
    # xl_utils.add_df_to_excel(positions, wb, 'pos', index=False)    
    # xl_utils.add_df_to_excel(pos_decomp, wb, 'TMP', index=False)    
    # xl_utils.add_df_to_excel(df, wb, 'df', index=True)    


def calc_Equity_VaR():
    wb = xw.Book(WORK_BOOK)
    
    # call engin to calculat VaR
    params = engine.read_params(wb)
    positions = engine.read_positions(wb)

    # Equity positions
    positions = positions[positions['Class']=='Equity']
    print('total positions:', len(positions))

    # calc VaR
    # positions = positions[positions['SecurityID']=='T10001167']
    positions['Class'] = positions['SC1'] + ' - ' + positions['SC2']
    DATA = engine.calc_VaR(positions, params)
    if 'Error' in DATA:
        print(DATA['Error'])
    
    # Maturity VaR
    
    agg_var = calc_hierarchy_var(DATA, ['Class'], 'H0')
    agg_var.index = [x[3:] if x!='H0' else 'Total' for x in agg_var.index]
    agg_var['MarketValue'] = positions.groupby(by=['Class'])['MarketValue'].sum()
    agg_var.loc['Total', 'MarketValue'] = positions['MarketValue'].sum()
    
    agg_var['VaR%'] = agg_var['tVaR'] / agg_var['MarketValue']
    agg_var['weight'] = agg_var['MarketValue'] / agg_var.loc['Total', 'MarketValue']
    agg_var['risk'] = agg_var['MarginalVaR'] / agg_var.loc['Total', 'MarginalVaR']
    agg_var.index.name = 'AssetClass'

    xl_utils.add_df_to_excel(agg_var, wb, 'Equity', addr='A1')    

    # Pos VaR
    pos_var = DATA['VaR']
    pos = DATA['Positions']
    pos_var = pos.merge(pos_var, on='pos_id', how='left')    
    pos_var['VaR%'] = pos_var['tVaR'] / pos_var['MarketValue']
    xl_utils.add_df_to_excel(pos_var, wb, 'pos_var', index=False)    

def sector_decomposition(pos_basket):

    # fund sector decompositions
    sector_decomp = fund_sectors.get(pos_basket['SecurityID'])

    # positions that have sector decomposition
    pos1 = pos_basket[pos_basket['SecurityID'].isin(sector_decomp['SecurityID'])]
    pos2 = pos_basket[~pos_basket['SecurityID'].isin(pos1['SecurityID'])]  
    print(len(pos_basket), len(pos1),  len(pos2))
    
    # sector proxy
    sectors = sector_proxy.get().set_index('Sector')[['Ticker', 'SecurityID']]

    # aggregate by securityID
    pos_mtm = pos1.groupby(by=['SecurityID'])['MarketValue'].sum()
    pos = pd.DataFrame(pos_mtm).reset_index()

    # decomp to sectors
    pos = pos.merge(sector_decomp[['SecurityID', 'Sector', 'Weight']], on='SecurityID', how='left')

    # add sector security_id
    pos = pos.rename(columns={'SecurityID': 'BasketID'})
    pos = pos.merge(sectors, on='Sector', how='left')

    # add government
    df = pos.loc[pos['Sector']=='Government']
    pos = pos.loc[pos['Sector'] !='Government']

    # 50% long, 50% short
    df['Weight'] = df['Weight'] * 0.5
    
    # Long term 
    df[['Ticker', 'SecurityID']] = sectors.loc['Government Long'].tolist()
    pos = pd.concat([pos, df], ignore_index=True)

    # short term 
    df[['Ticker', 'SecurityID']] = sectors.loc['Government Short'].tolist()
    pos = pd.concat([pos, df], ignore_index=True)
    
    # update MTM
    pos['MarketValue'] = pos['MarketValue'] * pos['Weight']
    
    pos = pd.concat([pos2, pos])
    
    # xl_utils.add_df_to_excel(pos, wb, 'pos', index=True)        
    
    return pos
    


def region_decomposition(pos_basket):

    # fund sector decompositions
    regon_decomp = fund_region.get(pos_basket['SecurityID']).rename(columns={'Weight': 'RegionWeight'})
    
    # decompose basket positions 
    pos = pos_basket.drop('Region', axis=1)
    pos = pos.merge(regon_decomp[['SecurityID', 'Region', 'RegionWeight']], on='SecurityID', how='left')
    
    # update MTM
    pos['MarketValue'] = pos['MarketValue'] * pos['RegionWeight']
    
    return pos
    
def rating_decomposition(pos_basket):
    print('number of baskets:', len(pos_basket))
    
    # fund sector decompositions
    rating_decomp = bond_fund_rating.get(pos_basket['SecurityID']).rename(columns={'Weight': 'RatingWeight'})

    # missing
    missing = set(pos_basket['SecurityID']).difference(rating_decomp['SecurityID'])
    if len(missing)>0:
        raise Exception("missing basket decomposition:", list(missing))
    
    # decompose basket positions 
    if 'Rating' in pos_basket.columns:
        pos = pos_basket.drop('Rating', axis=1)
    else:
        pos = pos_basket
    pos = pos.merge(rating_decomp[['SecurityID', 'Rating', 'RatingWeight']], on='SecurityID', how='left')
    print(len(pos))
    
    # update MTM
    pos['MarketValue'] = pos['MarketValue'] * pos['RatingWeight']
    
    return pos
    
def tenor_decomposition(pos_basket):
    print('number of baskets:', len(pos_basket))
    
    # fund tenor decompositions
    tenor_decomp = bond_fund_maturity.get(pos_basket['SecurityID'])
    tenor_decomp.rename(columns={'Maturity': 'MaturityBucket', 'Weight': 'TenorWeight'}, inplace=True)

    # missing
    missing = set(pos_basket['SecurityID']).difference(tenor_decomp['SecurityID'])
    if missing:
        raise Exception("missing basket decomposition:", list(missing))
    
    # decompose basket positions 
    if 'MaturityBucket' in pos_basket.columns:
        pos = pos_basket.drop('MaturityBucket', axis=1)
    else:
        pos = pos_basket
    pos = pos.merge(tenor_decomp[['SecurityID', 'MaturityBucket', 'TenorWeight']], on='SecurityID', how='left')
    print(len(pos))
    
    # tenor duration
    duration = get_tenor_duration()[['MaturityBucket', 'Duration']]
    pos = pos.merge(duration, on='MaturityBucket', how='left')
    pos['WeightedDuration'] = pos['TenorWeight'] * pos['Duration']
    waDuration = pos.groupby('SecurityID')['WeightedDuration'].sum()
    pos['BasketDuration'] = tools.df_series_merge(pos, waDuration, key='SecurityID')
    
    # update MTM
    pos['MarketValue'] = pos['MarketValue'] * pos['WeightedDuration'] / pos['BasketDuration']
    
    return pos


###############################################################################
# auxilary functions

def calc_VaR(params, positions):
    
    DATA = engine.calc_VaR(positions, params)
    if 'Error' in DATA:
        print(DATA['Error'])
        raise Exception(DATA['Error'])

    return DATA


def calc_var_by(params, positions, hierarchy, root = 'H0'):
    DATA = calc_VaR(params, positions)
    df = agg_var_by(DATA, hierarchy, root)
    return df
    
    
# hierarchy=['Class', 'SC1', 'SC2']
# root = 'H0'
def agg_var_by(DATA, hierarchy=['AssetClass', 'AssetType'], root = 'H0'):
    
    positions = DATA['Positions']
    positions_input = DATA['InputPositions']
    df = positions.copy()
    
    # make sure all hierarchy values are in position columns
    for col in hierarchy:
        if col not in positions.columns:
            print(f'add col to positions: {col}')
            # copy from positions_input
            if col not in positions_input.columns:
                raise Exception(f'can not find hierarchy {col} in position or position_input')
            df = df.merge(positions_input[col], left_on='pos_id', right_index=True, how='left')

    DATA['Positions'] = df

    if 'Marginal_tVaR' not in df.columns:
        var = DATA['VaR']
        pos_var = df.merge(var, on='pos_id', how='left')
    else:
        pos_var = df

    df = calc_hierarchy_var(DATA, hierarchy, root)
    df[['MarketValue', 'top_mVaR']] = agg_metrics(pos_var, hierarchy, root)
    df['Root'] = root
    
    # add the original positions back to DATA
    DATA['Positions'] = positions
    
    return df

# positions = pos_var
def agg_metrics(positions, hierarchy, root='H0'):
    metrics = ['MarketValue', 'Marginal_tVaR']
    
    mtm = pd.DataFrame()    
    while len(hierarchy) > 0 :
        m1 = positions.groupby(by=hierarchy)[metrics].sum()
        if len(hierarchy) > 1:
            m1.index = [root + '|' + '|'.join(x) for x in m1.index]
        else:
            m1.index = [root + '|' + x for x in m1.index]
        mtm = pd.concat([mtm, m1])
        hierarchy = hierarchy[:-1]         
        
    mtm.loc[root] = positions[metrics].sum()
    return mtm

def agg_mtm(positions, hierarchy, root):
    # add mtm
    
    mtm = pd.DataFrame()    
    while len(hierarchy) > 0 :
        m1 = positions.groupby(by=hierarchy)['MarketValue'].sum()
        if len(hierarchy) > 1:
            m1.index = [root + '|' + '|'.join(x) for x in m1.index]
        else:
            m1.index = [root + '|' + x for x in m1.index]
        mtm = pd.concat([mtm, m1])
        hierarchy = hierarchy[:-1]         
        
    mtm.loc[root] = positions['MarketValue'].sum()
    return mtm
    

PROJ_DATA_DIR = config['DATA_DIR'] / 'projects' / 'FIAE'
def dump_data():
    wb = xw.Book('FIAE Report.v3.xlsx')
    df = xl_utils.read_df_from_excel(wb, 'map', addr='A1')
    df.to_csv(PROJ_DATA_DIR / 'rating_map.csv', index=False)

    df = xl_utils.read_df_from_excel(wb, 'map', addr='N1')
    df.to_csv(PROJ_DATA_DIR / 'tenor_duration_map.csv', index=False)


def get_rating_map():
    df = pd.read_csv(PROJ_DATA_DIR / 'rating_map.csv')
    return df

def get_tenor_duration():
    df = pd.read_csv(PROJ_DATA_DIR / 'tenor_duration_map.csv')
    return df
    
def models():
    wb = xw.Book(WORK_DIR / 'Models.xlsx')
    
    
def fx_raw_data():
    filepath = config['DATA_DIR'] / 'raw_market_data' / 'FIAE.FX.20240709.xlsx'
    wb = xw.Book(filepath)
    
    
    
    data = {}
    for tab in [sht.name for sht in wb.sheets]:
        print(tab)

        ticker = tab.replace('-', '/')    
        sec_id = security_info.get_ID_by_Ticker([ticker])['SecurityID'].iloc[0]
        
        # tab = 'USD-CLP'
        df = xl_utils.read_df_from_excel(wb, tab, index=True)
        df.index = pd.to_datetime(df.index, format='%d.%m.%Y')
        data[sec_id] = df.iloc[:,0]
    
    df = pd.concat(data, axis=1)    
    df.index.name = 'Date'
    
    # write date to excel
    wb = xw.Book('FIAE.FX.xlsx')
    xl_utils.add_df_to_excel(df, wb, 'hist')    
    
    # to upload timeseries, go to xl_mkt_data.load_FIAE_raw() 
    
def write_to_excel(wb, DATA):

    # merge VaR with Positions
    pos = DATA['Positions']
    var = DATA['VaR']
    DATA['Positions'] = pos.merge(var, on='pos_id', how='left')
    

    # write to excel        
    for k, df in DATA.items():
        if k[-3:] == 'PnL': # skip PnL
            continue
        if k == 'Parameters': # skip
            continue
        
        if k == 'Positions': # rename 
            k = 'PosVaR'

        xl_utils.add_df_to_excel(df, wb, k)    
    
    
    
def write_pbi_to_excel(book, wb):
    for tab in book:
        xl_utils.add_df_to_excel(book[tab], wb, tab=tab)
    
    
    