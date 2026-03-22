# -*- coding: utf-8 -*-
"""
Created on Mon Sep  9 12:09:20 2024

@author: mgdin
"""

import pandas as pd
import xlwings as xw
from api import app, db
from trg_config import config
from engine import VaR_engine as engine
from security import security_info, security_sector, country_region, sector_proxy
from security import fund_sectors as fs
from engine.var_aggregation import calc_hierarchy_var

from mkt_data import mkt_timeseries
from models import risk_factors
from report import powerbi
from utils import var_utils, xl_utils, tools, data_utils, stat_utils


def pre_process():
    wb = xw.Book('BasketSectorModel.xlsx')

    # input positions
    pos_input = tools.read_positions(wb, 'pos_input')
    pos_input = pos_input[pos_input['Sector']=='Basket']
    pos_input = pos_input[pos_input['Class'] !='Alternative']
    print(len(pos_input))

    # fund sector decompositions
    fund_sectors = fs.get(pos_input['SecurityID'])
    # xl_utils.add_df_to_excel(fund_sectors, wb, 'fs', index=False)

    # sector proxy    
    sectors = sector_proxy.get().set_index('Sector')[['Ticker', 'SecurityID']]
    
    # decompose    
    pos_input = pos_input[pos_input['SecurityID'].isin(fund_sectors['SecurityID'])]
    pos_mtm = pos_input.groupby(by=['SecurityID'])['MarketValue'].sum()
    pos = pd.DataFrame(pos_mtm).reset_index()

    pos = pos.merge(fund_sectors[['SecurityID', 'Sector', 'Weight']], on='SecurityID', how='left')
    # xl_utils.add_df_to_excel(pos, wb, 'pos', index=False)

    # add sector security_id
    pos = pos.rename(columns={'SecurityID': 'BasketID'})
    pos = pos.merge(sectors, on='Sector', how='left')
    # xl_utils.add_df_to_excel(pos, wb, 'pos', index=False)

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
    xl_utils.add_df_to_excel(pos, wb, 'pos', index=False)

    for col in engine.POSITION_COLUMNS:
        if col not in pos.columns:
            print(f'missing {col}')
            pos[col] = None
    tools.write_positions(pos, wb)
    
    
    params = engine.read_params(wb)
    positions = engine.read_positions(wb)
    DATA = engine.calc_VaR(pos, params)

    if 'Error' in DATA:
        print(DATA['Error'])
        
        


    # calc agg var
    aggvar = agg_VaR(DATA)
    xl_utils.add_df_to_excel(aggvar, wb, 'AggVaR', index=False)
    

def agg_VaR(DATA):

    # add Account and Broker to the Positions
    input_pos = DATA['InputPositions']
    positions = DATA['Positions']
    positions['BasketID'] = tools.df_series_merge(positions, input_pos['BasketID'], key='pos_id')
    DATA['Positions'] = positions

    agg_var = pd.DataFrame()
    
    hierarchy = ['BasketID']
    df = calc_hierarchy_var(DATA, hierarchy, 'H0').reset_index()
    df['H_Group'] = 'H0'
    agg_var = pd.concat([agg_var, df])

    hierarchy = ['Sector']
    df = calc_hierarchy_var(DATA, hierarchy, 'H1').reset_index()
    df['H_Group'] = 'H1'
    agg_var = pd.concat([agg_var, df])

    return agg_var