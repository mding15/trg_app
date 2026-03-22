# -*- coding: utf-8 -*-
"""
Created on Fri May 10 10:36:53 2024

@author: mgdin
"""
import pandas as pd
import xlwings as xw

from trg_config import config
from utils import tools, xl_utils, mkt_data, date_utils

class ModelManager:
    def __init__(self, version):
        
        model_folder = config['MODEL_DIR'] / version 
        if not model_folder.exists():
            model_folder.mkdir(parents=True, exist_ok=True)
            print('mkdir:', model_folder)

        self.model_version   = version
        self.model_folder    = model_folder


    # params is a dict
    def save_paramters(self, params):
        filename = self.model_folder / 'parameters.csv'
        df = tools.dict_to_df(params)
        df.to_csv(filename, index=False)
        print('saved to:', filename)
    
    def read_parameter(self):
        filename = self.model_folder / 'parameters.csv'
        df = pd.read_csv(filename)
        params = df['Value'].to_dict()
        return params
        
    # securities is a dataframe
    def save_securities(self, securities):
        filename = self.model_folder / 'securities.csv'
        securities.to_csv(filename, index=False)
        print('saved to:', filename)
    
    def read_securities(self):
        filename = self.model_folder / 'securities.csv'
        df = pd.read_csv(filename)
        return df
        
    # core factors
    def save_core_factor(self, df):
        filename = self.model_folder / 'core_factors.csv'
        df.to_csv(filename, index=False)
        print('saved to:', filename)
    
    def read_core_factor(self):
        filename = self.model_folder / 'core_factors.csv'
        df = pd.read_csv(filename)
        return df
    
        
    
    
def test():
    wb = xw.Book('EquityModel.v1.1.xlsx')
    params =  tools.read_parameter(wb)

    model_version = params['Model Version']
    
    mmgr = ModelManager(model_version)
    mmgr.save_paramters(params)

    # Core factors
    df = xl_utils.read_df_from_excel(wb, 'CoreFactors')
    mmgr.save_core_factor(df)
    
    # securities
    securities = xl_utils.read_df_from_excel(wb, 'Securities')
    mmgr.save_securities(securities)
    
    # get timeseries
    start_date, end_date = params['TS Start Date'], params['TS End Date']
    sec_ids = securities['SecurityID'].to_list()
    prices = mkt_data.get_market_data(sec_ids, start_date, end_date)

    xl_utils.add_df_to_excel(prices, wb, 'mkt_data')
    
    # stat
    stat = pd.DataFrame(index=sec_ids)
    stat.index.name='SecurityID'
    stat['from_date'] = date_utils.get_first_date(prices)
    stat['end_date']  = date_utils.get_last_date(prices)
    stat['ts length'] = prices.count()
    xl_utils.add_df_to_excel(stat, wb, 'stat')






