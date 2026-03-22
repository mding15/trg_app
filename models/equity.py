# -*- coding: utf-8 -*-
"""
Created on Sun Jan  7 20:29:13 2024

@author: mgdin

"""

import numpy as np
import pandas as pd
import xlwings as xw

from trg_config import config
from models import model_utils
from utils import xl_utils as xl
from utils import mkt_data, tools, var_utils, date_utils, stat_utils

TS_MIN_LEN = 100
    
    
##################################################################################
class EquityModel:
    def __init__(self, securities, core_factors, start_date, end_date, model_id, submodel_id, model_name, num_simulation=1000):
        
        self.Securities     = securities
        self.CoreFactors    = core_factors
        self.start_date     = pd.to_datetime(start_date)
        self.end_date       = pd.to_datetime(end_date)
        self.model_id       = model_id
        self.submodel_id    = submodel_id
        self.model_name     = model_name
        self.num_simulation = int(num_simulation)
        self.Parameters     = self.get_model_params()
        
        self.model_folder   = config['MODEL_DIR'] / model_id / submodel_id
        
        self.exception = None
        self.sec_ids_no_ts = []
        self.regress_df  = None
        self.residual_df = None
        
        self.res_index = None
        self.stat_df  = None
        
        self.core_factor_timeseries = None
        self.security_timeseries    = None 
        
        self.core_dist = None
        self.idio_dist = None
        self.sys_dist  = None
        self.simulated_dist = None

        if not self.model_folder.exists():
            self.model_folder.mkdir(parents=True, exist_ok=True)
        

    def get_security_timeseries(self):
        if self.security_timeseries is None:
            sec_ids = self.Securities['SecurityID'].to_list()
    
            # historical price time series
            prices = mkt_data.get_market_data(sec_ids, self.start_date, self.end_date)
            
            # check missing timeseries
            missing = list(set(sec_ids).difference(prices.columns))
            if len(missing) > 0:
                missing_ids = ', '.join(missing)
                raise Exception(f'missing timeseries for: {missing_ids}')
            
            # business dates
            dates = date_utils.get_bus_dates(self.start_date, self.end_date)
            
            # standardize the index using business dates
            df = pd.DataFrame(index=pd.Index(dates, name='Date'))
            
            self.security_timeseries = df.merge(prices, left_index=True, right_index=True, how='left')

        return self.security_timeseries
        
    def get_core_factors_timeseries(self):
        if self.core_factor_timeseries is None:
            core_factors = self.CoreFactors
            sec_ids = core_factors['SecurityID'].to_list()
            
            # historical time series
            prices = mkt_data.get_market_data(sec_ids)
    
            # business dates
            dates = date_utils.get_bus_dates(self.start_date, self.end_date)
            
            # standardize the index using business dates
            df = pd.DataFrame(index=pd.Index(dates, name='Date'))
            prices = df.merge(prices, left_index=True, right_index=True, how='left')

            # use ticke as column names
            prices.columns = core_factors['Ticker'].to_list()
            
            self.core_factor_timeseries = prices

        return self.core_factor_timeseries    
    
    def check_count(self, df):
        count = df.count()
        bad = count[count != self.num_simulation]
        return bad

    def save_model_data(self, attr_name, index=False):
        df = getattr(self, attr_name)
        if df is not None:
            file_path = self.model_folder / f'{attr_name}.csv'
            df.to_csv(file_path, index=index)
            print('saved to:', file_path)
            
    def read_model_data(self, attr_name, index_col=None):
        file_path = self.model_folder / f'{attr_name}.csv'
        if file_path.exists():
            df = pd.read_csv(file_path, index_col=index_col)
            setattr(self, attr_name, df)
        else:
            print('can not find file:', file_path)

    def save_dist(self):
        var_utils.create_var_model(self.model_id, self.start_date, self.end_date, self.num_simulation)
        var_utils.set_model_id(self.model_id)
        var_utils.save_dist(self.simulated_dist, 'PRICE')
    
    def get_model_params(self):
        params = {'Model Name': self.model_name, 
                  'Model ID': self.model_id,
                  'Submodel ID': self.submodel_id,
                  'TS Start Date': self.start_date.strftime('%Y-%m-%d'),
                  'TS End Date': self.end_date.strftime('%Y-%m-%d'),
                  'Number of Simulations': self.num_simulation
                  }
        return tools.dict_to_df(params)

    def save_model(self):
        self.save_model_data('Parameters')
        self.save_model_data('Securities')
        self.save_model_data('CoreFactors')
        self.save_model_data('security_timeseries', True)
        self.save_model_data('core_factor_timeseries', True)
        self.save_model_data('res_index')  
        self.save_model_data('regress_df', True)  
        self.save_model_data('residual_df',True)  
        self.save_model_data('stat_df', True)  
        self.save_model_data('simulated_dist', True)  
        self.save_model_data('idio_dist', True)  
        self.save_model_data('sys_dist', True)  
        self.save_model_data('exception', True)  

        # update model info
        self.save_model_info()
        
        # save dist to VaR_file
        self.save_dist()
        
    def save_model_info(self):
        model_utils.update_model_info(self.model_id, self.submodel_id, self.Securities)
        
    @classmethod
    def load_model(cls, model_id, submodel_id):
        model = EquityModel.create_model_from_files(model_id, submodel_id)
    
        model.read_model_data('security_timeseries', index_col=0)
        model.read_model_data('core_factor_timeseries', index_col=0)
        model.read_model_data('res_index')
        model.read_model_data('regress_df', index_col=0)
        model.read_model_data('residual_df', index_col=0)
        model.read_model_data('stat_df', index_col=0)
        model.read_model_data('stat_df', index_col=0)
        model.read_model_data('simulated_dist', index_col=0)
        model.read_model_data('idio_dist', index_col=0)
        model.read_model_data('sys_dist', index_col=0)
        
        model.convert_dates()
        return model

    def convert_dates(self):
        self.security_timeseries.index     = pd.to_datetime(self.security_timeseries.index)
        self.core_factor_timeseries.index  = pd.to_datetime(self.core_factor_timeseries.index)
        self.residual_df.index  = pd.to_datetime(self.residual_df.index)
        self.stat_df['start_date']  = pd.to_datetime(self.stat_df['start_date'] )
        self.stat_df['end_date']  = pd.to_datetime(self.stat_df['end_date'] )
        
        
    @classmethod
    def create_model_from_files(cls, model_id, submodel_id):
        model_folder = config['MODEL_DIR'] / model_id / submodel_id
        if not model_folder.exists():
            raise Exception(f'folder does not existis: {model_folder}')
        
        filename = model_folder / 'Parameters.csv'
        params =  tools.read_parameter_csv(filename)
        
        filename = model_folder / 'Securities.csv'
        Securities = pd.read_csv(filename)
        
        filename = model_folder / 'CoreFactors.csv'
        CoreFactors =  pd.read_csv(filename)

        model = cls(Securities, CoreFactors,
                    start_date     = params['TS Start Date'],
                    end_date       = params['TS End Date'],
                    model_id       = params['Model ID'],
                    submodel_id    = params['Submodel ID'],
                    model_name     = params['Model Name'],
                    num_simulation = params['Number of Simulations'])
        
        return model
        
    def read_equity_beta(self):
        df = self.read_model_data('equity_betas.csv')
        df = df.set_index('SecurityID')
        return df
    
    def read_equity_residuals(self):
        df = self.read_model_data('equity_residuals.csv')
        df = df.set_in('Date')
        df.index = pd.to_datetime(df.index)
        return df
    
    def read_res_index(self):
        
        index = pd.read_csv('equity_res_index.csv')
        index['Date'] = pd.to_datetime(index['Date'])
        
        return index

        
    def get_core_factors_dist(self):
        #self = model
        if self.core_dist is None:
            prices = self.get_core_factors_timeseries()
            
            prices = prices.ffill()
            pct_return = prices.pct_change(1)
            pct_return = pct_return - pct_return.mean()
            log_return = np.log( 1 + pct_return )
            log_return.replace(0, np.nan, inplace=True)    
            
            # use res_index for now, this can random
            index = self.get_res_index()
            core_dist = log_return.reindex(index['Date'])
            core_dist = model_utils.fill_na_with_rand_sampling(core_dist)
            
            self.core_dist = core_dist
        
        return self.core_dist        

    # run regression against core factors
    # save betas to csv file
    def run_regression(self):
        
        Y = self.get_security_timeseries()
        X = self.get_core_factors_timeseries()

        # filter out timeseries that have length less than TS_MIN_LEN
        c = Y.count()
        sec_ids_no_ts = c[c<TS_MIN_LEN].index
        Y.drop(sec_ids_no_ts, axis=1, inplace=True)  
        df = self.Securities
        df = df[df['SecurityID'].isin(sec_ids_no_ts)].copy()
        df['Exception'] = 'No timeseries'
        self.exception = df
        self.Securities = self.Securities[~self.Securities['SecurityID'].isin(sec_ids_no_ts)]
        

        # results dataframes
        columns = X.columns.to_list() + ['R-Sq', 'Vol']
        regress_df  = pd.DataFrame(columns=columns)
        residual_df = pd.DataFrame()

        n = len(Y.columns)
        for i in range(n):
            # print(i)
            # i = 47
            df = pd.concat([Y.iloc[:,[i]], X], axis=1)
            df = data_transform(df)
            betas, b0, r_sq, y_vol, res =  stat_utils.linear_regression(df)
            
            sec_id = df.columns[0]
            regress_df.loc[sec_id] = np.append(betas, [r_sq, y_vol])
            res_df = pd.DataFrame(data = res, columns = [sec_id], index=df.index)
            
            residual_df = pd.concat([residual_df, res_df], axis=1)
            if i%100 == 99:
                print(f'securities regressed: {i+1}')
        print(f'securities regressed: {n}')
                
        regress_df.index.name = 'SecurityID'

        self.X = X
        self.Y = Y
        self.regress_df  = regress_df
        self.residual_df = residual_df
        self.stat_df  = self.calc_ts_stat()
        
    # use the latest 1000 residuals for now. 
    def get_res_index(self):
        if self.res_index is None:
            residuals = self.residual_df
    
            # latest 1000 dates that have residual values
            residuals = residuals.dropna(how='all')
            res_dates = residuals.index[-self.num_simulation:]
            self.res_index = pd.DataFrame({'Date':res_dates})
        return self.res_index

    # generate residuals
    def gen_residuals(self):    
        # self = model
        betas = self.regress_df
        residuals = self.residual_df
        index = self.get_res_index()
        residuals = index.merge(residuals, left_on='Date', right_index=True, how='left').set_index('Date')

        # check if beta and residuals have different sec list
        diff = set(betas.index).difference(residuals.columns)
        if len(diff) > 0:
            print('Error: betas and residuals have different security list')

        # number of residuals that are not na
        residual_count = residuals.count()
        self.residual_count = residual_count

        # fill na with normal draw that std = res_vol    
        for sec_id, num in residual_count.items():
            if num == self.num_simulation: # if no missing data
                continue
            
            n = self.num_simulation - num
            rsq, vol = betas.loc[sec_id][['R-Sq', 'Vol']]
            res_vol = vol * np.sqrt(1-rsq)
            
            eps = np.random.normal(0, res_vol, n)
            residuals.loc[residuals[sec_id].isna(), sec_id] = eps
            
        self.idio_dist  = residuals
        
        return residuals

    # generate equity distribution
    def sim_dist(self):
        
        # run regression against core_factors 
        self.run_regression()
        
        # core factors log returns
        core_factors = self.get_core_factors_dist()
        
        # security betas to core factors
        betas = self.regress_df
        residuals = self.gen_residuals()

        # N = num of simulation; nc = number of core factors
        N, nc = core_factors.shape

        # systematic returns
        sys_dist = np.dot(core_factors, betas.iloc[:,:nc].T)
        sys_dist = pd.DataFrame(sys_dist, columns=betas.index)
        self.sys_dist = sys_dist

        # idiosyncratic returns
        residuals = residuals[sys_dist.columns]

        # total log returns
        log_ret = sys_dist.values + residuals.values
        log_ret = pd.DataFrame(log_ret, columns=sys_dist.columns)
        simulated_dist = np.exp(log_ret) - 1

        
        self.stat_df['resi_cnt']  = self.residual_count
        self.stat_df['sys_vol']   = sys_dist.std()        
        self.stat_df['idio_vol']  = residuals.std()        
        self.stat_df['total_vol'] = log_ret.std()        
        self.stat_df['sim_vol'] = simulated_dist.std()        

        self.simulated_dist = simulated_dist

    #
    # timeseries stat
    #
    def calc_ts_stat(self):
    
        # concat core_factors and securities
        cf = self.CoreFactors.set_index('Ticker')[['Name']].rename(columns={'Name':'SecurityName'})
        sec = self.Securities.set_index('SecurityID')[['SecurityName']]
        df = pd.concat([cf, sec])
        df.index.name = 'SecurityID'
        
        # concat prices
        cf_prices = self.get_core_factors_timeseries()
        sc_prices = self.get_security_timeseries()
        prices = pd.concat([cf_prices, sc_prices], axis=1)
        
        # calc stats
        df['start_date'] = date_utils.get_first_date(prices)
        df['end_date']   = date_utils.get_last_date(prices)
        df['length']     = prices.count()
        df1 = prices.ffill().pct_change(1)
        df['mean'] = df1.mean()
        df['max']  = df1.max()
        df['min']  = df1.min()
        df['vol']  = model_utils.calc_ts_vol(prices)
        
        # var window vol
        prices1 = prices.iloc[-self.num_simulation:]
        df['cur_vol']  = model_utils.calc_ts_vol(prices1)

        return df

    def write_to_xl(self, wb):
        self.to_xl('Parameters',             wb, index=False)
        self.to_xl('Securities',             wb, index=False)
        self.to_xl('CoreFactors',            wb, index=False)
        self.to_xl('security_timeseries',    wb, index=True)
        self.to_xl('core_factor_timeseries', wb, index=True)
        self.to_xl('res_index',              wb, index=True)
        self.to_xl('regress_df',             wb, index=True)
        self.to_xl('residual_df',            wb, index=True)
        self.to_xl('stat_df',                wb, index=True)
        self.to_xl('simulated_dist',         wb, index=True)
        self.to_xl('idio_dist',              wb, index=True)
        self.to_xl('sys_dist',               wb, index=True)
        self.to_xl('core_dist',              wb, index=True)
        self.to_xl('exception',              wb, index=False)

    def to_xl(self, attr_name, wb, index=True):
        df = getattr(self, attr_name)
        if df is not None:
            xl.add_df_to_excel(df.iloc[:, :500], wb, attr_name, index)
        
######################################################################################
'''
<<beta.csv>>
R-Sq:	r-sq from linear regression, in log space, long date range
Vol:	security stdev in log space, long date range

<<stat.csv>
mean, max, min, vol:	in return space, long date range
curr_vol:   in return space, VaR window date range (1000 days)
sys_vol:	sec sim sys stdev in log space, VaR window
idio_vol:	sec sim idio stdev in log space, VaR window
total_vol   sec sim stdev in log space, VaR window
sim_vol:	sec sim stdev in return space, VaR window
'''
            
######################################################################################
def create_model(securities, core_factors, params):
    return EquityModel(securities, core_factors,
                       start_date     = params['TS Start Date'],
                       end_date       = params['TS End Date'],
                       model_id       = params['Model ID'],
                       submodel_id    = params['Submodel ID'],
                       model_name     = params['Model Name'],
                       num_simulation = params['Number of Simulations'])

def create_model_from_wb(wb):
    params =  tools.read_parameter(wb)
    securities = xl.read_df_from_excel(wb, 'Securities')
    core_factors = xl.read_df_from_excel(wb, 'CoreFactors')
    model = create_model(securities, core_factors, params)
    
    return model
    
def create_template():
    wb = xw.Book()
    
    # parameters
    df = pd.DataFrame(columns=['Value'], index=pd.Index([], name='Name'))
    
    df.loc['Model Name']            = 'TestModel'
    df.loc['Model ID']              = 'M_20240331'
    df.loc['Submodel ID']           = 'Equity'
    df.loc['TS Start Date']         = '01/01/2010'
    df.loc['TS End Date']           = '03/31/2024'
    df.loc['Number of Simulations'] = 1040
    xl.add_df_to_excel(df, wb, 'Parameters')
    
    # Securities
    df = pd.DataFrame(columns = ['Category', 'SecurityName', 'AssetClass', 'AssetType'], index=pd.Index([], name='SecurityID'))
    df.loc['T10000358'] = ['PRICE','Vanguard S&P 500 ETF - VOO US', 'Equity', 'Equity']
    df.loc['T10000991'] = ['PRICE', 'VANGUARD VALUE ETF', 'Equity', 'ETF']
    xl.add_df_to_excel(df, wb, 'Securities')
    
    # Core Factors
    df = pd.DataFrame(columns = ['Ticker', 'Name', 'Data Type'], index=pd.Index([], name='SecurityID'))
    df.loc['T10000001'] = ['SPX', 'S&P 500 Index', 'PRICE']
    df.loc['T10000921'] = ['AGG', 'iShares Bond ETF', 'PRICE']
    xl.add_df_to_excel(df, wb, 'CoreFactors')
    
    return wb

def create_template_from_model(model_id, submodel_id):
    wb = xw.Book()
        
    model = EquityModel.create_model_from_files(model_id, submodel_id)
    params = model.get_model_params()
    xl.add_df_to_excel(params, wb, 'Parameters', index=False)
    xl.add_df_to_excel(model.Securities, wb, 'Securities', index=False)
    xl.add_df_to_excel(model.CoreFactors, wb, 'CoreFactors', index=False)

    return wb
    
def data_transform(df):
    df = df.dropna()
    
    # calculate log returns
    df= df.pct_change(1)
    df = df.dropna()
    
    # de-mean
    #df = df - df.mean()
    
    # log return
    df = np.log(1+df)

    return df    
######################################################################################
# test
def test():

    wb = create_template()
    wb = xw.Book('M_20240531.xlsx')
    wb = xw.Book('Book4')

    # create model    
    model = create_model_from_wb(wb)
    # self = model
    
    # simulate distribution
    model.sim_dist()

    # write to excel
    model.write_to_xl(wb)

    # save model
    model.save_model()

    
def test2():
    model_id = 'M_20240510'   
    submodel_id = 'Equity.1'
    model = EquityModel.load_model(model_id, submodel_id)
    
    wb = xw.Book('Book16')
    model.write_to_xl(wb)
    
def debug():
    wb = xw.Book('Book13')
    model = create_model_from_wb(wb)
    self = model
    
    model.sim_dist()
    
    sec_id = 'T10000994'

    # test xl file
    filename = config['HOME_DIR'] / 'Models' / 'Equity' / 'EquityTest.xlsx'
    wb = xw.Book(filename)    

    # get prices
    cf_prices = model.get_core_factors_timeseries()
    df = model.get_security_timeseries()[[sec_id]]
    prices = pd.concat([df, cf_prices], axis=1)
    xl.add_df_to_excel(prices, wb, 'prices')

    # data clean
    df = prices.dropna()
    xl.add_df_to_excel(df, wb, 'prices2')

    # regression
    log_ret = model.data_transform(prices)
    xl.add_df_to_excel(log_ret, wb, 'regress')

    betas, b0, r_sq, y_vol, res = stat_utils.linear_regression(log_ret)
    reg = pd.DataFrame(columns=cf_prices.columns)    
    reg.loc[0] = betas

    reg.loc[0, 'r_sq'] = r_sq
    reg.loc[0, 'y_vol'] = y_vol

    xl.add_df_to_excel(reg, wb, 'regress', addr='I1', index=False)

    # simulation
    dates = model.res_index
    
    # residual
    sim_df = dates.merge(log_ret[[sec_id]], left_on='Date', right_index=True, how='left') 
    resi_df = model.residual_df[[sec_id]].rename(columns={sec_id: 'Residuals'})
    sim_df = sim_df.merge(resi_df, left_on='Date', right_index=True, how='left')
    
    # core_dist
    core_dist = model.core_dist
    core_dist.columns = [x+'_dist' for x in core_dist.columns]
    sim_df = sim_df.merge(core_dist, left_on='Date', right_index=True, how='left')

    # sys_dist and idio_dist
    sim_df['sys_dist'] = model.sys_dist[[sec_id]]
    
    # idio_dist
    idio_dist = model.idio_dist[[sec_id]].rename(columns={sec_id: 'idio_dist'})
    sim_df = sim_df.merge(idio_dist, left_on='Date', right_index=True, how='left')
    
    # simuldated dist
    sim_df['sim_dist'] = model.simulated_dist[[sec_id]]

    xl.add_df_to_excel(sim_df, wb, 'sim')


    
