# -*- coding: utf-8 -*-
"""
Created on Mon Nov 17 09:34:44 2025

@author: mgdin
"""
import pandas as pd
import numpy as np
import xlwings as xw

from utils import var_utils
from utils import xl_utils, stat_utils, tools, df_utils
from utils import date_utils    
from security import security_info
from mkt_data import mkt_timeseries
from database import db_utils
from models import model_utils, equity_model
from trg_config import config

file_path=r'C:\Users\mgdin\OneDrive\Documents\dev\TRG_App\Workbooks\VaR_model_wb.xlsx'

def main():
    """
    1. model_init() -- create a new model
    2. create_equity_model() -- generate distribution for equity securities
    3. create_ir_model() -- create IR model
    4. create_credit_benchmark()
    5. create_bond_model()
    
    """
    
    # 1. model_init()


# create a new model
# 1. new VaR file
# 2. new entry in db table: risk_model
# 3. new folder in data/model/M_20251031/model
# 4. new csv files in the model folder
# 5. create a new equity model
def model_init():
    wb = xw.Book(file_path)

    # parameters
    model_params = read_parameter(wb)
    model_id = model_params['Model ID']
    print(f'model_id: {model_id}')
    
    # create VaR file
    model_utils.create_var_model(model_params)
    model_utils.read_Model_Parameters(model_id)

    # insert into db table: risk_model
    insert_db_risk_model(model_id, "model calibration in Oct 2025")
    
    # create core factors
    generate_core_factors(wb, model_params)
    
    # always create equity model first
    create_equity_model(model_id, 'Equity.1')

    
    
def generate_core_factors(wb, model_params):
    corefactors = xl_utils.read_df_from_excel(wb, 'CoreFactors')
    model_utils.save_corefactors(model_params, corefactors)

    # corefactor timeseries    
    sec_ids = corefactors['SecurityID'].unique()
    hist_prices = mkt_timeseries.get(sec_ids)
    id_ticker_map = corefactors.drop_duplicates('SecurityID').set_index('SecurityID')['Ticker'].to_dict()
    hist_prices = hist_prices.rename(columns=id_ticker_map)
    
    start_date = model_params['TS Start Date']
    end_date = model_params['TS End Date']
    hist_prices=hist_prices.ffill()[(hist_prices.index>=start_date) & (hist_prices.index <= end_date)]
    model_utils.save_cf_timeseries(model_params, hist_prices)
    xl_utils.add_df_to_excel(hist_prices, wb, 'core_factor_timeseries')

    # generate index dates
    index_dates = generate_IndexDates(model_params, wb)
    model_utils.save_index_dates(model_params, index_dates)

    # generate core dist
    core_dist = gen_core_factors_dist(hist_prices, index_dates)
    model_utils.save_corefactor_dist(model_params, core_dist)
    xl_utils.add_df_to_excel(core_dist, wb, 'core_dist')

def gen_core_factors_dist(hist_prices, index_dates):
    
    prices = hist_prices.ffill()
    pct_return = prices.pct_change(1)
    pct_return = pct_return - pct_return.mean()
    log_return = np.log( 1 + pct_return )
    log_return.replace(0, np.nan, inplace=True)    
    
    # reindex
    core_dist = log_return.reindex(index_dates.index)
    core_dist = model_utils.fill_na_with_rand_sampling(core_dist)

    # xl_utils.add_df_to_excel(core_dist, wb, 'core_dist')
    return core_dist


def generate_IndexDates(model_params, wb):
    
    start_date = model_params['TS Start Date']
    end_date = model_params['TS End Date']
    dates = date_utils.get_bus_dates(start_date, end_date)

    # random sampling    
    n_sim = model_params['Number of Simulations']
    dates = dates[2:] # start from the third date 
    index_dates = np.random.choice(dates, size=n_sim, replace=False)
    index_dates = sorted(index_dates)
    df = pd.DataFrame({'Date': index_dates})

    xl_utils.add_df_to_excel(df, wb, 'index_dates', index=True)
    df = df.set_index('Date')
    # df.to_csv('df.csv', index=True)
    return df
    
###############################################################################
# 
# Equity Model
#
def create_equity_model(model_id, submodel_id):
    # model_id, submodel_id = 'M_20251031', 'Equity.1'
    print(f'model_id: {model_id}, submodel_id: {submodel_id}')

    # excel for outputs        
    eq_model_filepath = r'C:\Users\mgdin\dev\TRG_App\Models\Equity\M_20251031.Equity.1.xlsx'
    eq_wb = xw.Book(eq_model_filepath)    

    # create model parameters
    model_params = equity_model.create_params(model_id, submodel_id)

    # get securities
    securities = get_eq_securities()
 
    # get hist prices
    sec_ids = securities['SecurityID'].to_list()
    hist_prices = mkt_timeseries.get(sec_ids)
    start_date = model_params['TS Start Date']
    end_date = model_params['TS End Date']
    hist_prices=hist_prices[(hist_prices.index>=start_date) & (hist_prices.index <= end_date)]
    hist_prices.replace(0, np.nan, inplace=True)    
    
    df = securities.set_index('SecurityID').copy()
    stat = stat_utils.hist_stat(hist_prices)    

    df['StartDate'] = stat['StartDate']
    df['EndDate'] = stat['EndDate']
    df['Length'] = stat['Length']
    df['vol'] = model_utils.calc_ts_vol(hist_prices)

    # remove zero vol securities
    df = df[df['vol']>0]    
    securities = df.reset_index()
    xl_utils.add_df_to_excel(securities, eq_wb, 'Securities', index=False)        

    sec_ids = securities['SecurityID'].to_list()
    hist_prices = hist_prices[sec_ids]
    
    # run equity model
    DATA = equity_model.run_model(securities, hist_prices, model_id, submodel_id)

    # write results
    write_to_xl(eq_wb, DATA)

def delete_dist():
    model_id = 'M_20251031'
    var_utils.set_model_id(model_id)

    # securityID list that will be deleted
    sec_ids = ['T10000361']
    
    # show the current dists
    dists = var_utils.get_dist(sec_ids, 'PRICE')    
    calc_dist_stat(dists)

    # show current risk_factors
    get_risk_factors(sec_ids, model_id)
    
    # delete from database
    delete_riskfactors(sec_ids, model_id)
    
    # delete from VaR file
    var_utils.remove_dist(sec_ids, 'PRICE')
    

# load all model data related to sec_ids
def load_equity_data(sec_ids):
    wb = xw.Book('Book1.xlsx')
    sec_ids = xl_utils.read_df_from_excel(wb, 'SecurityID')['SecurityID'].to_list()
    model_id, submodel_id = 'M_20251031', 'Equity.1'

    
    tab = 'Parameters'
    df = model_utils.read_model_data(tab, model_id, submodel_id)    
    xl_utils.add_df_to_excel(df, wb, tab, index=False)

    tab = 'CoreFactors'
    df = model_utils.read_model_data(tab, model_id, submodel_id)    
    xl_utils.add_df_to_excel(df, wb, tab, index=False)
    core_tickers = df['Ticker'].to_list()
    
    tab = 'core_factors_timeseries'
    df = model_utils.read_model_data(tab, model_id, submodel_id)    
    xl_utils.add_df_to_excel(df, wb, tab, index=False)

    tab = 'IndexDates'
    df = model_utils.read_model_data(tab, model_id, submodel_id)    
    xl_utils.add_df_to_excel(df, wb, tab, index=False)

    df = model_utils.read_model_data('Securities', model_id, submodel_id)    
    df = df[df['SecurityID'].isin(sec_ids)]
    xl_utils.add_df_to_excel(df, wb, 'Securities', index=False)

    tab = 'hist_prices'
    df = model_utils.read_model_data(tab, model_id, submodel_id)    
    df = df[['Date'] + sec_ids]
    xl_utils.add_df_to_excel(df, wb, tab, index=False)
    
    tab = 'security_timeseries'
    df = model_utils.read_model_data(tab, model_id, submodel_id)    
    df = df[['Date'] + sec_ids]
    xl_utils.add_df_to_excel(df, wb, tab, index=False)
    

    tab = 'stat_df'
    df = model_utils.read_model_data(tab, model_id, submodel_id)    
    df = df[df['SecurityID'].isin(core_tickers + sec_ids)]
    xl_utils.add_df_to_excel(df, wb, tab, index=False)
    
    tab = 'regress_df'
    df = model_utils.read_model_data(tab, model_id, submodel_id)    
    df = df[df['SecurityID'].isin(sec_ids)]
    xl_utils.add_df_to_excel(df, wb, tab, index=False)

    tab = 'sys_dist'
    df = model_utils.read_model_data(tab, model_id, submodel_id)    
    df = df[sec_ids]
    xl_utils.add_df_to_excel(df, wb, tab, index=True)

    tab = 'idio_dist'
    df = model_utils.read_model_data(tab, model_id, submodel_id)    
    df = df[sec_ids]
    xl_utils.add_df_to_excel(df, wb, tab, index=True)

    tab = 'simulated_dist'
    df = model_utils.read_model_data(tab, model_id, submodel_id)    
    df = df[sec_ids]
    xl_utils.add_df_to_excel(df, wb, tab, index=True)

    tab = 'corefactor_dist'
    df = model_utils.read_model_data(tab, model_id, submodel_id)    
    xl_utils.add_df_to_excel(df, wb, tab, index=False)

    tab = 'residual_df'
    df = model_utils.read_model_data(tab, model_id, submodel_id)    
    df = df[['Date'] + sec_ids]
    xl_utils.add_df_to_excel(df, wb, tab, index=False)

###############################################################################
#
# Bond Model
# 
from models import credit_benchmark
from models import credit_model

def create_credit_benchmark():
    
    # credit_benchmark.update_FED_data()
    
    # credit_benchmark.model_credit_benchmarks()
    return
    
def create_bond_model():
    
    model_id, submodel_id = 'M_20251031', 'Bond.1'
    filepath = rf'C:\Users\mgdin\dev\TRG_App\Models\Bond\{model_id}.{submodel_id}.xlsx'
    bd_wb = xw.Book(filepath)    

    # create parameters
    model_params = credit_model.create_params(model_id, submodel_id)
    xl_utils.add_dict_to_excel(model_params, bd_wb, tab='Parameters')
    
    # get all bond securities
    bonds = get_usd_bond_securities() 
    xl_utils.add_df_to_excel(bonds, bd_wb, 'Securities', index=False)    
    
    bonds = xl_utils.read_df_from_excel(bd_wb, 'Securities')
    
    
def get_usd_bond_securities():
    sql = """
    select si."SecurityID", si."SecurityName", si."Currency", si."AssetClass", si."AssetType",  
    bi."MaturityDate", bi."IssuerTicker", bi."Rating", bi."Sector", bi."Country", bi."CouponRate", bi."CouponType", bi."PaymentFrequency", bi."Callable",
    bi."CallDate", bi."Formula", bi."Putable" , bi."DayCountBasis", bi."DatedDate", bi."FirstInterestPayment"  
    from security_info si left join bond_info bi on bi."SecurityID" = si."SecurityID"
    where si."AssetClass"='Bond' and si."AssetType"='Bond' and si."Currency" = 'USD'

    """
    df = db_utils.get_sql_df(sql)

    return df

def get_bond_yield(sec_ids):
    wb = xw.Book('M_20251031.Bond.1.xlsx')
    sec_ids = xl_utils.read_df_from_excel(wb, 'Securities')['SecurityID'].to_list()
    mkt_timeseries.get(sec_ids)
    hist_yield = mkt_timeseries.get(sec_ids, 'YIELD')

    sec_list =mkt_timeseries.get_mkt_data_sec_list()
    xl_utils.add_df_to_excel(sec_list, wb, 'sec_list')    
    
    
###############################################################################
#
# IR Model
# 
# to download the UST data, go to file: C:\Users\mgdin\dev\TRG_App\Models\Data\FED\USTreasury.xlsx
#
from models import ir_model
def create_ir_model():
    model_id, submodel_id = 'M_20251031', 'IR.1'

    # get US Treasure IR curve
    securities = get_ir_securities()    

    # run IR model
    DATA = ir_model.run_ir_model(model_id, submodel_id, securities)

    # write data to excel
    wb = xw.Book()    
    for tab in DATA:
        print(tab)
        if tab == 'Parameters':
            xl_utils.add_df_to_excel(df_utils.dict_to_df(DATA[tab]), wb, tab)
        elif tab == 'IndexDates':
            xl_utils.add_df_to_excel(DATA[tab].reset_index(), wb, tab)
        else:
            xl_utils.add_df_to_excel(DATA[tab], wb, tab)
    
        
    # save the excel file
    filepath = rf'C:\Users\mgdin\dev\TRG_App\Models\IR\ust_model.{model_id}.xlsx'
    wb.save(filepath) 
    
def get_ir_securities():
    sql = """
        select * from ir_curves ic where ic."CurveID" = 'UST'
    """    
    df = db_utils.get_sql_df(sql)
    return df    

#############################################################################
# sec_ids = ['T10000361']
# model_id = 'M_20251031'
def get_risk_factors(sec_ids, model_id=None):
    
    if model_id is None:
        model_id = var_utils.get_default_model_id()
        print(f'use default model_id: {model_id}')
        
    sec_ids_str = ",".join([f"'{x}'" for x in sec_ids])
    sql = f"""
    select rf.* from risk_factor rf, risk_model rm  
    where rf.model_id = rm.model_id and rm.model_name = '{model_id}' 
    and rf."SecurityID" in ({sec_ids_str})
    """
        
    df = db_utils.get_sql_df(sql)
    return df

def delete_riskfactors(sec_ids, model_id=None):
    if model_id is None:
        model_id = var_utils.get_default_model_id()
        print(f'use default model_id: {model_id}')
    
    db_model_id = model_utils.get_db_model_id(model_id)
    
    # delete from db
    df = pd.DataFrame({'SecurityID': sec_ids})
    df['model_id'] = db_model_id
    df['Category'] = 'DELTA'
    
    db_utils.delete_df('risk_factor', df)    
    
    
def write_to_xl(wb, DATA):

    tab = 'Parameters'
    xl_utils.add_df_to_excel(df_utils.dict_to_df(DATA[tab]), wb, tab)

    for tab in ['CoreFactors', 'exception', 'regress_df', 'stat_df']:
        print(tab)
        xl_utils.add_df_to_excel(DATA[tab].iloc[:,:100], wb, tab)
    
    DATA.keys()
    

# model_id = 'M_20251031'        
# description="model calibration in Oct 2025"
def insert_db_risk_model(model_id, description="model calibration"):
    df = db_utils.get_sql_df(f"select * from risk_model where model_name = '{model_id}'")
    if df.empty:
        table_df = pd.DataFrame({'model_name':[model_id], 'description': description})
        db_utils.insert_df('risk_model', table_df, key_column='model_name')
        print(f'insert 1 row into risk_model for {model_id}')
    else:
        print(f'skipped! found {model_id} in table risk_model')


def read_parameter(wb):
    model_params = tools.read_parameter(wb)
    model_params['Number of Simulations'] = int(model_params['Number of Simulations'])
    return model_params 
    
def get_eq_securities():
    sql = """
    select * from security_info_view where "AssetClass"='Equity'
    """
    securities = db_utils.get_sql_df(sql)
    
    # remove VIX
    securities = securities[securities['SecurityID'] !='T10000002']

    securities = securities[['SecurityID', 'SecurityName', 'Currency', 'AssetClass', 'AssetType', 'Ticker']]

    if securities.duplicated(['SecurityID']).sum() > 0:
        raise "get_eq_securities: found duplicated SecurityID"
    
    return securities

def get_eq_securities2(wb):
    sql = """
    select * from security_info_view where "AssetClass"='Bond' and "AssetType" in ('ETF', 'Fund')
    """
    securities = db_utils.get_sql_df(sql)


    wb = xw.Book('Book2')    
    xl_utils.add_df_to_excel(securities, wb, 'bond_etf', index=False)    

    dups = securities[securities.duplicated(['SecurityID'], keep=False)]
    xl_utils.add_df_to_excel(dups, wb, 'dups2', index=False)    


    return securities


# go through all files in model/submodel folders and summarize the stats
def model_summary(wb):
    wb = xw.Book('Book1')    
    
    model_id = 'M_20240531'
    model_folder = config['MODEL_DIR'] / model_id

    data = []    
    for submodel_dir in list(model_folder.glob('*')):
        submodel_id = submodel_dir.name
        if submodel_id in ['Model', 'Benchmark.0']:
            continue
        file_path = submodel_dir/'Securities.csv'
        if file_path.exists():
            df = pd.read_csv(file_path)
            df['Sub Model'] = submodel_id
            data.append(df)
        else:
            print(f'securitie file does not exists: {submodel_id}')
    
    securities = pd.concat(data)
    xl_utils.add_df_to_excel(securities, wb, 'model_sum')

    # duplicated 
    sec_dups = securities[securities.duplicated(subset=['SecurityID'], keep=False)]
    xl_utils.add_df_to_excel(sec_dups, wb, 'dups')

    # map sub model value to be in chronological order 
    eq_map = dict([(f"Equity.{i}",f"Equity.0.{i:02}") for i in range(1,13) ])
    for x in securities['Sub Model'].unique():
        if not x in eq_map:
            eq_map[x]=x
    securities['Sub Model'] = securities['Sub Model'].map(eq_map)

    # sort by column Sub Model
    df = securities.sort_values(by='Sub Model')
    
    # remove duplicates keep the latest       
    df = df.drop_duplicates(subset=['SecurityID'], keep='last')
    xl_utils.add_df_to_excel(df, wb, 'model_securities')



    

    
def test():
    wb = xw.Book(file_path)

    # all distributions in var file
    dists = list_dist(wb)

    # security info
    data = []
    cats = dists['Category'].unique()

    for cat in cats:
        if cat == 'META':
            continue
        print(cat)

        c_dists = dists[dists['Category']==cat]
        sec_ids = c_dists['SecurityID'].to_list()
        df = security_info.get_security_by_ID(sec_ids)
        df['Category'] = cat
        data.append(df)

    securities = pd.concat(data)
    xl_utils.add_df_to_excel(securities, wb, 'Security', index=False)
    
    # equity securities
    securities = xl_utils.read_df_from_excel(wb, 'Security')
    eq_securities = securities[securities['AssetClass']=='Equity' ]
    eq_securities = eq_securities[eq_securities['Category'] == 'PRICE']

    # get mkt_data
    sec_ids = eq_securities['SecurityID'].to_list()
    hist_price = mkt_timeseries.get(sec_ids)
    stat = stat_utils.hist_stat(hist_price)    
    xl_utils.add_df_to_excel(stat, wb, 'hist_price')    

    # missing
    missing = set(sec_ids).difference(stat.index)
    if missing:
        print(missing)
        missing_sec = eq_securities[eq_securities['SecurityID'].isin(missing)]
        xl_utils.add_df_to_excel(missing_sec, wb, 'missing_eq', index=False)
    xref = security_info.get_xref_by_ID(sec_ids)
    xref = xref[xref['REF_TYPE']=='YH']
    xl_utils.add_df_to_excel(xref, wb, 'xref')    

    sql = 'select * from mkt_data_source'    
    df = db_utils.get_sql_df(sql)    
    xl_utils.add_df_to_excel(df, wb, 'mkt_data_source', index=False)    

    sql = """
    select * from security_info si 
    where si."SecurityID" in (select "SecurityID" from mkt_data_source mds where mds."Source" = 'YH')
    and si."AssetClass" = 'Equity'
    """
    df = db_utils.get_sql_df(sql)    
    xl_utils.add_df_to_excel(df, wb, 'yh_equity', index=False)    


    
def list_dist(wb):
    # find all distributions in var file
    dists = var_utils.list_dist()

    # write to tab "List"
    xl_utils.add_df_to_excel(dists, wb, 'List', index=False)
    
    return dists


def dist_stat(wb):
    model_id = var_utils.get_model_id()
    print(f'model_id: {model_id}')
    dist_ids = var_utils.list_dist()
    xl_utils.add_df_to_excel(dist_ids, wb, 'dist_ids', index=True)

    cat = 'PRICE'
    sec_ids = dist_ids[dist_ids['Category']==cat]['SecurityID'].to_list()
    dists = var_utils.get_dist(sec_ids, cat)

    stat = calc_dist_stat(dists)
    dist_ids.merge(stat)

    wb = xw.Book('Book2')
    xl_utils.add_df_to_excel(stat, wb, 'stat', index=True)

    model_id = var_utils.get_default_model_id()
    var_utils.set_model_id(model_id)
    model_id = var_utils.get_model_id()
    print(f'model_id: {model_id}')
    dist_ids = var_utils.list_dist()
    sec_ids = dist_ids[dist_ids['Category']==cat]['SecurityID'].to_list()
    dists = var_utils.get_dist(sec_ids, cat)

    stat = calc_dist_stat(dists)
    xl_utils.add_df_to_excel(stat, wb, 'stat', index=True)
    
def calc_dist_stat(dists):

    sec_ids = dists.columns.tolist()

    # stat dataframe
    df = pd.DataFrame(index=sec_ids)

    # stats
    
    df['max']  = dists.max()
    df['min']  = dists.min()
    df['mean'] = dists.mean()
    df['vol'] = dists.std()
    df['Q_1%'] = dists.quantile(0.01)
    df['Q_5%'] = dists.quantile(0.05)
    df['Q_50%'] = dists.quantile(0.5)
    df['Q_95%'] = dists.quantile(0.95)
    df['Q_99%'] = dists.quantile(0.99)
    return df
