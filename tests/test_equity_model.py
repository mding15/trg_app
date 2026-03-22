# -*- coding: utf-8 -*-
"""
Created on Fri May 31 20:57:59 2024

@author: mgdin
"""
import pandas as pd
import xlwings as xw
from pathlib import Path

from trg_config import config
from models import equity
from utils import xl_utils as xl
from utils import stat_utils

def test():
    # equity model
    model_id, submodel_id = 'M_20240531', 'Equity.1'
    model = equity.EquityModel.create_model_from_files(model_id, submodel_id)

def debug():
    wb = equity.create_template()
    wb = xw.Book('Book1')

    model = equity.create_model_from_wb(wb)
    # self = model
    model.sim_dist()
    
    model.write_to_xl(wb, dist=True)
    
    #
    # debug one security
    
    sec_id = 'T10000994'

    # test xl file
    filename = Path(r'C:\Users\mgdin\dev\TRG_App\Models\Equity') / 'EquityTest.xlsx'
    wb = xw.Book(filename)    

    # get historical prices for core_factor and securities
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