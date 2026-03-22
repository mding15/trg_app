# -*- coding: utf-8 -*-
"""
Created on Tue Jun  4 16:22:10 2024

@author: mgdin
"""
import pandas as pd
from report.preprocess import preprocess


###############################################################################
# main function
def generate_report(DATA):

    # report pre-process
    preprocess(DATA)
    
    report = {}
    report['Portfolio Risk'] = calc_portfolio_risk(DATA)
    report['Allocation']     = calc_risk_contribution(DATA, by=['Class'])
    report['Region Risk']    = calc_risk_contribution(DATA, by=['Region'])

    return report

###############################################################################
def calc_portfolio_risk(DATA):
    # total portfolio risk    
    tot = DATA['TotalVaR']
    
    # benchmark risk    
    bm = DATA['BechmarkRisk']
    
    db_risk = pd.DataFrame(columns=['Name','Volatility','VaR','Sharpe Ratio - Vol','Sharpe Ratio - VaR'])
    db_risk.loc[0] = ['Portfolio', tot['Volatility'], tot['tVaR%'], tot['SR Vol'], tot['SR tVaR']]
    db_risk.loc[1] = ['Benchmark', bm['Vol'], bm['VaR%'], bm['SR Vol'], bm['SR VaR']]
    
    return db_risk


def calc_risk_contribution(DATA, by=['Class']):
    posVaR = DATA['Positions']
    risk = posVaR.groupby(by=by).sum()[['Weight', 'mgtVaR%']]
    risk.columns = ['Allocation', 'Risk Contribution']
    risk = risk.reset_index()    
    
    return risk
    

###############################################################################



    
    
