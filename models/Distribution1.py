# -*- coding: utf-8 -*-
"""

"""

import pandas as pd
import numpy as np 
from sklearn.linear_model import LinearRegression
from scipy.stats import norm

from trgapp.models import EquityDist as Equity
from trgapp import config

#
# Input data
#
equity_model_config    = config.MODEL_DIR / 'equity_model_configuration.csv'
equity_securities_file = config.MODEL_DIR / 'equity_securities.csv'
core_factor_file       = config.MODEL_DIR / 'core_factors.csv'

#
# Output data
#
equity_residual_dates = config.MODEL_DIR / 'equity_residual_dates.csv'
equity_residual       = config.MODEL_DIR / 'equity_residual.csv'
equity_regression     = config.MODEL_DIR / 'equity_regression.csv'
equity_exclusion      = config.MODEL_DIR / 'equity_exclusion.csv'
equity_distribution   = config.MODEL_DIR / 'equity_distribution.csv'



# Read securities from file
df_security = pd.read_csv(equity_securities_file)

# Get Security ID
securityID = df_security['SecurityID'].tolist()

#Get TS
ts = Equity.get_market_data(securityID)

#Log Return
log_return_ts =np.log(ts/ts.shift(1))
log_return_ts.replace([np.inf, -np.inf], np.nan, inplace = True)

# ts missing
securityID.remove('T10000015')

#Get Core
df_Factor = pd.read_excel('Equity.xlsx',sheet_name='Core')
FactorID = df_Factor['SecurityID'].tolist()
ts_Factor = Equity.get_market_data(FactorID)
log_return_ts_Factor = np.log(ts_Factor/ts_Factor.shift(1))
log_return_ts_Factor = log_return_ts_Factor.dropna()

#df_dist = pd.DataFrame(columns = ['XLB','XLC','XLE','XLF','XLI','XLK','XLP','XLU','XLV','XLY'])
df_dist = pd.DataFrame(columns = df_Factor['Core ID'])


beta = []
securities = []
alpha = []
R_sq = []
Security_Vol = []
Residual_Vol = []
from_date = []
to_date = []
length = []
res_list = []

for stock in securityID:  
    X = log_return_ts_Factor
    y = log_return_ts[stock].dropna()
    ind = X.index.intersection(y.index)
    if ind.isna().all():
        continue
    else:
        X = X.loc[ind]
        y = y.loc[ind]
    
        model = LinearRegression()
        model.fit(X,y)
        
        #Calculate R-Square
        r_squared = model.score(X, y)
        #Calculate Security Volatility
        sv = y.std()
        #Residual Volatility
        res = y - model.predict(X)
        res_vol = res.std()
        
        """print("Coefficients:", model.coef_)
        print("Intercept:", model.intercept_)
        print("Residual:", res)"""
        
        securities.append(stock)
        beta.append(model.coef_)
        alpha.append(model.intercept_)
        R_sq.append(r_squared)
        Security_Vol.append(sv)
        Residual_Vol.append(res_vol)
        from_date.append(ind[0])
        to_date.append(ind[-1])
        length.append(len(ind))
        res_list.append(res)
 

for row_data in beta:
    df_dist = pd.concat([df_dist,pd.DataFrame([row_data], columns = df_dist.columns)], ignore_index = True)
    #df_dist = df_dist.append(pd.Series(row_data, index = df_dist.columns), ignore_index = True)

df_dist.index = securities
df_dist['alpha'] = alpha
df_dist['R-Sq'] = R_sq
df_dist['Security Vol'] = Security_Vol
df_dist['Residual Vol'] = Residual_Vol
df_dist['from_date'] = from_date
df_dist['to_date'] = to_date
df_dist['length'] = length

excel_file = 'Regression.xlsx'
df_dist.to_excel(excel_file, index = False)

res_df = pd.concat(res_list, axis = 1)
res_df.to_excel('Residual.xlsx', index =True)


### Simulation 
#random pick 1000 dates
date = res_df.index
random_date = np.random.choice(date, 1000, replace = False)
#pick log_return and residuals for these days


for stock in securities[0:3]:
    i = 0
    while i<2:  
        lret_sim = log_return_ts[log_return_ts.index.isin(random_date)][stock]
        res_sim = res_df[res_df.index.isin(random_date)][stock]
        rv = df_dist[df_dist.index == stock]['Residual Vol']
        res_sim = res_sim.replace(np.nan, norm.ppf(np.random.rand(), 0, res_vol)) # bug res_vol
        
        # Calculate xi
        beta_sim = df_dist[df_dist.index == stock].iloc[:,:10]
        fmat = log_return_ts_Factor[log_return_ts_Factor.index.isin(random_date)]
        expected_lret = np.dot(fmat, np.transpose(beta_sim))
        expected_lret_df = pd.DataFrame(expected_lret, index = fmat.index, columns = ['expected lret'])
        expected_lret_df = pd.merge(expected_lret_df, res_sim, left_index = True, right_index = True, how = 'left')
        expected_lret_df['expected log return'] = expected_lret_df['expected lret'] + expected_lret_df[stock]
        expected_lret_df = expected_lret_df.drop(['expected lret', stock], axis = 1)
        expected_lret_df['expected return'] = np.exp(expected_lret_df['expected log return'])-1
        expected_lret_df.to_excel('expected return ' + stock + '-' + str(i) +'.xlsx', index = True)
        
        i+=1
    

######
# def test():
#     function()
    