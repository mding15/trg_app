# -*- coding: utf-8 -*-
"""
Created on Sun Jan  7 18:26:15 2024

@author: mgdin

Description:
    statistics tools
    
"""
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

from utils import date_utils

# multivariate regression
# y = df[0]
# X = df[1:]
def linear_regression(df):
    model = LinearRegression(fit_intercept=True)
    x = df.iloc[:,1:].values
    y = df.iloc[:, :1].values
    model.fit(x, y)

    betas = model.coef_[0]
    b0 = model.intercept_
    #betas = np.concatenate((b0, betas))
    r_sq = model.score(x, y)
    y_vol = np.std(y)
    res = (y - model.predict(x))[:,0]
    
    return betas, b0, r_sq, y_vol, res

def hist_stat(prices):

    sec_ids = prices.columns.tolist()

    # stat dataframe
    df = pd.DataFrame(index=sec_ids)

    # stats
    
    df['StartDate'] = date_utils.get_first_date(prices)
    df['EndDate']   = date_utils.get_last_date(prices)
    df['Length']    = prices.count()
    df['MaxValue']  = prices.max()
    df['MinValue']  = prices.min()
    df['AverageValue'] = prices.mean()
    df['StdValue'] = prices.std()
    
    return df
    
def dist_stat(dist):

    sec_ids = dist.columns.tolist()

    # stat dataframe
    df = pd.DataFrame(index=sec_ids)

    # stats
    df['min']  = dist.min()
    df['max']  = dist.max()
    df['mean'] = dist.mean()
    df['std'] = dist.std()
    df['q-1%'] = dist.quantile(0.01)
    df['q-5%'] = dist.quantile(0.05)
    df['q-50%'] = dist.quantile(0.5)
    df['q-95%'] = dist.quantile(0.95)
    df['q-99%'] = dist.quantile(0.99)
    return df
    