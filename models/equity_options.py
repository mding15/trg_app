#!/usr/bin/env python
# coding: utf-8

# In[2]:


import numpy as np
from scipy.stats import norm
from scipy import optimize

N = norm.cdf


def BS_CALL(S, K, T, r, sigma):
    d1 = (np.log(S/K) + (r + sigma**2/2)*T) / (sigma*np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return S * N(d1) - K * np.exp(-r*T)* N(d2)

def BS_PUT(S, K, T, r, sigma):
    d1 = (np.log(S/K) + (r + sigma**2/2)*T) / (sigma*np.sqrt(T))
    d2 = d1 - sigma* np.sqrt(T)
    return K*np.exp(-r*T)*N(-d2) - S*N(-d1)

def calc_sn_price(init_price, curr_price, r, sigma, T, PayOff, seed=15, n_sim=10000):
    # simulate price returns
    np.random.seed(seed)
    eps = np.random.normal(0,1,n_sim)
    s_prices = np.exp((r - 0.5 * sigma * sigma) * T + np.sqrt(T) *sigma * eps)
    
    factor = init_price / curr_price
    payoff = PayOff.sort_values(['Index']) * factor

    payoff_values = np.interp(s_prices, payoff['Index'], payoff['Payoff'])
    sn_price = np.mean(payoff_values) * np.exp(-T*r) / factor
    return sn_price


# # Implied Volatility

# In[12]:


def iv_call(price, S, K, T, r, x0=0.2):
    def f(sigma):
        return BS_CALL(S, K, T, r, sigma) - price

    try:
        sigma = optimize.newton(f, x0, maxiter=500, tol=1e-6)
    except RuntimeError:
        sigma = np.nan
    
    return sigma

def iv_put(price, S, K, T, r, x0=0.2):
    def f(sigma):
        return BS_PUT(S, K, T, r, sigma) - price

    try:
        sigma = optimize.newton(f, x0, maxiter=500, tol=1e-6)
    except RuntimeError:
        sigma = np.nan
    
    return sigma


# # Greeks

# In[ ]:


# delta, gamma, sensitivity to 1 unit change to S

# PnL = delta * dS
def call_delta_gamma(S, K, T, r, sigma):
    ds = S * 0.01
    price_base = BS_CALL(S, K, T, r, sigma)
    price_up   = BS_CALL(S+ds, K, T, r, sigma)
    price_dn   = BS_CALL(S-ds, K, T, r, sigma)

    delta_up = price_up - price_base
    delta_dn = price_dn - price_base
    delta = (delta_up - delta_dn)/(2*ds)
    gamma = (delta_up + delta_dn)/(ds**2)
    
    return delta, gamma

# gamma = d^2(P) / (dS)^2
# pnl = 0.5 * gamma * (dS)^2
def put_delta_gamma(S, K, T, r, sigma):
    ds = S * 0.01
    price_base = BS_PUT(S,    K, T, r, sigma)
    price_up   = BS_PUT(S+ds, K, T, r, sigma)
    price_dn   = BS_PUT(S-ds, K, T, r, sigma)

    delta_up = price_up - price_base
    delta_dn = price_dn - price_base
    delta = (delta_up - delta_dn)/(2*ds)
    gamma = (delta_up + delta_dn)/(ds**2)
    
    return delta, gamma

# Vega sensitivity to 0.01 (1 percentage) change to sigma
# PnL = vega * d(sigma)
def call_vega(S, K, T, r, sigma):
    ds = 0.01
    price_base = BS_CALL(S, K, T, r, sigma)
    price_up   = BS_CALL(S, K, T, r, sigma+ds)
    price_dn   = BS_CALL(S, K, T, r, sigma-ds)

    delta_up = price_up - price_base
    delta_dn = price_dn - price_base
    vega = (delta_up - delta_dn)/2
    
    return vega

def put_vega(S, K, T, r, sigma):
    ds = 0.01
    price_base = BS_PUT(S, K, T, r, sigma)
    price_up   = BS_PUT(S, K, T, r, sigma+ds)
    price_dn   = BS_PUT(S, K, T, r, sigma-ds)

    delta_up = price_up - price_base
    delta_dn = price_dn - price_base
    vega = (delta_up - delta_dn)/2
    
    return vega
    
    


# # Test

# In[4]:


# S,K,T,r, sigma = 1, 0.9, 1, 0.04, 0.2
# BS_CALL(S, K, T, r, sigma)


# In[5]:


# iv_call(0.16, S, K, T, r)


# In[ ]:




