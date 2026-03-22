# -*- coding: utf-8 -*-
"""
Created on Tue Oct 14 21:06:52 2025

@author: mgdin


https://en.wikipedia.org/wiki/Binary_option
"""

import numpy as np
from scipy.stats import norm
from scipy import optimize

N = norm.cdf

# pay $1 if S > K
def CALL(S, K, T, r, sigma):
    d1 = (np.log(S/K) + (r + sigma**2/2)*T) / (sigma*np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return np.exp(-r*T)* N(d2)

# pay $1 if S < K
def PUT(S, K, T, r, sigma):
    d1 = (np.log(S/K) + (r + sigma**2/2)*T) / (sigma*np.sqrt(T))
    d2 = d1 - sigma* np.sqrt(T)
    return np.exp(-r*T)*N(-d2)


def call_delta_gamma(S, K, T, r, sigma):
    ds = S * 0.01
    price_base = CALL(S, K, T, r, sigma)
    price_up   = CALL(S+ds, K, T, r, sigma)
    price_dn   = CALL(S-ds, K, T, r, sigma)

    delta_up = price_up - price_base
    delta_dn = price_dn - price_base
    delta = (delta_up - delta_dn)/(2*ds)
    gamma = (delta_up + delta_dn)/(ds**2)
    
    return delta, gamma

# gamma = d^2(P) / (dS)^2
# pnl = 0.5 * gamma * (dS)^2
def put_delta_gamma(S, K, T, r, sigma):
    ds = S * 0.01
    price_base = PUT(S,    K, T, r, sigma)
    price_up   = PUT(S+ds, K, T, r, sigma)
    price_dn   = PUT(S-ds, K, T, r, sigma)

    delta_up = price_up - price_base
    delta_dn = price_dn - price_base
    delta = (delta_up - delta_dn)/(2*ds)
    gamma = (delta_up + delta_dn)/(ds**2)
    
    return delta, gamma

# Vega sensitivity to 0.01 (1 percentage) change to sigma
# PnL = vega * d(sigma)
def call_vega(S, K, T, r, sigma):
    ds = 0.01
    price_base = CALL(S, K, T, r, sigma)
    price_up   = CALL(S, K, T, r, sigma+ds)
    price_dn   = CALL(S, K, T, r, sigma-ds)

    delta_up = price_up - price_base
    delta_dn = price_dn - price_base
    vega = (delta_up - delta_dn)/2
    
    return vega

def put_vega(S, K, T, r, sigma):
    ds = 0.01
    price_base = PUT(S, K, T, r, sigma)
    price_up   = PUT(S, K, T, r, sigma+ds)
    price_dn   = PUT(S, K, T, r, sigma-ds)

    delta_up = price_up - price_base
    delta_dn = price_dn - price_base
    vega = (delta_up - delta_dn)/2
    
    return vega


##############################################################################
# Test
def test():
    S = 100
    K = 130
    r = 0.025
    sigma = 0.2
    T = 0.75
    CALL(S, K, T, r, sigma)
    
    S = 100
    K = 70
    r = 0.025
    sigma = 0.2
    T = 0.75
    PUT(S, K, T, r, sigma)
    