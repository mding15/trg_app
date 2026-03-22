# # EXPORT FUNCTIONS

# In[3]:


import pandas as pd
import numpy as np
import xlwings as xw
from pathlib import Path
import datetime
import math
import re
#from matplotlib import pyplot as plt

from trg_config import config
from mkt_data import mkt_timeseries
#import utility_functions as uf

# estimate dirty price, based on ACT/ACT
def bond_dirty_price(c, T, q, clean_price):
    # c, T, q, clean_price = 0.028, 0.54, 2, 0.9853
    
    accured = T*q - math.floor(T*q)
    accured = c/q * (1-accured) 
    dirty_price = clean_price + accured
    
    return dirty_price

# # Calculate Yield

# # Bond price
# let v = 1/(1+y)
# bond value = c * (v-v^n+1) / (1-v) + v^n
# 
# Assume coupon frequency is semi-annual
# time to the next coupon date is: s/2
# maturity is T years, the total number of payments is n = int(T*2), s = 2T-n
# let v = (1+y)^-0.5
# bond value = (c * (v-v^n+1) / (1-v) + v^n) * v^s

# In[4]:

# y=0.05
# c=0.03
# T=5.6
# q = frequency
def bond_price(y, c, T, q=2):
    n = math.floor(T*q)
    s = q*T - n
    v=(1+y)**(-1/q)
    a = c/q if s>0 else 0 # the first cash flow

    value = ( a + c/q * (v - v**(n+1)) / (1-v) + v**n) * (v**s)
    return value

# dB/B = -D*dy
def bond_duration(y, c, T, q=2):
    dy=0.005
    D = (1-bond_price(y+dy, c, T, q) / bond_price(y, c, T, q)) / dy

    return D


# In[5]:


# dB/B = -D*dy + 0.5 * C * dy^2
def bond_convexity(y, c, T, q=2):
    dy=0.005
    C = (bond_price(y+dy, c, T, q) + bond_price(y-dy, c, T, q)) / bond_price(y, c, T, q) -2
    C = C / dy**2
    return C


# # Bond Yield

# In[6]:


from scipy import optimize
def bond_yield(c, T, q, value):
    def f(y):
        return bond_price(y,c,T, q) - value

    try:
        y = optimize.newton(f, 0.05, maxiter=100)
    except RuntimeError:
        y = np.nan
    
    return y


# # bucket of two IR risk

# In[7]:
def read_ust():
    filename = config['SEC_DIR'] / 'IR_Curves.csv'
    ust = pd.read_csv(filename)
    ust = ust[ust['CurveID'] == 'UST'].copy()
    return ust

# Treasury tenor and in years
ust_tenors = read_ust()

tenor_dict = ust_tenors.set_index('SecurityID')['Tenor'].to_dict()
tenor_dict['UST0M'] = 0

def calc_w1(t, T1, T2):
    t1, t2 = tenor_dict[T1], tenor_dict[T2]
    if t >= t2:
        return 0
    
    elif t <= t1:
        return 1
    
    else:
        w1 = (t2-t) / (t2-t1)
    
    return w1


# In[9]:


def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False


# In[10]:


def extract_coupon(name):
    # #.#%
    match = re.search(r'([\d]+\.[\d]+)%', name)
    if match:
        if isfloat(match.group(1)):
            return match.group(1)

    match = re.search(r'([\d]+\.[\d]+)\s', name)
    if match:
        coupon = match.group(1)
        if isfloat(coupon):
            if float(coupon) < 15: # should not be great than 15%
                return coupon


# # Risk Metrics

# In[1]:


# bonds: columns = ['Tenor']
def calc_riskfree_rate(bonds, price_date):
    ir_t = ust_tenors['Tenor'].to_list()
    ir_ids = ust_tenors['SecurityID'].to_list()
    
    ir_hist = get_ir_hist(ir_ids, price_date, price_date)
    ir_yield = ir_hist.iloc[0]
    return bonds['Tenor'].apply(lambda x: np.interp(x, ir_t, ir_yield[ir_ids]))


# # Extract bond maturity

# In[11]:


def extract_date(name):

    # mm/dd/yyyy
    match = re.search(r'(\d{2}/\d{2}/\d{4})', name)
    if match:
        return match.group(1)
    
    # mm/dd/yy
    match = re.search(r'(\d{2})/(\d{2})/(\d{2})', name)
    if match: 
        mm, dd, yy = match.group(1), match.group(2), match.group(3)
        return mm + '/' + dd + '/' + '20' + yy

    # yyyy-dd.mm.yy
    match = re.search(r'\d{4}-(\d{2}).(\d{2}).(\d{2})', name)
    if match: 
        dd, mm, yy = match.group(1), match.group(2), match.group(3)
        return mm + '/' + dd + '/' + '20' + yy

    else:
        return None


# ## Data Tools

# In[12]:

def get_ir_hist(ir_ids, from_date, to_date):

    ir_hist = mkt_timeseries.get(ir_ids, from_date, to_date, category='YIELD')

    ir_hist.fillna(method='ffill', inplace=True)
    ir_hist.fillna(method='bfill', inplace=True)
    ir_hist.index.name = 'Date'
    return ir_hist


# In[13]:


# bonds: index=SecurityID, columns=[Tenor]
# ir_hist: index=hist_date, column=[UST ID], value=UST yield
# results: index=hist_date, columns=[SecurityID], values=bond risk free rate
def calc_ir_yield(bonds, ir_hist):
    ir_t   = ust_tenors['Tenor'].to_list()
    ir_ids = ust_tenors['SecurityID'].to_list()

    # delta t = time to last date in years
    last_date = ir_hist.index[-1]
    ir_hist['delta_t'] = [(last_date - x).days/365 for x in ir_hist.index]

    ir_yields = pd.DataFrame(index=ir_hist.index)
    for ID in bonds.index:
        tenor = bonds.loc[ID, 'Tenor']
        ir_yields[ID] = ir_hist.apply(lambda x: np.interp(tenor + x.delta_t, ir_t, x[ir_ids]),axis=1)

    ir_hist.drop(columns=['delta_t'])
    return ir_yields


