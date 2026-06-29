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
def zero_bond_price(y, T):
    return (1+y)**(-T)

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
# ── UST curve — lazy-loaded from ir_curves table ──────────────────────────────

_ust_tenors: pd.DataFrame = None
_tenor_dict: dict         = None
_loaded                   = False


def _load_ust() -> None:
    global _ust_tenors, _tenor_dict, _loaded
    if _loaded:
        return
    from database2 import pg_connection
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT "SecurityID", "Tenor" FROM ir_curves WHERE "CurveID" = %s',
                ('UST',),
            )
            rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=['SecurityID', 'Tenor'])
    _ust_tenors = df
    _tenor_dict = df.set_index('SecurityID')['Tenor'].to_dict()
    _tenor_dict['UST0M'] = 0
    _loaded = True


def _get_ust_tenors() -> pd.DataFrame:
    _load_ust()
    return _ust_tenors


def _get_tenor_dict() -> dict:
    _load_ust()
    return _tenor_dict


def get_ust_tenors() -> pd.DataFrame:
    """Return UST tenor DataFrame, loading from ir_curves on first call."""
    return _get_ust_tenors()


def reload_ust() -> None:
    """Discard the cache and reload ir_curves from the database."""
    global _loaded
    _loaded = False
    _load_ust()


def calc_w1(t, T1, T2):
    td = _get_tenor_dict()
    t1, t2 = td[T1], td[T2]
    if t >= t2:
        return 0
    elif t <= t1:
        return 1
    else:
        w1 = (t2 - t) / (t2 - t1)
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
    ir_t   = _get_ust_tenors()['Tenor'].to_list()
    ir_ids = _get_ust_tenors()['SecurityID'].to_list()
    
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
    ir_t   = _get_ust_tenors()['Tenor'].to_list()
    ir_ids = _get_ust_tenors()['SecurityID'].to_list()

    # delta t = time to last date in years
    last_date = ir_hist.index[-1]
    ir_hist['delta_t'] = [(last_date - x).days/365 for x in ir_hist.index]

    ir_yields = pd.DataFrame(index=ir_hist.index)
    for ID in bonds.index:
        tenor = bonds.loc[ID, 'Tenor']
        ir_yields[ID] = ir_hist.apply(lambda x: np.interp(tenor + x.delta_t, ir_t, x[ir_ids]),axis=1)

    ir_hist.drop(columns=['delta_t'])
    return ir_yields


