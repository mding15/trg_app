# -*- coding: utf-8 -*-
"""
Created on Sun Mar 31 19:16:43 2024

@author: mgdin
"""

import pandas as pd
import xlwings as xw

from api import app, db
app.app_context().push()

from utils import xl_utils as xl
from trg_config import config
from security import security, maintenance
from security.security_info import (get_xref_by_ID,get_security_by_ID,get_ID_by_ISIN,
                                    get_security_by_sec_id_list,get_SecurityID_by_ref, 
                                    xref_to_postions,get_securities_with_xref,
                                    create_xref_by_type)

def get_all():
    wb = xw.Book('Book1')
    xref = get_xref_by_ID()
    xl.add_df_to_excel(xref, wb, 'xref', index=False)    
    
def test_security():
    # by SecurityID
    get_security_by_ID(['T10000009', 'T10000010'])

    # by ISIN
    sec_ids = get_ID_by_ISIN(['LU1717117896', 'US91282CEM91'])['SecurityID']
    get_security_by_ID(sec_ids)

    # by list of (ref_type, ref_id)
    sec_id_list = [('ISIN', 'LU0119620176'), ('CUSIP','74340XBN0'),('Ticker','GOOG'), ('TRG_ID', 'T10000033')]
    sec_id_list = [('CUSIP','90309KEF7')]
    sec_info = get_security_by_sec_id_list(sec_id_list)

    # search SecurityID    
    positions = xref_to_postions(sec_id_list)
    get_SecurityID_by_ref(positions)

    # security info and xref
    get_securities_with_xref(['T10000009', 'T10000010'])


def test():
    wb = xw.Book('Book1')
    # create_xref_by_type()
    xref_df = xl.read_df_from_excel(wb, 'xref')
    ref_type = 'CUSIP'
    create_xref_by_type(xref_df, ref_type)

    # create_security_and_xref()    
    new_securities = xl.read_df_from_excel(wb, 'new')
