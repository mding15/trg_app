# -*- coding: utf-8 -*-
"""
Created on Sun Jun  2 21:56:59 2024

@author: mgdin
"""
import pandas as pd
from flask import Flask

from trg_config import config
from database.models import SecurityInfo, SecurityXref
from api import app, db
from mkt_data import mkt_data_info
from xl_front import xl_mkt_data

def MIGRATION():
    # database 
    tables_create()

    # copy security csv to tables
    security_migration()
    
    # insert db stats
    mkt_data_info.insert_db_stat()















def tables_create():
    # Bind SQLAlchemy to the API app within the application context
    with app.app_context():
        db.create_all()  # Create tables if they don't exist
    
    # push the app context 
    app.app_context().push()

# drop table
# Security.__table__.drop(db.engine)

def security_migration():
    SECURITY_FILE       = config['SEC_DIR']  / 'securities.csv'
    SECURITY_XREF_FILE  = config['SEC_DIR']  / 'security_xref.csv'    

    Securities    = pd.read_csv(SECURITY_FILE)
    
    for i in range(len(Securities)):
        SecurityID, SecurityName, Currency, Source, AssetClass, AssetType,DateAdded = Securities.iloc[i]
        #print(SecurityID, SecurityName, Currency, Source, AssetClass, AssetType)
        sec_id = int(SecurityID[2:])
        print(sec_id, SecurityID)
        
        security = SecurityInfo(id=sec_id, 
                       SecurityID = SecurityID,
                       SecurityName = SecurityName,
                       Currency = Currency,
                       AssetClass = AssetClass,
                       AssetType = AssetType,
                       DataSource = Source
                       )
        db.session.add(security)
    db.session.commit()
    
    Security_xref = pd.read_csv(SECURITY_XREF_FILE)
    for i in range(len(Security_xref)):
    
        REF_ID, REF_TYPE, SecurityID, Source, DateAdded = Security_xref.iloc[i]
        print(REF_ID, REF_TYPE, SecurityID, Source)
        
        sec_xref = SecurityXref(
                       REF_ID = REF_ID,
                       REF_TYPE = REF_TYPE,
                       SecurityID = SecurityID,
                       DataSource = Source
                       )
        db.session.add(sec_xref)
    db.session.commit()
