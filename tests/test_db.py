# -*- coding: utf-8 -*-
"""
Created on Sat Jun  1 15:04:18 2024

@author: mgdin
"""
import pandas as pd

from trg_config import config
from api import app
from database import db

def rollback():
    with app.app_context():
        db.session().rollback()

    # push the app context 
    app.app_context().push()



# drop table
# Security.__table__.drop(db.engine)

def migration():
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


from sqlalchemy import inspect

# Function to get column names from a table
def get_column_names(table_name, engine):
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)
    column_names = [column['name'] for column in columns]
    return column_names

   
def test():
    
    # select all
    securities = SecurityInfo.query.all()
    for sec in securities:
        print(sec)


    new_securities = []
    # add new security
    sec_id = 1
    SecurityID = 'T90000000'
    
    SecurityName = 'Test1'
    Currency	 = 'USD'
    Source	     = 'Test'
    AssetClass	 = 'Equity'
    AssetType	 = 'Index'
    
    sec = SecurityInfo(
                   SecurityName = SecurityName,
                   Currency = Currency,
                   AssetClass = AssetClass,
                   AssetType = AssetType,
                   DataSource = Source
                   )

    new_securities.append(sec)
    
    for sec in new_securities:
        print(sec)
        db.session.add(sec)
    db.session.commit()

    for sec in new_securities:
        sec_id = sec.id
        sec.SecurityID = f'T{100000000+sec_id}'
        
    db.session.commit()
    
    ref_type = 'ISIN'
    ref_id = 'US0231351067'
    SecurityXref.query.filter_by(REF_ID=ref_id, REF_TYPE=ref_type).first()
    
