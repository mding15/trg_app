# -*- coding: utf-8 -*-
"""
Created on Wed Oct 29 22:24:11 2025

@author: mgdin

datahase migration
Copy postres to sqlite 

"""
# pip install sqlalchemy psycopg2-binary

# export POSTGRES_URL=postgresql+psycopg2://username:password@localhost/dbname

from sqlalchemy import create_engine
from sqlalchemy import Table, MetaData, Column, Integer, String

from sqlalchemy.orm import sessionmaker
import os
os.chdir(r'C:\Users\mgdin\OneDrive\Documents\dev\TRG_App\dev\trgapp')

from trg_config import config

def get_pg_url():
    dbname = 'postgres'        
    user = os.environ["PRS_USERNAME"]     
    password = os.environ["PRS_PASSWORD"]
    host = 'trg-input-database.c9sm826ie6uy.us-east-1.rds.amazonaws.com'
    port = '5432'
    POSTGRESQL_URI = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    return POSTGRESQL_URI 

get_pg_url()

def get_sqlite_url():
    SQLITE_URL = "sqlite:///sqlite_copy.db"
    return SQLITE_URL


# PostgreSQL and SQLite connection URLs
# POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql+psycopg2://user:password@localhost/dbname")
# SQLITE_URL = "sqlite:///sqlite_copy.db"
POSTGRES_URL=get_pg_url()
SQLITE_URL=get_sqlite_url()

# Create SQLAlchemy engines
pg_engine = create_engine(POSTGRES_URL)
sqlite_engine = create_engine(SQLITE_URL)

# Create sessions
PGSession = sessionmaker(bind=pg_engine)
SQLiteSession = sessionmaker(bind=sqlite_engine)
pg_session = PGSession()
sqlite_session = SQLiteSession()

# Reflect PostgreSQL metadata
pg_metadata = MetaData()
pg_metadata.reflect(bind=pg_engine)

# Helper function to clone columns and constraints
def clone_column(col):
    """Creates a new Column object by cloning the properties of an existing one."""
    
    # 1. Clone the Column, but strip its existing parent (table)
    # The .copy() method is the cleanest way to do this in modern SQLAlchemy.
    new_col = col.copy()
    
    # 2. Clone any attached constraints (like PrimaryKeyConstraint or Index)
    # This step is crucial for functional constraints like auto-incrementing IDs.
    if col.primary_key:
        new_col.primary_key = True
    if col.autoincrement:
        new_col.autoincrement = col.autoincrement
        
    # NOTE: More complex constraints (like foreign keys) require extra handling 
    # to ensure they point to the correct table in the *new* metadata. 
    # For a simple schema mirror, this basic cloning is often sufficient.
    
    return new_col

tb_list = [
'security_attribute', 'bond_info', 'port_parameters', 'security_xref', 'security_info', 'ir_curves', 'account_limit',
'upload_security', 'roles', 'risk_factor', 'approval', 'risk_model', 'pbi_report_url', 'portfolio_group', 'client', 
'pbi_current_report_url', 'user_entitilement', 'user', 'portfolio_info', 'class_expect_return', 'port_positions', 
'stat_static_data', 'stat_private_equity', ''
]
# Create tables in SQLite and copy data
sqlite_metadata = MetaData()

for table_name, table in pg_metadata.tables.items():
    print(f"Processing table: {table_name}")
    
    # CLONING THE COLUMNS IS THE KEY STEP
    cloned_columns = [clone_column(c) for c in table.columns]
    
    # Create table in SQLite using the newly cloned, unassigned columns
    # We pass the cloned columns directly as positional arguments
    new_table = Table(table_name, sqlite_metadata, *cloned_columns)

    # Create the table in the SQLite database
    new_table.create(bind=sqlite_engine, checkfirst=True)
    
    
    try:
        new_table.create(bind=sqlite_engine, checkfirst=True)
    except Exception as e:
        print(f"Error creating table {table_name} in SQLite: {e}")
        # Optionally continue to the next table
        continue

for table_name, table in pg_metadata.tables.items():
    print(f"Processing table: {table_name}")
    
    # Create table in SQLite
    new_table = Table(table_name, sqlite_metadata, *table.columns)
    new_table.create(bind=sqlite_engine, checkfirst=True)

    # Fetch data from PostgreSQL
    pg_data = pg_session.execute(table.select()).fetchall()
    if pg_data:
        rows = [dict(row._mapping) for row in pg_data]
        with sqlite_engine.begin() as conn:
            conn.execute(new_table.insert(), rows)

print("✅ All tables and data have been successfully copied from PostgreSQL to SQLite.")


# end migration
###############################################################################
from sqlalchemy import Table, MetaData, Column, Integer, String
# Assuming 'pg_metadata', 'sqlite_metadata', and 'sqlite_engine' are defined



for table_name, table in pg_metadata.tables.items():
    print(f"Processing table: {table_name}")
    
    # CLONING THE COLUMNS IS THE KEY STEP
    cloned_columns = [clone_column(c) for c in table.columns]
    
    # Create table in SQLite using the newly cloned, unassigned columns
    # We pass the cloned columns directly as positional arguments
    new_table = Table(table_name, sqlite_metadata, *cloned_columns)
    
    # You might also want to clone indexes and other constraints here
    for index in table.indexes:
        # Clone index creation logic here
        pass

    # Create the table in the SQLite database
    try:
        new_table.create(bind=sqlite_engine, checkfirst=True)
    except Exception as e:
        print(f"Error creating table {table_name} in SQLite: {e}")
        # Optionally continue to the next table
        continue