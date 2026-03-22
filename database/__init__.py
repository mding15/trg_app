# -*- coding: utf-8 -*-
"""
Created on Sat May 25 17:01:25 2024

@author: mgdin
"""
import os
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import inspect
from trg_config import config
import psycopg2

# SQLite Database
# dbname = config['sqlite_db'] 
# DATABASE_URI = f'sqlite:///{dbname}'


# Postgres database
dbname = 'postgres'        
user = os.environ["PRS_USERNAME"]     
password = os.environ["PRS_PASSWORD"]
host = 'trg-input-database.c9sm826ie6uy.us-east-1.rds.amazonaws.com'
port = '5432'

# PostgreSQL Connect
pg_conn_params = {
    'dbname': dbname,
    'user': user,
    'password': password,
    'host': host,
    'port': port 
}

def pg_create_connection():
    conn = psycopg2.connect(**pg_conn_params)
    return conn

DATABASE_URI = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'

db = SQLAlchemy()

# Function to get column names from a table
def get_column_names(table_name):
    inspector = inspect(db.engine)
    columns = inspector.get_columns(table_name)
    column_names = [column['name'] for column in columns]
    return column_names

class pg_connection:
    def __enter__(self):
        self.conn = pg_create_connection()
        return self.conn

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()
