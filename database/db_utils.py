# -*- coding: utf-8 -*-
"""
Created on Fri May 31 15:40:14 2024

@author: mgdin
"""
import pandas as pd
import numpy as np
from io import StringIO
from psycopg2 import sql

from api import app
from database import ms_sql_server as msss
from database import db, pg_connection


# for test
# import xlwings as xw
# from utils import xl_utils
# wb = xw.Book('Book2')

# query = """select * from "user" where username=%(username)s
# """    
# params={'username': 'test1@trg.com'}
# df = get_sql_df(query, params)
def get_sql_df(query, params=None):
    
    with app.app_context():
        df = pd.read_sql(query, con=db.engine, params=params)
    return df
    
#############################################################
from sqlalchemy import create_engine
def get_sql_df_ms():
    
    # Create connection string
    connection_string = (
        f"mssql+pyodbc://{msss.username}:{msss.password}@{msss.server}/{msss.database}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
    )

    
    # SQL query
    sql = "SELECT * FROM dm_port_consolidated WHERE report_id = 5234"
    
    # Load to DataFrame
    df = pd.read_sql(sql, engine=msss.get_engine())

    
    print(df.head())    

#############################################################
#
# insert data of a dataframe into a table
#
# table_name = 'port_positions'
# key_column='port_id'
def insert_df(table_name, df, key_column=None):
    if len(df):
        df = df.copy()
        
        # Replace NaT values in timestamp columns with None (NULL in SQL)
        for col in df.select_dtypes(include='datetime64[ns]').columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d').where(pd.notnull(df[col]), None)
            # df[col] = df[col].where(pd.notnull(df[col]), None)
        
        # Replace NaN values with None (NULL in SQL)
        df = df.where(pd.notnull(df), None)
            
        # delete existing rows to avoid duplicates
        if key_column:
            delete(table_name, key_column, df[key_column].unique())
            
        # get table columns
        columns = get_table_columns(table_name) 
        
        # find common columns
        columns = list(set(columns) & set(df.columns))
        df = df[columns]
    
        # generate SQL
        columns = df.columns.tolist() 
        columns = [f'"{x}"' for x in columns]
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders});"
        # print(query)
    
        # Insert data row by row
        with pg_connection() as conn:
            cur = conn.cursor()
            for idx, row in enumerate(df.itertuples(index=False)):
            # for row in df.itertuples(index=False):
                # cur.execute(query, row)
                try:
                    cur.execute(query, row)
                except Exception as e:
                    print(f"Error inserting row index {idx}: {e}")
                    print("Row data:", row)
                    conn.rollback()
                    raise
                conn.commit()


    print(f"Successfully inserted {len(df)} rows into {table_name}.")

def test_insert_df():

    table_name = 'yh_stock_profile'

    df = pd.read_csv('df.csv')
    insert_df(table_name, df)

# insert a bulk data of a dataframe into a table
# Warning: make sure remove duplicates before calling this function to avoid duplicates
#
# table_name = 'current_price'
# df = prices
def insert_bulk_df(table_name, df):

    try:        
        # get table columns
        columns = get_table_columns(table_name) 

        # find common columns
        columns = list(set(columns) & set(df.columns))
        df = df[columns].copy()

        # copy data csv IO
        output = StringIO()
        df.replace({np.nan: r'\N'}, inplace=True)  # \N is PostgreSQL's NULL for COPY operations
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)

        # copy data to database
        with pg_connection() as conn:
            with conn.cursor() as cursor:
                cursor.copy_from(output, table_name, columns=columns, null='\\N')
                conn.commit()
    
        print(f"Successfully inserted {len(df)} rows into {table_name}.")

    except Exception as e:
        print(f"insert_build_df failed: {str(e)}")

    

# Delete rows from table_name with column_name in values
#
# table_name = 'port_parameters'
# column_name = 'port_id'
# values = ['abc', 'def']
# key_column='port_id'
# values = df[key_column].unique()
# values = [today]
def delete(table_name, column_name, values):
    
    # Ensures Python native `int` type
    if isinstance(values, np.ndarray):
        if np.issubdtype(values.dtype, np.integer):
            values = list(map(int, values))

    placeholders = ', '.join(['%s'] * len(values))
    query = f"""
    DELETE FROM {table_name} WHERE "{column_name}" IN ({placeholders});
    """

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(values))
            conn.commit()

    print(f"deleted {cur.rowcount} row(s) from {table_name}")


def delete_df(table_name, df):
    
    columns = df.columns.to_list()
    columns = ",".join([f'"{x}"' for x in columns])
    placeholders = ', '.join(['%s'] * len(df))
    query = f"""
    DELETE FROM {table_name} WHERE ({columns}) IN ({placeholders});
    """

    data_tuples = [tuple(row) for row in df.to_numpy()]
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, data_tuples)
            conn.commit()

    print(f"deleted {cur.rowcount} row(s) from {table_name}")


def test_delete():
    
    table_name = 'upload_security'
    column_name = 'upload_name'
    values = ['abc', 'def']
    delete(table_name, column_name, values)
    
    # df = delta_rf.iloc[[0]]
    # df = df[['model_id', 'SecurityID', 'Category']]
    # table_name = 'risk_factor'
    # delete_df(table_name, df)
    
def call_procedure(proc_name, data_tuples):
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CALL {proc_name}(%s)", data_tuples)
        conn.commit()
    print(f"Procedure {proc_name} executed successfully.")
    
# call_procedure('UpdateSecurityInfo', (3665,))

###############################################################################
# auxilaries
def get_table_columns(table_name):
    try:
        with pg_connection() as conn:
            with conn.cursor() as cur:
                # Query to get column names for the given table
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s;", (table_name,))
                columns = [row[0] for row in cur.fetchall()]
            return columns
    except Exception as e:
        print(f"Error fetching columns: {e}")
        return []
    
def test_pg_connection():
    try:
        with pg_connection() as conn:
            print("pg connection succeeded")
    except:
        print("pg connection failed")
        
###############################################################################
def get_ft_users():
    return ['fintree', 'ftest555']


# copy value_obj to db_obj
def assign_db_obj(db_obj, value_obj):
    for key, value in vars(value_obj).items():
        if key not in ['id', '_sa_instance_state']:
            setattr(db_obj, key, value)

# Create a new report row in pbi_reports table
# report_id = 1
# creator = 'Michael'
# report_description = 'Test'
def create_report(report_id, creator, report_description):

    # Connect to the SQL Server
    conn = msss.create_connection()
    cursor = conn.cursor()

    # delete the row if exists
    delete_query = "delete dbo.dm_report_id where report_id=?"
    cursor.execute(delete_query, int(report_id))
    
    # Insert the new report
    insert_query = """
    INSERT INTO dbo.dm_report_id (report_id, creator, report_description)
    OUTPUT INSERTED.report_id
    VALUES (?, ?, ?);
    """

    # Execute the insert query
    cursor.execute(insert_query, (report_id, creator, report_description))
    report_id = cursor.fetchval()
    
    # Commit the transaction
    conn.commit()

    # Close the connection
    cursor.close()
    conn.close()
        
    return report_id