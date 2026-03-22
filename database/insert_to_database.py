# -*- coding: utf-8 -*-
"""
Created on Thu May  9 10:57:08 2024

@author: 37365
"""
import pandas as pd
from queue import Queue
from threading import Lock
from database import ms_sql_server

class ConnectionPool:
    def __init__(self, min_conns, max_conns):
        self.min_conns = min_conns
        self.max_conns = max_conns
        self.pool = Queue(max_conns)
        self.lock = Lock()
        self._initialize_pool()

    def _initialize_pool(self):
        for _ in range(self.min_conns):
            conn = ms_sql_server.create_connection()
            self.pool.put(conn)

    def get_connection(self):
        with self.lock:
            if self.pool.empty() and self.pool.qsize() < self.max_conns:
                return ms_sql_server.create_connection()
            return self.pool.get()

    def return_connection(self, conn):
        with self.lock:
            self.pool.put(conn)

    def close_all(self):
        while not self.pool.empty():
            conn = self.pool.get()
            conn.close()
            

def create_connection_pool():

    pool = ConnectionPool(min_conns=1, max_conns=10) #make connection to Azure SQL Database
    
    return pool

        
# get table name from database        
def get_db_table_name(cursor, dataframe_name):
    cursor.execute("SELECT databasetable FROM table_mapping WHERE enginetable = ?", (dataframe_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        return dataframe_name


# check if table already exists in database
def check_table_exists(cursor,  base_table_name, schema='dbo'):
    cursor.execute("SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", schema, base_table_name)
    exists = cursor.fetchone()
    if exists is not None:
        return exists
    else:
        print('not exists')
    

# if the table should be static data, then we should check the data already exists
# if yes, then skip, if not, then insert
def check_row_exists(cursor, table_name, row, columns, ignore_columns=None):
    if ignore_columns is None:
        ignore_columns = []
    
    # 排除要忽略的列
    filtered_columns = [col for col in columns if col not in ignore_columns]
    filtered_row = [row[col] for col in filtered_columns]

    column_checks = ' AND '.join([f"{col} = ?" for col in filtered_columns])
    query = f"SELECT 1 FROM {table_name} WHERE {column_checks}"
    cursor.execute(query, tuple(filtered_row))
    exists = cursor.fetchone()
    return exists is not None


def convert_dates(df):
    for column in df.columns:
        if 'Date' in column or 'date' in column:
            df[column] = pd.to_datetime(df[column])
    return df


# insert dataframe to database
# dataframe_name = 'dm_d_TS_Vol_VaR'
# df = results_processed[dataframe_name]['TS_Vol_VaR']
def insert_dataframe(df, dataframe_name, conn, check_duplicates=False):
    df = convert_dates(df)
    cursor = conn.cursor()
    db_table_name = get_db_table_name(cursor, dataframe_name)
    
    # if db_table_name and check_table_exists(cursor, db_table_name):
    columns = df.columns.tolist()
    placeholders = ', '.join(['?'] * len(columns))
    insert_query = f"INSERT INTO {db_table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    
    ignore_columns = ['insert_time']
    
    for index, row in df.iterrows():
        if check_duplicates:
            if not check_row_exists(cursor, db_table_name, row, columns, ignore_columns):
                cursor.execute(insert_query, tuple(row))
        else:
            cursor.execute(insert_query, tuple(row))
    
    conn.commit()
    cursor.close()


def delete_dataframe(df, dataframe_name, conn):
    if 'report_id' in df.columns:
        report_id = int(df['report_id'].iloc[0])
        with conn.cursor() as cursor:
            db_table_name = get_db_table_name(cursor, dataframe_name)
            query = f'DELETE {db_table_name} WHERE report_id=?'
            cursor.execute(query, report_id)
            conn.commit()
            
        
        
# nested_dataframes=results_processed
def insert_dataframe_dic(conn, nested_dataframes):
    for name, inner_dict in nested_dataframes.items():
        for df_key, df in inner_dict.items():
            # print(df_key, name)
            delete_dataframe(df, name, conn) # delete by report_id to avoid duplicates
            insert_dataframe(df, name, conn)
    
def close_connection(pool):
    if pool:
        pool.close_all()


 