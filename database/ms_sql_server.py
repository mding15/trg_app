# -*- coding: utf-8 -*-
"""
Created on Tue May 28 11:24:47 2024

@author: mgdin
"""
import os
import pyodbc as pyodbc
import pandas as pd
import threading
import pandas as pd
import numpy as np
import traceback
import time

server      = 'tailriskglobal.database.windows.net'
database    = 'tailriskglobal'
username    = os.environ["DB_USERNAME"]
password    = os.environ["DB_PASSWORD"]
driver      = '{ODBC Driver 18 for SQL Server}'
connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};uid={username};pwd={password};Encrypt=yes;TrustServerCertificate=no'

BATCH_SIZE = 500  # tune: 200–1000 is typical
CURSOR_TIMEOUT = 180  # 0 = no timeout; or set to a large seconds value
LOCK_TIMEOUT_MS = 30000

from api.logging_config import get_logger
logger = get_logger(__name__)

def create_connection():

    success = False
    for i in range(10): # try 10 times
        try:
            print(f"{i}: trying to establish connection to SQL Server ...")
            conn = pyodbc.connect(connectionString) #make connection to Azure SQL Database
            success = True
            break
        except Exception as e:
            logger.exception("connection error occurred")
            # print(e)
        
    
    if success:
        print("SQL Server connection established!")
        return conn
    else:
        raise Exception("SQL Server: Failed to connect")
            

###############################################
# SqlAlchemy engine
from sqlalchemy import create_engine
_engine = None
def get_engine():
    
    # Create connection string
    connection_string = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
    )

    global _engine
    
    # Create engine
    if _engine is None:
        _engine = create_engine(connection_string, pool_pre_ping=True)

    return _engine
################################################

# check if table already exists in database
def check_table_exists(cursor,  base_table_name, schema='dbo'):
    cursor.execute("SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", schema, base_table_name)
    exists = cursor.fetchone()
    if exists is not None:
        return True
    else:
        return False

def test_connection():
    conn = create_connection()
    conn.close()

def wakeup_server():
    thread = threading.Thread(target=test_connection)
    thread.start()

#################################################################################################
#
# insert data of a dataframe into a table
#
# table_name = 'dm_port_consolidated'
# key_column='report_id'
def insert_df(table_name, df, key_column=None, conn_p=None):
    try:
        insert_df_ex_2(table_name, df, key_column, conn_p)
    except Exception as e:
        logger.exception("MS SQL error occured")
        raise Exception(f'DB error occured! insert_df({table_name}) failed.')
    
# def insert_df_ex(table_name, df, key_column, conn_p):
#     if len(df) == 0:
#         print('inserted 0 rows')
#         return
    
#     # database connection
#     if conn_p:
#         conn = conn_p
#     else:
#         conn = create_connection()
    
#     # make a copy    
#     df = df.copy()

#     # date columns
#     for column in df.columns:
#         if 'Date' in column or 'date' in column:
#             df[column] = pd.to_datetime(df[column])

#     # Replace NaT values in timestamp columns with None (NULL in SQL)
#     for col in df.select_dtypes(include='datetime64[ns]').columns:
#         df[col] = df[col].dt.strftime('%Y-%m-%d').where(pd.notnull(df[col]), None)
#         # df[col] = df[col].where(pd.notnull(df[col]), None)
    
#     # Replace NaN values with None (NULL in SQL)
#     df.replace([np.nan, np.inf, -np.inf], None, inplace=True)

    
#     # delete existing rows to avoid duplicates
#     if key_column:
#         delete(table_name, key_column, df[key_column].unique(), conn)
        
#     # get table columns
#     columns = get_table_columns(table_name, conn) 
              
#     # find common columns
#     columns = list(set(columns) & set(df.columns))
#     df = df[columns]

#     # generate SQL
#     columns = df.columns.tolist() 
#     placeholders = ', '.join(['?'] * len(columns))
#     query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

#     # print(query)

#     # BATCH_SIZE=50
#     # # insert data into db
#     # if table_name in ['xxxx']:
#     #     print(f'insert table {table_name} in batches...')
#     #     for i in range(0, len(df), BATCH_SIZE):
#     #         batch = df.iloc[i:i + BATCH_SIZE]
#     #         insert_df_by_fast_exec(batch, query)
#     # else:
#     #     insert_df_by_fast_exec(df, query)
            
#     # insert by fast_executemany
#     with conn.cursor() as cur:
#         cur.fast_executemany = True
#         data = df.values.tolist()
#         cur.executemany(query, data)
    
#     # Insert data row by row
#     # with conn.cursor() as cur:
#     #     for row in df.itertuples(index=False):
#     #         cur.execute(query, tuple(row))
#     #     conn.commit()
    
#     if conn_p is None:
#         conn.close()
    
#     print(f"Successfully inserted {len(df)} rows into {table_name}.")

# improve the insert_df_ex
def _insert_normal_chunked(conn, query, data, table_name):
    with conn.cursor() as cur:
        cur.fast_executemany = False
        try:
            if CURSOR_TIMEOUT is not None:
                cur.timeout = CURSOR_TIMEOUT
        except Exception:
            pass
        try:
            cur.execute(f"SET LOCK_TIMEOUT {LOCK_TIMEOUT_MS}")
        except Exception:
            pass

        total = len(data); t0 = time.time()
        logger.info("Chunked insert start: table=%s rows=%s batch_size=%s", table_name, total, BATCH_SIZE)
        for i in range(0, total, BATCH_SIZE):
            j = min(i+BATCH_SIZE, total)
            chunk = data[i:j]
            logger.info("Chunk %d/%d (rows %d..%d) begin",
                        (i//BATCH_SIZE)+1, (total + BATCH_SIZE - 1)//BATCH_SIZE, i, j-1)
            try:
                cur.executemany(query, chunk)
                conn.commit()
                logger.info("Chunk %d committed, rows_in_chunk=%d, cursor_rowcount=%s",
            (i // BATCH_SIZE) + 1, len(chunk), getattr(cur, "rowcount", "n/a"))
            except Exception as e:
                logger.error("Chunk failed table=%s range=%d..%d err=%s", table_name, i, j-1, e, exc_info=True)
                logger.error("Sample row from failed chunk: %s", chunk[0] if chunk else None)
                raise
        logger.info("Chunked insert complete: table=%s total_rows=%s elapsed=%.2fs",
                    table_name, total, time.time()-t0)

def insert_df_ex_2(table_name, df, key_column, conn_p):
    if len(df) == 0:
        print('inserted 0 rows')
        return

    # 1) Get (or create) connection
    conn = conn_p or create_connection()

    # 2) Copy and normalize data (keep your original logic)
    df = df.copy()

    # datetime columns: pandas datetime -> Python datetime (or None)
    for column in df.columns:
        if 'Date' in column or 'date' in column:
            df[column] = pd.to_datetime(df[column], errors='coerce')

    for col in df.select_dtypes(include='datetime64[ns]').columns:
        df[col] = df[col].apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)

    # NaN / inf -> None
    df.replace([np.nan, np.inf, -np.inf], None, inplace=True)

    # object columns -> str or None (consistent types per column)
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].apply(lambda x: str(x) if x is not None and not pd.isna(x) else None)

    # Pre-calc max length for string columns (for setinputsizes – optional)
    string_col_maxlen = {
        col: int(df[col].map(lambda x: len(x) if x else 0).max() or 1)
        for col in df.select_dtypes(include='object').columns
    }

    # 3) Delete duplicates if key provided
    if key_column:
        delete(table_name, key_column, df[key_column].unique(), conn)

    # 4) Align columns with DB table
    columns = get_table_columns(table_name, conn)
    columns = list(set(columns) & set(df.columns))
    df = df[columns]

    # 5) Build SQL + data
    placeholders = ', '.join(['?'] * len(columns))
    query = f"INSERT INTO {table_name} ({', '.join(f'[{c}]' for c in columns)}) VALUES ({placeholders})"
    data = df.values.tolist()

    # 6) Try fast_executemany first
    try:
        with conn.cursor() as cur:
            cur.fast_executemany = True

            # Optional: hint input sizes for object (string) columns
            sizes = []
            for col in columns:
                if col in string_col_maxlen:
                    sizes.append((pyodbc.SQL_WVARCHAR, string_col_maxlen[col]))
                else:
                    sizes.append(None)
            try:
                cur.setinputsizes(*sizes)
            except Exception:
                pass  # not all drivers/types support this

            cur.executemany(query, data)
            conn.commit()
            print(f"Successfully inserted {len(df)} rows into {table_name} via fast_executemany.")
            if conn_p is None:
                conn.close()
            return
    except Exception as e:
        logger.warning(
            "fast_executemany failed for %s, will reconnect and fallback to chunked normal executemany: %s",
            table_name, e
        )
        # Don't close the external connection, just create a new one for fallback
        # The external connection should remain valid for subsequent operations

    # 7) Fallback: normal executemany in chunks (on a fresh connection)
    # temp_conn = create_connection()
    # try:
    #     temp_conn.autocommit = True
    #     _insert_normal_chunked(temp_conn, query, data, table_name)
    #     print(f"Successfully inserted {len(df)} rows into {table_name} via chunked executemany.")
    # finally:
    #     # always close the temp connection
    #     try:
    #         temp_conn.close()
    #     except Exception:
    #         pass
    #     # Don't close the external connection - let the caller manage it
    #     # Only close if we created the connection in this function
    #     if conn_p is None:
    #         try:
    #             conn.close()
    #         except Exception:
    #             pass




def insert_df_by_fast_exec(df, query):
    conn = create_connection()
    with conn.cursor() as cur:
        cur.fast_executemany = True
        data = df.values.tolist()
        cur.executemany(query, data)
    conn.close()
    
def insert_df_by_row(df, conn, query):
    
    # Insert data row by row
    with conn.cursor() as cur:
        for row in df.itertuples(index=False):
            # print(row)
            cur.execute(query, tuple(row))
        conn.commit()
    
# table_name='dm_port_consolidated'
# column_name='report_id'
# values = [123]
def delete(table_name, column_name, values, conn_p=None):
    
    # Ensures Python native `int` type
    if isinstance(values, np.ndarray):
        if np.issubdtype(values.dtype, np.integer):
            values = list(map(int, values))

    placeholders = ', '.join(['?'] * len(values))
    query = f"""
    DELETE FROM {table_name} WHERE {column_name} IN ({placeholders});
    """
    if conn_p:
        conn = conn_p
    else:
        conn = create_connection()
        
    
    with conn.cursor() as cur:
        cur.execute(query, tuple(values))
        conn.commit()

    if not conn_p:
        conn.close()

    print(f"deleted {cur.rowcount} row(s) from {table_name}")


def delete_df(table_name, df, conn_p=None):
    
    columns = df.columns.to_list()
    columns = ",".join([f'"{x}"' for x in columns])
    placeholders = ', '.join(['?'] * len(df))
    query = f"""
    DELETE FROM {table_name} WHERE ({columns}) IN ({placeholders});
    """

    if conn_p:
        conn = conn_p
    else:
        conn = create_connection()
        
    data_tuples = [tuple(row) for row in df.to_numpy()]
    
    with conn.cursor() as cur:
        cur.execute(query, data_tuples)
        conn.commit()

    if not conn_p:
        conn.close()
        
    print(f"deleted {cur.rowcount} row(s) from {table_name}")


###############################################################################
# auxilaries
# table_name = 'dm_port_consolidated'
def get_table_columns(table_name, conn_p=None):
    
    try:
        if conn_p:
            conn = conn_p
        else:
            conn = create_connection()
        
        with conn.cursor() as cur:
            # Query to get column names for the given table
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = ?;", (table_name,))
            columns = [row[0] for row in cur.fetchall()]

        if not conn_p:
            conn.close()

        return columns            
    except Exception as e:
        print(f"Error fetching columns: {e}")
        return []
    