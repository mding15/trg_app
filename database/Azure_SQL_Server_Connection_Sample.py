# -*- coding: utf-8 -*-
"""
Created on Mon Apr  1 13:00:28 2024

@author: Zhuoqun Zhan
"""
import os

#connect Microsoft Azure SQL Server
#run "pip install pyodbc" in python shell
import pyodbc as pyodbc
import pandas as pd

# server = 'your_server.database.windows.net'
# database = 'your_database'
# username = 'your_username'
# password = 'your_password'
# driver= '{ODBC Driver 18 for SQL Server}'

# Here is my sample, please rewrite your password
server = 'tailriskglobal.database.windows.net'
database = 'tailriskglobal'
username = os.environ["DB_USERNAME"] # if you dont add username here, please also delete uid={username} in connectionstring, then please look at the comment in the end
password = os.environ["DB_PASSWORD"] # if you dont add password here, please also delete pwd={password} in connectionstring, then please look at the comment in the end
driver= '{ODBC Driver 18 for SQL Server}'

connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};uid={username};pwd={password};Encrypt=yes;TrustServerCertificate=no;Authentication=ActiveDirectoryInteractive'

conn = pyodbc.connect(connectionString) #make connection to Azure SQL Database

cur=conn.cursor() #create a cursor to execute SQL in python

#cur.execute() will let you execute SQL in database. It can be any SQL code, not only select
cur.execute('Select * from dbo.DB_Risk') #search all data in data tale "db_risk" under schema "dbo"
# first method to print search result
rows=cur.fetchall()  
for row in rows:
    print(row)


# second method to print search result
query="Select * from dbo.DB_Risk"
dbrisk=pd.read_sql_query(query, conn)
print(dbrisk)


#if you want to insert data frame into database:
data={'sid':['001','002'],'sname':['apple','nvidia'],'stype':['stock','stock']}
df=pd.DataFrame(data)
table_name='test_increment' #already created in Azure SQL Server Database and already set auto increment on unique id

for index, row in df.iterrows():
    insert_query = f"INSERT INTO {table_name} (sname, stype) VALUES (?, ?)"
    cur.execute(insert_query, row['sname'], row['stype'])

# commit SQL to database
conn.commit()

query="Select * from dbo.test_increment"
test_increment=pd.read_sql_query(query, conn)
print(test_increment)



conn.close() #close connection


### When you connect to Azure SQL Server, there will be pop-up website let you fill in your password
### There might be a warning said "Script Fail" or something like that, please ignore it





# you can also use sqlalchemy to connect to database and insert data:
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from azure.identity import DefaultAzureCredential
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy import inspect

# Use Active Directory Interactive Authentification (Azure AD MFA)
connection_url = URL.create(
    "mssql+pyodbc",
    username=username,
    password=password,
    host=server,
    database=database,
    query={
        "driver": "ODBC Driver 18 for SQL Server",
        "authentication": "ActiveDirectoryInteractive"
    }
)

# create connection to database
engine = create_engine(connection_url)

# create dataframe which needs to be inserted
data={'sid':['001','002'],'sname':['apple','nvidia'],'stype':['stock','stock']} 
df=pd.DataFrame(data)
df_subset=df[['sname','stype']] #if we only want to insert the last two columns to database

table_name='test_increment'


# we need to check if the table already exist, if yes, then insert data, if not, then return error. 
# we have inspector because if there is no existing table, to_sql will automatically create one. We don't want this.
inspector = inspect(engine)

if not inspector.has_table(table_name):
    raise ValueError(f"The table '{table_name}' does not exist.")
else:
    df_subset.to_sql(name=table_name, con=engine,if_exists='append',index=False)
    


