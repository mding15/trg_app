# -*- coding: utf-8 -*-
"""
Created on Mon Apr  1 11:29:37 2024

@author: Zhuoqun Zhan
"""
import os

#connect to postgreSQL
# run "pip install psycopg2" in python shell
import psycopg2
import pandas as pd

# dbname = 'your_dbname'        
# user = 'your_username'        
# password = 'your_password'    
# host = 'your_host'            #'localhost' or other address
# port = 'your_port'  # default 5432
# options = '-c search_path= ' #if you have different shema, please fill your shema after =

#the following is my local postgreSQL database parameter
dbname = 'TestDB'        
user = os.environ["DB_USERNAME"]     
password = os.environ["DB_PASSWORD"]
host = 'localhost'            #  'localhost' or other address
port = '5432'
options = '-c search_path=original,public'


try:
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
        options=options #if you dont have any other schema than public, please delete this line
    )
    print("Database connection established.")
except Exception as e:
    print(f"An error occurred: {e}")
    
cur = conn.cursor() #create a cursor to execute SQL in python


cur.execute('select * from persons') #search all data in data table "Class" under schema "original"
# first method to print result
rows = cur.fetchall()
dbclass=pd.DataFrame(rows,columns=[desc[0] for desc in cur.description]) #put the results in data frame
print(dbclass)

# second method
for row in rows:
    print(row)
    
#third method:
query='select * from persons'
dbclass2=pd.read_sql_query(query, conn)    
print(dbclass2)

cur.execute("ROLLBACK") #if a previous SQL failed, please use this, then continue for the next SQL. Or it will stuck in your previous bug.

cur.close() #close cursor
conn.close() #close connection to db


