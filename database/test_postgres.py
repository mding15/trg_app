# -*- coding: utf-8 -*-
"""
Created on Mon Jul 22 17:24:19 2024

@author: mgdin
"""
import os
import psycopg2
import pandas as pd
from trg_config import config

# Postgres database
dbname = 'postgres'        
user = os.environ["PRS_USERNAME"]     
password = os.environ["PRS_PASSWORD"]
host = 'trg-input-database.c9sm826ie6uy.us-east-1.rds.amazonaws.com'
port = '5432'

def create_connection():

    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )

def test_connection():
    try:
        conn = create_connection()
        print("Database connection established.")
        conn.close()
    except Exception as e:
        print(f"An error occurred: {e}")
        
    
def test_db():
    username = 'test1@trg.com'
    sql = """
    select * from "user" u
    where username = %s 
    """
    conn = create_connection()
    with conn.cursor() as cursor:
        cursor.execute(sql, (username,))
        result = cursor.fetchall()
        for row in result:
            print(row)
    conn.close()

if __name__ == '__main__':
    test_connection()
