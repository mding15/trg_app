# -*- coding: utf-8 -*-
"""
Created on Sat May 25 12:46:45 2024

@author: mgdin
"""
import os
import pyodbc as pyodbc
import pandas as pd
import numpy as np
import xlwings as xw

from trg_config import config
from utils import xl_utils


def create_connection():
    server = 'tailriskglobal.database.windows.net'
    database = 'tailriskglobal'
    username = os.environ["DB_USERNAME"]
    password = os.environ["DB_PASSWORD"]
    driver= '{ODBC Driver 18 for SQL Server}'

    connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};uid={username};pwd={password};Encrypt=yes;TrustServerCertificate=no'
    conn = pyodbc.connect(connectionString) #make connection to Azure SQL Database
    
    return conn

# insert dataframe to database
def insert_table(df, table_name, conn):
    df = convert_dates(df)
    schema, full_base_name = table_name.split('.')
    cursor = conn.cursor()
    if check_table_exists(cursor, full_base_name, schema) is not None:
        columns = df.columns.tolist()
        placeholders = ', '.join(['?'] * len(columns))
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        for index, row in df.iterrows():
            cursor.execute(insert_query, tuple(row))
        
        conn.commit()
    else:
        print(f"Table '{table_name}' does not exist in the database. Aborting insert.")
    cursor.close()

def convert_dates(df):
    for column in df.columns:
        if 'Date' in column or 'date' in column:
            df[column] = pd.to_datetime(df[column])
    return df

# check if table already exists in database
def check_table_exists(cursor,  base_table_name, schema='dbo'):
    cursor.execute("SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", schema, base_table_name)
    exists = cursor.fetchone()
    if exists is not None:
        return exists
    else:
        print('not exists')
    
def test():
    conn =   create_connection()
    cur=conn.cursor() 
    
    table_name = 'dbo.ods_DB_Risk'
    query=f"Select * from {table_name}"
    dbrisk=pd.read_sql_query(query, conn)
    print(dbrisk)
    dbrisk.info()

    wb = xw.Book('Book6')
    xl_utils.add_df_to_excel(dbrisk, wb, 'dbrisk', index=False)

    df = xl_utils.read_df_from_excel(wb, 'dbrisk')
    df['model_id'] = df['model_id'].astype('Int64')
    df['bdp_id'] = df['bdp_id'].astype('Int64').astype(str)
    df = df.replace(np.nan, None)
    insert_table(df, table_name, conn)
    df.info()


    cur.close()
    conn.close()

    for index, row in df.iterrows():
        print(tuple(row))

###################
# sqlalchemy
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker

data = {
    'Column1': [1, 2, 3, 4],
    'Column2': ['A', 'B', 'C', 'D'],
    'Column3': [10.1, 20.2, 30.3, 40.4]
}
df = pd.DataFrame(data)

# Create a database connection
# Replace 'sqlite:///example.db' with your database connection string
# For example, PostgreSQL: 'postgresql://user:password@host:port/dbname'
dbfile = config['DB_DIR'] / 'example.db'
engine = create_engine(f'sqlite:///{dbfile}')

# Insert the DataFrame into a database table named 'my_table'
# If the table does not exist, it will be created
df.to_sql('my_table', con=engine, if_exists='replace', index=False)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# Reflect the table
metadata = MetaData()
metadata.reflect(bind=engine)
table_name = 'my_table'  # Replace with your table name
table = Table(table_name, metadata, autoload_with=engine)

# Query the table
query = session.query(table)

# Convert query results to a Pandas DataFrame
df = pd.read_sql(query.statement, engine)

session.close()

##########################################################
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker, declarative_base


# Database connection details
# server = 'your_server_name'  # e.g., 'localhost' or 'your_server_name'
# database = 'your_database_name'  # e.g., 'mydatabase'
# username = 'your_username'  # e.g., 'myusername'
# password = 'your_password'  # e.g., 'mypassword'

# Create a database connection
# connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'
# engine = create_engine(connection_string)

# Create a base class for declarative class definitions
Base = declarative_base()

# Define a class that maps to the table
class MyTable(Base):
    __tablename__ = 'my_table'  # Replace with your table name
    
    # Define columns
    Column1 = Column(Integer, primary_key=True)
    Column2 = Column(String)
    Column3 = Column(Float)
    
    def __repr__(self):
        return f"<MyTable(Column1={self.Column1}, Column2='{self.Column2}', Column3={self.Column3})>"


# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# Query all rows in the table
rows = session.query(MyTable).all()

# Print all rows
for row in rows:
    print(row)

# Query a specific row by primary key
row = session.get(MyTable, 1)  # Replace 1 with the desired primary key value
print(row)

# Convert query results to a Pandas DataFrame
df = pd.read_sql(session.query(MyTable).statement, engine)
print(df)

# Close the session
session.close()


###################################################################################
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

# Database connection details
server = 'your_server_name'  # e.g., 'localhost' or 'your_server_name'
database = 'your_database_name'  # e.g., 'mydatabase'
username = 'your_username'  # e.g., 'myusername'
password = 'your_password'  # e.g., 'mypassword'

# Create a database connection
connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'
engine = create_engine(connection_string)

# Create a base class for declarative class definitions
Base = declarative_base()

# Define the ParentTable class
class ParentTable(Base):
    __tablename__ = 'parent_table'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)

    def __repr__(self):
        return f"<ParentTable(id={self.id}, name='{self.name}')>"

# Define the ChildTable class
class ChildTable(Base):
    __tablename__ = 'child_table'
    id = Column(Integer, primary_key=True, autoincrement=True)
    parent_id = Column(Integer, ForeignKey('parent_table.id'))
    description = Column(String)

    # Define relationship to ParentTable
    parent = relationship('ParentTable', back_populates='children')

    def __repr__(self):
        return f"<ChildTable(id={self.id}, parent_id={self.parent_id}, description='{self.description}')>"

# Establish back-populates relationship in ParentTable
ParentTable.children = relationship('ChildTable', order_by=ChildTable.id, back_populates='parent')

# Create tables in the database
Base.metadata.create_all(engine)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# Add sample data
parent1 = ParentTable(name='Parent 1')
parent2 = ParentTable(name='Parent 2')
session.add(parent1)
session.add(parent2)
session.commit()

child1 = ChildTable(parent_id=parent1.id, description='Child 1 of Parent 1')
child2 = ChildTable(parent_id=parent1.id, description='Child 2 of Parent 1')
child3 = ChildTable(parent_id=parent2.id, description='Child 1 of Parent 2')
session.add(child1)
session.add(child2)
session.add(child3)
session.commit()

# Query and print data
parents = session.query(ParentTable).all()
for parent in parents:
    print(parent)
    for child in parent.children:
        print(f"  {child}")

# Close the session
session.close()

#############################################################################
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

# Database connection details
server = 'your_server_name'
database = 'your_database_name'
username = 'your_username'
password = 'your_password'

# Create a database connection
connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'
engine = create_engine(connection_string)

# Create a base class for declarative class definitions
Base = declarative_base()

# Define the ParentTable class
class ParentTable(Base):
    __tablename__ = 'parent_table'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)

    # Establish relationship to ChildTable with cascade delete
    children = relationship('ChildTable', back_populates='parent', cascade='all, delete-orphan')

    def __repr__(self):
        return f"<ParentTable(id={self.id}, name='{self.name}')>"

# Define the ChildTable class
class ChildTable(Base):
    __tablename__ = 'child_table'
    id = Column(Integer, primary_key=True, autoincrement=True)
    parent_id = Column(Integer, ForeignKey('parent_table.id'))
    description = Column(String)

    # Define relationship to ParentTable
    parent = relationship('ParentTable', back_populates='children')

    def __repr__(self):
        return f"<ChildTable(id={self.id}, parent_id={self.parent_id}, description='{self.description}')>"

# Create tables in the database
Base.metadata.create_all(engine)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# Add sample data
parent1 = ParentTable(name='Parent 1')
parent2 = ParentTable(name='Parent 2')
session.add(parent1)
session.add(parent2)
session.commit()

child1 = ChildTable(parent_id=parent1.id, description='Child 1 of Parent 1')
child2 = ChildTable(parent_id=parent1.id, description='Child 2 of Parent 1')
child3 = ChildTable(parent_id=parent2.id, description='Child 1 of Parent 2')
session.add(child1)
session.add(child2)
session.add(child3)
session.commit()

# Query and print data before deletion
print("Before deletion:")
parents = session.query(ParentTable).all()
for parent in parents:
    print(parent)
    for child in parent.children:
        print(f"  {child}")

# Delete a parent and cascade delete its children
session.delete(parent1)
session.commit()

# Query and print data after deletion
print("\nAfter deletion:")
parents = session.query(ParentTable).all()
for parent in parents:
    print(parent)
    for child in parent.children:
        print(f"  {child}")

# Close the session
session.close()



