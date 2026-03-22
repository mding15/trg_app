# -*- coding: utf-8 -*-
"""
Created on Tue May 28 10:29:24 2024

@author: mgdin
"""
import pandas as pd
import numpy as np
from pathlib import Path
import pyodbc as pyodbc

from report import powerbi
from database import ms_sql_server

# Note!!!!
#
# this file is replaced by report/powerbi::insert_results_to_db(results, username, report_description)
#


# {report_name : table_name}
table_map = {
    
'DimClasses.Class'              : 'ods_Class',
'DimClasses.Country'            : 'ods_Country',
'DimClasses.Currency'           : 'ods_Currency',
'DimClasses.GICS'               : 'ods_GICS',
'DimClasses.Region'             : 'ods_Region',
'DimClasses.SC1'                : 'ods_SC1',
'DimClasses.SC2'                : 'ods_SC2',
'DimClasses.Sector'             : 'ods_Sector',
'DimPositions.DimPositions'     : 'ods_DimPositions',
'Error_Positions.Positions'     : 'ods_Positions',
'Fact_AggTables.DB Risk'        : 'ods_DB_Risk',
'Fact_AggTables.Top Hedges'     : 'ods_Top_Hedges',
'Fact_AggTables.Top Risks'      : 'ods_Top_Risks',
'Fact_Bmk.Fact_Bmk'             : 'ods_Fact_Bmk',
'Fact_MgPositions.Class'        : 'ods_Class2',
'Fact_MgPositions.Fact_MgPositions': 'ods_Fact_MgPositions',
'Fact_MgPositions.SC1'          : 'ods_SC12',
'Fact_MgPositions.SC2'          : 'ods_SC22',
'Fact_Parameters.Fact_Parameters': 'ods_Fact_Parameters',
'Fact_Positions.Fact_Positions' : 'ods_Fact_Positions'
}

static_tables = [
    'ods_Class',
    'ods_Country',
    'ods_Currency',
    'ods_GICS',
    'ods_Region',
    'ods_SC1',
    'ods_SC2',
    'ods_Sector'
    ]

#
# Main function
#

# creator = 'Michael'
# report_description = 'Test Power BI report'

def insert_report_to_db(reports, creator, report_description):
    
    table_data = report_to_table(reports, static_tables)
    
    # create a new report
    report_id = create_report(creator, report_description)

    # connect to database    
    conn = ms_sql_server.create_connection()
    
    # insert all 
    for table_name, df in table_data.items():
        print(f'INSERT INTO TABLE {table_name} ...')
        # update report_id
        df['report_id'] = report_id
        insert_dataframe(df, table_name, conn)    
    
    conn.close()

    print(f'PBI reports have been inserted into database with report_id: {report_id}')

def insert_static_to_db(reports):
    
    table_data = report_to_table(reports)

    # connect to database    
    conn = ms_sql_server.create_connection()
    
    # insert all 
    for table_name, df in table_data.items():
        print(f'INSERT INTO TABLE {table_name} ...')
        insert_dataframe(df, table_name, conn)    
    
    conn.close()

    print('PBI reports have been inserted into database')
    
def test():
    rpt_folder = Path(r'C:\Users\mgdin\dev\TRG_App\data\powerbi\Demo\Model_1')    
    reports = powerbi.read_results(rpt_folder)
    insert_report_to_db(reports)
    
########################################################################################


# Create a new report row in pbi_reports table
#    creator = 'Michael'
#    report_description = 'Test'
def create_report(creator, report_description):

    # Connect to the SQL Server
    conn = ms_sql_server.create_connection()
    cursor = conn.cursor()

    # Insert the new report
    insert_query = """
    INSERT INTO dbo.pbi_reports (creator, report_description)
    OUTPUT INSERTED.report_id
    VALUES (?, ?);
    """

    # Execute the insert query
    cursor.execute(insert_query, (creator, report_description))
    report_id = cursor.fetchval()
    
    # Commit the transaction
    conn.commit()

    # Close the connection
    cursor.close()
    conn.close()
        
    return report_id

    
    
    

def insert_dataframe(df, table_name, conn, schema='dbo'):
    df = convert_df_to_db_table(df)
    cursor = conn.cursor()

    columns = df.columns.tolist()
    placeholders = ', '.join(['?'] * len(columns))
    insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    
    for index, row in df.iterrows():
        cursor.execute(insert_query, tuple(row))
    
    conn.commit()
    cursor.close()

def report_to_table(reports, exclude_tables=[]):
    table_data = {}
    for book in reports:
        for tab in reports[book]:
            report_name = f'{book}.{tab}'
            table_name = table_map[report_name]
            if table_name not in exclude_tables:
                print(report_name)
                df = reports[book][tab]
                table_data[table_name] = df

    return table_data
    
    
def convert_df_to_db_table(df):
    #replace the nan in dataframe to None
    df.replace([np.nan, np.inf, -np.inf], None, inplace=True)
    
    # replace the space and line to underline
    new_columns = [col.replace(' ', '_').replace('-', '_').replace('$','').replace('(','').replace(')','').replace('/','_') for col in df.columns]
    df.columns = new_columns

    df = convert_dates(df)
    
    return df 

def convert_dates(df):
    for column in df.columns:
        if 'Date' in column or 'date' in column:
            df[column] = pd.to_datetime(df[column])
    return df
    
        
