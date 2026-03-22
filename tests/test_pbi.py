# -*- coding: utf-8 -*-
"""
Created on Wed Jun 12 16:38:34 2024

@author: mgdin
"""
import xlwings as xw
import pandas as pd
from pathlib import Path

from trg_config import config
from engine import VaR_engine as engine
from report import powerbi as pbi
from utils import xl_utils
from database import model_aux
from database import ms_sql_server, db_utils

TEST_DATA_DIR = config['SRC_DIR'] / 'test_data'
REPORT_DIR= Path(r'C:\Users\mgdin\dev\TRG_App\Reports')
output_folder=Path(r'C:\Users\mgdin\Downloads\PBI_Results')

    
def test():
    
    wb = xw.Book('Book1')
    
    positions, params, limit = get_portfolio_db(port_id=5323)
            
    # run engine
    DATA = engine.calc_VaR(positions, params)

    # write engine results
    engine.write_to_excel(wb, DATA)


    # pbi data
    results = pbi.generate_report(DATA)        

    # write pbi results
    excel_file = TEST_DATA_DIR / f'{test_portfolio.stem}.pbi.xlsx'
    print(excel_file)
    pbi.write_results_xl(results, excel_file)
    

    # add posVaR to the results
    wb = xw.Book(REPORT_DIR / 'PBI Replicates.xlsx')
    results['PosVaR'] = {'PosVaR': DATA['Positions']}
    write_results_xl(results, wb)
    
    # xl_utils.add_df_to_excel(user_prices, wb, 'user_prices', index=True)

# port_id = 5232
# username = 'sys@trg.com'

def test_insert_msss(port_id, username):
    
    user = model_aux.get_user(username)
    client = user.client
    pgroup = model_aux.get_port_group(user)
    
    file_path = config['CLIENT_DIR'] / f"{client.client_id}" / f"{pgroup.pgroup_id}" / f'{port_id}.pbi.xlsx'
    results = xl_utils.read_book_xl(file_path)

    # write to database    
    report_description = 'test report'
    pbi.insert_results_to_db(results, port_id, username, report_description)    

def test_insert_table(port_id, tablename):
    port_id = 5232
    tablename = 'dm_fact_d_Parameters'
    print(f"port_id: {port_id}, tablename: {tablename}")
    
    report_id = port_id
    port = model_aux.get_portfolio_by_id(port_id)
    user = model_aux.get_user_by_id(port.created_user_id)
    # client_id = user.client_id
    # pgroup_id = port.port_group_id    
    file_path = config['CLIENT_DIR'] / f"{user.client_id}" / f"{port.port_group_id}" / f'{port_id}.pbi.xlsx'
    results = xl_utils.read_book_xl(file_path)
    results = pbi.process_nonetype_dataframes(results)
    results_processed = pbi.process_dataframes_with_report_id(results, report_id)
    
    conn = ms_sql_server.create_connection()
    df = results_processed[tablename]
    table_name = tablename
    key_column = 'report_id'
    conn_p = conn

    df1 = df.iloc[5:7,:]
    df1 = df
    ms_sql_server.insert_df(tablename, df1, 'report_id', conn)
    conn.close()
    
    df = results[tablename]


def get_portfolio_db(port_id=5234):
    positions = db_utils.get_sql_df('select * from port_positions where port_id=%(port_id)s', {'port_id': port_id})
    df = db_utils.get_sql_df('select * from port_parameters where port_id=%(port_id)s', {'port_id': port_id})
    params = df.iloc[0].to_dict()
    limit = db_utils.get_sql_df('select * from port_limit where port_id=%(port_id)s', {'port_id': port_id})
    return positions, params, limit


def get_portfolio_file(port_id=5234):
    test_portfolio = TEST_DATA_DIR / 'Model-1.debug.xlsx'
    print(test_portfolio.exists())
    wb = xw.Book(test_portfolio)
    
    # get parameters and positions
    params = engine.read_params(wb)
    positions = engine.read_positions(wb)

    return positions, params
    
# write results to xl
from utils import xl_utils
from openpyxl import load_workbook
def write_results_xl(results, workbook):
    
    # consolidate results to one book
    book = consolidate_books(results)
    
    # rename Positions to DimPositions
    book = rename_tab(book)
    
    # workbook file name
    excel_file = TEST_DATA_DIR / 'Model-1.xlsx'
    
    # write book to excel_file    
    xl_utils.write_book_to_xl(book, excel_file)
    
# consolidate to one book
def consolidate_books(results):
    one_book = {}
    for name in results:
        book = results[name]
        for tab in book:
            if tab in one_book:
                raise Exception(f'found repeated tab name: {tab}')
            one_book[tab] = book[tab]
    return one_book
    
# rename Positions to DimPositions            
def rename_tab(book):
    if 'Positions' in book:
        book['DimPositions'] = book.pop('Positions')

    return book

        