# -*- coding: utf-8 -*-
"""
Bloomberg data process

Created on Mon Feb 26 14:53:35 2024

@author: mgding
"""
import pandas as pd
import json
from pathlib import Path

from trg_config import config
from detl import bloomberg as bb
from utils import date_utils as ud
from security import security_info
from mkt_data   import mkt_timeseries, mkt_data_info

from api import app
app.app_context().push()

'''
Parameters used in HistData process
params = {
    'start_date': start_date
    'sec_file': sec_file_path
 }

'''
def get_HistData_params():
    
    filename = config['CNFG_DIR'] / 'bloomberg.json'
    with open(filename) as f:
        bb_config = json.load(f)

    # hist data config    
    hist_config = bb_config['HistData']
    
    # start_date
    start_date = pd.to_datetime(hist_config['start_date'])
    
    # security file
    sec_file_path = config['BB_DIR'] / hist_config['security_file']
        
    if not sec_file_path.exists():
        sec_file_path.parent.mkdir(parents=True, exist_ok=True)

        df = pd.DataFrame(columns=['DateAdded','BB_GLOBAL','SecurityID','SecurityName'])
        df.to_csv(sec_file_path, index=False)
        
    return start_date, sec_file_path

bb_start_date, sec_file_path = get_HistData_params()
last_cob_file = config['BB_DIR'] / 'last_cob.csv'

# folder = 'HistData/yyyy/mm'
def get_daily_folder(date):
    year, month = date.year, date.month
    out_dir = config['BB_DIR'] / 'HistData' / f'{year:04}' / f'{month:02}'
    if not out_dir.exists():
        print('making dir:', out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
    
    return out_dir


def get_security_data_filepath():
    folder = config['BB_DIR'] / 'SecurityData' 
    folder.mkdir(parents=True, exist_ok=True)
    ts = ud.filename_timestamp()
    filename = folder / f'SecurityData.{ts}.csv'
    return filename
    

# write data to file
# data - string
# file_path - file name with full path
def write_data(file_path, data):
    print(f'write to file: {file_path}')
    with open(file_path, 'w', newline='') as file:
        file.write(data)

# sec_id_type: TICKER, ISIN, CUSIP, BB_GLOBAL
def download_security(sec_id_list, sec_id_type='TICKER'):
    bb_api = bb.DL_Request() 
    
    # if sec_id_type == 'TICKER':
    #     sec_id_list = bb.ticker_list(sec_id_list)
    
    data = bb_api.DataRequest('SecurityData', sec_id_list, sec_id_type)
    
    filename = get_security_data_filepath()
    write_data(filename, data)
    return filename


# daily download function
# download prices for the securities in sec_list file
# sec_list_file = data/BB/HistData/sec_list.csv
# cob = close of bus date
# output two files: 
#    OneDay.yyyymmdd.csv -- one day prices for existing securities
#    History.yyyymmdd.csv -- history prices for new securities
#
def download_price(cob):
    #cob = ud.get_cob()
    
    print('***** download data for COB: ', cob, ' ******')
    pcob = ud.previous_bus_date(cob)
    cob_str = cob.strftime("%Y%m%d")
    
    # read security list
    df = pd.read_csv(sec_file_path)
    df['DateAdded'] = pd.to_datetime(df['DateAdded'])
    df = df[df['DateAdded'] <= cob] # only use securities that are added up to cob
    
    # find new security list
    sec_id_type = df.columns[1]
    sec_list = df[df['DateAdded'] <= pcob][sec_id_type].to_list()
    new_sec_list = df[df['DateAdded'] > pcob][sec_id_type].to_list()
    
    bb_api = bb.DL_Request()
    out_dir = get_daily_folder(cob)
    out_files = []
    
    # pull data from BB for the existing securities
    if len(sec_list) > 0:
        last_cob = last_download_cob()
        data, identifier = bb_api.HistoryRequest('OneDay', sec_list, sec_id_type, last_cob, cob)
        file_path = out_dir / f'{cob_str}.{identifier}.csv'
        write_data(file_path, data)
        out_files.append(file_path)
    
    # pull data from BB for the new securities
    if len(new_sec_list) > 0:
        start_date = bb_start_date
        data, identifier = bb_api.HistoryRequest('History', new_sec_list, sec_id_type, start_date, cob)
        file_path = out_dir / f'{cob_str}.{identifier}.csv'
        write_data(file_path, data)
        out_files.append(file_path)

    return out_files

# file_path = Path(r'C:\Users\mgdin\OneDrive\Documents\dev\TRG_App\dev\data\BB\HistData\2024\06\20240610.History797015.csv')
# out_files = [file_path] 

def add_mkt_data(out_files):
    for file_path in out_files:
        # file_path = out_files[0]
    
        # tag History indicate it is for new securities
        is_new = ( file_path.name[9:16] == 'History')
        
        # read prices
        df = pd.read_csv(file_path)
        prices = pd.pivot(df, columns='IDENTIFIER', index='DATE', values='PX_LAST')
        bb_ids = prices.columns.tolist()
        
        # convert BB_GLOBAL to SecurityID
        sec_id_map = get_sec_id_map(bb_ids)
        prices.columns = [sec_id_map[x] for x in bb_ids]
        
        # save to HDF
        if is_new:
            mkt_timeseries.save_new(prices, 'BB', 'PRICE')
        else:
            mkt_timeseries.update_existing(prices, 'BB', 'PRICE')

def get_sec_id_map(bb_ids):

    df = security_info.get_xref_by_ref_ids('BB_GLOBAL', bb_ids)
    missing = set(bb_ids).difference(df['BB_GLOBAL'])
    if len(missing) > 0:
        missing_str = ', '.join(list(missing))
        raise Exception(f'missing BB_GLOBAL IDs in SecurityInfo table: {missing_str}')
    sec_id_map = df.set_index('BB_GLOBAL')['SecurityID'].to_dict()
    return sec_id_map    


# get hist prices from bb 
def get_histPrice(sec_id_list, sec_id_type, start_date, end_date):
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    bb_api = bb.DL_Request()
    data, identifier = bb_api.HistoryRequest('History', sec_id_list, sec_id_type, start_date, end_date)
    
    out_dir = get_daily_folder(end_date)
    date_str = end_date.strftime("%Y%m%d")
    file_path = out_dir / f'{date_str}.{identifier}.csv'
    write_data(file_path, data)
    
    return data

# save last cob to a file
def update_last_cob(cob):
    cob_str = cob.strftime('%Y-%m-%d')
    df = pd.DataFrame({'COB': [cob_str]})
    df.to_csv(last_cob_file, index=False)

def last_download_cob():

    if last_cob_file.exists() == False:
        last_date = bb_start_date
    else:
        df = pd.read_csv(last_cob_file)
        last_date = df['COB'].max()
    
    return pd.to_datetime(last_date)
####################################################################################################
def gen_bb_sec_list_file():
    
    # select all YF_ID xrefs
    xrefs = security_info.xref_by_ref_ids('YF_ID')
    yf_sec_ids = [x.SecurityID for x in xrefs]

    # select all BB_ID xrefs
    xrefs = security_info.xref_by_ref_ids('BB_GLOBAL')
    bb_sec_ids = [x.SecurityID for x in xrefs]

    # remove yf_ids
    sec_ids = set(bb_sec_ids).difference(yf_sec_ids)

    # select securities
    securities = security_info.get_securities_with_xref(sec_ids, ref_types=['BB_GLOBAL'])
    
    # select 3 columns
    df = securities[['BB_GLOBAL', 'SecurityID', 'SecurityName']].copy()
    df['DateAdded'] = ud.today()

    # concat to current sec_file
    #params = get_HistData_params()
    sec_file = sec_file_path
    curr_df = pd.read_csv(sec_file)
    df = pd.concat([curr_df, df], ignore_index=True).drop_duplicates(subset=['BB_GLOBAL'], keep = 'first')
    
    df.to_csv(sec_file, index=False)


    
# download data for all cob dates between start and end date
def download_for_date_ranges(start_date, end_date):
    #start_date, end_date = '2/2/2024', '2/5/2024'
    dates = ud.get_bus_dates(start_date, end_date)
    for cob in dates:
        download_price(cob)
    
# download price data
def download_cob():
    cob = ud.get_cob()
    
    # generate sec_id list to download
    gen_bb_sec_list_file()
    
    # download from source
    out_files = download_price(cob)

    # update mkt_data
    add_mkt_data(out_files)
    
    # save the cob to a file
    update_last_cob(cob)


####################################################################################################
import xlwings as xw
from utils import xl_utils

def test():
    #start_date, end_date = '2/26/2024', '2/27/2024'
    #download_for_date_ranges(start_date, end_date)
    
    #cob = pd.to_datetime('2/28/2024')
    #download_price(cob)
    
    download_cob()
    
    download_security(['TSLA US Equity', 'AMZN US Equity'])
    
    
    bb_ids = ['BBG000BB6WG8', 'BBG000BB9KF2', 'BBG000BBHXQ3']
    filename = download_security(bb_ids, sec_id_type='BB_GLOBAL')
    
    wb = xw.Book('Book2')    
    df = pd.read_csv(filename)    
    xl_utils.add_df_to_excel(df, wb, 'BB_Sec')    

####################################################
if __name__ == '__main__':
    download_cob()