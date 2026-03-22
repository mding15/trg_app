# -*- coding: utf-8 -*-
"""
Created on Sat Jan  6 16:22:19 2024

@author: mgdin
"""
import os
from pathlib import Path
import pandas as pd
import datetime
import json
from io import StringIO
import csv
from dateutil import parser

from utils import xl_utils as xl

# In[] datetime

def timestamp():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def today():
    return datetime.datetime.now().strftime('%Y-%m-%d')

def file_ts():
    date = datetime.datetime.now()
    # return date.strftime('%Y%m%d.%H%M%S.%f')
    return date.strftime('%Y%m%d.%H%M%S')

# Yield successive n-sized chunks from l. 
def divide_chunks(l, n): 
	
	# looping till length l 
	for i in range(0, len(l), n): 
		yield l[i:i + n] 

def test_divide_chunks():
    l = [1,2,3,4,5,6]
    for s in divide_chunks(l, 3):
        print(s)

# In[] File utils

# check if folder exists, if not, create the folder
def get_folder(folder):
    if not folder.exists():
        folder.mkdir(parents=True, exist_ok=True)
        print('making directory:', folder)
    return folder

# In[] 

def split_list(lst, chunk_size):
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
# Example usage:
# my_list = [1, 2, 3, 4, 5, 6, 7, 8, 9]
# chunk_size = 5
# chunks = split_list(my_list, chunk_size)
# print(chunks)


#
# Statistical tools
#
def calc_stats(df):
    df1 = df.ffill().pct_change(1)
    df2 = pd.DataFrame(index=df1.columns)
    df2['mean'] = df1.mean()
    df2['max'] = df1.max()
    df2['min'] = df1.min()
    df2['std'] = df1.std()
    return df2

#
# read position from a excel workbook
# remove ' from the front of CUSIP
#
def read_positions(wb, tab='Positions'):
    def to_str(x, col):
        if isinstance(x, float):
            x = int(x)
            if col == 'CUSIP':
                x = f'{x: 09}'
        
        return f'{x}'
    
    positions = xl.read_df_from_excel(wb, tab)
    
    # remove ' from the front of CUSIP, and convert float to string
    for col in ['ISIN', 'CUSIP', 'Ticker']:
        if col in positions:
            positions[col].replace('nan', None, inplace=True)
            positions[col] = positions[col].apply(lambda x: x[1:] if isinstance(x, str) and x[0]=="'" else x)
            positions[col] = positions[col].apply(lambda x: x if x is None or isinstance(x, str) else to_str(x, col))
    
    return positions

#
# write position to a workbook
# if CUSIP is in the postion columns, add "'" to the cusip values
#
def write_positions(positions, wb, tab='Positions'):
    def add_quote(x):
        if x is None:
            return x
        elif isinstance(x, str):
            if x[0] == "'": # if already starts with '
                return x
            else:
                return f"'{x}"
        else:
            return f"'{x}"
            
    
    if 'CUSIP' in positions:
        df = positions.copy()
        df['CUSIP'] = df['CUSIP'].apply(lambda x: add_quote(x))
        xl.add_df_to_excel(df, wb, tab, index=False)  
        
    elif 'cusip' in positions:
        df = positions.copy()
        df['cusip'] = df['cusip'].apply(lambda x: add_quote(x))
        xl.add_df_to_excel(df, wb, tab, index=False)  

    else:
        xl.add_df_to_excel(positions, wb, tab, index=False)  

#
# read a workbook which has a "Parameters" tab
# return a dict of the parameters {name : value}
#
def read_parameter(wb):
    
    df = xl.read_df_from_excel(wb, 'Parameters', index=True)    

    params = df.iloc[:,0] .to_dict()
    
    return params

def read_portfolio(wb):
    params = read_parameter(wb)
    positions = read_positions(wb, tab='Positions')
    return params, positions    
    
# In[]
def save_parameter_csv(params, filename):
    save_dict_to_csv(params, filename)
    print(f'save params to: {filename}')

def read_parameter_csv(filename):
    return load_dict_from_csv(filename)

def save_positions_csv(positions, filename):
    positions.to_csv(filename, index=False)
    print(f'save positions to: {filename}')
    
def read_positions_csv(filename):
    positions = pd.read_csv(filename)

    # convert date to date type
    # positions['LastPriceDate'] = pd.to_datetime(positions['LastPriceDate'])
    # positions['MaturityDate'] = pd.to_datetime(positions['MaturityDate'])
    
    return positions    

def load_portfolio(folder, port_id):

    params = read_parameter_csv(folder / f'{port_id}.params.csv')
    positions = read_positions_csv(folder / f'{port_id}.positions.csv')
    
    return params, positions

def load_test_portfolio():
    folder = Path(r'C:\DATA\trgapp_data\clients\1\3')
    port_id = 100
    return load_portfolio(folder, port_id)
#
# convert dict to dataframe
#
def dict_to_df(params, columns=['Name', 'Value']):
    df = pd.DataFrame([params])
    df = df.T
    df = df.reset_index()    
    df.columns = columns
    return df

# to: fronat, end
# e.g. df_move_columns(df, ['SecurityID'], to='front'), will move the SecurityID column to the front
def df_move_columns(df, cols, to='front'):

    columns = [col for col in df if col not in cols]
    if to == 'front':
        columns = cols + columns
    else:
        columns = columns + cols
    
    return df[columns]    

#
# subset of a dictionary
#
def sub_dict(my_dict, subset):
    return dict((k, my_dict[k]) for k in my_dict if k in subset)

def dict_ex_subset(my_dict, subset):
    return dict((k, my_dict[k]) for k in my_dict if k not in subset)


# update df[value] with series[value] where df[key] = series.index
# e.g. df_update(positions, 'SecurityID', 'LastPrice', df.set_index('SecurityID')['Price'])
def df_update(df, key, value, series):
    return df[key].map(series).combine_first(df[value])
    
# return a series that indexed by df.index, series has index of key
# e.g. df_series_merge(df, positions['LastPrice'], key='SecurityID')
def df_series_merge(df, series, key):
    if series.name is None:
        series.name = 'value'
    name = series.name
    
    index_name = 'index' if df.index.name is None else df.index.name
    
    return (df[[key]]
              .reset_index()
              .merge(series, left_on=key, right_index=True, how='left')
              .set_index(index_name)[name])

def add_log(DATA, module, msg):
    log = DATA['Log']
    log.loc[len(log)] = [module, msg]
    DATA['Log'] = log


# In[] save dictionary to csv file
def save_dict_to_csv(data_dict, csv_filename):
    with open(csv_filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Write the header
        writer.writerow(["key", "value", "type"])
        
        # For each key-value pair, detect its type and write row
        for k, v in data_dict.items():
            if isinstance(v, datetime.datetime):
                # Convert datetime to ISO string
                writer.writerow([k, v.isoformat(), "datetime"])
            elif isinstance(v, int):
                writer.writerow([k, str(v), "int"])
            elif isinstance(v, float):
                writer.writerow([k, str(v), "float"])
            else:
                # Everything else we treat as a string
                writer.writerow([k, str(v), "string"])
        
        

# re-construct dictionary from csv file
# csv_filename = filename
def load_dict_from_csv(csv_filename):
    data_dict = {}
    with open(csv_filename, mode="r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)  # Reads each row as a dict
        for row in reader:
            key = row["key"]
            value = row["value"]
            value_type = row["type"]

            if value_type == "datetime":
                # Convert from ISO format to datetime object
                data_dict[key] = datetime.datetime.fromisoformat(value)
            elif value_type == "int":
                data_dict[key] = int(value)
            elif value_type == "float":
                data_dict[key] = float(value)
            else:
                data_dict[key] = value  # Keep as string

    return data_dict


#######################################################
# json utils

# convert datetime to str,
# convert dataframe to csv 
def convert_to_json(data):
    if isinstance(data, dict):
        for name in data:
            value = data[name]
            if isinstance(value, datetime.datetime):
                data[name] = value.strftime('%Y-%m-%d')
            elif isinstance(value, pd.DataFrame):
                data[name] = value.to_csv(index=False)
            elif isinstance(value, dict):
                data[name]  = convert_to_json(value)
            elif isinstance(value, list):
                data[name]  = [convert_to_json(x) for x in value]
                
    return data

def convert_to_json_str(data):
    return json.dumps(convert_to_json(data), indent=4)



def extract_df(input_data, name):
    csv_data = input_data[name]
    df = pd.read_csv(StringIO(csv_data))  
    return df

###########################################################
# generate filename in downloads folder
# if exists, use name2.ext, if exists use name3.ext, etc
def gen_file_name_in_downloads(filename):
    # Get the Downloads folder path for the current user
    downloads_folder = Path.home() / 'Downloads'

    # Base file name and extension
    filename = Path(filename)
    base_name = filename.stem 
    extension = filename.suffix

    # Start with the base file name
    file_name = base_name + extension
    file_path = downloads_folder / file_name

    # Check for existing files and adjust the name
    counter = 1
    while file_path.exists():
        file_name = f"{base_name}{counter}{extension}"
        file_path = downloads_folder / file_name
        counter += 1

    return file_path

###########################################################
import pickle
def pickle_dump(data, filename):
    with open(filename, "wb") as f:  # 'wb' means write in binary mode
        pickle.dump(data, f)
def pickle_load(filename):
    with open(filename, "rb") as f:  # 'rb' means read in binary mode
        loaded_data = pickle.load(f)
    return loaded_data

def pickle_port(positions, params, unknown_positions, limit, filename='port_data.pkl'):
    data = {'Positions': positions, 'Parameters': params,  'unknown_positions': unknown_positions,'Limit': limit}
    pickle_dump(data, 'port_data.pkl')
    
def pickle_load_port(filename='port_data.pkl'):
    data = pickle_load(filename)
    positions = data['Positions']
    params = data['Parameters']
    limit = data['Limit']
    unknown_positions = data['unknown_positions']
    
    return positions, params, unknown_positions, limit   

    
    