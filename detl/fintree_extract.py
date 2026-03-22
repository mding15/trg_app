# -*- coding: utf-8 -*-
"""
Created on Fri Jun 28 09:50:15 2024

@author: mgdin
"""
import io
import pandas as pd

from pathlib import Path
from trg_config import config

DATA_DIR = Path(r'C:\Users\mgdin\OneDrive - tailriskglobal.com\TRG\Clients\Fintree\DataFromFT')
FT_DIR = config['DATA_DIR'] / 'Fintree'

def extract_timeseries():
    date = '20240718'
    filename = 'Benchmark Price Series.csv'
    
    file_path = DATA_DIR / date / filename
    if not file_path.exists():
        print('File does not exist:', file_path)
        
    out_file_path = FT_DIR / f'{date}.{filename}'
    # Open the file in read mode
    with open(file_path, 'r') as infile, open(out_file_path, 'w') as outfile:
        # Read each line in the file
        for line in infile:
            line = line.replace(',', '.').replace(';',',')
            outfile.write(line)
            
            
            
    