# -*- coding: utf-8 -*-
"""
Created on Wed Mar  5 11:10:39 2025

@author: mgdin
"""

import argparse
from api import app
from detl import yh_extract

if __name__ == "__main__":
    yh_extract.extract_eod()    
    