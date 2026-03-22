# -*- coding: utf-8 -*-
"""
Created on Wed Feb 14 15:46:32 2024

@author: mgdin
"""
import os
from pathlib import Path


if 'MODEL_WORKBOOK_DIR' in os.environ:
    MODEL_WORKBOOK_DIR = Path(os.environ['MODEL_WORKBOOK_DIR'])
else:
    MODEL_WORKBOOK_DIR = Path('')
    
