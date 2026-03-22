# -*- coding: utf-8 -*-
"""
Created on Sat Jun 22 15:53:58 2024

@author: mgdin
"""

from trg_config import config
from database import ms_sql_server



if __name__ == "__main__":
    print('connecting to database...')
    
    conn = ms_sql_server.create_connection()
    conn.close()

    print('successful!')

