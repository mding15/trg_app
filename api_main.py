# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 11:48:39 2024

@author: mgdin
"""
import argparse
import os

from api import app

#
# command:
#    
#     python api_main -debug
#
if __name__ == '__main__':
    
    is_prod = os.environ.get("APP_ENV", "").lower() == "production"
    port    = 8000 if is_prod else 5050
    debug   = not is_prod

    parser = argparse.ArgumentParser()
    parser.add_argument('-config', help='configuration file name')
    parser.add_argument('-debug', action='store_true', default=debug, help='debug model')
    parser.add_argument('-port', type=int, default=port, help='port number')
    parser.add_argument('-test', action='store_true', help='use calculate_test() function')

    args = parser.parse_args()
    
    app.run(debug=args.debug, port=args.port)
