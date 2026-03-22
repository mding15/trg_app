# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 21:38:37 2025

@author: mgdin
"""
from datetime import datetime
import argparse
from detl import yf_extract2


def main(run_date):
    try:
        # Convert run_date to a datetime object to validate the format
        run_date = datetime.strptime(run_date, "%Y-%m-%d")
        print(f"run_date: {run_date.date()}")
        
        # run download_yf
        yf_extract2.download_yf(run_date)
        
    except ValueError:
        print("Error: run_date must be in YYYY-MM-DD format.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run script with a specific date.")
    parser.add_argument("--run_date", 
                        default=datetime.today().strftime("%Y-%m-%d"),
                        required=False, help="Date in YYYY-MM-DD format.")
    
    args = parser.parse_args()
    main(args.run_date)
