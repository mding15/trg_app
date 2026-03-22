from trg_config import config
config['DEBUG'] = True

from api import app
from api import portfolios
from detl import yh_extract
from mkt_data import mkt_data_info, mkt_data_extract
from tests import test_pbi
from database import sync_report_mapping
from utils import date_utils

def test():
    port_id = 5034
    username = 'test1@trg.com'
    
    rerun_portfolio(port_id, False)
    
    yh_pull(['SPY'])

def rerun_portfolio(port_id, insert_msss=True):
    with app.app_context():
        portfolios.rerun_portfolio(port_id, insert_msss)
        
def test_insert_msss(port_id, username):
    with app.app_context():
        test_pbi.test_insert_msss(port_id, username)

# pull one day prices from YH, save to db
def yh_eod():
    # skip weekend
    if not date_utils.is_weekday():
        print("it is weekend, skip")
        return
    
    with app.app_context():
        yh_extract.extract_eod()
    
# pull historical prices from YH, save to db and hdf, update mkt_data_info table
def yh_pull(tickers=[]):
    with app.app_context():
        mkt_data_extract.extract_yh_price(tickers)

# for all yh_tickers, extract hist_prices and insert yh_stock_price table
# def pull_yh_hist():
#     yh_extract.update_hist_price()

# copy yh_stock_price to hdf
def yh_db_2_hdf():
    with app.app_context():
        mkt_data_extract.yh_db_2_hdf()

# update table mkt_data_info from hdf
def update_mkt_data_info():
    with app.app_context():
        mkt_data_info.update_stat_by_sec_id()
        
# for all securities in current_security table, update their mkt_data_info
def update_curr_sec_mkt_data_info():
    with app.app_context():
        mkt_data_info.update_curr_sec()
    
# copy table report_mapping from Postgres to SQL server
def sync_report_mapping_whole():
    sync_report_mapping.sync_whole()

# copy table report_mapping from Postgres to SQL server
# only copy the newly changed report for efficiency
def sync_report_mapping_delta():
    sync_report_mapping.sync_delta()

