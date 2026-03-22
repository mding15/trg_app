# -*- coding: utf-8 -*-
"""
Created on Sun Mar 17 16:25:52 2024

@author: mgding

ROUTES:
    
data_request
    MarketData
        GetHistory
        GetSecurityList
    GetSecurity
    GetSecurityAll

calculate
    CalculateVaR

add_security
    AddSecurities

"""

import json
import traceback
import pandas as pd
import datetime


from trg_config import config
from api import data_pack
from api import scrubbing_portfolio as scrub
from api import run_calculation as run_calc
from api import create_account
from api import portfolios
from security import security_info
from engine import VaR_engine
from report import powerbi as pbi
from utils import mkt_data, tools, var_utils
from database import pbi_db_insert as pbi_db
from database import pg_create_connection
from database import db
from database import model_aux as db_aux
from database import sync_report_mapping
from database import models
from mkt_data import mkt_timeseries


from api import api_status_codes as api_code

# logger
from api.logging_config import get_logger
logger = get_logger(__name__)


# main function, dispatch requests to various functions
def get_response(route, username, input_data):

    logger.info('===== New Request =====')
    logger.info(f'route: {route}')
    logger.info(f'username: {username}')

    if input_data:    
        logger.info(json.dumps(input_data, indent=4))
    
    # Request Type / SubType
    request_category = input_data.get("Request")

    try:
        if route == 'get_dashboard':
            response, status = get_dashboard(username, input_data)
    
        elif route == 'upload_portfolio':
            response, status = upload_portfolio(username, input_data)
    
        elif route == 'delete_portfolios':
            response, status = delete_portfolios(username, input_data)
            
        elif route == 'run_calculation':
            response, status = run_calculation(username, input_data)

        elif route == 'get_user_approval_data':
            response, status = get_user_approval_data(username, input_data)

        elif route == 'update_user_approval':
            response, status = update_user_approval(username, input_data)
        
        elif route == 'get_entitlement':
            response, status = get_entitlement_data(username, input_data)
            
        elif route == 'update_entitlement':
            response, status = update_entitlement_data(username, input_data)

        elif route == 'get_entitlement_1client':
            response, status = get_entitlement_1client_data(username, input_data)

        elif route == 'get_upload_security':
            response, status = get_upload_security(username, input_data)


        ############################################################################
        
        elif route == 'data_request':
            if request_category == "MarketData":
                response, status = MarketData(username, input_data)

            elif request_category == "Distribution":
                response, status = GetDistribution(username, input_data)
                
            elif request_category in ["GetSecurity", "GetSecurityAll"]:
                response, status = GetSecurities(username, input_data)

            else:
                response, status = Request_Unknown(username, input_data)

        elif route == 'calculate':
            if request_category == "CalculateVaR":
                response, status = CalculateVaR(username, input_data)
            else:
                response, status = Request_Unknown(username, input_data)
            
        elif route == 'add_security':
            if request_category == "AddSecurities":
                response, status = AddSecurities(username, input_data)
                    
            else:
                response, status = Request_Unknown(username, input_data)

        elif route == 'test':
            response, status = api_test(username, input_data)

        else:
            response, status = Unknown_Route_Error(route, username, input_data)
            
    except HandlerException as e:
        print(e)
        response, status = e.response()

    
    # log
    logger.info('===== Response =====')
    logger.info(f'Status: {status}')
    
    if route not in ['data_request', 'get_dashboard']:
        logger.info(json.dumps(response, indent=4))
    
    return response, status

################################################################################################
# request market data
#   request_types = ["GetHistory", "GetSecurityList"]
def MarketData(username, input_data):

    # Extracting necessary information from input data
    request_category = input_data.get("Request")
    request_type     = input_data.get("Type")
    data_category  = input_data.get("Data Category")
    from_date = input_data.get("From Date")
    to_date = input_data.get("To Date")
    sec_list = input_data.get("SecurityID")

    # validate input data here
    # to be done
    
    if request_type == "GetHistory":
        # get history data 
        data = mkt_timeseries.get(sec_list, from_date, to_date, data_category)
        data_str = data.to_csv()
    elif request_type == "GetSecurityList":
        data = mkt_timeseries.get_mkt_data_sec_list()
        data_str = data.to_csv(index=False)
    else:
        data_str = 'Unknown request type'
    
    # pack response
    response = {
        "Request": request_category,
        "Type":    request_type,
        'Data Category': data_category,
        "DATA": data_str
    }

    return response, 200



################################################################################################
# request market data
#   request_types = Distribution
# 
# input_data = {
#     "Request" : "Distribution",
#     "Category": "PRICE",
#     "SecurityID": ['T001', 'T002']
#     }

def GetDistribution(username, input_data):

    # Extracting necessary information from input data
    request_category = input_data.get("Request")
    category  = input_data.get("Category")
    sec_list = input_data.get("SecurityID")

    sec_list_str = ",".join(sec_list)
    print(f'Request Distribution: sec_list=[{sec_list_str}]')
    
    # validate input data here
    # to be done
    
    data = var_utils.get_dist(sec_list, category=category)
    data_str = data.to_csv(index=False)
    
    # pack response
    response = {
        "Request": request_category,
        'Category': category,
        "DATA": data_str
    }

    return response, 200


################################################################################################
class HandlerException(Exception):
    def __init__(self, message='Error', code=api_code.ERROR_REQUEST_FAILED):
        self.message = message
        self.code = code
        super().__init__(self.message)
        
    def __repr__(self):
        return f"Status: {self.code}, Message: {self.message}"

    def response(self):
        return {'Error': self.message}, self.code

def get_dashboard(username, input_data):
    
    user = models.User.query.filter_by(username=username).first()
    if user.role == 'superadmin': # show all portfolio
        sql = """select pi2.port_id, pg.pgroup_id as group_id,
        concat(pi2.port_id,' - ', pi2.port_name) as portfolio_name, 
        to_char(pi2.as_of_date, 'YYYY-MM-DD') as as_of_date,
        to_char(pi2.market_value, '$999,999,999.00') as market_value, 
        pi2.tail_measure, pi2.risk_horizon, pi2.benchmark,
        to_char(pi2.create_date, 'YYYY-MM-DD') as create_date, 
        pi2.created_by, pi2.status, pi2.message, pi2.filename, pr.report_url 
        from portfolio_info pi2
        left join portfolio_group pg on pi2.port_group_id = pg.pgroup_id 
        left join pbi_report_url pr on pr.client_id = pg.client_id and pr.is_active = 1
        order by pi2.port_id desc 
        """    
    elif user.role == "admin":
        sql = """
        SELECT pi2.port_id,
               pg.pgroup_id AS group_id,
               CONCAT(pi2.port_id, ' - ', pi2.port_name) AS portfolio_name,
               TO_CHAR(pi2.as_of_date,  'YYYY-MM-DD')     AS as_of_date,
               TO_CHAR(pi2.market_value, '$999,999,999.00') AS market_value,
               pi2.tail_measure,
               pi2.risk_horizon,
               pi2.benchmark,
               TO_CHAR(pi2.create_date, 'YYYY-MM-DD')     AS create_date,
               pi2.created_by,
               pi2.status,
               pi2.message,
               pi2.filename,
               pr.report_url
          FROM "user" u
          LEFT JOIN portfolio_group pg ON u.client_id = pg.client_id
          JOIN portfolio_info pi2 ON pg.pgroup_id = pi2.port_group_id
          LEFT JOIN pbi_report_url  pr ON u.client_id = pr.client_id
                                      AND pr.is_active = 1
        
        where username = %s
        union all 
        select pi2.port_id, pi2.port_group_id as group_id, CONCAT(pi2.port_id, ' - ', pi2.port_name) AS portfolio_name, TO_CHAR(pi2.as_of_date,  'YYYY-MM-DD') AS as_of_date,
        TO_CHAR(pi2.market_value, '$999,999,999.00') AS market_value,
        pi2.tail_measure,
        pi2.risk_horizon,
        pi2.benchmark,
        TO_CHAR(pi2.create_date, 'YYYY-MM-DD')     AS create_date,
        'Demo' as created_by,
        pi2.status,
        pi2.message,
        pi2.filename,
        pr.report_url
        from portfolio_info pi2 ,
        pbi_current_report_url pr
        where pi2.port_id = '100' and pr.is_active = 1
        ORDER BY port_id desc
        
        
        """
    else:
        sql = """
        SELECT pi2.port_id,
               pg.pgroup_id AS group_id,
               CONCAT(pi2.port_id, ' - ', pi2.port_name) AS portfolio_name,
               TO_CHAR(pi2.as_of_date,  'YYYY-MM-DD')     AS as_of_date,
               TO_CHAR(pi2.market_value, '$999,999,999.00') AS market_value,
               pi2.tail_measure,
               pi2.risk_horizon,
               pi2.benchmark,
               TO_CHAR(pi2.create_date, 'YYYY-MM-DD')     AS create_date,
               pi2.created_by,
               pi2.status,
               pi2.message,
               pi2.filename,
               pr.report_url
          FROM "user" u
          JOIN user_entitilement ue ON ue.user_id = u.user_id
                                   AND ue.permission = 'view'
          LEFT JOIN portfolio_group pg ON pg.pgroup_id = ue.port_group_id
          JOIN portfolio_info pi2 ON pg.pgroup_id = pi2.port_group_id
          LEFT JOIN pbi_report_url  pr ON pr.client_id = pg.client_id
                                      AND pr.is_active = 1
        where username = %s
        union all 
        select pi2.port_id, pi2.port_group_id as group_id, CONCAT(pi2.port_id, ' - ', pi2.port_name) AS portfolio_name, TO_CHAR(pi2.as_of_date,  'YYYY-MM-DD') AS as_of_date,
        TO_CHAR(pi2.market_value, '$999,999,999.00') AS market_value,
        pi2.tail_measure,
        pi2.risk_horizon,
        pi2.benchmark,
        TO_CHAR(pi2.create_date, 'YYYY-MM-DD')     AS create_date,
        'Demo' as created_by,
        pi2.status,
        pi2.message,
        pi2.filename,
        pr.report_url
        from portfolio_info pi2 ,
        pbi_current_report_url pr
        where pi2.port_id = '100' and pr.is_active = 1
        ORDER BY port_id desc
        """
        
    pg_conn = pg_create_connection()
    
    response = {
        "TableData": []
        }
    
    
    with pg_conn.cursor() as cursor:
        cursor.execute(sql, (username,))
        result = cursor.fetchall()
        
        columns = [x.name for x in cursor.description]
        for row in result:
            response["TableData"].append(
                dict(zip(columns,row))
                )

    return response, 200


def get_upload_security(username, input_data):
    user = models.User.query.filter_by(username=username).first()
    
    # check permission
    

    sql = "select * from upload_security order by create_date desc"
        
    pg_conn = pg_create_connection()
    
    response = {
        "TableData": []
        }
    
    
    with pg_conn.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
        
        columns = [x.name for x in cursor.description]
        for row in result:
            response["TableData"].append(
                dict(zip(columns,row))
                )

    # convert datetime to str
    response = tools.convert_to_json(response)
    
    return response, 200

def upload_portfolio(username, input_data):
    return {'message': 'upload portfolio successed'}, 200   

    if "PORT_ID" not in input_data:
        raise HandlerException('missing PORT_ID in input data', api_code.ERROR_INPUT_DATA)
    
    try:
        port_id = input_data['PORT_ID']
        scrub.scrubbing_portfolio(port_id)

        return {'message': f'scrubbing portfolio {port_id} finished successfully'}, 200   
    
    except Exception as e:
        raise HandlerException(str(e), api_code.ERROR_INPUT_DATA)
        
    
def run_calculation(username, input_data):
    if "PORT_ID" not in input_data:
        raise HandlerException('missing PORT_ID in input data', api_code.ERROR_INPUT_DATA)
    
    try:
        port_id = input_data['PORT_ID']
        run_calc.run_calculation(port_id, username)
        
        return {'message': f'calculation for portfolio {port_id} finished successfully'}, 200   
        
    except Exception as e:
        raise HandlerException(str(e), api_code.ERROR_CALCULATION_FAILED)

#
# input_data = {
#   'PORT_ID_LIST': [1,2,3,4,7, 8, 9]
# }
# delete_portfolios('test', input_data)
#

def delete_portfolios(username, input_data):
    if "PORT_ID_LIST" not in input_data:
        raise HandlerException('missing PORT_ID_LIST in input data', api_code.ERROR_INPUT_DATA)
        
    port_id_list = input_data['PORT_ID_LIST']

    # delete portfolio from db
    portfolios.delete_portfolios(port_id_list)
    
    port_id_str = ', '.join([f'id: {x}' for x in port_id_list])
    
    return {'message': f'portfolios {port_id_str} have been deleted'}, 200
    
def check_input_data(input_data):
    for col in ['Request', 'ClientID', 'PortfolioID', 'Positions', 'Parameters']:
        if col not in input_data:
            raise HandlerException(f"Can not find {col} in input data", api_code.ERROR_INPUT_DATA)


def get_user_dir(username):
    user_dir = config['CLIENT_DIR'] / username
    if not user_dir.exists():
        user_dir.mkdir(parents=True, exist_ok=True)
    return user_dir
        
def get_portfolio_dir(username, portfolio_id):
    user_dir = get_user_dir(username)
    portfolio_dir =  user_dir / portfolio_id
    if not portfolio_dir.exists():
        portfolio_dir.mkdir(parents=True, exist_ok=True)
    return portfolio_dir



def save_input_data(portfolio_dir, input_data, ts):

    filename =  portfolio_dir / f'input_data.{ts}.json'
    with open(filename, 'w') as f:
        json.dump(input_data, f)

def save_response_data(portfolio_dir, response, code, ts):
    
    response_data = {'Status': code,
            'Response': response}
    filename =  portfolio_dir / f'response_data.{ts}.json'
    with open(filename, 'w') as f:
        json.dump(response_data, f)
    
def CalculateVaR(username, input_data):
    check_input_data(input_data)
    
    client_id = input_data['ClientID']
    portfolio_id = input_data['PortfolioID']

    # save input_data
    portfolio_dir = get_portfolio_dir(username, portfolio_id)    
    ts = tools.file_ts()
    save_input_data(portfolio_dir, input_data, ts)
    
    # get parameters
    params = input_data['Parameters']
    positions = data_pack.extract_df(input_data, 'Positions')

    # determin save to database options    
    if 'SkipDatabase' in params:
        skip_db = (params['SkipDatabase'].upper() == 'YES')
    else:
        skip_db = False
    
    # response
    response = {}
    response['ClientID'] = client_id
    response['PortfolioID'] = portfolio_id

    try:
        DATA = VaR_engine.calc_VaR(positions, params)
        if 'Error' in DATA:
            raise Exception(DATA['Error'])
        results = pbi.generate_report(DATA)
        response['PBI_DATA'] = data_pack.pack_pbi(results)
        
        if skip_db == False:
            report_description = f'Client: {client_id}, Portfolio: {portfolio_id}'
            pbi.insert_results_to_db(results, username, report_description)
        
        code = 200
        
    except Exception as e:
        response['Error'] = str(e)
        print('VaR Engine Error:', str(e))
        traceback.print_exc()
        code = api_code.ERROR_CALCULATION_FAILED
    
    save_response_data(portfolio_dir, response, code, ts)
    return response, code

# add new securities to database
def AddSecurities(username, input_data):

    # response
    response = {}
    response['ClientID']  = input_data['ClientID']
    response['Request']   = "AddSecurity"

    new_securities = data_pack.extract_df(input_data, 'NewSecurity')        
    try:
        security_info.create_security_and_xref(new_securities)
        response['NewSecurity'] = new_securities.to_csv(index=False)
        code = 200
    except Exception as e:
        response['Error'] = str(e)
        code = api_code.ERROR_ADD_SECURITY

    return response, code

# add new securities to database
def GetSecurities(username, input_data):
    request_category = input_data.get("Request")
    
    # response
    response = {}
    response['ClientID']  = input_data['ClientID']
    response['Request']   = request_category

    if request_category == 'GetSecurityAll':
        securities = security_info.get_security_by_sec_id_list([])
    else:
        # sec_id_list = [('ISIN', 'LU0119620176'), ('CUSIP','74340XBN0'),('Ticker','GOOG'), ('TRG_ID', 'T10000425')]
        sec_id_list = input_data['SecurityIdList']
        securities = security_info.get_security_by_sec_id_list(sec_id_list)

    response['Security'] = securities.to_csv(index=False)
    code = 200

    return response, code

def Request_Unknown(username, input_data):

    # Extracting necessary information from input data
    client_id = input_data.get("Client ID")
    request_category = input_data.get("Request")

    # pack response
    response = {
        "Client ID": client_id,
        "Request": request_category,
        "Error:": f'Unknown request: {request_category}'
    }

    return response, api_code.ERROR_REQUEST_UNKNOWN


def Unknown_Route_Error(route, username, input_data):
    
    print(f'Panic: fatal Error, unknown route: {route}')

    # pack response
    response = {
        "Status": 'failed',
        "Error:": f'Server encontered error in handling {route}'
    }

    return response, api_code.ERROR_REQUEST_UNKNOWN

###########################################################################
#update_entitlement
def check_permission(username, route='user_approval', group_id=None):
    # print(f'check_permission: {username}, {route}, {group_id}')
    
    user = models.User.query.filter_by(username=username).first()
    
    if route == 'user_approval':
        return user.role in ['superadmin', 'support']
    
    if route == 'sup_portfolios':
        return user.role in ['superadmin', 'support']
    
    if route == 'sup_port_group':
        return user.role in ['superadmin', 'support']
    
    if route == 'sup_port_group_status':
        return user.role in ['superadmin', 'support']
    
    if route == 'portfolios':
        if user.role in ['superadmin', 'support', 'admin']:
            return True
        
        if group_id is None:
            return True
        
        entitlements = db_aux.get_entitlements(user.user_id, group_id)
        if entitlements:
            return True
            
    if route == 'folder_summary':
        if user.role in ['superadmin', 'support', 'admin']:
            return True
        
        entitlements = db_aux.get_entitlements(user.user_id, group_id)
        if 'view' in entitlements:
            return True

    if route == 'upload_portfolio':
        if user.role in ['superadmin', 'support', 'admin']:
            return True
        
        entitlements = db_aux.get_entitlements(user.user_id, group_id)
        if 'upload' in entitlements:
            return True

    if route == 'download':
        if user.role in ['superadmin', 'support', 'admin']:
            return True
        
        if group_id is None:
            return False
        
        entitlements = db_aux.get_entitlements(user.user_id, group_id)
        if 'download' in entitlements:
            return True

    if route == 'run_portfolio':
        if user.role in ['superadmin', 'support', 'admin']:
            return True
        
        entitlements = db_aux.get_entitlements(user.user_id, group_id)
        if 'execute' in entitlements:
            return True
    
    if route == 'update_entitlement':
        if user.role in ['superadmin', 'support', 'admin']:
            return True
    
    if route == 'create_group':
        if user.role in ['superadmin', 'support', 'admin']:
            return True
            
        
    return False

# get permission

# def get_users():
#     users = models.User.query.all()

#     # put pending users in the front
#     pending_users = []
#     approved_users = []
#     for user in users: # convert user to dictionary
#         if user.approval == 0:
#             pending_users.append(user.__dict__)
#         else:
#             approved_users.append(user.__dict__)
            
#     users = pending_users + approved_users
    
#     return users


def update_user(user_id, approval_id, role):
    print(f'updating user ... user_id: {user_id}, approval: {approval_id}, role: {role}')
    user = models.User.query.get(user_id)
    if user:
        user.approval = approval_id  
        user.role = role  
        db.session.commit()
        
        # sync sql server database
        sync_report_mapping.sync_delta_thread()

    else:
        print(f'[Warning] update_user - user not found: {user_id}')
        
    return user
    
def get_user_approval_data(username, input_data=None):
    print('get_user_approval_data ...')
    # check permission
    if not check_permission(username, 'user_approval'):
        return {"error": "Permission denied"}, 403


    sql = """
    select  u.user_id, concat(u.firstname, ' ', u.lastname) as username, u.email, a.status as approval, u.role, u.client_id, u.create_date  
    from "user" u
    left join approval a on a.id = u.approval 
    order by u.create_date desc 
    """
    
    data = []
    pg_conn = pg_create_connection()
    with pg_conn.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
        
        columns = [x.name for x in cursor.description]
        for row in result:
            data.append(
                dict(zip(columns,row))
                )

    # convert datetime to string in ISO format
    response = []
    for x in data:
        response.append(
            {
                key: (value.isoformat() if isinstance(value, (datetime.date, datetime.datetime)) else value)
                for key, value in x.items()
            }
        )
    
    return response, 200
    
    
    # users = get_users()  
    # approvals = models.Approval.query.all()
    # roles = models.Roles.query.all()

    # #
    # response = {
    #     "users": [
    #         {
    #             "user_id": user['user_id'],
    #             "username": user['username'],
    #             "approval_status": models.Approval.query.get(user['approval']).status if user['approval'] else None,
    #             "current_role": user['role']
    #         }
    #         for user in users
    #     ],
    #     "approval_choices": [{"id": approval.id, "status": approval.status} for approval in approvals],
    #     "role_choices": [{"id": role.id, "role": role.role} for role in roles],
    # }

    # return response, 200

def get_approval_ids():
    approvals = models.Approval.query.all()
    return {x.status : x.id for x in approvals}
    
    
# update permission
def update_user_approval(username, input_data):
    # check current permission
    if not check_permission(username, 'user_approval'):
        return {"error": "Permission denied"}, 403

    if not input_data or "updates" not in input_data:
        return {"error": "Invalid request data"}, 400

    approval_ids = get_approval_ids()
    updates = input_data["updates"]  # [{"user_id": 1, "approval_id": "pending", "role": "Admin"}, ...]
    
    try: 
        for update in updates:
            user_id = update.get("user_id")
            new_approval = update.get("approval")
            new_role = update.get("role")
            new_approval_id = approval_ids.get(new_approval)
    
            if not user_id or new_approval_id is None or not new_role:
                print(f'invalid data: user_id: {user_id}, approval: {new_approval}, role: {new_role}')
                continue  # skip useless data

            if new_approval_id==1:
                create_account.approve_user(user_id)

            update_user(user_id, new_approval_id, new_role)
    
        return {"message": "Users updated successfully"}, 200

    except Exception as e:
        db.session.rollback()
        return {"error": f"Failed to update users: {str(e)}"}, 500
        

#update entitlement
# username='mding@trg.com'
# input_data={}

def get_entitlement_data(username, input_data):
    
    current_user = models.User.query.filter_by(username=username).first()

    if current_user.role == 'superadmin':
        clients = models.Client.query.all()
        portfolio_groups = []
        users = []
        entitlements = []
    elif current_user.role == 'admin':
        clients = models.Client.query.filter_by(client_id=current_user.client_id).all()
        portfolio_groups = models.Portfolio_Group.query.filter_by(client_id=current_user.client_id).all()
        users = models.User.query.filter_by(client_id=current_user.client_id).all()
        entitlements = get_entitlement_by_client(clients[0].client_id)
    else:
        return {'status': 'error', 'message': 'Invalid role!'}, 403

    return build_entitlement_response(clients, portfolio_groups, users, entitlements)

def get_entitlement_by_client(client_id):
    # client_id = 1013
    
    sql = """
    SELECT ue.user_id, ue.port_group_id, ue."permission"  
    FROM "user" u, user_entitilement ue 
    where u.client_id = %s
    and u.user_id = ue.user_id 
    """

    pg_conn = pg_create_connection()
    with pg_conn.cursor() as cursor:
        cursor.execute(sql, (client_id,))
        result = cursor.fetchall()
        

    return result
    
    
# client_id = 1013
# get entitlement data for just one client
def get_entitlement_1client_data(username, input_data):
    client_id = input_data.get('client_id')
    if not client_id:
        return {'status': 'error', 'message': 'missing client_id'}, 403
    
    current_user = models.User.query.filter_by(username=username).first()
    if current_user.role == 'admin': # only allowed for the same company
        if current_user.client_id != client_id:
            return {'status': 'error', 'message': 'permission denied!'}, 403
    else: 
        if current_user.role != 'superadmin': # only superadmin is allowed
            return {'status': 'error', 'message': 'Invalid role!'}, 403
    
    clients = models.Client.query.filter_by(client_id=client_id).all()
    portfolio_groups = models.Portfolio_Group.query.filter_by(client_id=client_id).all()
    users = models.User.query.filter_by(client_id=client_id).all()    
    entitlements = get_entitlement_by_client(client_id)
    
    return build_entitlement_response(clients, portfolio_groups, users, entitlements)
    

def build_entitlement_response(clients, portfolio_groups, users, entitlements):
    clients = [{'client_id': x.client_id, 'client_name':x.client_name} for x in clients]
    portfolio_groups = [{'group_id': x.pgroup_id, 'group_name':x.group_name} for x in portfolio_groups]
    users = [{'user_id': x.user_id, 'username': x.username} for x in users]
    entitlements = [{'user_id': x[0], 'group_id': x[1], 'permission': x[2]} for x in entitlements]
    
    return {
        'status': 'success',
        'clients': clients,
        'users': users,
        'portfolioGroups': portfolio_groups,
        'entitlements': entitlements
    }, 200

# input_data = {
#     'updates': {
#         'client_id': 123,
#         'user_id': 123,
#         'group_id': 123,
#         'permissions': ['view', 'upload']
#         }
#     }
    
def update_entitlement_data(username, input_data):

    data = input_data.get('updates')
    if not data:
        return {'status': 'error', 'message': 'missing updates data!'}, 430
    
    client_id = data.get('client_id')
    if not client_id:
        return {'status': 'error', 'message': 'missing data: client_id'}, 430

    user_id = data.get('user_id')
    if not user_id:
        return {'status': 'error', 'message': 'missing data: user_id'}, 430

    group_id = data.get('group_id')
    if not group_id:
        return {'status': 'error', 'message': 'missing data: group_id'}, 430

    request_permissions = data.get('permissions')
    if not request_permissions:
        return {'status': 'error', 'message': 'missing data: permissions'}, 430

    # check access permission
    current_user = models.User.query.filter_by(username=username).first()
    if current_user.role == 'admin': # only allowed for the same company
        if current_user.client_id != client_id:
            return {'status': 'error', 'message': 'permission denied!'}, 403
    else: 
        if current_user.role != 'superadmin': # only superadmin is allowed
            return {'status': 'error', 'message': 'Invalid role!'}, 403

    # check if IDs are valid            
    client = models.Client.query.filter_by(client_id=client_id).first()
    if not client:
        return {'status': 'error', 'message': f'invalid client_id ({client_id})'}, 431

    user = models.User.query.filter_by(user_id=user_id).first()
    if not user:
        return {'status': 'error', 'message': f'invalid user_id ({user_id})'}, 431

    group = models.Portfolio_Group.query.filter_by(pgroup_id=group_id).first()
    if not group:
        return {'status': 'error', 'message': f'invalid group_id ({group_id})'}, 431

    # current entitlements
    entitlements = models.User_Entitilement.query.filter_by(
        user_id=user_id, port_group_id=group_id
    ).all()
    
    current_permissions = [e.permission for e in entitlements]
    print(current_permissions)
    print(request_permissions)
    
    changed = False
    for permission in ['view', 'upload', 'download', 'execute']:
        ret = update_permission(
            permission, request_permissions.get(permission), current_permissions, 
            user_id, group_id
        )
        changed = changed | ret

    
    # sync sql server database
    if changed:
        sync_report_mapping.sync_delta_thread()
    
    return {'status': 'success', 'message': 'Entitlement updated successfully!'}, 200

def update_permission(permit, add, current_permissions, user_id, pgroup_id):
    changed = False
    if add:
        if permit not in current_permissions:
            db_aux.save_entitlements(user_id, pgroup_id, permit)
            changed = True
    else: # delete from database
        if permit in current_permissions:
            db_aux.remove_entitlements(user_id, pgroup_id, permit)
            changed = True
            
    return changed
    
def save_entitlement(user, pgroup, permission):
    USER_PERMISSIONS = ['view', 'upload', 'download', 'execute']
    if permission == 'all':
        for permit in USER_PERMISSIONS:
            db_aux.save_entitlements(user.user_id, pgroup.pgroup_id, permit)
    else:
        db_aux.save_entitlements(user.user_id, pgroup.pgroup_id, permission)
    
    print(f'saved entitlement - user: {user.user_id} - group: {pgroup.pgroup_id}, permission: {permission}' )

############################################################################
def api_test(username, input_data):

    # Extracting necessary information from input data
    client_id = input_data.get("Client ID")
    portfolio_name = input_data.get("Portfolio Name")

    # Dummy hardcoded response
    response = {
        "Client ID": client_id,
        "Portfolio Name": portfolio_name,
        "Summary VaR": {
            "Header": "SecurityID,Volatility,VaR",
            "Data": [
                "1,2,3",
                "1,2,3"
            ]
        },
    }

    return response, 200

############################################################################
def test_portfolio():
    
    filename = config['SRC_DIR'] / 'test' / 'data' / 'Demo.Model_1.json'
    with open(filename, 'r') as f:
        data = json.load(f)
        
    params = data['Paramaters']
    positions = data_pack.extract_df(data, 'Positions')
    
    return positions, params

def test():
    username = 'mding@trg.com'
    get_user_approval_data(username)
