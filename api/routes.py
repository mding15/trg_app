# -*- coding: utf-8 -*-
"""
Created on Sun Mar 17 16:21:32 2024

@author: mgding

# register and login
api/register {firstName, lastName, email, companyName}
api/login {username, password}
api/verify_token
api/forget_password {email}
api/reset_password
api/change_password

# portfolio 
api/upload_portfolio
api/delete_portfolios {PORT_ID_LIST}
api/get_dashboard
api/download
api/download_file

# support
api/user_approval/data
api/user_approval/update
api/get_entitlement
api/get_entitlement_1client
api/update_entitlement
api/upload_security
api/get_upload_security
api/sup_download
api/rerun_portfolio/<port_id>
api/run_account/<account_id>/<as_of_date>

# miscellaneouse
api/schduleDemo
api/set_sso_cookie

# legacy
/api/data_request
/api/calculate
/api/add_security
/api/risk_calculator
/api/test



"""
import requests
from flask import request, jsonify, send_from_directory, make_response
from flasgger import Swagger
import datetime
import sqlalchemy.exc

from trg_config import config

from api import app, request_handler, bcrypt, swagger
from api import request_handler_ft
from api import create_account, schedule_demo_handler, request_demo_handler, sso_cookie
from api import portfolios
from api.auth import token_required, ops_role_required, authenticate
from api import upload_handler
from api import account_mgmt

from database.models import User as User
from database import db_utils, model_aux, ms_sql_server


from api.logging_config import get_logger
logger = get_logger(__name__)


#####################################################################################
# Request / response logging

@app.before_request
def log_request():
    logger.info(f'[REQUEST]  {request.method} {request.path} — from {request.remote_addr}')

@app.after_request
def log_response(response):
    logger.info(f'[RESPONSE] {request.method} {request.path} — {response.status_code}')
    return response


#####################################################################################
# # register and login

@app.route('/api/register', methods=['POST'])
def register():
    
    try:
        create_account.create_account(request.get_json())
        return jsonify({'message': 'register success'}), 200
    except Exception as e:
        print(e)
        return jsonify({'error': str(e)}), 402

@app.route('/api/login', methods=['POST'])
def login():

    try:
        token, user = authenticate()
        # ms_sql_server.wakeup_server()

        resp = make_response(jsonify({
            'token': token,
            'role': user.role,
            'email': user.email,
            'firstname': user.firstname,
            'lastname': user.lastname
        }))

        return resp
    except sqlalchemy.exc.SQLAlchemyError as e:
        logger.error(f'Database error during login: {e}')
        return jsonify({'error': 'Service temporarily unavailable. Please try again later.'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 401

OPS_ROLES = {'admin', 'superadmin', 'support'}

@app.route('/api/ops/login', methods=['POST'])
def ops_login():
    try:
        token, user = authenticate()
        if user.role not in OPS_ROLES:
            return jsonify({'error': 'Access denied. Ops portal requires admin, superadmin, or support role.'}), 403
        return jsonify({
            'token': token,
            'role': user.role,
            'email': user.email,
            'firstname': user.firstname,
            'lastname': user.lastname
        })
    except sqlalchemy.exc.SQLAlchemyError as e:
        logger.error(f'Database error during ops login: {e}')
        return jsonify({'error': 'Service temporarily unavailable. Please try again later.'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 401

@app.route('/api/verify_token/<token>', methods=['GET'])
def verify_token(token):
    user = create_account.verify_reset_token(token)
    if user is None:
        return jsonify({'message': 'This is an invalid or expired token'}), 402
    else:
        return jsonify({'email': user.email}), 200

@app.route('/api/forget_password', methods=['POST'])
def forget_password():
    data = request.get_json()
    try:
        create_account.forget_password(data)
        return jsonify({'message': 'Please check your email'}), 200
    except Exception as e:
        message = str(e)
        print(message)
        return jsonify({'message': message}), 402

@app.route('/api/change_password', methods=['POST'])
@token_required
def change_password(username):
    data = request.get_json()
    try:
        create_account.change_password(username, data)
        return jsonify({'message': 'Your password has been successfully changed!'}), 200
    except Exception as e:
        message = str(e)
        print(message)
        return jsonify({'message': message}), 402
    
@app.route('/api/reset_password', methods=['POST'])
def reset_password():
    data = request.get_json()
    try:
        token = data.get('token')
        password = data.get('password')
        create_account.reset_password(token, password)
        return jsonify({'message': 'reset password success'}), 200
    except Exception as e:
        message = str(e)
        print(message)
        return jsonify({'message': message}), 402

############################################################################################
# PORTFOLIO related
    
@app.route('/api/upload_portfolio', methods=['POST'])
@token_required
def upload_portfolio(username):
    return portfolios.handle_upload_portfolio("upload_portfolio", username, request) 

# delete portfolio 
@app.route("/api/delete_portfolios", methods=['POST'])
@token_required
def delete_portfolios(username):
    return api_request('delete_portfolios', username, request)

@app.route('/api/get_dashboard', methods=['POST'])
@token_required
def get_dashboard(username):
    return api_request('get_dashboard', username, request)


@app.route("/api/download/<group_id>/<filename>")
@token_required
def download(username, group_id, filename):
    
    user = User.query.filter_by(username=username).first()
    if user.role in ['superadmin', 'support']:
        client = model_aux.get_client_from_pgroup(group_id)
        client_id = client.client_id
    else:
        client_id = user.client_id

    folder_path = portfolios.get_group_folder(client_id, group_id)
    
    return send_from_directory(folder_path, filename)

    
# download template files
@app.route("/api/download_file/<filename>")
@token_required
def download_file(username, filename):
    return send_from_directory(config['PUBLIC_DIR'], filename)


# @app.route('/api/scrubbing_portfolio', methods=['POST'])
# @token_required
# def scrubbing_portfolio(username):
#     return api_request('scrubbing_portfolio', username, request)

# @app.route('/api/run_calculation', methods=['POST'])
# @token_required
# def run_calculation(username):
#     return api_request('run_calculation', username, request)
           

############################################################################################
# SUPPORT

#user_approval
@app.route("/api/user_approval/data", methods=['GET','POST'])
@ops_role_required
def get_user_approval_data(username):
    print('received: /api/user_approval_data')
    return api_request('get_user_approval_data', username, request)

@app.route("/api/user_approval/update", methods=['POST'])
@ops_role_required
def update_user_approval(username):
    return api_request('update_user_approval', username, request)

#user_entitlement
@app.route("/api/get_entitlement")
@ops_role_required
def get_entitlement(username):
    return api_request('get_entitlement', username)

@app.route("/api/update_entitlement", methods=['POST'])
@ops_role_required
def update_entitlement(username):
    return api_request('update_entitlement', username, request)

@app.route("/api/get_entitlement_1client", methods=['POST'])
@ops_role_required
def get_entitlement_1client(username):
    return api_request('get_entitlement_1client', username, request)

#####################################################################################
# ACCOUNT MANAGEMENT (ops)

@app.route('/api/ops/clients', methods=['GET'])
@ops_role_required
def ops_get_clients(username):
    try:
        return jsonify(account_mgmt.get_clients()), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/ops/accounts', methods=['GET'])
@ops_role_required
def ops_get_accounts(username):
    client_id = request.args.get('client_id', type=int)
    if client_id is None:
        return jsonify({'error': 'client_id is required'}), 400
    try:
        return jsonify(account_mgmt.get_accounts(client_id)), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/ops/account_access', methods=['GET'])
@ops_role_required
def ops_get_account_access(username):
    account_id = request.args.get('account_id', type=int)
    if account_id is None:
        return jsonify({'error': 'account_id is required'}), 400
    try:
        return jsonify(account_mgmt.get_account_access(account_id)), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/ops/client_users', methods=['GET'])
@ops_role_required
def ops_get_client_users(username):
    client_id = request.args.get('client_id', type=int)
    if client_id is None:
        return jsonify({'error': 'client_id is required'}), 400
    try:
        return jsonify(account_mgmt.get_client_users(client_id)), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/ops/account_access', methods=['POST'])
@ops_role_required
def ops_add_account_access(username):
    data = request.get_json() or {}
    account_id = data.get('account_id')
    user_id = data.get('user_id')
    is_default = data.get('is_default', False)
    if not account_id or not user_id:
        return jsonify({'error': 'account_id and user_id are required'}), 400
    try:
        new_id = account_mgmt.add_account_access(account_id, user_id, is_default)
        return jsonify({'id': new_id}), 201
    except ValueError as e:
        return jsonify({'error': str(e)}), 409
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/ops/accounts', methods=['POST'])
@ops_role_required
def ops_create_account(username):
    data = request.get_json() or {}
    account_name = data.get('account_name', '').strip()
    short_name = data.get('short_name', '').strip()
    owner_id = data.get('owner_id')
    client_id = data.get('client_id')
    parent_account_id = data.get('parent_account_id') or None
    if not account_name or not owner_id or not client_id:
        return jsonify({'error': 'account_name, owner_id, and client_id are required'}), 400
    try:
        new_id = account_mgmt.create_account(account_name, short_name, owner_id, client_id, parent_account_id)
        return jsonify({'account_id': new_id}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/upload_security', methods=['POST'])
@token_required
def upload_security(username):
    return upload_handler.upload("upload_security", username, request) 

@app.route('/api/get_upload_security', methods=['POST'])
@token_required
def get_upload_security(username):
    return api_request('get_upload_security', username, request)

@app.route("/api/sup_download/<category>/<filename>")
@token_required
def sup_download(username, category, filename):
    user = User.query.filter_by(username=username).first()
    if user.role not in ['superadmin', 'support']:
        return jsonify({'error': 'permison denied'}, 402)

    
    return handle_sup_download(username, category, filename)


@app.route('/api/rerun_portfolio/<port_id>')
@token_required
def rerun_portfolio(username, port_id):
    user = User.query.filter_by(username=username).first()
    if user.role not in ['superadmin', 'support']:
        return jsonify({'error': 'permison denied'}, 402)

    try:
        portfolios.rerun_portfolio(port_id)
    
        return jsonify({'message': 're-run portfolio successed'}), 200
    except Exception as e:
        return jsonify({'message': f're-run portfolio failed: Error {str(e)}'}), 402


from process import process
@app.route('/api/run_account/<account_id>/<as_of_date>')
@token_required
def run_account(username, account_id, as_of_date):
    user = User.query.filter_by(username=username).first()
    if user.role not in ['superadmin', 'support']:
        return jsonify({'error': 'permison denied'}, 402)

    try:
        print(f"API: run_account({account_id}, {as_of_date})")
        process.process_account(account_id, as_of_date)
    
        return jsonify({'message': f'run_account({account_id}, {as_of_date}) successed!'}), 200
    except Exception as e:
        return jsonify({'message': f'run_account({account_id}, {as_of_date}) failed: Error {str(e)}'}), 402



#################################################################################
# miscollaneous

@app.route('/api/requestDemo', methods=['POST'])
def request_demo():
    try:
        request_demo_handler.handle_request(request.get_json())
        return jsonify({'message': 'success'}), 200
    except Exception as e:
        print(e)
        return jsonify({'error': str(e)}), 402

# schedule demo will be removed in the future. Please use requestDemo API to request demo and our sales team will contact you shortly.
@app.route('/api/scheduleDemo', methods=['POST'])
def schedule_demo():
    
    try:
        schedule_demo_handler.handle_request(request.get_json())
        return jsonify({'message': 'success'}), 200
    except Exception as e:
        print(e)
        return jsonify({'error': str(e)}), 402

@app.route('/api/set_sso_cookie', methods=['POST'])
@token_required
def set_sso_cookie(username):
    return sso_cookie.request(username)

#################################################################################
# Not UI related API

@app.route('/api/data_request', methods=['POST'])
@token_required
def data_request(username):
    return api_request('data_request', username, request)


@app.route('/api/calculate', methods=['POST'])
@token_required
def calculate(username):
    return api_request('calculate', username, request)

# get securities by ISIN, Cusip, Tickr, etc..
@app.route('/api/add_security', methods=['POST'])
def security(username):
    return api_request('add_security', username, request)



#### for FinTree only
@app.route('/api/risk_calculator', methods=['POST'])
@token_required
def risk_calculator(username):
    """
    Calculate Portfolio Risk
    Receives and processes client data, then returns the results.
    ---
    tags:
      - Portfolio Risk Calculation
    consumes:
      - application/json
    parameters:
      - name: token
        in: query
        description: Authentication token
        required: true

      - in: body
        name: Payload
        description: Client input data
        required: true
        schema:
          $ref: '#/definitions/Payload'
    responses:
      200:
        description: Data processed successfully, results returned.
        schema:
          $ref: '#/definitions/Response'
      401:
        description: Unauthorized access.
    """
    return api_request_ft('risk_calculator', username, request)

################################################################################################

@app.route('/api/test', methods=['POST'])
@token_required
def test(username):
    
    print(f'route: /api/test {username}')
    return api_request('test', username, request)


def api_request(route, username, request=None):
    
    if request:
        input_data = request.json
    else:
        input_data = {}

    response, status = request_handler.get_response(route, username, input_data)
    return jsonify(response), status

ft_user_routes = ['risk_calculator']
def api_request_ft(route, username, request):
    if route in ft_user_routes:
        response, status = request_handler_ft.get_response(route, username, request.json)
    else:
        status = 403 # Permission denied
        response = {
            'Status' : 'Failed',
            'Error'  : 'Execution permission denied'
            }
    return jsonify(response), status
    

def handle_sup_download(username, category, filename):
    category_folder = {
        'model': config['MODEL_DIR'] / 'Upload',
        'public': config['DATA_DIR'] / 'public'
        }
    
    folder_path = category_folder[category]
    return send_from_directory(folder_path, filename)
    

#######
description = '''<p>TRG API supports JWT Authentication. All API calls require JWT token. You can obtain a token by providing your TRG credential via <em>/api/login</em> API call as shown below. A token expires in 24 hours. You need to obtain a new token after your token expires. </p>
<p>TRG API host is: <a  href="https://engine.tailriskglobal.com/api/apidocs">engine.tailriskglobal.com</a></p>
<p>For detail information regarding JWT, please refer to <a  href="https://jwt.io">JSON Web Token (JWT)</a>.</p>
'''

general_info = {
    'title': 'Tail Risk Global API Document',
    'version': '1.0.0',  # Update with your actual version
    
    'description': description
    
    # 'contact': {
    #     'name': 'Tail Risk Global LLC',
    #     'email': 'mding@tailriskglobal.com',
    #     'url': 'https://tailriskglobal.com'  # Optional website URL
    #}
}


definitions = {
    'Payload': {
        'type': 'object',
        'properties': {
            'Request': {
                'type': 'string',
                'example': 'RiskCalculator'
            },
            'Client ID': {
                'type': 'string',
                'example': 'C12345'
            },
            'Portfolio Name': {
                'type': 'string',
                'example': 'Growth Model'
            },
            'Portfolio ID': {
                'type': 'string',
                'example': 'Model_1'
            },
            'Report Date': {
                'type': 'string',
                'example': '2024-02-25'
            },

            'Risk Horizon': {
                'type': 'string',
                'example': 'Month'
            },
            
            'Confidence Level': {
                'type': 'float',
                'example': 0.95
            },
            
            'Benchmark': {
                'type': 'string',
                'example': 'BM_20_80'
            },

            'Benchmark Name': {
                'type': 'string',
                'example': 'Equity/Bond 20%-80%'
            },
            
            'Positions': {
                'type': 'string',
                'example': "Security Name,Nemo,ISIN,Market Value,Last Price,Last Price Date,Asset Currency\niShares Core MSCI Pacific ETF,IPAC.P,US46434V6965,100000.0,71.277892,2024-05-31,USD\niShares Asia 50 ETF,AIA.O,US4642884302,100000.0,68.7291209999999,2024-05-31,USD\niShares 1-3 Yr International Treasury Bond ETF,ISHG.O,US4642881258,100000.0,70.09,2024-05-31,USD\niShares US & Intl High Yield Corp Bond ETF,GHYG.K,US4642861789,100000.0,52.3382,2024-05-31,USD\niShares Gold Trust,IAU,US4642852044,100000.0,43.99,2024-05-31,USD\niShares US Real Estate ETF,IYR.P,US4642877397,100000.0,99.418982,2024-05-31,USD\nCash,USD.CCY,,100000.0,1.0,2024-05-31,USD\n"
            }

        },
        'required': ['Request', 'Client ID', 'Portfolio ID', 'Report Date', 'Positions', 'Risk Horizon', 'Confidence Level', 'Benchmark']
    },

    'Response': {
        'type': 'object',
        'properties': {
            'Status': {
                'type': 'string',
                'example': 'Success'
            },
            'Request': {
                'type': 'string',
                'example': 'RiskCalculator'
            },
            'Request ID': {
                'type': 'string',
                'example': '8015f9383e'
            },
            'Client ID': {
                'type': 'string',
                'example': 'C12345'
            },
            'Portfolio ID': {
                'type': 'string',
                'example': 'Model_1'
            },

            'Allocation': {
                'type': 'string',
                'example': 'Class,Allocation,VaR\nCash,0.14285714285714285,0.0\nCommodities & Digital'
            },
            'Portfolio Risk': {
                'type': 'string',
                'example': 'Name,Volatility,VaR,Sharpe Ratio - Vol,Sharpe Ratio - VaR'
            },
        }
    }

}

if not swagger.template:
    swagger.template = {}
swagger.template['definitions'] = definitions
swagger.template['info'] = general_info


###################################
# DASHBOARD
from dashboard.positions import get_positions as _fetch_positions
from dashboard.positions import get_portfolio_summary as _fetch_portfolio_summary
from dashboard.positions_db import (
    get_accounts_for_user,
    user_has_account_access,
    read_portfolio_summary,
    read_asset_allocation,
    read_risk_alerts,
    count_risk_alerts,
    read_var_limit,
    read_risk_parameters,
    read_risk_measures,
    compute_chart_data,
    get_top_risk_contributors,
    get_broker_summary,
)
from dashboard.concentration_db import read_concentrations
from dashboard.stress_test import read_stress_results
from dashboard.guage_data import build_gauge_data
from dashboard.static_data import (
    RISK, FACTOR_EXPOSURES_V2, ASSET_ALLOCATION_DRILLDOWN,
    RISK_METRICS, RISK_ADJUSTED_RETURN, TOP_RISKS, VAR_HISTORY,
    RISK_PARAMETERS, RISK_SUMMARY_MOCK, RISK_CONCENTRATIONS,
    RISK_CONTRIB_MOCK,
    RISK_ASSET_LEVELS, RISK_REGION_LEVELS, RISK_INDUSTRY_LEVELS, RISK_CURRENCY_LEVELS,
    STRESS_SCENARIOS_V2,
    PORTFOLIO_SUMMARY, PORTFOLIO_POSITIONS, PORTFOLIO_CHART, PORTFOLIO_ALLOC,
)
from dashboard.allocation_drilldown import get_alloc_drilldown_data
from dashboard.portfolio_chart import get_portfolio_chart_data
from dashboard.portfolio_allocation import get_portfolio_allocation as _fetch_portfolio_allocation
from dashboard.bar_chart_data import (
    read_asset_drilldown,
    read_industry_drilldown,
    read_region_drilldown,
    read_currency_drilldown,
)
from dashboard.settings_params import (
    PARAMETER_OPTIONS,
    read_account_parameters,
    write_account_parameters,
)
from dashboard.settings_limits import (
    read_account_limits,
    write_account_limits,
)
from dashboard.historical import get_historical_data


def _get_account_id(require_access: bool = True, username: str = None):
    """Parse account_id from query args and optionally verify access.

    Returns (account_id, error_response) where error_response is None on success.
    """
    account_id = request.args.get("account_id", type=int)
    if account_id is None:
        return None, (jsonify({"error": "account_id is required"}), 400)
    if require_access and not user_has_account_access(username, account_id):
        return None, (jsonify({"error": "Access denied"}), 403)
    return account_id, None


# ── Summary page ──────────────────────────────────────────────────────────────

@app.route("/api/summary/metrics")
@token_required
def get_metrics(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        data = read_portfolio_summary(account_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(data)


@app.route("/api/summary/chart/<range_key>")
@token_required
def get_chart(username, range_key):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        data = compute_chart_data(account_id, range_key)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(data)


@app.route("/api/summary/portfolio")
@token_required
def get_summary_portfolio(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        rows = read_asset_allocation(account_id)
        # Portfolio table view: asset class breakdown with returns and VaR
        data = [
            {
                "assetClass":   r["assetClass"],
                "marketValue":  r["marketValue"],
                "weight":       r["weight"],
                "periodReturn": r["periodReturn"],
                "varContrib":   r["varContrib"],
            }
            for r in rows
        ]
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(data)


@app.route("/api/summary/risk")
@token_required
def get_summary_risk(username):
    # RISK bar chart remains static until redesign
    return jsonify(RISK)


@app.route("/api/summary/allocation")
@token_required
def get_allocation(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    return jsonify(get_alloc_drilldown_data(account_id))


@app.route("/api/summary/brokers")
@token_required
def get_summary_brokers(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        data = get_broker_summary(account_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(data)


@app.route("/api/summary/concentrations")
@token_required
def get_summary_concentrations(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        data = read_concentrations(account_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(data)


@app.route("/api/summary/top_risk")
@token_required
def get_summary_top_risk(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        data = get_top_risk_contributors(account_id, n=5)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(data)



@app.route("/api/summary/gauges")
@token_required
def get_summary_gauges(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        g = build_gauge_data(account_id)
        data = {
            "sharpe": {
                "value":  g["sharpe_value"],
                "target": g["sharpe_target"],
                "band":   g["sharpe_band"],
            },
            "varLimit": {
                "value": g["var_value"],
                "limit": g["var_limit"],
                "band":  g["var_band"],
            },
        }
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(data)


# ── Risk page ─────────────────────────────────────────────────────────────────

@app.route("/api/risk/parameters")
@token_required
def get_risk_parameters(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    return jsonify(read_risk_parameters(account_id))


@app.route("/api/risk/summary")
@token_required
def get_risk_summary(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        g = build_gauge_data(account_id)
    except Exception:
        g = None
    response = {
        "measures": read_risk_measures(account_id),
        "gaugeSharpe": {
            "value":  g["sharpe_value"]  if g else 0.21,
            "target": g["sharpe_target"] if g else 0.15,
            "band":   g["sharpe_band"]   if g else 0.05,
            "max":    g["sharpe_max"]    if g else 0.65,
        },
        "gaugeRisk": {
            "value":        g["var_value"]         if g else 16_900_000,
            "limit":        g["var_limit"]         if g else 25_000_000,
            "band":         g["var_band"]          if g else 1_250_000,
            "readingValue": g["var_reading_value"] if g else "16.9",
            "readingUnit":  g["var_reading_unit"]  if g else "M",
            "targetLabel":  g["var_target_label"]  if g else "25.0M",
        },
    }
    return jsonify(response)


@app.route("/api/risk/contributions")
@token_required
def get_risk_contributions(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        data = get_top_risk_contributors(account_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(data)


@app.route("/api/risk/concentrations")
@token_required
def get_risk_concentrations(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        data = read_concentrations(account_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(data)


@app.route("/api/risk/asset_allocation")
@token_required
def get_risk_asset_allocation(username):
    return jsonify(ASSET_ALLOCATION_DRILLDOWN)


@app.route("/api/risk/asset")
@token_required
def get_risk_asset(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        data = _fetch_portfolio_allocation(account_id, "asset")
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    if not data:
        return jsonify({"error": "No data"}), 404
    return jsonify(data["levels"])


@app.route("/api/risk/industry")
@token_required
def get_risk_industry(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        data = _fetch_portfolio_allocation(account_id, "industry")
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    if not data:
        return jsonify({"error": "No data"}), 404
    return jsonify(data["levels"])


@app.route("/api/risk/region")
@token_required
def get_risk_region(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        data = _fetch_portfolio_allocation(account_id, "region")
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    if not data:
        return jsonify({"error": "No data"}), 404
    return jsonify(data["levels"])


@app.route("/api/risk/currency")
@token_required
def get_risk_currency(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        data = _fetch_portfolio_allocation(account_id, "currency")
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    if not data:
        return jsonify({"error": "No data"}), 404
    return jsonify(data["levels"])


@app.route("/api/risk/risk_metrics")
@token_required
def get_risk_metrics(username):
    return jsonify(RISK_METRICS)


@app.route("/api/risk/risk_adjusted_return")
@token_required
def get_risk_adjusted_return(username):
    return jsonify(RISK_ADJUSTED_RETURN)


@app.route("/api/risk/top_risks")
@token_required
def get_top_risks(username):
    return jsonify(TOP_RISKS)


@app.route("/api/risk/var_history")
@token_required
def get_var_history(username):
    period = request.args.get("period", "3M")
    data = VAR_HISTORY.get(period, VAR_HISTORY["3M"])
    return jsonify(data)


@app.route("/api/risk/factors")
@token_required
def get_risk_factors(username):
    # Remains static until redesign
    return jsonify(FACTOR_EXPOSURES_V2)


@app.route("/api/stress/scenarios")
@token_required
def get_stress_scenarios(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    return jsonify(STRESS_SCENARIOS_V2)


@app.route("/api/risk/alerts")
@token_required
def get_risk_alerts(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        data = read_risk_alerts(account_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(data)

# ── Settings page ─────────────────────────────────────────────────────────────

@app.route("/api/settings/parameters")
@token_required
def get_settings_parameters(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        values = read_account_parameters(account_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"values": values, "options": PARAMETER_OPTIONS})


@app.route("/api/settings/parameters", methods=["PUT"])
@token_required
def put_settings_parameters(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    body = request.get_json(silent=True) or {}
    try:
        write_account_parameters(account_id, body)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"ok": True})


@app.route("/api/settings/limits")
@token_required
def get_settings_limits(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        data = read_account_limits(account_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(data)


@app.route("/api/settings/limits", methods=["PUT"])
@token_required
def put_settings_limits(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    body = request.get_json(silent=True) or {}
    try:
        write_account_limits(account_id, body)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"ok": True})


# ── Historical page ───────────────────────────────────────────────────────────

@app.route("/api/historical")
@token_required
def get_historical(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        data = get_historical_data(account_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(data)


# ── Accounts ──────────────────────────────────────────────────────────────────

@app.route("/api/accounts")
@token_required
def get_accounts(username):
    accounts = get_accounts_for_user(username)
    return jsonify(accounts)

# ── Portfolio page ────────────────────────────────────────────────────────────

@app.route("/api/portfolio/summary")
@token_required
def get_portfolio_summary(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        d = _fetch_portfolio_summary(account_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    if not d:
        return jsonify({})
    return jsonify({
        "aum":            d.get("aum"),
        "unrealizedGain": d.get("unrealizedGain"),
        "asOfDate":       d.get("asOfDate"),
        "returns": [
            {"label": "SI",    "value": d.get("siReturn")},
            {"label": "3Y",    "value": d.get("threeYearReturn")},
            {"label": "12M",   "value": d.get("oneYearReturn")},
            {"label": "YTD",   "value": d.get("ytdReturn")},
            {"label": "Month", "value": d.get("mtdReturn")},
            {"label": "Today", "value": d.get("dayReturn")},
        ],
    })


@app.route("/api/portfolio/positions")
@token_required
def get_positions(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        data = _fetch_positions(account_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(data)


@app.route("/api/portfolio/chart/<range_key>")
@token_required
def get_portfolio_chart(username, range_key):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    data = get_portfolio_chart_data(account_id, range_key)
    if data is None:
        return jsonify({"error": f"No chart data for range: {range_key}"}), 404
    return jsonify(data)


@app.route("/api/portfolio/allocation")
@token_required
def get_portfolio_allocation(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    slice_key = request.args.get("slice", "asset")
    try:
        data = _fetch_portfolio_allocation(account_id, slice_key)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    if not data:
        return jsonify({"error": f"No data for slice: {slice_key}"}), 404
    return jsonify(data)

