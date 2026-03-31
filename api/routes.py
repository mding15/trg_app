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

from trg_config import config

from api import app, request_handler, bcrypt, swagger
from api import request_handler_ft
from api import create_account, schedule_demo_handler, request_demo_handler, sso_cookie
from api import portfolios
from api.auth import token_required, authenticate
from api import upload_handler

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
@token_required
def get_user_approval_data(username):
    print('received: /api/user_approval_data')
    return api_request('get_user_approval_data', username, request)

@app.route("/api/user_approval/update", methods=['POST'])
@token_required
def update_user_approval(username):
    return api_request('update_user_approval', username, request)

#user_entitlement
@app.route("/api/get_entitlement")
@token_required
def get_entitlement(username):
    return api_request('get_entitlement', username)

@app.route("/api/update_entitlement", methods=['POST'])
@token_required
def update_entitlement(username):
    return api_request('update_entitlement', username, request)

@app.route("/api/get_entitlement_1client", methods=['POST'])
@token_required
def get_entitlement_1client(username):
    return api_request('get_entitlement_1client', username, request)

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
    compute_chart_data,
)
from dashboard.stress_test import read_stress_results
from dashboard.static_data import (
    RISK, RISK_CONTRIBUTIONS, FACTOR_EXPOSURES_V2, ASSET_ALLOCATION_DRILLDOWN,
)


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
    try:
        rows = read_asset_allocation(account_id)
        # Allocation chart view: portfolio vs benchmark weights
        data = [
            {
                "assetClass": r["assetClass"],
                "weight":     r["weight"],
                "bmkWeight":  r["bmkWeight"],
            }
            for r in rows
        ]
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(data)


# ── Risk page ─────────────────────────────────────────────────────────────────

@app.route("/api/risk/summary")
@token_required
def get_risk_summary(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        ps = read_portfolio_summary(account_id)
        if not ps:
            return jsonify({}), 200
        data = {
            "asOfDate":     ps.get("asOfDate"),
            "var1d95":      ps.get("var1d95"),
            "var1d95Pct":   ps.get("var1d95Pct"),
            "var1d99":      ps.get("var1d99"),
            "var1d99Pct":   ps.get("var1d99Pct"),
            "var10d99":     ps.get("var10d99"),
            "var10d99Pct":  ps.get("var10d99Pct"),
            "es1d95":       ps.get("es1d95"),
            "es1d95Pct":    ps.get("es1d95Pct"),
            "es99":         ps.get("es99"),
            "es99Pct":      ps.get("es99Pct"),
            "volatility":   ps.get("volatility"),
            "sharpe":       ps.get("sharpe"),
            "beta":         ps.get("beta"),
            "maxDrawdown":  ps.get("maxDrawdown"),
            "topFiveConc":  ps.get("topFiveConc"),
            "varLimitPct":  read_var_limit(account_id),
            "activeAlerts": count_risk_alerts(account_id),
        }
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(data)


@app.route("/api/risk/contributions")
@token_required
def get_risk_contributions(username):
    # Remains static until redesign
    return jsonify(RISK_CONTRIBUTIONS)


@app.route("/api/risk/asset_allocation")
@token_required
def get_risk_asset_allocation(username):
    return jsonify(ASSET_ALLOCATION_DRILLDOWN)


@app.route("/api/risk/factors")
@token_required
def get_risk_factors(username):
    # Remains static until redesign
    return jsonify(FACTOR_EXPOSURES_V2)


@app.route("/api/risk/stress")
@token_required
def get_risk_stress(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        data = read_stress_results(account_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(data)


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
    try:
        account_id = request.args.get("account_id", type=int)
        if account_id is None:
            return jsonify({"error": "account_id is required"}), 400
        if not user_has_account_access(username, account_id):
            return jsonify({"error": "Access denied"}), 403
        summary = _fetch_portfolio_summary(account_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(summary)

@app.route("/api/portfolio/positions")
@token_required
def get_positions(username):
    try:
        account_id = request.args.get("account_id", type=int)
        if account_id is None:
            return jsonify({"error": "account_id is required"}), 400
        if not user_has_account_access(username, account_id):
            return jsonify({"error": "Access denied"}), 403
        positions = _fetch_positions(account_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(positions)

