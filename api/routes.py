# -*- coding: utf-8 -*-
"""
Created on Sun Mar 17 16:21:32 2024

@author: mgding

Client-facing API routes. Ops-portal routes (user approval, entitlements, ...)
live in api/ops_routes.py.

# register and login
POST   api/register                     {firstName, lastName, email, companyName}
POST   api/login                        {username, password}
GET    api/verify_token/<token>
POST   api/forget_password              {email}
POST   api/reset_password
POST   api/change_password

# portfolio (legacy upload/download)
POST   api/upload_portfolio
POST   api/delete_portfolios            {PORT_ID_LIST}
POST   api/get_dashboard
GET    api/download/<group_id>/<filename>
GET    api/download_file/<filename>

# support
POST   api/upload_security
POST   api/get_upload_security
GET    api/sup_download/<category>/<filename>
GET    api/rerun_portfolio/<port_id>
GET    api/run_account/<account_id>/<as_of_date>

# impersonation (superadmin)
POST   api/impersonate/<target_username>
GET    api/impersonate/whoami

# dashboard: summary
GET    api/summary/metrics
GET    api/summary/chart/<range_key>
GET    api/summary/portfolio
GET    api/summary/risk
GET    api/summary/allocation
GET    api/summary/brokers
GET    api/summary/concentrations
GET    api/summary/top_risk
GET    api/summary/gauges

# dashboard: risk
GET    api/risk/parameters
GET    api/risk/summary
GET    api/risk/contributions
GET    api/risk/concentrations
GET    api/risk/asset_allocation
GET    api/risk/asset
GET    api/risk/industry
GET    api/risk/region
GET    api/risk/currency
GET    api/risk/risk_metrics
GET    api/risk/risk_adjusted_return
GET    api/risk/top_risks
GET    api/risk/var_history
GET    api/risk/factors
GET    api/risk/alerts
GET    api/security-level
GET    api/alternatives/summary
GET    api/stress/scenarios

# dashboard: settings
GET    api/settings/parameters
PUT    api/settings/parameters
GET    api/settings/limits
PUT    api/settings/limits
GET    api/settings/presets

# dashboard: misc
GET    api/historical
GET    api/accounts
GET    api/dashboard/message

# dashboard: holdings
GET    api/holdings/summary
GET    api/holdings/positions
GET    api/holdings/chart/<range_key>
GET    api/holdings/allocation

# portfolios page
GET    api/portfolios
POST   api/portfolios/upload
DELETE api/portfolios/<pid>
GET    api/portfolios/<pid>/download
POST   api/portfolios/<port_id>/clone
GET    api/portfolios/tracked
GET    api/portfolios/adhoc
GET    api/position-history

# broker connections
GET    api/broker/settings
DELETE api/broker/settings/<sid>
GET    api/broker/feeds
POST   api/broker/request-auth

# what-if
GET    api/whatif/portfolios
GET    api/whatif/portfolio/<port_id>/allocations
POST   api/whatif/portfolio/<port_id>/metrics
GET    api/whatif/alternatives/positions
GET    api/whatif/alternatives/panel
POST   api/whatif/alternatives/calculate

# miscellaneous
POST   api/requestDemo
POST   api/scheduleDemo
POST   api/set_sso_cookie

# legacy
POST   api/data_request
POST   api/calculate
POST   api/add_security
POST   api/risk_calculator
POST   api/test

"""
import requests
from flask import request, jsonify, send_from_directory, make_response
from flasgger import Swagger
import datetime
import sqlalchemy.exc

from trg_config import config

from api import app, bcrypt, swagger
from api.dispatch import api_request, api_request_ft
from api import create_account, schedule_demo_handler, request_demo_handler, sso_cookie
from api import portfolios
from api.auth import token_required, authenticate, create_impersonation_token
from api import upload_handler

from dashboard.portfolios_page import (
    list_portfolios,
    delete_portfolio         as pp_delete_portfolio_impl,
    list_broker_feeds,
    get_broker_settings, delete_broker_setting, create_broker_setting,
    list_tracked_portfolios, list_adhoc_portfolios, list_position_history,
)
from dashboard.upload_portfolio import upload_portfolio as pp_upload_portfolio_impl
from dashboard.whatif import get_whatif_portfolios, get_whatif_allocations, get_whatif_alt_positions, get_whatif_alternatives_panel, post_whatif_alternatives_calculate, post_whatif_metrics

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

# ── Impersonation ─────────────────────────────────────────────────────────────

@app.route('/api/impersonate/<target_username>', methods=['POST'])
@token_required
def start_impersonation(username, target_username):
    requester = User.query.filter_by(username=username).first()
    if not requester or requester.role != 'superadmin':
        return jsonify({'error': 'Superadmin role required'}), 403
    target = User.query.filter_by(username=target_username).first()
    if not target:
        return jsonify({'error': f'User {target_username!r} not found'}), 404
    if target.role == 'superadmin':
        return jsonify({'error': 'Cannot impersonate another superadmin'}), 403
    token = create_impersonation_token(username, target_username)
    return jsonify({'token': token, 'impersonating': target_username}), 200


@app.route('/api/impersonate/whoami', methods=['GET'])
@token_required
def impersonate_whoami(username):
    import jwt as _jwt
    raw = request.args.get('token', '')
    data = _jwt.decode(raw, app.config['SECRET_KEY'], algorithms=['HS256'])
    impersonator = data.get('impersonator')
    return jsonify({
        'username':         username,
        'impersonator':     impersonator,
        'is_impersonating': impersonator is not None,
    }), 200


@app.route('/api/test', methods=['POST'])
@token_required
def test(username):
    
    print(f'route: /api/test {username}')
    return api_request('test', username, request)



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
from dashboard.stress_scenarios import get_stress_scenarios as _get_stress_scenarios
from dashboard.alternatives import get_alt_history, build_subclasses, get_alt_positions, get_alt_gauges
from dashboard import security_level
from dashboard.guage_data import build_gauge_data
from dashboard.static_data import (
    RISK, FACTOR_EXPOSURES_V2, ASSET_ALLOCATION_DRILLDOWN,
    RISK_METRICS, RISK_ADJUSTED_RETURN, TOP_RISKS, VAR_HISTORY,
    RISK_PARAMETERS, RISK_SUMMARY_MOCK, RISK_CONCENTRATIONS,
    RISK_CONTRIB_MOCK,
    RISK_ASSET_LEVELS, RISK_REGION_LEVELS, RISK_INDUSTRY_LEVELS, RISK_CURRENCY_LEVELS,
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
from dashboard.settings_presets import get_presets
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
                "value":  g["sharpe_var_value"],
                "target": g["sharpe_var_target"],
                "band":   g["sharpe_var_band"],
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
        "gaugeRisk": {
            "value":        g["var_value"]         if g else 16_900_000,
            "limit":        g["var_limit"]         if g else 25_000_000,
            "band":         g["var_band"]          if g else 1_250_000,
            "readingValue": g["var_reading_value"] if g else "16.9",
            "readingUnit":  g["var_reading_unit"]  if g else "M",
            "targetLabel":  g["var_target_label"]  if g else "25.0M",
        },
        "gaugeSharpeVar": {
            "value":  g["sharpe_var_value"]  if g else 0.21,
            "target": g["sharpe_var_target"] if g else 0.25,
            "band":   g["sharpe_var_band"]   if g else 0.05,
            "max":    g["sharpe_var_max"]    if g else 0.85,
        },
        "gaugeVol": {
            "value": g["vol_value"] if g else 10.0,
            "bmk":   g["vol_bmk"]   if g else 7.5,
            "band":  g["vol_band"]  if g else 0.5,
            "max":   g["vol_max"]   if g else 12.75,
        },
        "gaugeSharpeVol": {
            "value":  g["sharpe_vol_value"]  if g else 0.21,
            "target": g["sharpe_vol_target"] if g else 0.25,
            "band":   g["sharpe_vol_band"]   if g else 0.05,
            "max":    g["sharpe_vol_max"]    if g else 0.85,
        },
        "gaugeSharpeES": {
            "value":  g["sharpe_es_value"]  if g else 0.15,
            "target": g["sharpe_es_target"] if g else 0.12,
            "band":   g["sharpe_es_band"]   if g else 0.024,
            "max":    g["sharpe_es_max"]    if g else 0.138,
        },
        "gaugeES": {
            "value":        g["es_value"]         if g else 24_100_000,
            "limit":        g["es_limit"]         if g else 32_500_000,
            "band":         g["es_band"]          if g else 1_625_000,
            "readingValue": g["es_reading_value"] if g else "24.1",
            "readingUnit":  g["es_reading_unit"]  if g else "M",
            "targetLabel":  g["es_target_label"]  if g else "32.5M",
        },
        "gaugeBeta": {
            "value": g["beta_value"] if g else 1.2,
            "bmk":   g["beta_bmk"]  if g else 1.0,
            "band":  g["beta_band"] if g else 0.10,
            "max":   g["beta_max"]  if g else 2.0,
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


# ── Security Level page ────────────────────────────────────────────────────────

@app.route("/api/security-level")
@token_required
def get_security_level(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        data = security_level.get_security_level(account_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(data)


# ── Alternatives page ──────────────────────────────────────────────────────────

@app.route("/api/alternatives/summary")
@token_required
def get_alternatives_summary(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        history = get_alt_history(account_id)
    except Exception as e:
        logger.error(f"get_alt_history failed for account_id={account_id}: {e}")
        history = {"labels": [], "series": []}
    try:
        positions_raw = get_alt_positions(account_id)
        alt_alloc     = build_subclasses(positions_raw)
        subclasses    = alt_alloc["subclasses"]
        drill         = alt_alloc["drill"]
        positions     = positions_raw
    except Exception as e:
        logger.error(f"get_alt_positions failed for account_id={account_id}: {e}")
        positions  = []
        subclasses = []
        drill      = {}
    try:
        gauges = get_alt_gauges(account_id)
    except Exception as e:
        logger.error(f"get_alt_gauges failed for account_id={account_id}: {e}")
        gauges = {"var": {}, "sharpe": {}}
    return jsonify({
        "history":    history,
        "subclasses": subclasses,
        "drill":      drill,
        "positions":  positions,
        "gauges":     gauges,
    })


@app.route("/api/stress/scenarios")
@token_required
def get_stress_scenarios(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    return jsonify(_get_stress_scenarios(account_id))


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


@app.route("/api/settings/presets")
@token_required
def get_settings_presets(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    return jsonify(get_presets(account_id))


# ── Historical page ───────────────────────────────────────────────────────────

@app.route("/api/historical")
@token_required
def get_historical(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    freq = request.args.get('freq', 'weekly')
    try:
        data = get_historical_data(account_id, freq=freq)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(data)


# ── Accounts ──────────────────────────────────────────────────────────────────

@app.route("/api/accounts")
@token_required
def get_accounts(username):
    accounts = get_accounts_for_user(username)
    return jsonify(accounts)


@app.route("/api/dashboard/message")
@token_required
def get_dashboard_message(username):
    from dashboard.dashboard_msg import dashboard_msg
    try:
        msg = dashboard_msg(username)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"message": msg})

# ── Holdings page ─────────────────────────────────────────────────────────────

@app.route("/api/holdings/summary")
@token_required
def get_holdings_summary(username):
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


@app.route("/api/holdings/positions")
@token_required
def get_holdings_positions(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        data = _fetch_positions(account_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(data)


@app.route("/api/holdings/chart/<range_key>")
@token_required
def get_holdings_chart(username, range_key):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    data = get_portfolio_chart_data(account_id, range_key)
    if data is None:
        return jsonify({"error": f"No chart data for range: {range_key}"}), 404
    return jsonify(data)


@app.route("/api/holdings/allocation")
@token_required
def get_holdings_allocation(username):
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


# ── Portfolios page ────────────────────────────────────────────────────────────

@app.route('/api/portfolios', methods=['GET'])
@token_required
def pp_list_portfolios(username):
    return jsonify(list_portfolios(username)), 200


@app.route('/api/portfolios/upload', methods=['POST'])
@token_required
def pp_upload_portfolio(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    port_type   = request.form.get('type', '').strip() or None
    description = request.form.get('description', '').strip() or None

    if port_type == 'tracked':
        name = ('Positions ' + datetime.date.today().strftime('%Y-%m-%d'))
    elif port_type == 'adhoc':
        name = request.form.get('name', '').strip()
        if not name:
            return jsonify({'error': 'Portfolio name is required'}), 400
    else:
        name = request.form.get('name', '').strip()
        if not name:
            return jsonify({'error': 'Portfolio name is required'}), 400

    try:
        entry = pp_upload_portfolio_impl(username, name, request, account_id,
                                         port_type=port_type, description=description)
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    return jsonify(entry), 201


@app.route('/api/portfolios/<pid>', methods=['DELETE'])
@token_required
def pp_delete_portfolio(username, pid):
    if not pp_delete_portfolio_impl(pid, username):
        return jsonify({'error': 'Not found'}), 404
    return jsonify({'ok': True}), 200


@app.route('/api/portfolios/<pid>/download', methods=['GET'])
@token_required
def pp_download_portfolio(username, pid):
    from database2 import pg_connection
    from dashboard.upload_portfolio import get_portfolio_file_path

    # ── Step 1: fetch the portfolio_info row and check permission ────────────
    # Tracked portfolios belong to an account — access is via account_access.
    # Adhoc (and legacy, port_type IS NULL) portfolios belong to whoever
    # uploaded them — access is via matching client_id on the "user" table.
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT filename, client_id, account_id, port_type
                FROM portfolio_info
                WHERE port_id = %s
                """,
                (pid,),
            )
            row = cur.fetchone()
    if not row:
        return jsonify({'error': 'Not found'}), 404
    filename, client_id, account_id, port_type = row

    with pg_connection() as conn:
        with conn.cursor() as cur:
            if port_type == 'tracked':
                cur.execute(
                    """
                    SELECT 1
                    FROM account_access aa
                    JOIN "user" u ON u.user_id = aa.user_id
                    WHERE aa.account_id = %s AND u.username = %s
                    """,
                    (account_id, username),
                )
            else:
                cur.execute(
                    """
                    SELECT 1
                    FROM "user"
                    WHERE username = %s AND client_id = %s
                    """,
                    (username, client_id),
                )
            has_access = cur.fetchone() is not None
    if not has_access:
        return jsonify({'error': 'Access denied'}), 403

    # ── Step 2: resolve the file path directly from portfolio_info ───────────
    file_path = get_portfolio_file_path(client_id, filename)
    if not file_path.exists():
        return jsonify({'error': 'File not found on server'}), 404
    return send_from_directory(file_path.parent, file_path.name, as_attachment=True)


@app.route('/api/portfolios/<int:port_id>/clone', methods=['POST'])
@token_required
def pp_clone_portfolio(username, port_id):
    from dashboard.upload_portfolio import clone_portfolio as _clone_portfolio_impl
    body = request.get_json(silent=True) or {}
    name = (body.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'Portfolio name is required'}), 400
    target_weights = body.get('weights') or None
    try:
        new_port_id = _clone_portfolio_impl(port_id, name, username, target_weights=target_weights)
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    return jsonify({'port_id': new_port_id}), 201


@app.route('/api/broker/settings', methods=['GET'])
@token_required
def pp_list_broker_settings(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    try:
        return jsonify(get_broker_settings(account_id)), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/broker/settings/<int:sid>', methods=['DELETE'])
@token_required
def pp_delete_broker_setting(username, sid):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    if not delete_broker_setting(account_id, sid, deleted_by=username):
        return jsonify({'error': 'Not found or access denied'}), 404
    return jsonify({'ok': True}), 200


@app.route('/api/broker/feeds', methods=['GET'])
@token_required
def pp_list_broker_feeds(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    return jsonify(list_broker_feeds(account_id)), 200


@app.route('/api/broker/request-auth', methods=['POST'])
@token_required
def pp_request_broker_auth(username):
    data    = request.get_json() or {}
    broker  = data.get('broker',     '').strip()
    account = data.get('account',    '').strip()
    acc_id  = data.get('account_id')
    name    = (data.get('name') or '').strip() or None
    if not broker:
        return jsonify({'error': 'Broker is required'}), 400
    if not account:
        return jsonify({'error': 'Account number is required'}), 400
    if not acc_id:
        return jsonify({'error': 'account_id is required'}), 400
    if not user_has_account_access(username, acc_id):
        return jsonify({'error': 'Access denied'}), 403
    try:
        entry = create_broker_setting(username, acc_id, broker, account, name)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    return jsonify({'ok': True, 'setting': entry}), 201


@app.route('/api/portfolios/tracked', methods=['GET'])
@token_required
def pp_list_tracked_portfolios(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    return jsonify(list_tracked_portfolios(account_id)), 200


@app.route('/api/portfolios/adhoc', methods=['GET'])
@token_required
def pp_list_adhoc_portfolios(username):
    return jsonify(list_adhoc_portfolios(username)), 200


@app.route('/api/position-history', methods=['GET'])
@token_required
def pp_position_history(username):
    account_id, err = _get_account_id(username=username)
    if err:
        return err
    return jsonify(list_position_history(account_id)), 200


# ── What-If Analysis ──────────────────────────────────────────────────────────

@app.route('/api/whatif/portfolios', methods=['GET'])
@token_required
def whatif_portfolios(username):
    try:
        account_id = request.args.get('account_id', type=int)
        return jsonify(get_whatif_portfolios(username, account_id)), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/whatif/portfolio/<int:port_id>/allocations', methods=['GET'])
@token_required
def whatif_allocations(username, port_id):
    return jsonify(get_whatif_allocations(port_id)), 200


@app.route('/api/whatif/alternatives/positions', methods=['GET'])
@token_required
def whatif_alternatives_route(username):
    account_id = request.args.get('account_id', type=int)
    return jsonify(get_whatif_alt_positions(account_id)), 200


@app.route('/api/whatif/alternatives/calculate', methods=['POST'])
@token_required
def whatif_alternatives_calculate_route(username):
    body = request.get_json(silent=True) or {}
    account_id = request.args.get('account_id', type=int)
    positions  = body.get('positions', [])
    try:
        return jsonify(post_whatif_alternatives_calculate(account_id, positions)), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/whatif/alternatives/panel', methods=['GET'])
@token_required
def whatif_alternatives_panel_route(username):
    account_id = request.args.get('account_id', type=int)
    try:
        return jsonify(get_whatif_alternatives_panel(account_id)), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/whatif/portfolio/<int:port_id>/metrics', methods=['POST'])
@token_required
def whatif_metrics_route(username, port_id):
    body = request.get_json(silent=True) or {}
    weights = body.get('weights', {})
    if not weights:
        return jsonify({'error': 'weights required'}), 400
    try:
        return jsonify(post_whatif_metrics(port_id, weights)), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
