# -*- coding: utf-8 -*-
"""
ops_routes.py — All endpoints used by the trg_ops internal portal, gated by
@ops_role_required (admin/superadmin/support), split out of routes.py to keep
that file from growing without bound.

api_request() (the shared route dispatcher used by the SUPPORT section below)
lives in api/dispatch.py, imported by both this file and routes.py — neither
route-definition file depends on the other.
"""
import sqlalchemy.exc

from flask import request, jsonify

from api import app
from api.auth import ops_role_required, authenticate
from api.logging_config import get_logger
from api.dispatch import api_request
from account import account_mgmt

logger = get_logger(__name__)

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


#####################################################################################
# NEW SECURITY SETUP (ops) — see security/new_security.py, which this reuses
# as a library (also used by maintenance/process_new_security.py and
# maintenance/insert_new_security.py, the CLI entry points for this workflow).

@app.route('/api/ops/new-security/scan', methods=['GET'])
@ops_role_required
def ops_new_security_scan(username):
    from security.new_security import _fetch_new_securities, write_csv, df_to_records
    try:
        df = _fetch_new_securities()
        csv_path = write_csv(df) if not df.empty else None
        return jsonify({'rows': df_to_records(df), 'csv_path': str(csv_path) if csv_path else None}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/ops/new-security/insert', methods=['POST'])
@ops_role_required
def ops_new_security_insert(username):
    import pandas as pd
    from database2 import pg_connection
    from security.new_security import validate_and_normalize, process_rows, write_results_csv

    data = request.get_json() or {}
    rows = data.get('rows')
    dry_run = bool(data.get('dry_run', True))
    if not rows:
        return jsonify({'error': 'rows is required'}), 400

    try:
        df = validate_and_normalize(pd.DataFrame(rows))
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    insert_logger = get_logger('ops_new_security_insert')
    try:
        with pg_connection() as conn:
            with conn.cursor() as cur:
                results = process_rows(cur, df, dry_run, insert_logger)
            if not dry_run:
                conn.commit()
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    if not dry_run and results:
        csv_path = write_results_csv(results)
        return jsonify({'results': results, 'csv_path': str(csv_path)}), 200

    return jsonify({'results': results, 'csv_path': None}), 200


#####################################################################################
# SECURITY (ops) — read-only browse of security_info

@app.route('/api/ops/securities', methods=['GET'])
@ops_role_required
def ops_get_securities(username):
    from security.security_info import get_securities_with_ref
    from security.new_security import df_to_records
    try:
        df = get_securities_with_ref().drop(columns=['id'])
        return jsonify(df_to_records(df)), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
