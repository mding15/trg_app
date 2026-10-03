# -*- coding: utf-8 -*-
"""
maintenance_routes.py — Endpoints for operational/maintenance use (pulling raw
model data off the server, etc.), gated by @ops_role_required
(admin/superadmin/support). Client-side scripts live in maintenance/.

POST   api/maint/dist                   {rf_ids: [...], category: 'PRICE', model_id: None}  -> CSV
POST   api/maint/dist/upload            multipart: file=<csv>, category, model_id, dry_run  (superadmin)
POST   api/maint/account/<id>/recalc    ?date=YYYY-MM-DD  -> 202, runs process2/run_tracked_account.py  (superadmin)
"""
import io
import re
import datetime
import threading

import pandas as pd
from flask import request, jsonify, make_response

from trg_config import config
from api import app
from api.auth import ops_role_required
from api.logging_config import get_logger
from database.models import User
from database2 import pg_connection
from utils import var_utils, hdf_utils

logger = get_logger(__name__)

MODEL_ID_RE = re.compile(r'^[A-Za-z0-9_]+$')
CATEGORY_ALIAS = {'DELTA': 'PRICE', 'VEGA': 'VOL'}   # same mapping as var_utils.get_dist


def _resolve_var_file(model_id):
    """VaR HDF file for model_id (None -> the server's current model).

    Deliberately does not call var_utils.set_model_id(): that swaps a module
    global and would change the model for every other request in this process.
    """
    if not model_id:
        return var_utils.get_VaR_file()
    if not MODEL_ID_RE.match(model_id):
        raise ValueError(f'invalid model_id: {model_id!r}')
    var_file = config['VaR_DIR'] / f'VaR.{model_id}.h5'
    if not var_file.exists():
        raise ValueError(f'model file not found: {var_file.name}')
    return var_file


def _is_superadmin(username):
    user = User.query.filter_by(username=username).first()
    return bool(user and user.role == 'superadmin')


def _model_id_of(var_file):
    return var_file.stem.split('.', 1)[1]   # VaR.M_20251231.h5 -> M_20251231


@app.route('/api/maint/dist', methods=['POST'])
@ops_role_required
def maint_get_dist(username):
    """Return var_utils.get_dist(rf_ids, category) as CSV (index + one column per rf_id).

    IDs not found in the HDF store are simply absent from the columns.
    """
    body = request.get_json(silent=True) or {}
    rf_ids = body.get('rf_ids')
    category = body.get('category', 'PRICE')

    if not rf_ids or not isinstance(rf_ids, list):
        return jsonify({'error': 'rf_ids must be a non-empty list'}), 400
    rf_ids = [str(x) for x in rf_ids]

    try:
        var_file = _resolve_var_file(body.get('model_id'))
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    category = CATEGORY_ALIAS.get(category, category)
    logger.info(f'{username}: get_dist {var_file.name} category={category} n_ids={len(rf_ids)}')
    try:
        dist = hdf_utils.read(rf_ids, category, var_file)
    except Exception as e:
        logger.exception('get_dist failed')
        return jsonify({'error': str(e)}), 500

    buf = io.StringIO()
    dist.index.name = 'index'
    dist.to_csv(buf, index=True)

    resp = make_response(buf.getvalue())
    resp.headers['Content-Type'] = 'text/csv'
    return resp


@app.route('/api/maint/dist/upload', methods=['POST'])
@ops_role_required
def maint_upload_dist(username):
    """Save the uploaded distribution CSV (index + one column per rf_id) into the VaR HDF file.

    Same validation as var_utils.save_dist (row count must equal the model's META length),
    plus: values must be numeric with no NaN. Existing series that get overwritten are
    first backed up to VaR_DIR/backup/dist.<model>.<category>.<timestamp>.csv, which can
    be re-uploaded to undo. dry_run=true validates and reports without writing.
    """
    if not _is_superadmin(username):
        return jsonify({'error': 'Superadmin role required'}), 403

    upload = request.files.get('file')
    if upload is None:
        return jsonify({'error': 'missing file'}), 400
    category = request.form.get('category', 'PRICE')
    category = CATEGORY_ALIAS.get(category, category)
    dry_run = request.form.get('dry_run', '').lower() in ('1', 'true', 'yes')

    try:
        var_file = _resolve_var_file(request.form.get('model_id'))
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    if category == 'META':
        return jsonify({'error': 'cannot upload to META category'}), 400

    try:
        dist = pd.read_csv(upload, index_col=0)
    except Exception as e:
        return jsonify({'error': f'cannot parse CSV: {e}'}), 400
    dist.columns = dist.columns.astype(str)

    if dist.empty:
        return jsonify({'error': 'CSV has no data'}), 400
    if dist.columns.duplicated().any():
        dups = sorted(set(dist.columns[dist.columns.duplicated()]))
        return jsonify({'error': f'duplicate rf_ids: {dups}'}), 400
    non_numeric = [c for c in dist.columns if not pd.api.types.is_numeric_dtype(dist[c])]
    if non_numeric:
        return jsonify({'error': f'non-numeric columns: {non_numeric}'}), 400
    has_nan = dist.columns[dist.isna().any()].tolist()
    if has_nan:
        return jsonify({'error': f'columns with NaN: {has_nan}'}), 400

    meta = hdf_utils.read(['length'], 'META', var_file)
    length = int(meta['length'].iloc[0])
    if len(dist) != length:
        return jsonify({'error': f'distribution length ({len(dist)}) does not match model length ({length})'}), 400

    rf_ids = dist.columns.tolist()
    existing = hdf_utils.read(rf_ids, category, var_file)
    overwritten = [c for c in rf_ids if c in set(map(str, existing.columns))]
    new = [c for c in rf_ids if c not in set(overwritten)]

    result = {
        'model_id': _model_id_of(var_file),
        'category': category,
        'length': length,
        'new': new,
        'overwritten': overwritten,
        'backup_file': None,
        'dry_run': dry_run,
    }
    logger.info(f'{username}: upload_dist {var_file.name} category={category} '
                f'new={len(new)} overwritten={len(overwritten)} dry_run={dry_run}')
    if dry_run:
        return jsonify(result), 200

    try:
        if overwritten:
            backup_dir = config['VaR_DIR'] / 'backup'
            backup_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = backup_dir / f'dist.{result["model_id"]}.{category}.{ts}.csv'
            backup = existing[overwritten]
            backup.index.name = 'index'
            backup.to_csv(backup_file, index=True)
            result['backup_file'] = str(backup_file)

        hdf_utils.save(dist.reset_index(drop=True), category, var_file)
    except Exception as e:
        logger.exception('upload_dist failed')
        return jsonify({'error': str(e), **result}), 500

    return jsonify(result), 200


@app.route('/api/maint/account/<int:account_id>/recalc', methods=['POST'])
@ops_role_required
def maint_recalc_account(username, account_id):
    """Run the account-level pipeline (process2/run_tracked_account.py) for one tracked
    account in the background: tracked_proc → parent merge → VaR → alternative VaR →
    stress test → dashboard. Returns 202 right away; the outcome goes to
    ../log/run_tracked_account_<id>_<date>_<ts>.log and the API log.
    """
    if not _is_superadmin(username):
        return jsonify({'error': 'Superadmin role required'}), 403

    as_of_date = request.args.get('date') or None
    if as_of_date:
        try:
            datetime.datetime.strptime(as_of_date, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': f'invalid date: {as_of_date!r} (expected YYYY-MM-DD)'}), 400

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT 1 FROM account WHERE account_id = %s', (account_id,))
            if cur.fetchone() is None:
                return jsonify({'error': f'account_id {account_id} not found'}), 404

    def _run():
        from process2.run_tracked_account import launch
        ok, message = launch(account_id, as_of_date)
        (logger.info if ok else logger.error)(f'{username}: recalc account_id={account_id} date={as_of_date}: {message}')

    logger.info(f'{username}: recalc account_id={account_id} date={as_of_date} started')
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({
        'message': 'account recalculation started',
        'account_id': account_id,
        'as_of_date': as_of_date,
        'log': f'log/run_tracked_account_{account_id}_*.log',
    }), 202
