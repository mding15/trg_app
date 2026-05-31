"""
dashboard/whatif.py — What-If Analysis endpoints.

Routes (registered in routes.py):
    GET  /api/whatif/portfolios
    GET  /api/whatif/portfolio/<port_id>/allocations
    GET  /api/whatif/portfolio/<port_id>/alternatives
    POST /api/whatif/portfolio/<port_id>/metrics
"""
from __future__ import annotations

import math
import os
import sys
# When run directly (python dashboard/whatif.py), Python adds dashboard/ to sys.path
# but not trg_app/. This inserts trg_app/ so that database2 and dashboard.* resolve.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd

from database2 import pg_connection
from dashboard.portfolio_allocation import (
    _fetch_flat,
    _fetch_flat_port,
    _latest_as_of_date,
    build_alloc_slices,
)
from dashboard.concentration_db import read_concentrations
from dashboard.concentration_calc import compute_concentrations, load_limits
from dashboard.allocation_drilldown import get_alloc_drilldown_data
from models import alternative_model
from process2 import var_engine

# ── Risk model parameters (edit here to update the model) ────────────────────

_CLASS_KEYS = ['fi', 'eq', 'alt', 'ma', 'mm']

_RISK = {
    'fi':  {'vol': 4.2,  'vrisk': 0.68, 'sp': 0.45, 'sv': 0.62},
    'eq':  {'vol': 14.8, 'vrisk': 1.15, 'sp': 0.72, 'sv': 0.58},
    'alt': {'vol': 9.2,  'vrisk': 0.92, 'sp': 0.61, 'sv': 0.54},
    'ma':  {'vol': 7.5,  'vrisk': 0.85, 'sp': 0.55, 'sv': 0.51},
    'mm':  {'vol': 0.8,  'vrisk': 0.12, 'sp': 0.12, 'sv': 0.18},
}

_CORR = {
    ('fi',  'eq'):  -0.15,
    ('fi',  'alt'):  0.10,
    ('fi',  'ma'):   0.05,
    ('fi',  'mm'):   0.02,
    ('eq',  'alt'):  0.45,
    ('eq',  'ma'):   0.60,
    ('eq',  'mm'): -0.05,
    ('alt', 'ma'):   0.35,
    ('alt', 'mm'):   0.02,
    ('ma',  'mm'):   0.01,
}

_CONC_ROWS = [
    {'name': 'Asset Class', 'base': 3.6, 'limit': 4.0},
    {'name': 'Region',      'base': 2.8, 'limit': 3.0},
    {'name': 'Currency',    'base': 2.2, 'limit': 2.5},
    {'name': 'Industry',    'base': 1.5, 'limit': 2.0},
    {'name': 'Single Name', 'base': 0.4, 'limit': 0.5},
]


# ── Calculation functions ─────────────────────────────────────────────────────

def _calc_metrics(weights: dict) -> dict:
    keys = _CLASS_KEYS
    var_sum = 0.0
    for i, ki in enumerate(keys):
        wi = weights.get(ki, 0) / 100.0
        var_sum += wi * wi * _RISK[ki]['vol'] ** 2
        for kj in keys[i + 1:]:
            wj = weights.get(kj, 0) / 100.0
            r  = _CORR.get((ki, kj), _CORR.get((kj, ki), 0.0))
            var_sum += 2 * wi * wj * _RISK[ki]['vol'] * _RISK[kj]['vol'] * r

    vol     = math.sqrt(max(var_sum, 0))
    wtd_vol = sum(weights.get(k, 0) / 100.0 * _RISK[k]['vol'] for k in keys) or 1.0
    div     = vol / wtd_vol

    vrisk     = sum(weights.get(k, 0) / 100.0 * _RISK[k]['vol'] * _RISK[k]['vrisk'] for k in keys)
    vrisk    *= div
    sharpe_vol = sum(weights.get(k, 0) / 100.0 * _RISK[k]['sp'] for k in keys) * (1 + (1 - div) * 0.3)
    sharpe_var = sum(weights.get(k, 0) / 100.0 * _RISK[k]['sv'] for k in keys) * (1 + (1 - div) * 0.25)

    return {
        'vol':       round(vol, 1),
        'vrisk':     round(vrisk, 1),
        'sharpeVol': round(sharpe_vol, 2),
        'sharpeVar': round(sharpe_var, 2),
    }


def _calc_conc(weights: dict) -> list:
    vals   = [weights.get(k, 0) / 100.0 for k in _CLASS_KEYS]
    hhi    = sum(w * w for w in vals)
    hhi_eq = 1.0 / len(_CLASS_KEYS)
    factor = hhi / hhi_eq if hhi_eq else 1.0
    return [
        {'name': row['name'], 'ratio': round(row['base'] * factor, 2), 'limit': row['limit']}
        for row in _CONC_ROWS
    ]


def _fetch_metrics(port_id: int) -> dict | None:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT vol_pct, var_1d_95, sharpe_vol, sharpe_var
                FROM whatif_portfolio_metrics
                WHERE port_id = %s
                """,
                (port_id,),
            )
            row = cur.fetchone()
    if row is None:
        return None
    vol_pct, var_1d_95, sharpe_vol, sharpe_var = row
    return {
        'vol':       round(float(vol_pct),   1) if vol_pct   is not None else None,
        'vrisk':     round(float(var_1d_95), 2) if var_1d_95 is not None else None,
        'sharpeVol': round(float(sharpe_vol), 2) if sharpe_vol is not None else None,
        'sharpeVar': round(float(sharpe_var), 2) if sharpe_var is not None else None,
    }


def post_whatif_metrics(port_id: int, weights: dict) -> dict:
    """Return risk metrics and concentration ratios for a given weight vector."""
    db = _fetch_metrics(port_id) or {}
    return {
        'vol':       db.get('vol'),
        'vrisk':     db.get('vrisk'),
        'sharpeVol': db.get('sharpeVol'),
        'sharpeVar': db.get('sharpeVar'),
        'conc':      _calc_conc(weights),
    }

# ── Null weight vector returned when no position data is available ────────────

_NULL_WEIGHTS = {'fi': None, 'eq': None, 'alt': None, 'ma': None, 'mm': None, 'ot': None}

# ── Mock allocation drill data ────────────────────────────────────────────────

MOCK_ALLOC = {
    'asset_class': {
        'all': {
            'parent': None, 'parentLabel': 'Asset Class',
            'rows': [
                {'label': 'Equity',       'mv': 35.0, 'var': 45.6, 'child': 'eq-sub'},
                {'label': 'Fixed Income', 'mv': 28.9, 'var': 18.4, 'child': 'fi-sub'},
                {'label': 'Money Market', 'mv': 14.7, 'var':  6.2, 'child': None},
                {'label': 'Alternatives', 'mv': 12.1, 'var': 20.8, 'child': 'alt-sub'},
                {'label': 'Cash',         'mv':  9.3, 'var':  9.0, 'child': None},
            ],
        },
        'eq-sub': {
            'parent': 'all', 'parentLabel': 'Equity',
            'rows': [
                {'label': 'Large Cap', 'mv': 52, 'var': 65, 'child': None},
                {'label': 'Mid Cap',   'mv': 22, 'var': 18, 'child': None},
                {'label': 'Small Cap', 'mv': 14, 'var': 10, 'child': None},
                {'label': 'Intl',      'mv': 12, 'var':  7, 'child': None},
            ],
        },
        'fi-sub': {
            'parent': 'all', 'parentLabel': 'Fixed Income',
            'rows': [
                {'label': 'Govt Bonds', 'mv': 38, 'var': 28, 'child': None},
                {'label': 'Corp IG',    'mv': 32, 'var': 30, 'child': None},
                {'label': 'Corp HY',    'mv': 18, 'var': 26, 'child': None},
                {'label': 'MBS',        'mv': 12, 'var': 16, 'child': None},
            ],
        },
        'alt-sub': {
            'parent': 'all', 'parentLabel': 'Alternatives',
            'rows': [
                {'label': 'Private Equity', 'mv': 45, 'var': 50, 'child': None},
                {'label': 'Hedge Funds',    'mv': 35, 'var': 32, 'child': None},
                {'label': 'Real Estate',    'mv': 20, 'var': 18, 'child': None},
            ],
        },
    },
    'region': {
        'all': {
            'parent': None, 'parentLabel': 'Region',
            'rows': [
                {'label': 'North America', 'mv': 55.2, 'var': 60.1, 'child': 'na-sub'},
                {'label': 'Europe',        'mv': 20.8, 'var': 18.3, 'child': None},
                {'label': 'Asia Pacific',  'mv': 14.0, 'var': 13.5, 'child': None},
                {'label': 'Emerging Mkt',  'mv':  7.5, 'var':  6.4, 'child': None},
                {'label': 'Other',         'mv':  2.5, 'var':  1.7, 'child': None},
            ],
        },
        'na-sub': {
            'parent': 'all', 'parentLabel': 'North America',
            'rows': [
                {'label': 'USA',    'mv': 82, 'var': 85, 'child': None},
                {'label': 'Canada', 'mv': 12, 'var':  9, 'child': None},
                {'label': 'Mexico', 'mv':  6, 'var':  6, 'child': None},
            ],
        },
    },
    'industry': {
        'all': {
            'parent': None, 'parentLabel': 'Industry',
            'rows': [
                {'label': 'Technology',  'mv': 28.5, 'var': 38.2, 'child': None},
                {'label': 'Financials',  'mv': 18.2, 'var': 15.1, 'child': None},
                {'label': 'Healthcare',  'mv': 12.4, 'var': 10.8, 'child': None},
                {'label': 'Consumer',    'mv': 11.0, 'var':  9.3, 'child': None},
                {'label': 'Industrials', 'mv':  8.6, 'var':  7.2, 'child': None},
                {'label': 'Energy',      'mv':  7.3, 'var':  9.8, 'child': None},
                {'label': 'Other',       'mv': 14.0, 'var':  9.6, 'child': None},
            ],
        },
    },
    'currency': {
        'all': {
            'parent': None, 'parentLabel': 'Currency',
            'rows': [
                {'label': 'USD',   'mv': 68.4, 'var': 72.1, 'child': None},
                {'label': 'EUR',   'mv': 14.2, 'var': 12.3, 'child': None},
                {'label': 'GBP',   'mv':  7.8, 'var':  7.0, 'child': None},
                {'label': 'JPY',   'mv':  5.2, 'var':  4.8, 'child': None},
                {'label': 'Other', 'mv':  4.4, 'var':  3.8, 'child': None},
            ],
        },
    },
}

# ── Mock alternatives / illiquids ─────────────────────────────────────────────

MOCK_ALTS = [
    {'id': 'BCX51', 'security_id': 'T0001', 'name': 'Apollo Debt BDC Offshore',  'subclass': 'Private Credit',    'corr': -0.05, 'index': 'Bloomberg US Agg',  'alt_exposure':  2_500_000, 'market_value':  2_500_000},
    {'id': 'BDP71', 'security_id': 'T0002', 'name': 'Blackstone BXPE (TE)',       'subclass': 'Private Equity',    'corr':  0.65, 'index': 'S&P 500',           'alt_exposure':  4_800_000, 'market_value':  4_800_000},
    {'id': 'BCQ06', 'security_id': 'T0003', 'name': 'Blue Owl Cred Inc Corp',     'subclass': 'Private Credit',    'corr': -0.02, 'index': 'Bloomberg US Agg',  'alt_exposure':  3_200_000, 'market_value':  3_200_000},
    {'id': 'BCJ30', 'security_id': 'T0004', 'name': 'Blackstone BCRED-O',         'subclass': 'Private Credit',    'corr':  0.05, 'index': 'Bloomberg US Agg',  'alt_exposure':  1_750_000, 'market_value':  1_750_000},
    {'id': 'VNQ',   'security_id': 'T0005', 'name': 'Vanguard Real Estate ETF',   'subclass': 'Real Estate/REITs', 'corr':  0.72, 'index': 'FTSE NAREIT All',   'alt_exposure':  6_100_000, 'market_value':  6_100_000},
]

# ── Weight fetch helpers ──────────────────────────────────────────────────────

def _fetch_weights_batch(cur, port_ids: list) -> dict:
    """
    Single query: returns {port_id: {class_code: pct}} for all port_ids.
    Unmapped / NULL class values are bucketed as 'ot'.
    port_ids absent from port_position_var are omitted from the result.
    """
    if not port_ids:
        return {}
    cur.execute(
        """
        SELECT
            pv.port_id,
            COALESCE(acm.class_code, 'ot') AS class_code,
            SUM(pv.market_value)            AS mv
        FROM port_position_var pv
        LEFT JOIN asset_class_map acm ON acm.asset_class = pv."class"
        WHERE pv.port_id = ANY(%s)
          AND pv.market_value IS NOT NULL
        GROUP BY pv.port_id, COALESCE(acm.class_code, 'ot')
        """,
        (port_ids,),
    )
    raw: dict = {}
    for port_id, class_code, mv in cur.fetchall():
        raw.setdefault(port_id, {})[class_code] = float(mv)

    result = {}
    for port_id, class_mvs in raw.items():
        total = sum(class_mvs.values())
        if total == 0:
            continue
        result[port_id] = {code: round(mv / total * 100, 1) for code, mv in class_mvs.items()}
    return result


def _fetch_params_batch(cur, port_ids: list) -> dict:
    """Return {port_id: params dict} with risk settings for tracked and adhoc portfolios."""
    if not port_ids:
        return {}
    cur.execute(
        """
        SELECT pi.port_id,
               ap.risk_horizon, ap.risk_measure, ap.base_currency, ap.benchmark, ap.exp_return
        FROM portfolio_info pi
        JOIN account_parameters ap ON ap.account_id = pi.account_id
        WHERE pi.port_type = 'tracked'
          AND pi.port_id = ANY(%s)

        UNION ALL

        SELECT pi.port_id,
               pp."RiskHorizon", pp."TailMeasure", pp."BaseCurrency", pp."Benchmark", pp."ExpectedReturn"
        FROM portfolio_info pi
        JOIN port_parameters pp ON pp.port_id = pi.port_id
        WHERE pi.port_type = 'adhoc'
          AND pi.port_id = ANY(%s)
        """,
        (port_ids, port_ids),
    )
    return {
        row[0]: {
            'risk_horizon':  row[1],
            'risk_measure':  row[2],
            'base_currency': row[3],
            'benchmark':     row[4],
            'exp_return':    row[5],
        }
        for row in cur.fetchall()
    }


def _fetch_weights_account(cur, account_id: int, as_of_date) -> dict | None:
    """
    Single query: returns {class_code: pct} for the given account/date from position_var.
    Returns None when no rows are found.
    """
    cur.execute(
        """
        SELECT
            COALESCE(acm.class_code, 'ot') AS class_code,
            SUM(pv.market_value)            AS mv
        FROM position_var pv
        LEFT JOIN asset_class_map acm ON acm.asset_class = pv."class"
        WHERE pv.account_id = %s
          AND pv.as_of_date = %s
          AND pv.market_value IS NOT NULL
        GROUP BY COALESCE(acm.class_code, 'ot')
        """,
        (account_id, as_of_date),
    )
    rows = cur.fetchall()
    if not rows:
        return None
    class_mvs = {code: float(mv) for code, mv in rows}
    total = sum(class_mvs.values())
    if total == 0:
        return None
    return {code: round(mv / total * 100, 1) for code, mv in class_mvs.items()}


# ── Handler functions called from routes.py ───────────────────────────────────
# Fetches the user's processed portfolios to populate the What-If analysis page.
def get_whatif_portfolios(username: str, account_id: int | None = None) -> list:
    """Return the user's processed portfolios with real asset-class weights from position data."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.port_id, p.port_name, p.as_of_date, p.mv,
                       p.exp_ret, p.vol_pct, p.var_1d_95, p.sharpe_vol, p.sharpe_var
                FROM whatif_portfolio_metrics p
                JOIN "user" u ON p.client_id = u.client_id
                WHERE u.username = %s AND p.account_id IS NULL
                ORDER BY p.port_id DESC
                LIMIT 10
                """,
                (username,),
            )
            rows = cur.fetchall()

            account_row = None
            if account_id is not None:
                cur.execute(
                    """
                    SELECT port_id, port_name, as_of_date, mv,
                           exp_ret, vol_pct, var_1d_95, sharpe_vol, sharpe_var
                    FROM whatif_portfolio_metrics
                    WHERE account_id = %s
                    """,
                    (account_id,),
                )
                account_row = cur.fetchone()

            # Batch-fetch weights for all uploaded portfolios — single query.
            port_ids = [row[0] for row in rows]
            weights_by_port = _fetch_weights_batch(cur, port_ids)

            # Fetch weights for the account portfolio if present.
            account_weights = None
            if account_row is not None:
                account_weights = _fetch_weights_account(cur, account_id, account_row[2])

            # Batch-fetch risk parameters for all portfolios.
            all_port_ids = port_ids + ([account_row[0]] if account_row is not None else [])
            params_by_port = _fetch_params_batch(cur, all_port_ids)

    def _to_entry(row, weights, params=None):
        port_id, name, as_of_date, mv, exp_ret, vol_pct, var_1d_95, sharpe_vol, sharpe_var = row
        size = round(float(mv) / 1_000_000, 2) if mv else 0.0
        # _calc_conc needs numeric values; treat None as 0, skip if no data at all.
        conc_input = {k: (weights.get(k) or 0) for k in _CLASS_KEYS}
        conc = _calc_conc(conc_input) if any(conc_input.values()) else None
        p = params or {}
        return {
            'id':            port_id,
            'name':          name,
            'as_of_date':    as_of_date.strftime('%d/%m/%y') if as_of_date else '—',
            'size':          size,
            'weights':       weights,
            'exp_ret':       round(float(exp_ret), 4) if exp_ret is not None else None,
            'vol':           round(float(vol_pct), 4) if vol_pct is not None else None,
            'vrisk':         round(float(var_1d_95), 2) if var_1d_95 is not None else None,
            'sharpeVol':     round(float(sharpe_vol), 4) if sharpe_vol is not None else None,
            'sharpeVar':     round(float(sharpe_var), 4) if sharpe_var is not None else None,
            'conc':          conc,
            'risk_horizon':  p.get('risk_horizon'),
            'risk_measure':  p.get('risk_measure'),
            'base_currency': p.get('base_currency'),
            'benchmark':     p.get('benchmark'),
            'exp_return':    p.get('exp_return'),
        }

    result = [
        _to_entry(row, weights_by_port.get(row[0], _NULL_WEIGHTS.copy()), params_by_port.get(row[0]))
        for row in rows
    ]
    if account_row is not None:
        result.insert(0, _to_entry(account_row, account_weights or _NULL_WEIGHTS.copy(), params_by_port.get(account_row[0])))
    return result


_WHATIF_SLICES = ["asset", "region", "industry", "currency"]


def get_whatif_allocations(port_id: int) -> dict:
    """
    Return allocation drill-down data for the four What-If slices.
    Fetches from position_var (live) or port_position_var (ad-hoc).
    Returns {} if no data is available.
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT account_id FROM portfolio_info WHERE port_id = %s",
                (port_id,),
            )
            row = cur.fetchone()

        if row is None:
            return {}

        account_id = row[0]

        if account_id is not None:
            as_of_date = _latest_as_of_date(conn, account_id)
            if as_of_date is None:
                return {}
            df = _fetch_flat(conn, account_id, as_of_date)
        else:
            df = _fetch_flat_port(conn, port_id)

    if df.empty:
        return {}

    slices = build_alloc_slices(df, _WHATIF_SLICES)
    slices["asset_class"] = slices.pop("asset")
    return slices


def get_whatif_alt_positions(account_id: int | None) -> list:
    """Return illiquid/alternatives positions for an account from position_var + alternative_model."""
    if account_id is None:
        return MOCK_ALTS

    with pg_connection() as conn:
        as_of_date = _latest_as_of_date(conn, account_id)
        if as_of_date is None:
            return MOCK_ALTS

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pv.ticker, pv.security_id, pv.security_name, pv.sc1, am.proxy_correl, am.proxy_name, pv.market_value
                FROM position_var pv
                JOIN alternative_model am ON pv.security_id = am.security_id
                WHERE pv.account_id = %s
                  AND pv.as_of_date = %s
                  AND pv."class" = 'Alternatives'
                """,
                (account_id, as_of_date),
            )
            rows = cur.fetchall()

    if not rows:
        return MOCK_ALTS

    return [
        {
            'id':          r[0],
            'security_id': r[1],
            'name':        r[2],
            'subclass':    r[3],
            'corr':        float(r[4]) if r[4] is not None else 0.0,
            'index':       r[5],
            'alt_exposure':     float(r[6]) if r[6] is not None else 0.0,
        }
        for r in rows
    ]


# ── Alternatives panel constants (backend source of truth, phase 1) ───────────

_ALT_ORIGINAL = {
    'var':         16.9,
    'vol':          5.9,
    'beta':         1.20,
    'raer':         0.21,
    'var_limit':   25.0,
    'raer_target':  0.15,
    'raer_max':     0.65,
    'risk_max':    35.0,
}

_ALT_ALLOC_DATA = {
    'rows': [
        {'label': 'Equity',       'mv': 46.3, 'var': 55.6, 'child': 'eq-sub'},
        {'label': 'Fixed Income', 'mv': 18.9, 'var': 12.4, 'child': 'fi-sub'},
        {'label': 'Money Market', 'mv': 14.7, 'var':  6.2, 'child': 'mm-sub'},
        {'label': 'Alternatives', 'mv': 12.1, 'var': 16.8, 'child': 'alt-sub'},
        {'label': 'Commodities',  'mv':  5.0, 'var':  7.2, 'child': 'com-sub'},
        {'label': 'Cash',         'mv':  3.0, 'var':  1.8, 'child': None},
    ],
}

_ALT_ALLOC_DRILL = {
    'eq-sub':  {'parent': 'all', 'parentLabel': 'Equity', 'rows': [
        {'label': 'Large Cap', 'mv': 52,   'var': 65,   'child': 'eq-large'},
        {'label': 'Mid Cap',   'mv': 22,   'var': 18,   'child': None},
        {'label': 'Small Cap', 'mv': 14,   'var': 10,   'child': None},
        {'label': 'Intl',      'mv': 12,   'var':  7,   'child': None},
    ]},
    'fi-sub':  {'parent': 'all', 'parentLabel': 'Fixed Income', 'rows': [
        {'label': 'Govt Bonds', 'mv': 38, 'var': 28, 'child': None},
        {'label': 'Corp IG',    'mv': 32, 'var': 30, 'child': None},
        {'label': 'Corp HY',    'mv': 18, 'var': 26, 'child': None},
        {'label': 'MBS',        'mv': 12, 'var': 16, 'child': None},
    ]},
    'mm-sub':  {'parent': 'all', 'parentLabel': 'Money Market', 'rows': [
        {'label': 'T-Bills', 'mv': 55, 'var': 30, 'child': None},
        {'label': 'Repo',    'mv': 30, 'var': 50, 'child': None},
        {'label': 'MMF',     'mv': 15, 'var': 20, 'child': None},
    ]},
    'alt-sub': {'parent': 'all', 'parentLabel': 'Alternatives', 'rows': [
        {'label': 'Private Eq.',  'mv': 45, 'var': 50, 'child': 'alt-pe'},
        {'label': 'Hedge Funds',  'mv': 35, 'var': 32, 'child': None},
        {'label': 'Real Estate',  'mv': 20, 'var': 18, 'child': None},
    ]},
    'com-sub': {'parent': 'all', 'parentLabel': 'Commodities', 'rows': [
        {'label': 'Energy',      'mv': 42, 'var': 52, 'child': None},
        {'label': 'Metals',      'mv': 35, 'var': 30, 'child': None},
        {'label': 'Agriculture', 'mv': 23, 'var': 18, 'child': None},
    ]},
    'eq-large': {'parent': 'eq-sub', 'parentLabel': 'Large Cap', 'rows': [
        {'label': 'AAPL',  'mv': 14,   'var': 13.5, 'child': None},
        {'label': 'MSFT',  'mv': 11,   'var':  6.8, 'child': None},
        {'label': 'NVDA',  'mv':  9.2, 'var': 14.1, 'child': None},
        {'label': 'AMZN',  'mv':  7,   'var':  7.2, 'child': None},
        {'label': 'GOOGL', 'mv':  7.5, 'var':  3.8, 'child': None},
        {'label': 'Other', 'mv': 51.3, 'var': 54.6, 'child': None},
    ]},
    'alt-pe':  {'parent': 'alt-sub', 'parentLabel': 'Private Eq.', 'rows': [
        {'label': 'KKR XII',   'mv': 40, 'var': 45, 'child': None},
        {'label': 'BX IX',     'mv': 35, 'var': 38, 'child': None},
        {'label': 'Apollo XI', 'mv': 25, 'var': 17, 'child': None},
    ]},
}


def _fetch_positions_for_var(conn, account_id: int, as_of_date) -> pd.DataFrame:
    """Fetch position_var rows needed for VaR engine and portfolio aggregation."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pos_id, security_id, market_value, expected_return, beta,
                   "class" AS asset_class, region, currency, sector, ticker, security_name, sc1
            FROM position_var
            WHERE account_id = %s AND as_of_date = %s
            """,
            (account_id, as_of_date),
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


def _build_alloc_from_result(result_df: pd.DataFrame):
    """
    Aggregate VaR result DataFrame into (data_dict, drill_dict) for AllocVarCanvas.
    data_dict  = {'rows': [{'label': class, 'mv': %, 'var': %, 'child': key_or_None}]}
    drill_dict = {child_key: {'parent': 'all', 'parentLabel': class,
                              'rows': [{'label': sc1, 'mv': %, 'var': %, 'child': None}]}}
    Percentages are of total portfolio MV / total portfolio VaR.
    """
    df = result_df.copy()
    df['MarketValue'] = pd.to_numeric(df['MarketValue'], errors='coerce').fillna(0.0)
    df['mg_var_95']   = pd.to_numeric(df.get('mg_var_95', pd.Series(dtype=float)), errors='coerce').fillna(0.0)
    df['asset_class'] = df['asset_class'].fillna('Other')
    df['sc1']         = df['sc1'].fillna('Other') if 'sc1' in df.columns else 'Other'

    total_mv  = float(df['MarketValue'].sum())
    total_var = float(df['mg_var_95'].sum())
    if total_mv == 0:
        return _ALT_ALLOC_DATA, _ALT_ALLOC_DRILL

    def _slug(s: str) -> str:
        return s.lower().replace(' ', '-').replace('/', '-').replace('&', '').replace(',', '') + '-sub'

    by_class = (
        df.groupby('asset_class', dropna=False)
          .agg(mv=('MarketValue', 'sum'), var=('mg_var_95', 'sum'))
          .reset_index()
          .sort_values('mv', ascending=False)
    )

    rows  = []
    drill = {}

    for _, r in by_class.iterrows():
        cls       = str(r['asset_class'])
        mv_p      = round(float(r['mv'])  / total_mv  * 100, 1)
        var_p     = round(float(r['var']) / total_var * 100, 1) if total_var else 0.0
        child_key = _slug(cls)

        sub = df[df['asset_class'] == cls]
        sub_by_sc1 = (
            sub.groupby('sc1', dropna=False)
               .agg(mv=('MarketValue', 'sum'), var=('mg_var_95', 'sum'))
               .reset_index()
               .sort_values('mv', ascending=False)
        )
        sub_total_mv  = float(sub['MarketValue'].sum())
        sub_total_var = float(sub['mg_var_95'].sum())

        drill_rows = []
        for _, sr in sub_by_sc1.iterrows():
            sc1    = str(sr['sc1'])
            smv_p  = round(float(sr['mv'])  / sub_total_mv  * 100, 1) if sub_total_mv  > 0 else 0.0
            svar_p = round(float(sr['var']) / sub_total_var * 100, 1) if sub_total_var > 0 else 0.0
            drill_rows.append({'label': sc1, 'mv': smv_p, 'var': svar_p, 'child': None})

        has_child = len(drill_rows) > 1
        rows.append({'label': cls, 'mv': mv_p, 'var': var_p, 'child': child_key if has_child else None})
        if has_child:
            drill[child_key] = {'parent': 'all', 'parentLabel': cls, 'rows': drill_rows}

    return {'rows': rows}, drill


def post_whatif_alternatives_calculate(account_id: int | None, positions: list) -> dict:
    """Return modified risk metrics for adjusted alternative positions."""
    _stub_fallback = None
    try:
        if account_id is not None:
            with pg_connection() as conn:
                base = _fetch_alt_original(conn, account_id)
        else:
            base = _ALT_ORIGINAL
        _stub_fallback = {
            'var':  round(base['var']  * 1.1, 1),
            'vol':  round(base['vol']  * 1.1, 1),
            'beta': round(base['beta'] * 1.1, 2),
            'raer': round(base['raer'] * 1.1, 2),
        }
    except Exception:
        _stub_fallback = {k: round(v * 1.1, 2) for k, v in _ALT_ORIGINAL.items() if k in ('var', 'vol', 'beta', 'raer')}

    if not positions or account_id is None:
        return _stub_fallback

    try:
        # Step 1: build correl dict and get adhoc PnL distributions
        correl    = {p['security_id']: p['corr'] for p in positions if p.get('security_id')}
        adhoc_pnl = alternative_model.alternative_model_adhoc(correl)
        if adhoc_pnl.empty:
            return _stub_fallback

        # Step 2: fetch all positions from position_var
        with pg_connection() as conn:
            as_of_date = _latest_as_of_date(conn, account_id)
        if as_of_date is None:
            return _stub_fallback

        with pg_connection() as conn:
            df = _fetch_positions_for_var(conn, account_id, as_of_date)
        if df.empty:
            return _stub_fallback

        # Step 3: override market_value with alt_exposure where security_id matches
        exposure_map = {
            p['security_id']: p['alt_exposure']
            for p in positions
            if p.get('security_id') and p.get('alt_exposure') is not None
        }
        if exposure_map:
            mask = df['security_id'].isin(exposure_map)
            df.loc[mask, 'market_value'] = df.loc[mask, 'security_id'].map(exposure_map)

        # Step 4a: compute concentrations on snake_case df (before VaR engine rename)
        df['market_value'] = pd.to_numeric(df['market_value'], errors='coerce').fillna(0.0)
        try:
            with pg_connection() as conn:
                limits = load_limits(conn, account_id)
            concentrations = compute_concentrations(account_id, as_of_date, df, limits)
        except Exception as e:
            print(f'[whatif] concentrations failed: {e}')
            concentrations = []

        # Step 4b: rename for VaR engine and run calc_var
        engine_df = df.rename(columns={'security_id': 'SecurityID', 'market_value': 'MarketValue'})
        var_metrics = var_engine.calc_var(engine_df, adhoc_pnl=adhoc_pnl)
        result = engine_df.set_index('pos_id').join(var_metrics, how='left').reset_index()

        # Step 5: aggregate to portfolio level
        mv       = pd.to_numeric(result['MarketValue'], errors='coerce').fillna(0.0)
        aum      = float(mv.sum())
        if aum == 0:
            return _stub_fallback

        def _sum(col):
            s = pd.to_numeric(result[col], errors='coerce').sum()
            return float(s) if not pd.isna(s) else None

        var_1d_95 = _sum('mg_var_95')
        sum_mstd  = _sum('mg_std')

        vol = round(float(sum_mstd) / aum * math.sqrt(252) * 100, 4) if sum_mstd else None

        default_beta = result['asset_class'].map({'Cash': 0.0, 'Fixed Income': 0.5}).fillna(1.0)
        beta_series  = pd.to_numeric(result['beta'], errors='coerce').fillna(default_beta)
        beta = round(float((mv * beta_series).sum() / aum), 4)

        er           = pd.to_numeric(result['expected_return'], errors='coerce')
        total_return = float((mv * er).sum()) / aum if er.notna().any() else None

        if total_return is not None and var_1d_95 and var_1d_95 != 0:
            sharpe_var = round(total_return / (var_1d_95 / aum) / math.sqrt(252), 4)
        else:
            sharpe_var = None

        # Step 6: build asset-allocation chart data from the modified result
        try:
            alloc_data, alloc_drill = _build_alloc_from_result(result)
            alloc = {'data': alloc_data, 'drill': alloc_drill}
        except Exception as e:
            print(f'[whatif] alloc build failed: {e}')
            alloc = None

        return {
            'var':            round(var_1d_95, 2) if var_1d_95 is not None else _stub_fallback['var'],
            'vol':            round(vol, 4)       if vol       is not None else _stub_fallback['vol'],
            'beta':           beta,
            'raer':           sharpe_var          if sharpe_var is not None else _stub_fallback['raer'],
            'concentrations': concentrations,
            'alloc':          alloc,
        }

    except Exception as e:
        print(f'[whatif] alternatives/calculate Phase 3 failed: {e}')
        return _stub_fallback


def _fetch_alt_original(conn, account_id: int) -> dict:
    """Fetch live risk metrics and limits for the Alternatives panel; falls back to _ALT_ORIGINAL."""
    as_of_date = _latest_as_of_date(conn, account_id)
    if as_of_date is None:
        return _ALT_ORIGINAL

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT var_1d_95, volatility, beta, sharpe_vol
            FROM db_portfolio_summary
            WHERE account_id = %s AND as_of_date = %s
            """,
            (account_id, as_of_date),
        )
        row = cur.fetchone()

    if row is None:
        return _ALT_ORIGINAL

    var, vol, beta, raer = (float(v) if v is not None else None for v in row)
    if any(v is None for v in (var, vol, beta, raer)):
        return _ALT_ORIGINAL

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT limit_category, limit_value
            FROM account_limit
            WHERE account_id = %s
              AND limit_category IN ('var_limit_dollar', 'target_sharpe_vol')
            """,
            (account_id,),
        )
        limits = {r[0]: float(r[1]) for r in cur.fetchall() if r[1] is not None}

    var_limit   = limits.get('var_limit_dollar',  _ALT_ORIGINAL['var_limit'])
    raer_target = limits.get('target_sharpe_vol', _ALT_ORIGINAL['raer_target'])
    risk_max    = round(max(var,  var_limit)   * 1.4, 1)
    raer_max    = round(max(raer, raer_target) * 1.4, 2)

    return {
        'var':         round(var,        1),
        'vol':         round(vol,        1),
        'beta':        round(beta,       2),
        'raer':        round(raer,       2),
        'var_limit':   round(var_limit,  1),
        'raer_target': round(raer_target, 2),
        'risk_max':    risk_max,
        'raer_max':    raer_max,
    }


def _fetch_alt_alloc(account_id: int):
    """Return (data, drill) for the AllocVarCanvas chart from live position_var data.
    Falls back to the mock constants if no DB rows are found."""
    drilldown = get_alloc_drilldown_data(account_id)
    if not drilldown or 'all' not in drilldown:
        return _ALT_ALLOC_DATA, _ALT_ALLOC_DRILL
    data  = {'rows': drilldown['all']['rows']}
    drill = {k: v for k, v in drilldown.items() if k != 'all'}
    return data, drill


def get_whatif_alternatives_panel(account_id: int | None) -> dict:
    """Return Alternatives tab panel data: base risk metrics, concentrations, alloc/VaR breakdown."""
    if account_id is not None:
        with pg_connection() as conn:
            original = _fetch_alt_original(conn, account_id)
    else:
        original = _ALT_ORIGINAL

    concentrations = read_concentrations(account_id) if account_id is not None else []
    alloc_data, alloc_drill = _fetch_alt_alloc(account_id) if account_id is not None else (_ALT_ALLOC_DATA, _ALT_ALLOC_DRILL)

    return {
        'original': original,
        'concentrations': concentrations,
        'alloc': {
            'data':  alloc_data,
            'drill': alloc_drill,
        },
    }

# ── Tests ─────────────────────────────────────────────────────────────────────

_TESTS = {}

def _test(name):
    """Decorator that registers a named test."""
    def decorator(fn):
        _TESTS[name] = fn
        return fn
    return decorator


@_test('alt_original')
def _test_alt_original(account_id, _portfolios):
    print("=== _fetch_alt_original ===")
    with pg_connection() as conn:
        result = _fetch_alt_original(conn, account_id)
    for k, v in result.items():
        print(f'  {k:<14} = {v}')


@_test('portfolios')
def _test_portfolios(account_id, _portfolios):
    print("=== get_whatif_portfolios ===")
    portfolios = get_whatif_portfolios("testuser", account_id=account_id)
    for p in portfolios:
        print(p)


@_test('allocations')
def _test_allocations(_account_id, portfolios):
    print("=== get_whatif_allocations ===")
    result = get_whatif_allocations(portfolios[0]['id'])
    print(result)


@_test('alternatives')
def _test_alternatives(account_id, _portfolios):
    print("=== get_whatif_alt_positions ===")
    result = get_whatif_alt_positions(account_id)
    for r in result:
        print(r)


@_test('metrics')
def _test_metrics(_account_id, portfolios):
    print("=== post_whatif_metrics ===")
    weights = {'fi': 30, 'eq': 40, 'alt': 20, 'ma': 5, 'mm': 5}
    result = post_whatif_metrics(portfolios[0]['id'], weights)
    print(result)


@_test('alloc')
def _test_alloc(account_id, _portfolios):
    print("=== _fetch_alt_alloc ===")
    data, drill = _fetch_alt_alloc(account_id)
    print(f"  top-level rows ({len(data['rows'])}):")
    for r in data['rows']:
        print(f"    {r['label']:<20} mv={r['mv']:6.2f}%  var={r['var']:6.2f}%  child={r['child']}")
    print(f"  drill keys: {list(drill.keys())}")


@_test('panel')
def _test_panel(account_id, _portfolios):
    print("=== get_whatif_alternatives_panel ===")
    result = get_whatif_alternatives_panel(account_id)
    print(f"  original:       {result['original']}")
    print(f"  concentrations: {result['concentrations']}")
    print(f"  alloc keys:     {list(result['alloc'].keys())}")


def test(names=None, account_id=1011):
    """Run one or more named tests. Runs all if names is None."""
    to_run = list(_TESTS.keys()) if not names else names
    invalid = [n for n in to_run if n not in _TESTS]
    if invalid:
        print(f"Unknown test(s): {', '.join(invalid)}")
        print(f"Available: {', '.join(_TESTS)}")
        return

    # Pre-fetch portfolios once — needed by allocations and metrics tests
    portfolios = get_whatif_portfolios("testuser", account_id=account_id)

    for name in to_run:
        _TESTS[name](account_id, portfolios)
        print()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='What-If analysis function tests')
    parser.add_argument(
        'tests', nargs='*',
        help=f'Tests to run (default: all). Choices: {", ".join(_TESTS)}',
    )
    parser.add_argument(
        '--account', type=int, default=1011,
        help='account_id to use (default: 1011)',
    )
    args = parser.parse_args()
    test(names=args.tests or None, account_id=args.account)
