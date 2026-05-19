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

from database2 import pg_connection
from dashboard.portfolio_allocation import (
    _fetch_flat,
    _fetch_flat_port,
    _latest_as_of_date,
    build_alloc_slices,
)

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
    {'id': 'BCX51', 'name': 'Apollo Debt BDC Offshore',  'subclass': 'Private Credit',    'alloc': 4.0,  'corrEq': -0.05, 'corrFi': 0.55, 'capCall': 0.0},
    {'id': 'BDP71', 'name': 'Blackstone BXPE (TE)',       'subclass': 'Private Equity',    'alloc': 4.0,  'corrEq':  0.65, 'corrFi': 0.10, 'capCall': 0.0},
    {'id': 'BCQ06', 'name': 'Blue Owl Cred Inc Corp',     'subclass': 'Private Credit',    'alloc': 4.0,  'corrEq': -0.02, 'corrFi': 0.60, 'capCall': 0.0},
    {'id': 'BCJ30', 'name': 'Blackstone BCRED-O',         'subclass': 'Private Credit',    'alloc': 3.9,  'corrEq':  0.05, 'corrFi': 0.50, 'capCall': 0.0},
    {'id': 'VNQ',   'name': 'Vanguard Real Estate ETF',   'subclass': 'Real Estate/REITs', 'alloc': 1.6,  'corrEq':  0.72, 'corrFi': 0.15, 'capCall': 0.0},
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

    def _to_entry(row, weights):
        port_id, name, as_of_date, mv, exp_ret, vol_pct, var_1d_95, sharpe_vol, sharpe_var = row
        size = round(float(mv) / 1_000_000, 2) if mv else 0.0
        # _calc_conc needs numeric values; treat None as 0, skip if no data at all.
        conc_input = {k: (weights.get(k) or 0) for k in _CLASS_KEYS}
        conc = _calc_conc(conc_input) if any(conc_input.values()) else None
        return {
            'id':        port_id,
            'name':      name,
            'as_of_date': as_of_date.strftime('%d/%m/%y') if as_of_date else '—',
            'size':      size,
            'weights':   weights,
            'exp_ret':   round(float(exp_ret), 4) if exp_ret is not None else None,
            'vol':       round(float(vol_pct), 4) if vol_pct is not None else None,
            'vrisk':     round(float(var_1d_95), 2) if var_1d_95 is not None else None,
            'sharpeVol': round(float(sharpe_vol), 4) if sharpe_vol is not None else None,
            'sharpeVar': round(float(sharpe_var), 4) if sharpe_var is not None else None,
            'conc':      conc,
        }

    result = [
        _to_entry(row, weights_by_port.get(row[0], _NULL_WEIGHTS.copy()))
        for row in rows
    ]
    if account_row is not None:
        result.insert(0, _to_entry(account_row, account_weights or _NULL_WEIGHTS.copy()))
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


def get_whatif_alternatives(port_id: int) -> list:
    """Return illiquid/alternatives positions for a portfolio (mock in phase 1)."""
    return MOCK_ALTS
