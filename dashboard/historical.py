import math

from database2 import pg_connection

# ── Risk model ─────────────────────────────────────────────────────────────────
_RISK = {
    'fi':  {'vol': 4.2,  'vrisk': 0.68, 'sp': 0.45, 'sv': 0.62},
    'eq':  {'vol': 14.8, 'vrisk': 1.15, 'sp': 0.72, 'sv': 0.58},
    'alt': {'vol': 9.2,  'vrisk': 0.92, 'sp': 0.61, 'sv': 0.54},
    'ma':  {'vol': 7.5,  'vrisk': 0.85, 'sp': 0.55, 'sv': 0.51},
    'mm':  {'vol': 0.8,  'vrisk': 0.12, 'sp': 0.12, 'sv': 0.18},
}
_CORR = {
    'fi_eq': -0.15, 'fi_alt': 0.10, 'fi_ma': 0.05, 'fi_mm': 0.02,
    'eq_alt': 0.45, 'eq_ma': 0.60,  'eq_mm': -0.05,
    'alt_ma': 0.35, 'alt_mm': 0.02, 'ma_mm': 0.01,
}
_CLASS_KEYS = ['fi', 'eq', 'alt', 'ma', 'mm']

# ── Display labels ─────────────────────────────────────────────────────────────
# Equity listed first so it receives the primary bar color on asset allocation charts.
_ASSET_CLASS_DISPLAY = [
    {'key': 'eq',  'label': 'Equity'},
    {'key': 'fi',  'label': 'Fixed Income'},
    {'key': 'alt', 'label': 'Alternatives'},
    {'key': 'ma',  'label': 'Multi-Asset'},
    {'key': 'mm',  'label': 'Money Market'},
]
_REGION_LABELS   = ['North America', 'Europe', 'Asia Pacific', 'Emerging Mkts', 'Other']
_CURRENCY_LABELS = ['USD', 'EUR', 'GBP', 'JPY', 'Other']

# ── Static mock snapshots ──────────────────────────────────────────────────────
_SNAPSHOTS = [
    {'name': 'FIAE – Apr 2024', 'date': '30/04/24', 'size': 177.66, 'weights': {'fi': 45, 'eq': 20, 'alt': 9,  'ma': 20, 'mm': 6 }},
    {'name': 'FIAE – Jun 2024', 'date': '30/06/24', 'size': 180.10, 'weights': {'fi': 25, 'eq': 50, 'alt': 9,  'ma': 10, 'mm': 6 }},
    {'name': 'FIAE – Sep 2024', 'date': '30/09/24', 'size': 182.50, 'weights': {'fi': 35, 'eq': 30, 'alt': 25, 'ma': 5,  'mm': 5 }},
    {'name': 'FIAE – Dec 2024', 'date': '31/12/24', 'size': 185.00, 'weights': {'fi': 20, 'eq': 20, 'alt': 10, 'ma': 40, 'mm': 10}},
    {'name': 'FIAE – Apr 2026', 'date': '10/04/26', 'size': 190.00, 'weights': {'fi': 40, 'eq': 35, 'alt': 15, 'ma': 5,  'mm': 5 }},
]

# ── Static region / currency data (per snapshot, per label) ───────────────────
_REGION_ALLOC   = [[52,18,14,10,6],[38,25,20,12,5],[60,15,12,8,5],[35,30,18,12,5],[48,20,16,11,5]]
_REGION_VAR     = [[58,15,13,9,5], [42,22,18,13,5],[65,12,11,7,5],[40,28,16,11,5],[54,18,14,9,5]]
_CURRENCY_ALLOC = [[55,20,10,8,7], [42,28,12,10,8],[62,18,8,7,5], [38,25,15,12,10],[50,22,12,9,7]]
_CURRENCY_VAR   = [[60,18,9,8,5],  [46,25,11,10,8],[67,15,7,6,5], [42,22,14,12,10],[55,20,11,8,6]]

# ── Concentration base ratios ──────────────────────────────────────────────────
_CONC_BASE = {
    'asset_class': 3.6,
    'region':      2.8,
    'currency':    2.2,
    'industry':    1.5,
    'single_name': 0.4,
}

# ── Allocation / gauge limit defaults ─────────────────────────────────────────
_ALLOC_LIMIT_DEFAULTS = {
    'asset_class':  30.0,
    'region':       55.0,
    'region_var':   60.0,
    'industry':     25.0,
    'industry_var': 25.0,
    'currency':     60.0,
    'currency_var': 65.0,
}
_GAUGE_DEFAULTS = {'var_pct': 7.0, 'raer': 0.15}


# ── Metric computation ─────────────────────────────────────────────────────────

def _calc_metrics(w):
    var_sum = 0.0
    for i, ki in enumerate(_CLASS_KEYS):
        wi = w.get(ki, 0) / 100
        var_sum += wi * wi * _RISK[ki]['vol'] ** 2
        for kj in _CLASS_KEYS[i + 1:]:
            wj  = w.get(kj, 0) / 100
            ck  = f'{ki}_{kj}'
            r   = _CORR.get(ck, _CORR.get(f'{kj}_{ki}', 0.0))
            var_sum += 2 * wi * wj * _RISK[ki]['vol'] * _RISK[kj]['vol'] * r

    vol          = math.sqrt(max(var_sum, 0.0))
    weighted_vol = sum(w.get(k, 0) / 100 * _RISK[k]['vol'] for k in _CLASS_KEYS) or 1.0
    div_factor   = vol / weighted_vol
    var_val      = sum(w.get(k, 0) / 100 * _RISK[k]['vol'] * _RISK[k]['vrisk'] for k in _CLASS_KEYS) * div_factor
    sharpe_vol   = sum(w.get(k, 0) / 100 * _RISK[k]['sp'] for k in _CLASS_KEYS) * (1 + (1 - div_factor) * 0.3)
    sharpe_var   = sum(w.get(k, 0) / 100 * _RISK[k]['sv'] for k in _CLASS_KEYS) * (1 + (1 - div_factor) * 0.25)

    total_var_w  = sum(w.get(k, 0) / 100 * _RISK[k]['vol'] * _RISK[k]['vrisk'] for k in _CLASS_KEYS) or 1.0
    vol_contrib  = {k: round(w.get(k, 0) / 100 * _RISK[k]['vol'] / (vol or 1) * 100, 1) for k in _CLASS_KEYS}
    var_contrib  = {k: round(w.get(k, 0) / 100 * _RISK[k]['vol'] * _RISK[k]['vrisk'] / total_var_w * 100, 1) for k in _CLASS_KEYS}

    return {
        'vol':        round(vol, 1),
        'var':        round(var_val, 1),
        'sharpe_vol': round(sharpe_vol, 2),
        'sharpe_var': round(sharpe_var, 2),
        'vol_contrib': vol_contrib,
        'var_contrib': var_contrib,
    }


def _calc_concentration(w):
    vals      = [w.get(k, 0) / 100 for k in _CLASS_KEYS]
    hhi       = sum(v * v for v in vals)
    equal_hhi = 1.0 / len(_CLASS_KEYS)
    scale     = hhi / equal_hhi
    return {dim: round(_CONC_BASE[dim] * scale, 2) for dim in _CONC_BASE}


# ── Limit reader ───────────────────────────────────────────────────────────────

def _read_limits(account_id):
    try:
        from dashboard.settings_limits import read_account_limits
        db   = read_account_limits(account_id)
        conc = db.get('concentration', {})
        risk = db.get('risk', {})
    except Exception:
        conc, risk = {}, {}

    return {
        'allocation': dict(_ALLOC_LIMIT_DEFAULTS),
        'gauge': {
            'var_pct': risk.get('var_limit_pct') or _GAUGE_DEFAULTS['var_pct'],
            'raer':    _GAUGE_DEFAULTS['raer'],
        },
        'concentration': {
            'asset_class': conc.get('con_limit_asset_pct')    or 4.0,
            'region':      conc.get('con_limit_region_pct')   or 3.0,
            'currency':    conc.get('con_limit_currency_pct') or 2.5,
            'industry':    conc.get('con_limit_industry_pct') or 2.0,
            'single_name': conc.get('con_limit_name_pct')     or 0.5,
        },
    }


# ── DB helpers (phase-1 charts) ────────────────────────────────────────────────

def _read_historical_db(account_id: int) -> list[dict]:
    """Return up to 5 weekly db_portfolio_summary rows in chronological order."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT as_of_date FROM db_portfolio_summary "
                "WHERE account_id = %s ORDER BY as_of_date DESC",
                (account_id,),
            )
            all_dates = [row[0] for row in cur.fetchall()]

    if not all_dates:
        return []

    # Greedy weekly selection: pick a date only if ≥7 days before the last picked
    selected, last_picked = [], None
    for d in all_dates:
        if last_picked is None or (last_picked - d).days >= 7:
            selected.append(d)
            last_picked = d
            if len(selected) == 5:
                break

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT as_of_date, aum, day_pnl, volatility,
                       var_1d_95, sharpe_vol, sharpe_var
                FROM db_portfolio_summary
                WHERE account_id = %s AND as_of_date = ANY(%s)
                ORDER BY as_of_date ASC
                """,
                (account_id, selected),
            )
            cols = ['as_of_date', 'aum', 'day_pnl', 'volatility',
                    'var_1d_95', 'sharpe_vol', 'sharpe_var']
            return [dict(zip(cols, row)) for row in cur.fetchall()]


_CONC_CATEGORIES = ['Asset Class', 'Region', 'Currency', 'Industry', 'Single Name']


def _read_concentrations_db(account_id: int, dates: list) -> dict:
    """Return {category: [ratio_or_None per date]} for the given dates."""
    if not dates:
        return {cat: [] for cat in _CONC_CATEGORIES}

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT as_of_date, category, ratio
                FROM db_concentrations
                WHERE account_id = %s AND as_of_date = ANY(%s)
                """,
                (account_id, dates),
            )
            rows = cur.fetchall()

    lookup = {(r[0], r[1]): (float(r[2]) if r[2] is not None else None) for r in rows}
    return {
        cat: [lookup.get((d, cat)) for d in dates]
        for cat in _CONC_CATEGORIES
    }


def _read_limits_db(account_id: int) -> dict:
    """Return {var_limit_dollar: float|None, target_sharpe_var: float|None}."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT limit_category, limit_value FROM account_limit "
                "WHERE account_id = %s AND limit_category = ANY(%s)",
                (account_id, ['var_limit_dollar', 'target_sharpe_var']),
            )
            rows = cur.fetchall()
    return {r[0]: float(r[1]) for r in rows if r[1] is not None}


# ── DB helpers (phase-2 breakdown charts) ─────────────────────────────────────

_PINNED_FIRST = {
    'asset_class': 'Equity',
    'region':      'North America',
    'currency':    'USD',
}


def _order_categories(breakdown_type: str, cats: set, pin_override: str | None = None) -> list:
    """Sort categories alphabetically, moving the pinned-first category to the front.

    pin_override takes precedence over _PINNED_FIRST for dynamic pinning.
    """
    pinned = pin_override if pin_override is not None else _PINNED_FIRST.get(breakdown_type)
    ordered = sorted(c for c in cats if c != pinned)
    if pinned and pinned in cats:
        ordered.insert(0, pinned)
    return ordered


def _read_breakdowns_db(account_id: int, dates: list) -> dict:
    """Return {breakdown_type: {category: {weight: [...pct], var_contrib: [...pct]}}}."""
    if not dates:
        return {}
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT as_of_date, breakdown_type, category, weight, var_contrib
                FROM db_portfolio_breakdown
                WHERE account_id = %s AND as_of_date = ANY(%s)
                """,
                (account_id, dates),
            )
            rows = cur.fetchall()
    if not rows:
        return {}

    lookup = {}
    cats_by_type = {}
    for as_of_date, breakdown_type, category, weight, var_contrib in rows:
        lookup[(breakdown_type, as_of_date, category)] = (
            round(float(weight) * 100, 2) if weight is not None else None,
            round(float(var_contrib) * 100, 2) if var_contrib is not None else None,
        )
        cats_by_type.setdefault(breakdown_type, set()).add(category)

    # For industry, pin the category with the largest weight on the latest date
    latest = dates[-1]
    industry_cats = cats_by_type.get('industry', set())
    industry_pin = (
        max(industry_cats, key=lambda c: lookup.get(('industry', latest, c), (None, None))[0] or 0)
        if industry_cats else None
    )

    result = {}
    for btype, cats in cats_by_type.items():
        pin = industry_pin if btype == 'industry' else None
        ordered = _order_categories(btype, cats, pin_override=pin)
        result[btype] = {
            cat: {
                'weight':      [lookup.get((btype, d, cat), (None, None))[0] for d in dates],
                'var_contrib': [lookup.get((btype, d, cat), (None, None))[1] for d in dates],
            }
            for cat in ordered
        }
    return result


# ── Public entry point ─────────────────────────────────────────────────────────

def get_historical_data(account_id):
    limits    = _read_limits(account_id)
    rows      = _read_historical_db(account_id)
    db_limits = _read_limits_db(account_id)
    alloc_lim = limits['allocation']

    if not rows:
        empty = {'series': []}
        return {
            'labels': [],
            'charts': {k: empty for k in [
                'volatility', 'var', 'sharpeVol', 'sharpeVar', 'pnlVsVar', 'concentrations',
                'assetAlloc', 'assetVar', 'regionAlloc', 'regionVar',
                'industryAlloc', 'industryVar', 'ccyAlloc', 'ccyVar',
            ]},
        }

    db_labels = [r['as_of_date'].strftime('%m/%d/%y') for r in rows]
    dates     = [r['as_of_date'] for r in rows]

    # VaR K/M unit — determined by the first non-None var_1d_95 value
    var_vals  = [r['var_1d_95'] for r in rows]
    first_var = next((v for v in var_vals if v is not None), None)
    var_unit    = 'K' if (first_var is not None and first_var < 1_000_000) else 'M'
    var_divisor = 1_000 if var_unit == 'K' else 1_000_000

    def _scale(v):
        return round(float(v) / var_divisor, 1) if v is not None else None

    var_limit_raw    = db_limits.get('var_limit_dollar')
    var_limit_scaled = round(float(var_limit_raw) / var_divisor, 1) if var_limit_raw else None
    sharpe_var_target = db_limits.get('target_sharpe_var') or 0.15

    conc_data = _read_concentrations_db(account_id, dates)
    bd        = _read_breakdowns_db(account_id, dates)

    def _bd_series(breakdown_type, metric):
        cats = bd.get(breakdown_type, {})
        return [{'name': cat, 'data': cats[cat][metric]} for cat in cats]

    return {
        'labels': db_labels,
        'charts': {
            'volatility': {
                'series': [{'name': 'Volatility', 'data': [r['volatility'] for r in rows]}],
            },
            'var': {
                'series': [{'name': 'VaR 95% 1D', 'data': [_scale(v) for v in var_vals]}],
                'limit':  var_limit_scaled,
                'unit':   var_unit,
            },
            'sharpeVol': {
                'series': [{'name': 'Sharpe Ratio (Vol)', 'data': [r['sharpe_vol'] for r in rows]}],
            },
            'sharpeVar': {
                'series': [{'name': 'Sharpe Ratio (VaR)', 'data': [r['sharpe_var'] for r in rows]}],
                'target': sharpe_var_target,
            },
            'pnlVsVar': {
                'series': [
                    {'name': f'PnL (${var_unit})',      'data': [_scale(r['day_pnl'])   for r in rows]},
                    {'name': f'VaR 95% (${var_unit})',  'data': [-_scale(r['var_1d_95']) if _scale(r['var_1d_95']) is not None else None for r in rows]},
                ],
                'unit': var_unit,
            },
            'concentrations': {
                'series': [
                    {'name': cat, 'data': conc_data[cat]}
                    for cat in _CONC_CATEGORIES
                    if cat in conc_data
                ],
            },
            'assetAlloc': {
                'series':     _bd_series('asset_class', 'weight'),
                'limit':      alloc_lim['asset_class'],
                'limitLabel': f"Equity limit {alloc_lim['asset_class']:.0f}%",
            },
            'assetVar': {
                'series':     _bd_series('asset_class', 'var_contrib'),
                'limit':      alloc_lim['asset_class'],
                'limitLabel': f"Equity limit {alloc_lim['asset_class']:.0f}%",
            },
            'regionAlloc': {
                'series':     _bd_series('region', 'weight'),
                'limit':      alloc_lim['region'],
                'limitLabel': f"N.America limit {alloc_lim['region']:.0f}%",
            },
            'regionVar': {
                'series':     _bd_series('region', 'var_contrib'),
                'limit':      alloc_lim['region_var'],
                'limitLabel': f"N.America limit {alloc_lim['region_var']:.0f}%",
            },
            'industryAlloc': {
                'series':     _bd_series('industry', 'weight'),
                'limit':      alloc_lim['industry'],
                'limitLabel': f"Top sector limit {alloc_lim['industry']:.0f}%",
            },
            'industryVar': {
                'series':     _bd_series('industry', 'var_contrib'),
                'limit':      alloc_lim['industry_var'],
                'limitLabel': f"Top sector limit {alloc_lim['industry_var']:.0f}%",
            },
            'ccyAlloc': {
                'series':     _bd_series('currency', 'weight'),
                'limit':      alloc_lim['currency'],
                'limitLabel': f"USD limit {alloc_lim['currency']:.0f}%",
            },
            'ccyVar': {
                'series':     _bd_series('currency', 'var_contrib'),
                'limit':      alloc_lim['currency_var'],
                'limitLabel': f"USD limit {alloc_lim['currency_var']:.0f}%",
            },
        },
    }
