"""
security_name_parser.py — Extract structured fields from free-text
security_name strings, as a regex-based rule engine organized by security
type.

Design:
    parse_security(name, asset_class, asset_type, is_option) is the entry
    point. It uses the EXISTING classification columns from position_var
    (asset_class / asset_type / is_option) to decide which type-specific
    parser to run — it does not try to guess the type from the name text.
    Each type-specific parser (parse_option, parse_bond, ...) returns
    whatever fields apply to that type; fields that don't apply, or that
    couldn't be extracted, are None.

    This is meant to grow over time: add a new parse_<type>() function and
    a new branch in parse_security() for each new security type / field you
    want to support. Nothing here guesses — if a name doesn't match any
    known pattern, `parsed=False` and the fields stay None, so gaps are
    visible rather than silently wrong.

Fields currently extracted:
    Option : underlying_ticker, option_type (Call/Put), strike, maturity_date
    Bond   : issuer, coupon_rate (as a percentage number, e.g. 3.375 for
             3.375%), maturity_date

Usage (as a library):
    from security.security_name_parser import parse_security
    result = parse_security(name, asset_class, asset_type, is_option)
"""
from __future__ import annotations

import re
from datetime import date


# ── Shared helpers ────────────────────────────────────────────────────────────

_DATE_RE = re.compile(r'(\d{1,2})/(\d{1,2})/(\d{2,4})')


def _to_date(mm: str, dd: str, yy: str) -> date | None:
    mm_i, dd_i, yy_i = int(mm), int(dd), int(yy)
    if yy_i < 100:
        yy_i += 2000 if yy_i < 70 else 1900
    try:
        return date(yy_i, mm_i, dd_i)
    except ValueError:
        return None


def _empty(*fields: str) -> dict:
    return {f: None for f in fields}


# ── Options ───────────────────────────────────────────────────────────────────
# e.g. "Call SPY 3/19/27 730", "CALL VIX    02/17/27    24.000"

_OPTION_RE = re.compile(
    r'^\s*(?P<type>CALL|PUT)\s+'
    r'(?P<underlying>[A-Z][A-Z.]*)\s+'
    r'(?P<mm>\d{1,2})/(?P<dd>\d{1,2})/(?P<yy>\d{2,4})\s+'
    r'(?P<strike>\d+(?:\.\d+)?)\s*$',
    re.IGNORECASE,
)

_OPTION_FIELDS = ('option_type', 'underlying_ticker', 'strike', 'maturity_date')


def parse_option(name: str) -> dict:
    result = {**_empty(*_OPTION_FIELDS), 'parsed': False}
    if not name:
        return result
    m = _OPTION_RE.match(name)
    if not m:
        return result
    result['option_type'] = m.group('type').capitalize()
    result['underlying_ticker'] = m.group('underlying')
    result['strike'] = float(m.group('strike'))
    result['maturity_date'] = _to_date(m.group('mm'), m.group('dd'), m.group('yy'))
    result['parsed'] = True
    return result


# ── Bonds ─────────────────────────────────────────────────────────────────────
# General shape: "<issuer>[-<ticker>] <coupon>[%] <maturity>[ '<yy>][ <tag>]*"
# Coupon can be a decimal ("1.400", "4.5", "4"), a fraction ("3 3/8", "4 1/2"),
# or the literal "BILL" for zero-coupon T-bills — with or without a trailing
# "%" (attached or space-separated). Usually appears right before the
# maturity date, but occasionally right after it instead (_COUPON_HEAD_RE).
# Trailing tokens after the maturity date ('27, MTN, FRN, USD, rating parens)
# are ignored for now — they don't feed any of the requested fields yet.

_COUPON_TAIL_RE = re.compile(
    r'(?P<coupon>\d+\s+\d+/\d+|\d+/\d+|\d+(?:\.\d+)?|BILL)\s*%?\s*$',
    re.IGNORECASE,
)
_COUPON_HEAD_RE = re.compile(
    r'^\s*(?P<coupon>\d+\s+\d+/\d+|\d+/\d+|\d+(?:\.\d+)?)\s*%'
)

_BOND_FIELDS = ('issuer', 'coupon_rate', 'maturity_date')


def _coupon_to_number(raw: str) -> float:
    """Convert '4', '4.5', '4 1/2', '3/8', or 'BILL' (zero-coupon T-bill) to
    a plain percentage number."""
    raw = raw.strip()
    if raw.upper() == 'BILL':
        return 0.0
    if '/' in raw:
        parts = raw.split()
        if len(parts) == 2:
            whole, frac = parts
            num, den = frac.split('/')
            return float(whole) + float(num) / float(den)
        num, den = raw.split('/')
        return float(num) / float(den)
    return float(raw)


def parse_bond(name: str) -> dict:
    result = {**_empty(*_BOND_FIELDS), 'parsed': False}
    if not name:
        return result

    date_match = _DATE_RE.search(name)
    if not date_match:
        return result  # e.g. bond funds/ETFs with no coupon/maturity in the name

    before = name[:date_match.start()].rstrip()
    after = name[date_match.end():]

    coupon_match = _COUPON_TAIL_RE.search(before)
    if coupon_match:
        result['issuer'] = before[:coupon_match.start()].rstrip(' -')
        result['coupon_rate'] = _coupon_to_number(coupon_match.group('coupon'))
        result['maturity_date'] = _to_date(*date_match.groups())
        result['parsed'] = True
        return result

    # Reversed layout: coupon appears after the maturity date instead of before
    # (e.g. "JPMORGAN CHASE & CO. HYBRID 06/01/2028 2.182%").
    coupon_match = _COUPON_HEAD_RE.match(after)
    if coupon_match:
        result['issuer'] = before.rstrip(' -')
        result['coupon_rate'] = _coupon_to_number(coupon_match.group('coupon'))
        result['maturity_date'] = _to_date(*date_match.groups())
        result['parsed'] = True
        return result

    return result


# ── Dispatcher ────────────────────────────────────────────────────────────────

def parse_security(name: str, asset_class: str | None, asset_type: str | None,
                    is_option: bool | None) -> dict:
    """Dispatch to the right type-specific parser based on the EXISTING
    classification columns (not guessed from the name), and return a dict
    with a uniform set of keys across all types (unused fields are None)."""
    fields = {
        'security_type':     None,
        'underlying_ticker': None,
        'option_type':       None,
        'strike':            None,
        'issuer':            None,
        'coupon_rate':       None,
        'maturity_date':     None,
        'parsed':            False,
    }

    # Try the option pattern whenever the flag says so, OR whenever the name
    # itself unambiguously matches (a leading "CALL"/"PUT" is unambiguous —
    # nothing else in this data starts that way). The latter matters for
    # brand-new securities where is_option hasn't been set yet either.
    option_result = parse_option(name) if name else {'parsed': False}
    if is_option or option_result.get('parsed'):
        fields['security_type'] = 'Option'
        fields.update(option_result)
        return fields

    if asset_class == 'Bond':
        fields['security_type'] = 'Bond'
        fields.update(parse_bond(name))
        return fields

    # Not yet handled (Equity, Alternative, Structured Note, Cash, ...) —
    # add a parse_<type>() above and a branch here to extend.
    fields['security_type'] = asset_class or asset_type or None
    return fields
