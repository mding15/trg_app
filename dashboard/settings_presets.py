import math
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from database2 import pg_connection
from dashboard.settings_limits import _CONC_KEYS, _RISK_KEYS, _ALLOC_KEYS

_SECTION_MAP = (
    ("concentration", set(_CONC_KEYS)),
    ("risk",          set(_RISK_KEYS)),
    ("alloc",         set(_ALLOC_KEYS)),
)


def _fetch_latest_aum(account_id):
    """Return most recent AUM for account in dollars, or None if unavailable."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT aum FROM db_portfolio_summary "
                "WHERE account_id = %s ORDER BY as_of_date DESC LIMIT 1",
                (account_id,),
            )
            row = cur.fetchone()
    return float(row[0]) if row and row[0] is not None else None


def get_presets(account_id):
    """Return {preset_name: {concentration: {...}, risk: {...}, alloc: {...}}}.

    var_limit_dollar is computed as ceil(AUM * var_limit_pct / 100 / 1_000_000)
    and returned in $m. Set to None if AUM is unavailable.
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT preset_name, limit_category, limit_value FROM risk_preset")
            rows = cur.fetchall()

    presets = {}
    for preset_name, limit_category, limit_value in rows:
        if preset_name not in presets:
            presets[preset_name] = {"concentration": {}, "risk": {}, "alloc": {}}
        for section, keys in _SECTION_MAP:
            if limit_category in keys:
                presets[preset_name][section][limit_category] = float(limit_value)
                break

    aum = _fetch_latest_aum(account_id)
    for preset in presets.values():
        var_limit_pct = preset["risk"].get("var_limit_pct")
        if aum is not None and var_limit_pct is not None:
            raw = aum * var_limit_pct / 100
            if raw < 1_000_000:
                preset["risk"]["var_limit_dollar"] = math.ceil(raw / 100_000) / 10
            else:
                preset["risk"]["var_limit_dollar"] = math.ceil(raw / 1_000_000)
        else:
            preset["risk"]["var_limit_dollar"] = None

    return presets


def test(account_id=1003):
    import json
    result = get_presets(account_id)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    import sys
    aid = int(sys.argv[1]) if len(sys.argv) > 1 else 1003
    test(aid)
