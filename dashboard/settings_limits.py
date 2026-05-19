from database2 import pg_connection

_CONC_KEYS = [
    "con_limit_asset_pct",
    "con_limit_region_pct",
    "con_limit_currency_pct",
    "con_limit_industry_pct",
    "con_limit_name_pct",
    "var_con_asset_pct",
    "var_con_region_pct",
    "var_con_currency_pct",
    "var_con_industry_pct",
    "var_con_name_pct",
]

_RISK_KEYS = [
    "var_limit_pct",
    "var_limit_dollar",
    "vol_limit_pct",
]

_ALLOC_KEYS = [
    "alloc_equity_pct",
    "alloc_fixedincome_pct",
    "alloc_alternative_pct",
    "alloc_multiasset_pct",
    "alloc_cash_pct",
    "var_alloc_equity_pct",
    "var_alloc_fixedincome_pct",
    "var_alloc_alternative_pct",
    "var_alloc_multiasset_pct",
    "var_alloc_cash_pct",
]

ALL_KEYS = _CONC_KEYS + _RISK_KEYS + _ALLOC_KEYS


def read_account_limits(account_id):
    """Return {concentration: {...}, risk: {...}} from account_limit for the given account."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT limit_category, limit_value FROM account_limit "
                "WHERE account_id = %s AND limit_category = ANY(%s)",
                (account_id, ALL_KEYS),
            )
            rows = {row[0]: float(row[1]) for row in cur.fetchall()}

    return {
        "concentration": {k: rows.get(k) for k in _CONC_KEYS},
        "risk":          {k: rows.get(k) for k in _RISK_KEYS},
        "alloc":         {k: rows.get(k) for k in _ALLOC_KEYS},
    }


def write_account_limits(account_id, values):
    """Archive current rows to account_limit_history, then replace with new values.

    values: {concentration: {key: val, ...}, risk: {key: val, ...}}
    """
    flat = {}
    for section in ("concentration", "risk", "alloc"):
        flat.update(values.get(section) or {})

    with pg_connection() as conn:
        with conn.cursor() as cur:
            # Archive current rows
            cur.execute(
                "INSERT INTO account_limit_history (account_id, limit_category, limit_value, valid_from) "
                "SELECT account_id, limit_category, limit_value, now() "
                "FROM account_limit WHERE account_id = %s",
                (account_id,),
            )
            # Remove old rows
            cur.execute("DELETE FROM account_limit WHERE account_id = %s", (account_id,))
            # Insert new rows (skip None values)
            rows_to_insert = [
                (account_id, k, v)
                for k, v in flat.items()
                if k in ALL_KEYS and v is not None
            ]
            if rows_to_insert:
                cur.executemany(
                    "INSERT INTO account_limit (account_id, limit_category, limit_value) VALUES (%s, %s, %s)",
                    rows_to_insert,
                )
        conn.commit()
