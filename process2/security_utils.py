"""
security_utils.py — Shared helpers for creating securities and cross-reference entries.
"""
from __future__ import annotations


def create_security(
    cur,
    security_name: str,
    currency: str,
    asset_class: str | None,
    asset_type: str | None,
    data_source: str,
) -> str:
    """Insert a new row into security_info and return the generated SecurityID."""
    cur.execute(
        """
        INSERT INTO security_info
            ("SecurityName", "Currency", "AssetClass", "AssetType", "DataSource")
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
        """,
        (security_name, currency, asset_class, asset_type, data_source),
    )
    new_id = cur.fetchone()[0]
    security_id = f'T1{str(new_id).zfill(7)}'
    cur.execute(
        'UPDATE security_info SET "SecurityID" = %s WHERE id = %s',
        (security_id, new_id),
    )
    return security_id


def create_option_info(
    cur,
    security_id: str,
    option_type: str | None,
    option_class: str | None,
    maturity,
    strike: float | None,
    underlying: str | None,
    underlying_sec_id: str | None,
) -> None:
    """Insert a new row into option_info for security_id."""
    cur.execute(
        """
        INSERT INTO option_info
            (security_id, option_type, option_class, maturity, strike, underlying, underlying_sec_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (security_id, option_type, option_class, maturity, strike, underlying, underlying_sec_id),
    )


def add_xref_if_missing(
    cur,
    security_id: str,
    ref_type: str,
    ref_id: str,
    data_source: str,
    exch_code: str = '',
) -> None:
    """Insert a security_xref row if ref_id is non-empty and not already present."""
    if not ref_id or not ref_id.strip():
        return
    cur.execute(
        'SELECT 1 FROM security_xref WHERE "REF_TYPE" = %s AND "REF_ID" = %s',
        (ref_type, ref_id),
    )
    if cur.fetchone():
        return
    cur.execute(
        """
        INSERT INTO security_xref ("REF_ID", "REF_TYPE", "SecurityID", "DataSource", "ExchCode")
        VALUES (%s, %s, %s, %s, %s)
        """,
        (ref_id, ref_type, security_id, data_source, exch_code or None),
    )
