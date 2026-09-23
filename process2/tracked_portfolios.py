"""
tracked_portfolios.py — Resolve the current tracked portfolio(s) for an
account_id (or all accounts) from portfolio_info.

Shared by process2/tracked_proc_positions.py (the daily tracked-positions
job) and maintenance/update_port_position.py (--account-id) — factored out
here so the "which port_id is current for this account" rule lives in
exactly one place.

Public API:
    load_tracked_portfolios(cur, account_id=None) -> list[dict]
"""
from __future__ import annotations

from datetime import datetime


def load_tracked_portfolios(cur, account_id: int | None = None) -> list[dict]:
    """
    Fetch portfolios where port_type='tracked', then keep only the latest
    upload_dt per account_id. If account_id is given, restrict to that
    account (returned list has at most one entry in that case). Excludes
    filename='auto feed' rows and any account_id that is itself a
    parent_account_id (rollup accounts — their children carry the real
    portfolios).

    Note: "current" is decided by latest upload_dt, not by the largest
    port_id — the two usually agree since port_id is a serial assigned in
    upload order, but upload_dt is the actual source of truth.

    Returns dicts with keys: port_id, account_id, port_name, upload_dt.
    """
    if account_id is not None:
        cur.execute(
            """
            SELECT port_id, account_id, port_name, upload_dt
            FROM portfolio_info
            WHERE port_type = 'tracked' AND filename != 'auto feed' AND account_id = %s
              AND account_id NOT IN (SELECT parent_account_id FROM account WHERE parent_account_id IS NOT NULL)
            """,
            (account_id,),
        )
    else:
        cur.execute(
            """
            SELECT port_id, account_id, port_name, upload_dt
            FROM portfolio_info
            WHERE port_type = 'tracked' AND filename != 'auto feed'
              AND account_id NOT IN (SELECT parent_account_id FROM account WHERE parent_account_id IS NOT NULL)
            """
        )
    rows = [
        {'port_id': r[0], 'account_id': r[1], 'port_name': r[2], 'upload_dt': r[3]}
        for r in cur.fetchall()
    ]

    # Deduplicate: keep latest upload_dt per account_id
    latest: dict[int, dict] = {}
    for r in rows:
        acc = r['account_id']
        if acc not in latest or (r['upload_dt'] or datetime.min) > (latest[acc]['upload_dt'] or datetime.min):
            latest[acc] = r
    return list(latest.values())
