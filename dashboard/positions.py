"""
positions.py — API-facing reader for dashboard portfolio data.

Serves pre-computed data from db_portfolio_summary and db_positions.
Data is populated daily by dashboard_process.py.
"""
from __future__ import annotations

from dashboard.positions_db import get_account_ids_for_user, read_portfolio_summary, read_positions


def get_portfolio_summary(username) -> dict:
    # Use the first account_id the user has access to.
    # TODO: support multiple accounts per user when needed.
    account_ids = get_account_ids_for_user(username)
    if not account_ids:
        return {}
    return read_portfolio_summary(account_ids[0])


def get_positions(username) -> list[dict]:
    # Use the first account_id the user has access to.
    # TODO: support multiple accounts per user when needed.
    account_ids = get_account_ids_for_user(username)
    if not account_ids:
        return []
    return read_positions(account_ids[0])
