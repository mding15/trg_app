"""
positions.py — API-facing reader for dashboard portfolio data.

Serves pre-computed data from db_portfolio_summary and db_positions.
Data is populated daily by dashboard_process.py.
"""
from __future__ import annotations

from dashboard.positions_db import read_portfolio_summary, read_positions


def get_portfolio_summary(account_id: int) -> dict:
    return read_portfolio_summary(account_id)


def get_positions(account_id: int) -> list[dict]:
    return read_positions(account_id)
