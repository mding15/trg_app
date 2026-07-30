# -*- coding: utf-8 -*-
"""
dashboard/dashboard_msg.py — Dashboard banner messages.

Public API:
    dashboard_msg(username) -> str | None
"""
from __future__ import annotations

from database2 import pg_connection


def _count_unset_up_securities(username: str) -> int:
    """Count distinct securities, across every account username has access
    to, that haven't been set up in the system yet (blank or NULL security_id).

    Only each account's own latest as_of_date is considered (different
    accounts can be as-of different dates), so a security that was missing
    security_id on an older date but has since been set up doesn't get
    counted."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(DISTINCT pv.security_name)
                FROM position_var pv, account_access aa, "user" u
                WHERE pv.account_id = aa.account_id
                  AND aa.user_id = u.user_id
                  AND u.username = %s
                  AND (pv.security_id = '' OR pv.security_id IS NULL)
                  AND pv.as_of_date = (
                      SELECT MAX(pv2.as_of_date)
                      FROM position_var pv2
                      WHERE pv2.account_id = pv.account_id
                  )
                """,
                (username,),
            )
            return cur.fetchone()[0]


def dashboard_msg(username: str) -> str | None:
    """Return a banner message about securities not yet set up in the
    system, for every account `username` has access to. Returns None if
    there's nothing to report."""
    count = _count_unset_up_securities(username)
    if count == 0:
        return None
    noun = "security" if count == 1 else "securities"
    verb = "has" if count == 1 else "have"
    return (
        f"{count} {noun} in your portfolios {verb} not yet been fully "
        "configured in our system. Our team is completing this setup."
    )
