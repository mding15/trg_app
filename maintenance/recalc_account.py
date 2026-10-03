# -*- coding: utf-8 -*-
"""
recalc_account.py — Ask a trg_app server to recalculate one tracked account now.

Calls POST /api/maint/account/<id>/recalc[?date=YYYY-MM-DD], which runs
process2/run_tracked_account.py on the server in the background:
tracked_proc → parent merge → VaR → alternative VaR → stress test → dashboard.
The server answers right away (202); the outcome is in the server's
log/run_tracked_account_<id>_<date>_<ts>.log.

Usage:
    python maintenance/recalc_account.py 7
    python maintenance/recalc_account.py 7 --date 2026-10-02
    python maintenance/recalc_account.py 7 8 9 --host https://<server>

Credentials: $api_username / $api_password (from trg_app/.env), or
--username/--password, or --token. Needs the superadmin role.
"""
import argparse
import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

DEFAULT_HOST = "http://localhost:5050"


def api_login(host, username, password):
    resp = requests.post(
        f"{host}/api/login",
        json={"username": username, "password": password},
        timeout=30,
    )
    if resp.status_code != 200:
        sys.exit(f"Login failed ({resp.status_code}): {resp.text}")
    return resp.json()["token"]


def recalc_account(host, token, account_id, as_of_date=None):
    """Start a recalculation; return (status_code, body)."""
    params = {"token": token}
    if as_of_date:
        params["date"] = as_of_date
    resp = requests.post(f"{host}/api/maint/account/{account_id}/recalc", params=params, timeout=60)
    try:
        body = resp.json()
    except ValueError:
        body = resp.text
    return resp.status_code, body


def main():
    parser = argparse.ArgumentParser(description="Recalculate tracked account(s) on a trg_app server now.")
    parser.add_argument("account_ids", nargs="+", type=int, metavar="ACCOUNT_ID")
    parser.add_argument("--date", metavar="YYYY-MM-DD",
                        help="as-of date (default: the server's proc_asof_date)")
    parser.add_argument("--host", default=os.environ.get("API_HOST", DEFAULT_HOST),
                        help=f"API host (default: {DEFAULT_HOST}, or $API_HOST)")
    parser.add_argument("--username", default=os.environ.get("api_username"))
    parser.add_argument("--password", default=os.environ.get("api_password"))
    parser.add_argument("--token", default=os.environ.get("api_token"),
                        help="use this JWT instead of logging in")
    args = parser.parse_args()

    if args.token:
        token = args.token
    else:
        if not args.username or not args.password:
            sys.exit("Need --token, or --username/--password (or $api_username/$api_password)")
        token = api_login(args.host, args.username, args.password)

    failed = False
    for account_id in args.account_ids:
        code, body = recalc_account(args.host, token, account_id, args.date)
        if code == 202:
            print(f"account {account_id}: started (date={body.get('as_of_date') or 'server default'}) "
                  f"- see server {body.get('log')}")
        else:
            failed = True
            detail = body.get("error", body) if isinstance(body, dict) else body
            print(f"account {account_id}: FAILED ({code}): {json.dumps(detail) if not isinstance(detail, str) else detail}",
                  file=sys.stderr)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
