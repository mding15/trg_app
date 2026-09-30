# -*- coding: utf-8 -*-
"""
Generic CLI tool for calling trg_app API GET endpoints.

Logs in with username/password to get a JWT (like cmd_remote.py's api_login()),
then calls the given path with that token plus any query params.

Examples:
    python maintenance/call_api.py /api/summary/concentrations --account_id 1008
    python maintenance/call_api.py /api/summary/metrics -p account_id=1008
    python maintenance/call_api.py /api/accounts --host http://localhost:5050

Credentials default to the api_username / api_password env vars (same
convention as cmd_remote.py), read from trg_app/.env if present, or pass
--username/--password directly. Pass --token to skip login and use an
existing JWT instead (e.g. one copied from the browser).
"""
import argparse
import json
import os
import re
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

DEFAULT_HOST = "http://localhost:5050"

API_DIR = Path(__file__).resolve().parents[1] / "api"
ROUTE_FILES = ["routes.py", "ops_routes.py"]
ROUTE_RE = re.compile(
    r"""@app\.route\(\s*['"]([^'"]+)['"](?:\s*,\s*methods\s*=\s*\[([^\]]*)\])?\s*\)"""
)
PATH_PARAM_RE = re.compile(r"<(?:[^:>]+:)?([^>]+)>")


def discover_get_paths():
    paths = set()
    for filename in ROUTE_FILES:
        text = (API_DIR / filename).read_text(encoding="utf-8")
        for path, methods_str in ROUTE_RE.findall(text):
            if methods_str:
                methods = [m.strip().strip("'\"") for m in methods_str.split(",")]
                if "GET" not in methods:
                    continue
            paths.add(path)
    return sorted(paths)


def fill_path_params(path):
    def replace(match):
        name = match.group(1)
        return input(f"  value for <{name}>: ").strip()

    return PATH_PARAM_RE.sub(replace, path)


def prompt_for_path():
    paths = discover_get_paths()
    if not paths:
        sys.exit("No path given and no GET routes discovered")

    print("No API path given. Choose one:")
    for i, path in enumerate(paths, start=1):
        print(f"  {i:>3}) {path}")

    choice = input("Enter number (or type a path): ").strip()
    if choice.isdigit() and 1 <= int(choice) <= len(paths):
        path = paths[int(choice) - 1]
    elif choice:
        path = choice
    else:
        sys.exit("No path selected")

    if "<" in path:
        path = fill_path_params(path)
    return path


def api_login(host, username, password):
    resp = requests.post(
        f"{host}/api/login",
        json={"username": username, "password": password},
        timeout=30,
    )
    if resp.status_code != 200:
        sys.exit(f"Login failed ({resp.status_code}): {resp.text}")
    return resp.json()["token"]


def parse_params(pairs):
    params = {}
    for pair in pairs:
        if "=" not in pair:
            sys.exit(f"--param must be KEY=VALUE, got: {pair}")
        key, value = pair.split("=", 1)
        params[key] = value
    return params


def main():
    parser = argparse.ArgumentParser(description="Call a trg_app API GET endpoint.")
    parser.add_argument("path", nargs="?",
                         help="API path, e.g. /api/summary/concentrations "
                              "(omit to choose from a list)")
    parser.add_argument("-p", "--param", action="append", default=[], metavar="KEY=VALUE",
                         help="extra query param, repeatable")
    parser.add_argument("--account_id", type=int, help="shortcut for -p account_id=VALUE")
    parser.add_argument("--host", default=os.environ.get("API_HOST", DEFAULT_HOST),
                         help=f"API host (default: {DEFAULT_HOST}, or $API_HOST)")
    parser.add_argument("--username", default=os.environ.get("api_username"),
                         help="login username (default: $api_username)")
    parser.add_argument("--password", default=os.environ.get("api_password"),
                         help="login password (default: $api_password)")
    parser.add_argument("--token", default=os.environ.get("api_token"),
                         help="use this JWT instead of logging in (default: $api_token)")
    args = parser.parse_args()

    if not args.path:
        args.path = prompt_for_path()

    params = parse_params(args.param)
    if args.account_id is not None:
        params["account_id"] = args.account_id

    if args.token:
        token = args.token
    else:
        if not args.username or not args.password:
            sys.exit(
                "Need --token, or --username/--password (or $api_username/$api_password env vars)"
            )
        token = api_login(args.host, args.username, args.password)

    params["token"] = token
    url = f"{args.host}{args.path}"

    resp = requests.get(url, params=params, timeout=30)

    try:
        body = resp.json()
    except ValueError:
        body = resp.text

    if resp.status_code != 200:
        print(f"Failed: {resp.status_code}", file=sys.stderr)
        if isinstance(body, (dict, list)):
            print(json.dumps(body, indent=2), file=sys.stderr)
        else:
            print(body, file=sys.stderr)
        sys.exit(1)

    print(json.dumps(body, indent=2))


if __name__ == "__main__":
    main()
