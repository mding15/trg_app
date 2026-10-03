# -*- coding: utf-8 -*-
"""
dist_remote.py — Download / upload risk-factor distributions from/to a trg_app server.

  get   POST /api/maint/dist         var_utils.get_dist(rf_ids, category) on the server,
                                     written to data/maintenance/CSV/dist.{category}.csv
  put   POST /api/maint/dist/upload  save a local distribution CSV into the server's
                                     VaR HDF file (superadmin only)

The CSV layout is the same for both (index + one column per rf_id, as written by
dist_dump.py / var_utils.export_dist), so a `get` file can be edited and `put` back.

Usage:
    python maintenance/dist_remote.py get --ids T10000001 T10000003
    python maintenance/dist_remote.py get --input my_ids.csv --category SPREAD --model M_20251231
    python maintenance/dist_remote.py put dist.PRICE.csv --dry-run
    python maintenance/dist_remote.py put dist.PRICE.csv --ids T10000001 --model M_20251231
    python maintenance/dist_remote.py --host https://<server> put dist.SPREAD.csv --category SPREAD

get: rf_ids come from --ids, else the first column of --input
     (default: data/maintenance/CSV/security_ids.csv).
put: FILE is a path, or a filename inside data/maintenance/CSV/. --ids uploads only
     those columns. rf_ids that already exist on the server are overwritten; the server
     backs up the old series first (VaR_DIR/backup/) and reports the backup file.
     --dry-run validates and reports new/overwritten rf_ids without writing.
--model targets VaR.<model>.h5 on the server (default: the server's current model).

Credentials: $api_username / $api_password (from trg_app/.env), or
--username/--password, or --token. get needs an ops role
(admin/superadmin/support); put needs superadmin.
"""
import argparse
import io
import os
import sys
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _paths import CSV_DIR

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

DEFAULT_HOST = "http://localhost:5050"
DEFAULT_INPUT = CSV_DIR / "security_ids.csv"


def api_login(host, username, password):
    resp = requests.post(
        f"{host}/api/login",
        json={"username": username, "password": password},
        timeout=30,
    )
    if resp.status_code != 200:
        sys.exit(f"Login failed ({resp.status_code}): {resp.text}")
    return resp.json()["token"]


def read_ids(input_path):
    if not input_path.exists():
        sys.exit(f"Input file not found: {input_path}")
    df = pd.read_csv(input_path)
    return df.iloc[:, 0].dropna().astype(str).tolist()


def resolve_file(file):
    path = Path(file)
    if path.exists():
        return path
    if (CSV_DIR / file).exists():
        return CSV_DIR / file
    sys.exit(f"File not found: {file} (also looked in {CSV_DIR})")


# ── API calls ─────────────────────────────────────────────────────────────────

def get_dist(host, token, rf_ids, category="PRICE", model_id=None):
    """Return (dist DataFrame, list of missing rf_ids)."""
    resp = requests.post(
        f"{host}/api/maint/dist",
        params={"token": token},
        json={"rf_ids": rf_ids, "category": category, "model_id": model_id},
        timeout=300,
    )
    if resp.status_code != 200:
        sys.exit(f"Failed ({resp.status_code}): {resp.text}")

    text = resp.text.strip()
    dist = pd.read_csv(io.StringIO(text), index_col=0) if text else pd.DataFrame()
    found = set(map(str, dist.columns))
    missing = [x for x in rf_ids if x not in found]
    return dist, missing


def put_dist(host, token, dist, category="PRICE", model_id=None, dry_run=False):
    """Upload dist; return the server's JSON result."""
    buf = io.StringIO()
    dist.index.name = "index"
    dist.to_csv(buf, index=True)

    data = {"category": category, "dry_run": str(dry_run).lower()}
    if model_id:
        data["model_id"] = model_id

    resp = requests.post(
        f"{host}/api/maint/dist/upload",
        params={"token": token},
        data=data,
        files={"file": ("dist.csv", buf.getvalue(), "text/csv")},
        timeout=300,
    )
    try:
        body = resp.json()
    except ValueError:
        body = {"error": resp.text}
    if resp.status_code != 200:
        sys.exit(f"Failed ({resp.status_code}): {body.get('error', body)}")
    return body


# ── Subcommands ───────────────────────────────────────────────────────────────

def cmd_get(args, token):
    rf_ids = args.ids or read_ids(args.input)
    if not rf_ids:
        sys.exit("No rf_ids given")

    print(f"Requesting {len(rf_ids)} rf_ids (category={args.category}) from {args.host} ...")
    dist, missing = get_dist(args.host, token, rf_ids, args.category, args.model)

    print(f"  Requested: {len(rf_ids)}  |  Returned: {dist.shape[1]}  |  Missing: {len(missing)}")
    if missing:
        print(f"  Missing rf_ids: {missing}")
    if dist.empty:
        print("No distributions returned; nothing written.")
        return

    out_path = args.output or CSV_DIR / f"dist.{args.category}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    dist.to_csv(out_path, index=True)
    print(f"Done. {dist.shape[0]} rows x {dist.shape[1]} cols written to {out_path}")


def cmd_put(args, token):
    path = resolve_file(args.file)
    dist = pd.read_csv(path, index_col=0)
    dist.columns = dist.columns.astype(str)

    if args.ids:
        not_in_file = [x for x in args.ids if x not in dist.columns]
        if not_in_file:
            sys.exit(f"--ids not found in {path.name}: {not_in_file}")
        dist = dist[args.ids]

    print(f"Uploading {path} ({dist.shape[0]} rows x {dist.shape[1]} cols, "
          f"category={args.category}) to {args.host}"
          f"{' [DRY RUN]' if args.dry_run else ''} ...")
    result = put_dist(args.host, token, dist, args.category, args.model, args.dry_run)

    print(f"  Model: {result['model_id']}  |  Category: {result['category']}  |  Length: {result['length']}")
    print(f"  New: {len(result['new'])}  |  Overwritten: {len(result['overwritten'])}")
    if result["overwritten"]:
        print(f"  Overwritten rf_ids: {result['overwritten']}")
    if result["dry_run"]:
        print("Dry run: nothing written.")
    else:
        if result["backup_file"]:
            print(f"  Server backup of overwritten series: {result['backup_file']}")
        print("Done.")


def main():
    parser = argparse.ArgumentParser(
        description="Download / upload risk-factor distributions from/to a trg_app server.")
    parser.add_argument("--host", default=os.environ.get("API_HOST", DEFAULT_HOST),
                        help=f"API host (default: {DEFAULT_HOST}, or $API_HOST)")
    parser.add_argument("--username", default=os.environ.get("api_username"))
    parser.add_argument("--password", default=os.environ.get("api_password"))
    parser.add_argument("--token", default=os.environ.get("api_token"),
                        help="use this JWT instead of logging in")
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--category", default="PRICE",
                        help="distribution category, e.g. PRICE, VOL, IR, SPREAD (default: PRICE)")
    common.add_argument("--model", help="model_id on the server, e.g. M_20251231 "
                                        "(default: server's current model)")

    p_get = sub.add_parser("get", parents=[common], help="download distributions")
    p_get.add_argument("--ids", nargs="+", metavar="RF_ID",
                       help="rf_ids to fetch (overrides --input)")
    p_get.add_argument("--input", type=Path, default=DEFAULT_INPUT,
                       help=f"CSV with rf_ids in the first column (default: {DEFAULT_INPUT})")
    p_get.add_argument("--output", type=Path,
                       help="output CSV (default: CSV_DIR/dist.{category}.csv)")

    p_put = sub.add_parser("put", parents=[common], help="upload distributions (superadmin)")
    p_put.add_argument("file", help="distribution CSV (path, or filename in CSV_DIR)")
    p_put.add_argument("--ids", nargs="+", metavar="RF_ID",
                       help="upload only these columns of the file")
    p_put.add_argument("--dry-run", action="store_true",
                       help="validate and report new/overwritten rf_ids without writing")

    args = parser.parse_args()

    if args.token:
        token = args.token
    else:
        if not args.username or not args.password:
            sys.exit("Need --token, or --username/--password (or $api_username/$api_password)")
        token = api_login(args.host, args.username, args.password)

    if args.command == "get":
        cmd_get(args, token)
    else:
        cmd_put(args, token)


if __name__ == "__main__":
    main()
