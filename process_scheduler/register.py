"""
process_scheduler/register.py

Registers or updates scheduler jobs defined in jobs.json.

Each job's script_path is relative to BASE_PATH and will be expanded
to an absolute path before being sent to the scheduler API.

Usage:
  python register.py               # register all jobs in jobs.json
  python register.py as_of_date    # register a single job by ID
"""

import sys
import json
import urllib.request
import urllib.error
from pathlib import Path

SCHEDULER_URL = 'https://engine.tailriskglobal.com/scheduler'
BASE_PATH     = '/home/ec2-user'
JOBS_FILE     = Path(__file__).parent / 'jobs.json'


def _load_jobs() -> list[dict]:
    with open(JOBS_FILE) as f:
        return json.load(f)


def _build_payload(job: dict) -> dict:
    payload = dict(job)
    payload['script_path'] = f"{BASE_PATH}/{job['script_path'].lstrip('/')}"
    return payload


def register_job(job: dict) -> None:
    """Register or update a single job in the scheduler via PUT, falling back to POST."""
    process_id = job['id']
    payload    = _build_payload(job)

    # Try PUT first (update existing)
    url = f'{SCHEDULER_URL}/api/processes/{process_id}'
    put_body = {k: v for k, v in payload.items() if k != 'id'}
    req = urllib.request.Request(
        url, data=json.dumps(put_body).encode(), method='PUT',
        headers={'Content-Type': 'application/json'},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            body = json.loads(resp.read())
            print(f"Updated:  {body.get('id')} — {body.get('name')}")
            return
    except urllib.error.HTTPError as e:
        if e.code != 404:
            print(f"Error {e.code} updating {process_id}: {e.read().decode()}")
            raise

    # 404 — process doesn't exist yet, create it via POST
    url = f'{SCHEDULER_URL}/api/processes'
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(), method='POST',
        headers={'Content-Type': 'application/json'},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            body = json.loads(resp.read())
            print(f"Created:  {body.get('id')} — {body.get('name')}")
    except urllib.error.HTTPError as e:
        print(f"Error {e.code} creating {process_id}: {e.read().decode()}")
        raise


def register_all() -> None:
    jobs = _load_jobs()
    print(f"Registering {len(jobs)} job(s) from {JOBS_FILE}")
    for job in jobs:
        register_job(job)


def register_by_id(process_id: str) -> None:
    jobs = _load_jobs()
    matches = [j for j in jobs if j['id'] == process_id]
    if not matches:
        print(f"No job with id '{process_id}' found in {JOBS_FILE}")
        sys.exit(1)
    register_job(matches[0])


if __name__ == '__main__':
    if len(sys.argv) == 2:
        register_by_id(sys.argv[1])
    else:
        register_all()
