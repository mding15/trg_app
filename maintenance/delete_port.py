"""
delete_port.py — Delete all rows for a given port_id across portfolio tables.

Tables deleted (child-first to satisfy FK constraints):
    port_position_var, port_positions, port_parameters, port_limit, portfolio_info

Usage:
    python delete_port.py --port-id N [--dry-run]
"""
from __future__ import annotations

import argparse
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from database2 import pg_connection

TABLES = [
    "port_position_var",
    "port_positions",
    "port_parameters",
    "port_limit",
    "portfolio_info",
]


def _count(cur, table: str, port_id: int) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE port_id = %s", (port_id,))
    return cur.fetchone()[0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete all rows for a port_id from portfolio tables.")
    parser.add_argument("--port-id", type=int, required=True, help="port_id to delete")
    parser.add_argument("--dry-run", action="store_true", help="Print row counts without deleting")
    args = parser.parse_args()

    port_id = args.port_id

    with pg_connection() as conn:
        with conn.cursor() as cur:

            # Verify port exists
            cur.execute("SELECT port_name FROM portfolio_info WHERE port_id = %s", (port_id,))
            row = cur.fetchone()
            if not row:
                print(f"port_id={port_id} not found in portfolio_info. Nothing to do.")
                sys.exit(0)

            port_name = row[0]
            print(f"port_id={port_id}  name='{port_name}'")
            if args.dry_run:
                print("*** DRY RUN — no changes will be written ***")
            print()

            # Show row counts per table
            counts = {t: _count(cur, t, port_id) for t in TABLES}
            for table, n in counts.items():
                print(f"  {table:<22} {n:>6} row(s)")
            print()

            if args.dry_run:
                print("Dry run complete. No rows deleted.")
                return

            # Confirm
            answer = input(f"Delete all rows for port_id={port_id}? [y/N] ").strip().lower()
            if answer != "y":
                print("Aborted.")
                sys.exit(0)

            # Delete child-first
            for table in TABLES:
                cur.execute(f"DELETE FROM {table} WHERE port_id = %s", (port_id,))
                print(f"  deleted {cur.rowcount:>6} row(s) from {table}")

        conn.commit()

    print("\nDone.")


if __name__ == "__main__":
    main()
