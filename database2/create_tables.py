"""
create_tables.py — thin runner that executes tables.sql against the DB.
All DDL is defined in tables.sql; this file is just the Python entry point.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from database2 import pg_connection


def create_tables() -> None:
    sql_path = os.path.join(os.path.dirname(__file__), "tables.sql")
    with open(sql_path) as f:
        sql = f.read()

    stmts = [s.strip() for s in sql.split(";") if s.strip()]

    errors: list[str] = []
    with pg_connection() as conn:
        for stmt in stmts:
            try:
                with conn.cursor() as cur:
                    cur.execute(stmt)
            except Exception as e:
                conn.rollback()
                msg = str(e).strip()
                if "already exists" not in msg:
                    errors.append(f"{msg}\n  Statement: {stmt[:120]}")
        conn.commit()

    if errors:
        for err in errors:
            print(f"ERROR: {err}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    create_tables()
    print("Tables created/migrated successfully.")
