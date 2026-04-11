# -*- coding: utf-8 -*-
"""
database2/__init__.py — PostgreSQL connection for the database2 / process2 layer.

Provides:
    pg_connection         — context manager that opens and closes a psycopg2 connection
    pg_create_connection() — raw connection factory (use pg_connection where possible)
    get_proc_asof_date()  — read as_of_date from proc_asof_date table (returns ISO str)
"""
import json
import os
import psycopg2

# Load credentials from config/app_config.json (located in the parent of trg_app)
_config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'app_config.json')
with open(_config_path, 'r') as _f:
    _config = json.load(_f)

# Connection parameters
_pg_conn_params = {
    'dbname':   'postgres',
    'user':     _config['PRS_USERNAME'],
    'password': _config['PRS_PASSWORD'],
    'host':     'trg-input-database.c9sm826ie6uy.us-east-1.rds.amazonaws.com',
    'port':     '5432',
}


def pg_create_connection():
    return psycopg2.connect(**_pg_conn_params)


class pg_connection:
    """Context manager for a psycopg2 connection.

    Usage:
        with pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(...)
            conn.commit()
    """
    def __enter__(self):
        self.conn = pg_create_connection()
        return self.conn

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()


def get_proc_asof_date() -> str:
    """Return as_of_date from proc_asof_date table as 'YYYY-MM-DD'.

    Raises RuntimeError if the table is empty or the value is NULL.
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT as_of_date FROM proc_asof_date LIMIT 1')
            row = cur.fetchone()
            if not row or row[0] is None:
                raise RuntimeError('proc_asof_date table is empty — cannot determine as_of_date')
            return row[0].isoformat() if hasattr(row[0], 'isoformat') else str(row[0])
