# -*- coding: utf-8 -*-
"""
account_mgmt.py — DB helpers for the Account Management ops page.

All queries target the database2 Postgres instance (pg_connection).
"""
from database2 import pg_connection


def get_clients():
    """Return all clients ordered by name."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT client_id, client_name FROM client ORDER BY client_name')
            return [{'client_id': r[0], 'client_name': r[1]} for r in cur.fetchall()]


def get_accounts(client_id):
    """Return all accounts for a client, joined with owner username and parent account name."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.account_id, a.account_name, a.short_name,
                       a.owner_id, u.username AS owner_username,
                       a.parent_account_id, pa.account_name AS parent_account_name,
                       a.client_id, a.create_time
                FROM account a
                JOIN "user" u ON u.user_id = a.owner_id
                LEFT JOIN account pa ON pa.account_id = a.parent_account_id
                WHERE a.client_id = %s
                ORDER BY a.account_id
                """,
                (client_id,),
            )
            cols = [
                'account_id', 'account_name', 'short_name',
                'owner_id', 'owner_username',
                'parent_account_id', 'parent_account_name',
                'client_id', 'create_time',
            ]
            rows = []
            for r in cur.fetchall():
                d = dict(zip(cols, r))
                d['create_time'] = d['create_time'].isoformat() if d['create_time'] else None
                rows.append(d)
            return rows


def get_account_access(account_id):
    """Return all account_access rows for an account, joined with user info."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT aa.id, aa.account_id, aa.user_id, aa.is_default, aa.updated_at,
                       u.username, u.email, u.firstname, u.lastname
                FROM account_access aa
                JOIN "user" u ON u.user_id = aa.user_id
                WHERE aa.account_id = %s
                ORDER BY aa.is_default DESC, u.username
                """,
                (account_id,),
            )
            cols = [
                'id', 'account_id', 'user_id', 'is_default', 'updated_at',
                'username', 'email', 'firstname', 'lastname',
            ]
            rows = []
            for r in cur.fetchall():
                d = dict(zip(cols, r))
                d['updated_at'] = d['updated_at'].isoformat() if d['updated_at'] else None
                rows.append(d)
            return rows


def get_client_users(client_id):
    """Return users belonging to a client plus all ops-role users, for dropdown population."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_id, username, email, firstname, lastname, role
                FROM "user"
                WHERE client_id = %s OR role IN ('admin', 'superadmin', 'support')
                ORDER BY username
                """,
                (client_id,),
            )
            cols = ['user_id', 'username', 'email', 'firstname', 'lastname', 'role']
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def add_account_access(account_id, user_id, is_default):
    """
    Insert a new account_access row.
    Raises ValueError if the (account_id, user_id) pair already exists.
    Returns the new row id.
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM account_access WHERE account_id = %s AND user_id = %s',
                (account_id, user_id),
            )
            if cur.fetchone():
                raise ValueError('User already has access to this account')
            cur.execute(
                """
                INSERT INTO account_access (account_id, user_id, is_default)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (account_id, user_id, bool(is_default)),
            )
            new_id = cur.fetchone()[0]
        conn.commit()
    return new_id


def create_account(account_name, short_name, owner_id, client_id, parent_account_id):
    """
    Insert a new account row.
    Returns the new account_id.
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO account (account_name, short_name, owner_id, client_id, parent_account_id)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING account_id
                """,
                (
                    account_name,
                    short_name or None,
                    owner_id,
                    client_id,
                    parent_account_id or None,
                ),
            )
            new_id = cur.fetchone()[0]
        conn.commit()
    return new_id
