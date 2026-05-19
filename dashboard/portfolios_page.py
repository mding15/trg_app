# -*- coding: utf-8 -*-
"""
dashboard/portfolios_page.py — Implementation for the Portfolios page APIs.

Mock-backed (Phase 1):
    (none remaining)

DB-backed:
    list_portfolios(username)
    delete_portfolio(pid, username)
    list_broker_feeds(account_id)
    get_broker_settings(account_id)
    delete_broker_setting(account_id, sid)
    create_broker_setting(username, account_id, broker, broker_account_ref, name)
"""
from __future__ import annotations

import datetime
import logging

from database2 import pg_connection
from dashboard.upload_portfolio import get_portfolio_file_path

logger = logging.getLogger(__name__)


def _migrate_schema() -> None:
    """Add port_type and description columns to portfolio_info if missing (idempotent)."""
    try:
        with pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    ALTER TABLE portfolio_info
                        ADD COLUMN IF NOT EXISTS port_type   VARCHAR(20) NULL,
                        ADD COLUMN IF NOT EXISTS description TEXT        NULL
                """)
            conn.commit()
    except Exception as e:
        logger.warning('portfolio_info schema migration skipped: %s', e)

_migrate_schema()


# ── Portfolios (DB-backed) ─────────────────────────────────────────────────────

def list_portfolios(username: str) -> list:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    pi.port_id,
                    pi.port_name,
                    pi.filename,
                    pi.upload_dt,
                    pi.as_of_date,
                    pi.created_by,
                    pi.market_value,
                    (SELECT COUNT(*) FROM port_positions pp WHERE pp.port_id = pi.port_id),
                    pi.status,
                    pi.message,
                    pi.account_id
                FROM portfolio_info pi
                JOIN "user" u ON u.client_id = pi.client_id
                WHERE u.username = %s
                  AND pi.account_id IS NULL
                ORDER BY pi.port_id DESC
                """,
                (username,),
            )
            rows = cur.fetchall()
    return [
        {
            'id':          str(row[0]),
            'name':        row[1],
            'file':        row[2],
            'upload_dt':   row[3].strftime('%Y-%m-%d %H:%M') if row[3] else '—',
            'as_of_date':  row[4].strftime('%Y-%m-%d')       if row[4] else '—',
            'uploaded_by': row[5] or '—',
            'mv':          float(row[6]) if row[6] is not None else None,
            'positions':   row[7],
            'status':      row[8] or '—',
            'message':     row[9],
            'account_id':  row[10],
        }
        for row in rows
    ]



def delete_portfolio(pid: str, username: str) -> bool:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pi.filename, pi.client_id FROM portfolio_info pi
                JOIN "user" u ON u.client_id = pi.client_id
                WHERE pi.port_id = %s AND u.username = %s AND pi.account_id IS NULL
                """,
                (pid, username),
            )
            row = cur.fetchone()
            if row is None:
                return False
            filename, client_id = row

            # rename file on disk before removing the DB record
            if filename:
                _rename_deleted_file(client_id, filename)

            cur.execute("DELETE FROM port_position_var WHERE port_id = %s", (pid,))
            cur.execute("DELETE FROM port_positions    WHERE port_id = %s", (pid,))
            cur.execute("DELETE FROM port_parameters   WHERE port_id = %s", (pid,))
            cur.execute("DELETE FROM port_limit        WHERE port_id = %s", (pid,))
            cur.execute("DELETE FROM portfolio_info    WHERE port_id = %s", (pid,))
        conn.commit()
    return True


def list_tracked_portfolios(account_id: int) -> list:
    """Return tracked-position manual uploads for the account, newest first."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    pi.port_id,
                    pi.port_name,
                    pi.filename,
                    pi.upload_dt,
                    pi.as_of_date,
                    pi.created_by,
                    pi.market_value,
                    (SELECT COUNT(*) FROM port_positions pp WHERE pp.port_id = pi.port_id),
                    pi.status,
                    pi.message,
                    pi.description
                FROM portfolio_info pi
                WHERE pi.account_id = %s
                  AND pi.port_type = 'tracked'
                ORDER BY pi.port_id DESC
                """,
                (account_id,),
            )
            rows = cur.fetchall()
    return [
        {
            'id':          str(row[0]),
            'name':        row[1],
            'file':        row[2],
            'upload_dt':   row[3].strftime('%Y-%m-%d %H:%M') if row[3] else '—',
            'as_of_date':  row[4].strftime('%Y-%m-%d')       if row[4] else '—',
            'uploaded_by': row[5] or '—',
            'mv':          float(row[6]) if row[6] is not None else None,
            'positions':   int(row[7])   if row[7] is not None else None,
            'status':      row[8] or '—',
            'message':     row[9],
            'description': row[10],
        }
        for row in rows
    ]


def list_adhoc_portfolios(username: str) -> list:
    """Return ad-hoc analysis portfolios for this user, newest first."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    pi.port_id,
                    pi.port_name,
                    pi.filename,
                    pi.upload_dt,
                    pi.as_of_date,
                    pi.market_value,
                    (SELECT COUNT(*) FROM port_positions pp WHERE pp.port_id = pi.port_id),
                    pi.status,
                    pi.message
                FROM portfolio_info pi
                JOIN "user" u ON u.client_id = pi.client_id
                WHERE u.username = %s
                  AND pi.port_type = 'adhoc'
                ORDER BY pi.port_id DESC
                """,
                (username,),
            )
            rows = cur.fetchall()
    return [
        {
            'id':         'AH-' + str(row[0]).zfill(5),
            'port_id':    str(row[0]),
            'name':       row[1],
            'file':       row[2],
            'upload_dt':  row[3].strftime('%Y-%m-%d %H:%M') if row[3] else '—',
            'as_of_date': row[4].strftime('%Y-%m-%d')       if row[4] else '—',
            'mv':         float(row[5]) if row[5] is not None else None,
            'positions':  int(row[6])   if row[6] is not None else None,
            'status':     row[7] or '—',
            'message':    row[8],
        }
        for row in rows
    ]


def list_position_history(account_id: int) -> list:
    """Merged position history: broker feeds + tracked manual uploads, newest first."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH account_ids AS (
                    SELECT %(aid)s AS account_id
                    UNION
                    SELECT account_id FROM account WHERE parent_account_id = %(aid)s
                )
                SELECT
                    'broker'::text                              AS source,
                    bfs.last_feed                               AS dt,
                    ba.broker || ' · ' || ba.broker_account     AS source_detail,
                    bfs.feed_date                               AS as_of_date,
                    bfs.mv,
                    bfs.positions,
                    bfs.securities,
                    'Processed'::text                           AS status,
                    NULL::int                                   AS port_id
                FROM broker_feed_summary bfs
                JOIN broker_account ba ON ba.id = bfs.broker_account_id
                WHERE ba.account_id IN (SELECT account_id FROM account_ids)

                UNION ALL

                SELECT
                    'manual'::text                              AS source,
                    pi.upload_dt                                AS dt,
                    pi.created_by || ' · ' || pi.filename       AS source_detail,
                    pi.as_of_date,
                    pi.market_value                             AS mv,
                    (SELECT COUNT(*)              FROM port_positions pp WHERE pp.port_id = pi.port_id)::int AS positions,
                    (SELECT COUNT(DISTINCT pp."SecurityID") FROM port_positions pp WHERE pp.port_id = pi.port_id)::int AS securities,
                    pi.status::text,
                    pi.port_id
                FROM portfolio_info pi
                WHERE pi.account_id IN (SELECT account_id FROM account_ids)
                  AND pi.port_type = 'tracked'

                ORDER BY dt DESC NULLS LAST
                """,
                {'aid': account_id},
            )
            rows = cur.fetchall()
    return [
        {
            'source':        row[0],
            'datetime':      row[1].strftime('%Y-%m-%d  %H:%M') if row[1] else '—',
            'source_detail': row[2] or '—',
            'as_of_date':    row[3].strftime('%Y-%m-%d') if row[3] else '—',
            'mv':            float(row[4]) if row[4] is not None else None,
            'positions':     int(row[5])   if row[5] is not None else None,
            'securities':    int(row[6])   if row[6] is not None else None,
            'status':        row[7] or '—',
            'port_id':       str(row[8])   if row[8] is not None else None,
        }
        for row in rows
    ]


def _rename_deleted_file(client_id: int, filename: str) -> None:
    file_path = get_portfolio_file_path(client_id, filename)
    if not file_path.exists():
        return
    try:
        ts   = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        stem = file_path.stem
        ext  = file_path.suffix.lstrip('.')
        new_name = f'{stem}.deleted.{ts}.{ext}' if ext else f'{stem}.deleted.{ts}'
        file_path.rename(file_path.parent / new_name)
        logger.info(f'renamed deleted portfolio file to: {new_name}')
    except Exception as e:
        logger.warning(f'could not rename portfolio file {file_path}: {e}')


# ── Broker feeds (mock) ────────────────────────────────────────────────────────

def list_broker_feeds(account_id: int) -> list:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH account_ids AS (
                    SELECT %s AS account_id
                    UNION
                    SELECT account_id FROM account WHERE parent_account_id = %s
                )
                SELECT
                    ba.id,
                    ba.broker,
                    ba.broker_account,
                    ba.name,
                    bfs.last_feed,
                    bfs.feed_date     AS as_of_date,
                    bfs.mv,
                    bfs.positions,
                    bfs.securities,
                    bfs.transactions,
                    bfs.tax_lots
                FROM broker_feed_summary bfs
                JOIN broker_account ba ON ba.id = bfs.broker_account_id
                WHERE ba.account_id IN (SELECT account_id FROM account_ids)
                ORDER BY bfs.last_feed DESC
                """,
                (account_id, account_id),
            )
            rows = cur.fetchall()
    return [
        {
            'id':           str(row[0]),
            'broker':       row[1],
            'account':      row[2],
            'name':         row[3] or '—',
            'last_feed':    row[4].strftime('%Y-%m-%d %H:%M') if row[4] else '—',
            'as_of_date':   row[5].strftime('%Y-%m-%d')       if row[5] else '—',
            'mv':           float(row[6])  if row[6]  is not None else None,
            'positions':    row[7],
            'securities':   row[8],
            'transactions': row[9],
            'tax_lots':     row[10],
        }
        for row in rows
    ]


# ── Broker settings (DB-backed) ────────────────────────────────────────────────

def get_broker_settings(account_id: int) -> list:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH account_ids AS (
                    SELECT %s AS account_id
                    UNION
                    SELECT account_id FROM account WHERE parent_account_id = %s
                )
                SELECT
                    ba.id,
                    ba.broker,
                    ba.broker_account,
                    ba.name,
                    ba.routing_code,
                    ba.updated_at,
                    u.username  AS setup_by,
                    ba.auth_expiry,
                    ba.status
                FROM broker_account ba
                LEFT JOIN "user" u ON u.user_id = ba.setup_user_id
                WHERE ba.account_id IN (SELECT account_id FROM account_ids)
                ORDER BY ba.broker, ba.broker_account
                """,
                (account_id, account_id),
            )
            rows = cur.fetchall()
    return [
        {
            'id':           str(row[0]),
            'broker':       row[1],
            'account':      row[2],
            'name':         row[3],
            'routing_code': row[4] or '—',
            'setup_time':   row[5].strftime('%Y-%m-%d %H:%M') if row[5] else '—',
            'setup_by':     row[6] or '—',
            'auth_expiry':  row[7].strftime('%Y-%m-%d') if row[7] else '—',
            'status':       row[8],
        }
        for row in rows
    ]


def delete_broker_setting(account_id: int, sid: int, deleted_by: str | None = None) -> bool:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO broker_account_hist
                    (broker_account_id, account_id, broker_account, broker,
                     routing_code, name, setup_user_id, auth_expiry, status, updated_at,
                     deleted_by)
                SELECT id, account_id, broker_account, broker,
                       routing_code, name, setup_user_id, auth_expiry, status, updated_at,
                       %s
                FROM broker_account
                WHERE id = %s AND account_id = %s
                """,
                (deleted_by, sid, account_id),
            )
            cur.execute(
                "DELETE FROM broker_account WHERE id = %s AND account_id = %s",
                (sid, account_id),
            )
            deleted = cur.rowcount > 0
        conn.commit()
    return deleted


def create_broker_setting(
    username: str,
    account_id: int,
    broker: str,
    broker_account_ref: str,
    name: str | None,
) -> dict:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT user_id FROM "user" WHERE username = %s', (username,))
            row = cur.fetchone()
            setup_user_id = row[0] if row else None

            cur.execute(
                """
                INSERT INTO broker_account
                    (account_id, broker_account, broker, routing_code, name, setup_user_id, status)
                VALUES (%s, %s, %s, NULL, %s, %s, 'Pending')
                RETURNING id, updated_at
                """,
                (account_id, broker_account_ref, broker, name, setup_user_id),
            )
            new_id, updated_at = cur.fetchone()
        conn.commit()
    return {
        'id':           str(new_id),
        'broker':       broker,
        'account':      broker_account_ref,
        'name':         name,
        'routing_code': '—',
        'setup_time':   updated_at.strftime('%Y-%m-%d %H:%M') if updated_at else '—',
        'setup_by':     username,
        'auth_expiry':  '—',
        'status':       'Pending',
    }
