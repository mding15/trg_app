"""
db_pnl_stat.py — DB persistence for security-level P&L distribution statistics.

Provides save_pnl_stat(), called by calc_linear_product_pnl, calc_bond_pnl,
and calc_treasury_pnl after computing dist_stat() to write results to the
security_pnl_stat table instead of (or alongside) timestamped CSV files.

Usage:
    python process2/db_pnl_stat.py               # purge with default 31-day cutoff
    python process2/db_pnl_stat.py --days 60     # purge with custom cutoff
    python process2/db_pnl_stat.py --dry-run     # preview rows that would be deleted
"""
from __future__ import annotations

import os
import sys
from datetime import date

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from psycopg2.extras import execute_batch

from database2 import pg_connection

# Maps dist_stat() column names → DB column names
_COL_MAP = {
    'min':   'min',
    'max':   'max',
    'mean':  'mean',
    'std':   'std',
    'q-1%':  'q_1pct',
    'q-5%':  'q_5pct',
    'q-50%': 'q_50pct',
    'q-95%': 'q_95pct',
    'q-99%': 'q_99pct',
    'es-5%': 'es_5pct',
    'es-1%': 'es_1pct',
}

_DB_COLS = [
    'as_of_date', 'security_id', 'pnl_type',
    'min', 'max', 'mean', 'std',
    'q_1pct', 'q_5pct', 'q_50pct', 'q_95pct', 'q_99pct',
    'es_5pct', 'es_1pct',
]

_SQL_UPSERT = """
    INSERT INTO security_pnl_stat
        (as_of_date, security_id, pnl_type,
         min, max, mean, std,
         q_1pct, q_5pct, q_50pct, q_95pct, q_99pct,
         es_5pct, es_1pct)
    VALUES
        (%(as_of_date)s, %(security_id)s, %(pnl_type)s,
         %(min)s, %(max)s, %(mean)s, %(std)s,
         %(q_1pct)s, %(q_5pct)s, %(q_50pct)s, %(q_95pct)s, %(q_99pct)s,
         %(es_5pct)s, %(es_1pct)s)
    ON CONFLICT (as_of_date, security_id, pnl_type) DO UPDATE SET
        min         = EXCLUDED.min,
        max         = EXCLUDED.max,
        mean        = EXCLUDED.mean,
        std         = EXCLUDED.std,
        q_1pct      = EXCLUDED.q_1pct,
        q_5pct      = EXCLUDED.q_5pct,
        q_50pct     = EXCLUDED.q_50pct,
        q_95pct     = EXCLUDED.q_95pct,
        q_99pct     = EXCLUDED.q_99pct,
        es_5pct     = EXCLUDED.es_5pct,
        es_1pct     = EXCLUDED.es_1pct,
        insert_time = NOW()
"""


def purge_pnl_stat(cutoff_days: int = 31) -> int:
    """
    Delete rows from security_pnl_stat older than cutoff_days, preserving month-end data.

    Month-end is defined as the last business day (Mon-Fri) of each calendar month.
    Holidays are not considered — only weekends are excluded.

    Returns the number of rows deleted.
    """
    sql = """
        DELETE FROM security_pnl_stat
        WHERE as_of_date < CURRENT_DATE - %(cutoff)s * INTERVAL '1 day'
          AND as_of_date <> (
              -- last business day of as_of_date's month:
              -- find last calendar day, then step back over any weekend
              CASE EXTRACT(DOW FROM (DATE_TRUNC('month', as_of_date) + INTERVAL '1 month - 1 day')::date)
                  WHEN 6 THEN (DATE_TRUNC('month', as_of_date) + INTERVAL '1 month - 1 day')::date - 1
                  WHEN 0 THEN (DATE_TRUNC('month', as_of_date) + INTERVAL '1 month - 1 day')::date - 2
                  ELSE        (DATE_TRUNC('month', as_of_date) + INTERVAL '1 month - 1 day')::date
              END
          )
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {'cutoff': cutoff_days})
            deleted = cur.rowcount
        conn.commit()
    return deleted


def save_pnl_stat(stats: pd.DataFrame, as_of_date, pnl_type: str) -> int:
    """
    Upsert security_pnl_stat rows for (as_of_date, pnl_type).

    stats:      DataFrame from stat_utils.dist_stat() — index=SecurityID
    as_of_date: date or ISO string
    pnl_type:   'LINEAR' | 'BOND' | 'TREASURY'

    Returns the number of rows written.
    """
    if stats.empty:
        return 0

    as_of = pd.to_datetime(as_of_date).date() if not isinstance(as_of_date, date) else as_of_date

    df = stats.rename(columns=_COL_MAP).copy()
    df.index.name = 'security_id'
    df = df.reset_index()
    df['as_of_date'] = as_of
    df['pnl_type']   = pnl_type
    df = df[[c for c in _DB_COLS if c in df.columns]]

    rows = df.to_dict(orient='records')

    with pg_connection() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, _SQL_UPSERT, rows)
        conn.commit()

    return len(rows)


_SENS_UPSERT = """
    INSERT INTO security_sensitivity
        (as_of_date, security_id,
         tenor, delta, gamma, vega, iv,
         ir_tenor, yield, duration, convexity,
         spread_duration, spread_convexity,
         skewness, kurtosis)
    VALUES
        (%(as_of_date)s, %(security_id)s,
         %(tenor)s, %(delta)s, %(gamma)s, %(vega)s, %(iv)s,
         %(ir_tenor)s, %(yield)s, %(duration)s, %(convexity)s,
         %(spread_duration)s, %(spread_convexity)s,
         %(skewness)s, %(kurtosis)s)
    ON CONFLICT (as_of_date, security_id) DO UPDATE SET
        tenor            = EXCLUDED.tenor,
        delta            = EXCLUDED.delta,
        gamma            = EXCLUDED.gamma,
        vega             = EXCLUDED.vega,
        iv               = EXCLUDED.iv,
        ir_tenor         = EXCLUDED.ir_tenor,
        yield            = EXCLUDED.yield,
        duration         = EXCLUDED.duration,
        convexity        = EXCLUDED.convexity,
        spread_duration  = EXCLUDED.spread_duration,
        spread_convexity = EXCLUDED.spread_convexity,
        skewness         = EXCLUDED.skewness,
        kurtosis         = EXCLUDED.kurtosis,
        insert_time      = NOW()
"""

# Maps DataFrame column names → DB column names for all sensitivity fields.
# Only columns present in the DataFrame are used; absent ones default to None.
_SENS_COL_MAP = {
    'SecurityID':      'security_id',
    'Tenor':           'tenor',
    'Delta':           'delta',
    'Gamma':           'gamma',
    'Vega':            'vega',
    'IV':              'iv',
    'IR_Tenor':        'ir_tenor',
    'Yield':           'yield',
    'Duration':        'duration',
    'Convexity':       'convexity',
    'SpreadDuration':  'spread_duration',
    'SpreadConvexity': 'spread_convexity',
    'Skewness':        'skewness',
    'Kurtosis':        'kurtosis',
}

_SENS_DB_COLS = [
    'as_of_date', 'security_id',
    'tenor', 'delta', 'gamma', 'vega', 'iv',
    'ir_tenor', 'yield', 'duration', 'convexity',
    'spread_duration', 'spread_convexity',
    'skewness', 'kurtosis',
]


def save_security_sensitivity(securities: pd.DataFrame, as_of_date) -> int:
    """
    Upsert security_sensitivity rows from a security analytics DataFrame.

    Required column: SecurityID.
    All sensitivity columns (Tenor, Delta, Gamma, Vega, IV, IR_Tenor, Yield,
    Duration, Convexity, SpreadDuration, SpreadConvexity) are optional —
    any column absent from the DataFrame is stored as NULL.

    Callers:
        calc_linear_product_pnl  — Delta=1, all others NULL
        calc_bond_pnl            — Tenor, IR_Tenor, Yield, Duration, SpreadDuration
        calc_treasury_pnl        — Tenor, IR_Tenor, Yield, Duration (no SpreadDuration)

    Returns the number of rows written.
    """
    if securities.empty:
        return 0

    as_of = pd.to_datetime(as_of_date).date() if not isinstance(as_of_date, date) else as_of_date

    df = pd.DataFrame()
    for src_col, db_col in _SENS_COL_MAP.items():
        df[db_col] = securities[src_col] if src_col in securities.columns else None

    for col in _SENS_DB_COLS:
        if col not in df.columns:
            df[col] = None
    df['as_of_date'] = as_of

    rows = df[_SENS_DB_COLS].to_dict(orient='records')

    with pg_connection() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, _SENS_UPSERT, rows)
        conn.commit()

    return len(rows)


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import argparse
    import logging

    logging.basicConfig(level=logging.INFO, format='%(levelname)s  %(message)s')
    log = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        description='Purge old rows from security_pnl_stat, keeping month-end dates.'
    )
    parser.add_argument(
        '--days', type=int, default=31, metavar='N',
        help='Delete rows older than N days (default: 31).',
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Preview rows that would be deleted without making any changes.',
    )
    args = parser.parse_args()

    if args.dry_run:
        sql_preview = """
            SELECT as_of_date, pnl_type, COUNT(*) AS rows
            FROM security_pnl_stat
            WHERE as_of_date < CURRENT_DATE - %(cutoff)s * INTERVAL '1 day'
              AND as_of_date <> (
                  CASE EXTRACT(DOW FROM (DATE_TRUNC('month', as_of_date) + INTERVAL '1 month - 1 day')::date)
                      WHEN 6 THEN (DATE_TRUNC('month', as_of_date) + INTERVAL '1 month - 1 day')::date - 1
                      WHEN 0 THEN (DATE_TRUNC('month', as_of_date) + INTERVAL '1 month - 1 day')::date - 2
                      ELSE        (DATE_TRUNC('month', as_of_date) + INTERVAL '1 month - 1 day')::date
                  END
              )
            GROUP BY as_of_date, pnl_type
            ORDER BY as_of_date, pnl_type
        """
        with pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_preview, {'cutoff': args.days})
                rows = cur.fetchall()

        if not rows:
            log.info('Dry run: no rows would be deleted (cutoff=%d days).', args.days)
        else:
            total = sum(r[2] for r in rows)
            log.info('Dry run: %d rows would be deleted (cutoff=%d days):', total, args.days)
            for as_of, pnl_type, count in rows:
                log.info('  %s  %-10s  %d rows', as_of, pnl_type, count)
    else:
        n = purge_pnl_stat(cutoff_days=args.days)
        log.info('Purged %d rows (cutoff=%d days).', n, args.days)
