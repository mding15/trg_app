"""
update_port_position.py — Resolve and persist SecurityID for port_positions
rows that were left unresolved at upload time.

dashboard/process_uploaded_portfolio.py's step 2b (lookup_security_ids())
resolves SecurityID via TRG_ID -> ISIN -> CUSIP -> BB_GLOBAL -> Ticker at
upload time. A position whose security wasn't in security_info/security_xref
yet stays SecurityID=NULL in port_positions, and nothing currently re-checks
it later. This script re-runs that same lookup (reusing
process2/security_lookup.py::lookup_security_ids, not reimplementing it) for
one portfolio's still-unresolved rows, and for every row that resolves,
persists SecurityID plus the security_attribute-derived columns
ExpectedReturn/OptionType/PaymentFrequency/MaturityDate/OptionStrike/
UnderlyingSecurityID/CouponRate — the same fields step 2c's
update_security_info() would have filled in, left NULL originally only
because SecurityID was unknown at the time.

Scope is a single port_id, given either directly via --port-id or resolved
from --account-id via process2/tracked_portfolios.py::load_tracked_portfolios
(the same "current tracked portfolio for this account" logic
process2/tracked_proc_positions.py uses — latest upload_dt per account_id,
not the largest port_id). Rows that still don't resolve (security truly not
set up yet) are left untouched. Triggering a VaR recalculation for the
affected portfolio is out of scope — a separate, manual step.

Usage:
    python maintenance/update_port_position.py --port-id 1234
    python maintenance/update_port_position.py --account-id 42
    python maintenance/update_port_position.py --port-id 1234 --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection
from process2.security_lookup import lookup_security_ids
from process2.tracked_portfolios import load_tracked_portfolios

_ATTR_COLS = [
    'expected_return', 'option_type', 'payment_frequency', 'maturity_date',
    'option_strike', 'underlying_security_id', 'coupon_rate',
]

_UPDATE_SQL = """
    UPDATE port_positions
    SET "SecurityID" = %(SecurityID)s,
        "ExpectedReturn" = %(expected_return)s,
        "OptionType" = %(option_type)s,
        "PaymentFrequency" = %(payment_frequency)s,
        "MaturityDate" = %(maturity_date)s,
        "OptionStrike" = %(option_strike)s,
        "UnderlyingSecurityID" = %(underlying_security_id)s,
        "CouponRate" = %(coupon_rate)s
    WHERE port_id = %(port_id)s AND "ID" = %(ID)s
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger('update_port_position')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s', '%H:%M:%S')
    )
    logger.addHandler(handler)
    return logger


def _fetch_missing(cur, port_id: int) -> pd.DataFrame:
    cur.execute(
        """
        SELECT "ID", "ISIN", "CUSIP", "Ticker"
        FROM port_positions
        WHERE port_id = %s AND ("SecurityID" IS NULL OR "SecurityID" = '')
        """,
        (port_id,),
    )
    cols = [d[0] for d in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)


def _fetch_attributes(cur, security_ids: list[str]) -> dict[str, dict]:
    """{security_id: {expected_return, option_type, payment_frequency,
    maturity_date, option_strike, underlying_security_id, coupon_rate}}"""
    if not security_ids:
        return {}
    cur.execute(
        f"""
        SELECT security_id, {', '.join(_ATTR_COLS)}
        FROM security_attribute
        WHERE security_id = ANY(%s)
        """,
        (security_ids,),
    )
    cols = [d[0] for d in cur.description]
    return {row[0]: dict(zip(cols, row)) for row in cur.fetchall()}


# ── Core logic ────────────────────────────────────────────────────────────────

def build_rows(cur, port_id: int) -> tuple[list[dict], int]:
    """Returns (update-ready rows for resolved positions, count still unresolved)."""
    missing = _fetch_missing(cur, port_id)
    if missing.empty:
        return [], 0

    resolved = lookup_security_ids(missing)
    unresolved_count = int(resolved['SecurityID'].isna().sum())
    resolved = resolved[resolved['SecurityID'].notna()]

    security_ids = resolved['SecurityID'].unique().tolist()
    attrs = _fetch_attributes(cur, security_ids)

    rows = []
    for _, r in resolved.iterrows():
        attr = attrs.get(r['SecurityID'], {})
        row = {'port_id': port_id, 'ID': r['ID'], 'SecurityID': r['SecurityID']}
        for col in _ATTR_COLS:
            row[col] = attr.get(col)
        rows.append(row)

    return rows, unresolved_count


def _resolve_port_id(cur, port_id: int | None, account_id: int | None, log: logging.Logger) -> int | None:
    """port_id as given, or resolved from account_id via load_tracked_portfolios
    (the current tracked portfolio for that account — latest upload_dt, not the
    largest port_id). Returns None if account_id has no tracked portfolio."""
    if port_id is not None:
        return port_id

    portfolios = load_tracked_portfolios(cur, account_id)
    if not portfolios:
        log.info(f'No tracked portfolio found for account_id={account_id} — nothing to do.')
        return None

    portfolio = portfolios[0]
    log.info(
        f"Resolved account_id={account_id} -> port_id={portfolio['port_id']} "
        f"(port_name={portfolio['port_name']!r}, upload_dt={portfolio['upload_dt']})"
    )
    return portfolio['port_id']


def run(port_id: int | None = None, account_id: int | None = None, dry_run: bool = False) -> None:
    log = _setup_logger()

    with pg_connection() as conn:
        with conn.cursor() as cur:
            port_id = _resolve_port_id(cur, port_id, account_id, log)
            if port_id is None:
                return

            rows, unresolved_count = build_rows(cur, port_id)

            if not rows and not unresolved_count:
                log.info(f'No port_positions rows missing SecurityID for port_id={port_id} — nothing to do.')
                return

            log.info(f'port_id={port_id}: {len(rows)} row(s) resolved, {unresolved_count} still unresolved')
            for row in rows:
                log.info(
                    f"  ID={row['ID']}  SecurityID={row['SecurityID']}  "
                    f"option_type={row['option_type']}  underlying_security_id={row['underlying_security_id']}"
                )

            if dry_run:
                log.info('─' * 60)
                log.info('DRY RUN — no data will be written to the database')
                return

            if not rows:
                log.info('Nothing resolved — no update to perform.')
                return

            updated = 0
            errors = 0

            log.info('Updating …')
            for row in rows:
                try:
                    cur.execute('SAVEPOINT row_sp')
                    cur.execute(_UPDATE_SQL, row)
                    cur.execute('RELEASE SAVEPOINT row_sp')
                    updated += 1
                except Exception as e:
                    cur.execute('ROLLBACK TO SAVEPOINT row_sp')
                    log.warning(f"  ID={row['ID']} error — {str(e)[:160]}")
                    errors += 1

        conn.commit()

    log.info('─' * 60)
    log.info(f'Done.  Updated: {updated}  Errors: {errors}  Still unresolved: {unresolved_count}')


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resolve and persist SecurityID (and related security_attribute columns) "
                    "for a portfolio's port_positions rows still missing it.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'Examples:\n'
            '  python maintenance/update_port_position.py --port-id 1234\n'
            '  python maintenance/update_port_position.py --account-id 42\n'
            '  python maintenance/update_port_position.py --port-id 1234 --dry-run\n'
        ),
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--port-id', type=int, metavar='PORT_ID',
                        help='port_id whose port_positions rows should be checked')
    group.add_argument('--account-id', type=int, metavar='ACCOUNT_ID',
                        help="Resolve to this account's current tracked portfolio "
                             '(latest upload_dt) instead of specifying --port-id directly')
    parser.add_argument('--dry-run', action='store_true',
                         help='Preview rows that would be updated without writing to the DB')
    args = parser.parse_args()

    run(port_id=args.port_id, account_id=args.account_id, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
