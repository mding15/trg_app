"""
update_sec_attr_for_options.py — Insert missing security_attribute rows for option securities.

security_attribute (PK security_id) holds risk-model attributes used by
process2/update_security_info.py to enrich positions. Option securities are
defined in option_info, but not every option automatically gets a
security_attribute row. This script finds option_info security_ids with no
matching security_attribute row and inserts one for each, deriving values as
follows:

    security_id             option_info.security_id
    security_name           security_info."SecurityName"
    class                   constant 'Derivatives'
    sc1                     constant 'Option'
    sc2                     NULL
    country/region/sector/  copied from the underlying's own security_attribute
    industry/currency/      row (option_info.underlying_sec_id), when one exists;
    expected_return         else fallback to 'United States'/'North America'/
                             'Basket'/'Basket'/'USD'/NULL
    option_type             option_info.option_type
    payment_frequency       NULL
    maturity_date           option_info.maturity
    option_strike           option_info.strike
    underlying_security_id  option_info.underlying_sec_id
    coupon_rate             NULL
    isin / cusip            NULL
    ticker                  security_xref."REF_ID" where "REF_TYPE"='Ticker'
                             for that security_id (most recent "DateAdded" if
                             more than one row)

security_ids that already have a security_attribute row are left untouched
(ON CONFLICT (security_id) DO NOTHING) — existing rows, including manually
curated ones, are never overwritten by this script.

Usage:
    python maintenance/update_sec_attr_for_options.py
    python maintenance/update_sec_attr_for_options.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection

_UNDERLYING_FALLBACK = {
    'country':         'United States',
    'region':          'North America',
    'sector':          'Basket',
    'industry':        'Basket',
    'currency':        'USD',
    'expected_return': None,
}

_INHERITED_COLS = ('country', 'region', 'sector', 'industry', 'currency', 'expected_return')

_INSERT_SQL = """
    INSERT INTO security_attribute
        (security_id, security_name, expected_return, currency, "class", sc1, sc2,
         country, region, sector, industry, option_type, payment_frequency,
         maturity_date, option_strike, underlying_security_id, coupon_rate,
         isin, cusip, ticker)
    VALUES
        (%(security_id)s, %(security_name)s, %(expected_return)s, %(currency)s, %(class)s, %(sc1)s, %(sc2)s,
         %(country)s, %(region)s, %(sector)s, %(industry)s, %(option_type)s, %(payment_frequency)s,
         %(maturity_date)s, %(option_strike)s, %(underlying_security_id)s, %(coupon_rate)s,
         %(isin)s, %(cusip)s, %(ticker)s)
    ON CONFLICT (security_id) DO NOTHING
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger('update_sec_attr_for_options')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s', '%H:%M:%S')
    )
    logger.addHandler(handler)
    return logger


def _fetch_missing_options(cur) -> list[dict]:
    """option_info rows (joined to security_info for the name) that have no
    security_attribute row yet."""
    cur.execute(
        """
        SELECT oi.security_id, oi.option_type, oi.maturity, oi.strike,
               oi.underlying_sec_id, si."SecurityName"
        FROM option_info oi
        JOIN security_info si ON si."SecurityID" = oi.security_id
        LEFT JOIN security_attribute sa ON sa.security_id = oi.security_id
        WHERE sa.security_id IS NULL
        """
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _fetch_underlying_attributes(cur, underlying_ids: list[str]) -> dict[str, dict]:
    """{security_id: {country, region, sector, industry, currency, expected_return}}
    for the given underlying security_ids, from their own security_attribute row."""
    if not underlying_ids:
        return {}
    cur.execute(
        """
        SELECT security_id, country, region, sector, industry, currency, expected_return
        FROM security_attribute
        WHERE security_id = ANY(%s)
        """,
        (underlying_ids,),
    )
    cols = [d[0] for d in cur.description]
    return {row[0]: dict(zip(cols, row)) for row in cur.fetchall()}


def _fetch_tickers(cur, security_ids: list[str]) -> dict[str, str]:
    """{security_id: REF_ID} for REF_TYPE='Ticker', picking the most recently
    added row per security_id when more than one exists."""
    if not security_ids:
        return {}
    cur.execute(
        """
        SELECT DISTINCT ON ("SecurityID") "SecurityID", "REF_ID"
        FROM security_xref
        WHERE "REF_TYPE" = 'Ticker' AND "SecurityID" = ANY(%s)
        ORDER BY "SecurityID", "DateAdded" DESC, id DESC
        """,
        (security_ids,),
    )
    return {row[0]: row[1] for row in cur.fetchall()}


# ── Core logic ────────────────────────────────────────────────────────────────

def build_rows(cur) -> list[dict]:
    """Return one insert-ready dict per option_info security_id that is
    currently missing from security_attribute."""
    missing = _fetch_missing_options(cur)
    if not missing:
        return []

    underlying_ids = sorted({r['underlying_sec_id'] for r in missing if r['underlying_sec_id']})
    underlying_attrs = _fetch_underlying_attributes(cur, underlying_ids)

    security_ids = [r['security_id'] for r in missing]
    tickers = _fetch_tickers(cur, security_ids)

    rows = []
    for r in missing:
        underlying_attr = underlying_attrs.get(r['underlying_sec_id'], {})
        row = {
            'security_id':            r['security_id'],
            'security_name':          r['SecurityName'],
            'class':                  'Derivatives',
            'sc1':                    'Option',
            'sc2':                    None,
            'option_type':            r['option_type'],
            'payment_frequency':      None,
            'maturity_date':          r['maturity'],
            'option_strike':          r['strike'],
            'underlying_security_id': r['underlying_sec_id'],
            'coupon_rate':            None,
            'isin':                   None,
            'cusip':                  None,
            'ticker':                 tickers.get(r['security_id']),
        }
        for col in _INHERITED_COLS:
            row[col] = underlying_attr.get(col) if underlying_attr.get(col) is not None else _UNDERLYING_FALLBACK[col]
        rows.append(row)

    return rows


def run(dry_run: bool = False) -> None:
    log = _setup_logger()

    with pg_connection() as conn:
        with conn.cursor() as cur:
            rows = build_rows(cur)

            if not rows:
                log.info('No option_info security_ids are missing a security_attribute row — nothing to do.')
                return

            log.info(f'{len(rows)} option security_id(s) missing a security_attribute row:')
            for row in rows:
                log.info(
                    f"  {row['security_id']}  '{row['security_name']}'  ticker={row['ticker']}  "
                    f"underlying={row['underlying_security_id']}  sector={row['sector']}"
                )

            if dry_run:
                log.info('─' * 60)
                log.info('DRY RUN — no data will be written to the database')
                return

            inserted = 0
            conflicts = 0
            errors = 0

            log.info('Inserting …')
            for row in rows:
                try:
                    cur.execute('SAVEPOINT row_sp')
                    cur.execute(_INSERT_SQL, row)
                    if cur.rowcount == 0:
                        cur.execute('RELEASE SAVEPOINT row_sp')
                        log.warning(f"  {row['security_id']} skipped — already exists (conflict)")
                        conflicts += 1
                    else:
                        cur.execute('RELEASE SAVEPOINT row_sp')
                        inserted += 1
                except Exception as e:
                    cur.execute('ROLLBACK TO SAVEPOINT row_sp')
                    log.warning(f"  {row['security_id']} error — {str(e)[:160]}")
                    errors += 1

        conn.commit()

    log.info('─' * 60)
    log.info(f'Done.  Inserted: {inserted}  Conflicts: {conflicts}  Errors: {errors}  Total: {len(rows)}')


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Insert missing security_attribute rows for option securities.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'Examples:\n'
            '  python maintenance/update_sec_attr_for_options.py\n'
            '  python maintenance/update_sec_attr_for_options.py --dry-run\n'
        ),
    )
    parser.add_argument('--dry-run', action='store_true',
                         help='Preview rows that would be inserted without writing to the DB')
    args = parser.parse_args()

    run(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
