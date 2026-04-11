"""
process_mssb_positions.py — Process raw mssb_posit data and insert into proc_positions.

Steps:
    1. Fetch all mssb_posit rows for the given feed_date.
    2. Batch-load account_id cache from broker_account for all accounts in the feed.
       Rows whose account is not found are skipped and logged — no new entries are created.
    3. Batch-load security_id cache from security_xref for all ISINs and CUSIPs in the feed.
       Securities not found are created on the fly and added to the cache.
    4. Batch-load asset_class map from broker_asset_class_map for broker='MSSB'.
       Map security_code → asset_class; NULL if not found, logged once per unmapped code.
    5. Assign position_id 1..n (only counting rows that were successfully processed).
    6. Archieve to proc_positions_hist.
       For each account_id in the feed:
         - Move rows with as_of_date < feed_date to proc_positions_hist (overwrite if already there).
         - Delete rows with as_of_date = feed_date (will be replaced).
    7. Insert processed rows into proc_positions.

Usage:
    python process_mssb_positions.py                                  # feed_date from proc_asof_date table
    python process_mssb_positions.py --date 2025-01-15                # specific date
    python process_mssb_positions.py --all                            # all dates in mssb_posit
    python process_mssb_positions.py --account-id 5                   # one account, default date
    python process_mssb_positions.py --date 2025-01-15 --account-id 5 # one account, specific date
    python process_mssb_positions.py --all --account-id 5             # one account, all dates
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from database2 import pg_connection, get_proc_asof_date


# ── logging setup ──────────────────────────────────────────────────────────────

def _setup_logger(feed_date, account_id=None) -> logging.Logger:
    log_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'log')
    os.makedirs(log_dir, exist_ok=True)
    account_suffix = f'_account{account_id}' if account_id is not None else ''
    log_file = os.path.join(
        log_dir,
        f'process_mssb_positions_{feed_date}{account_suffix}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    )
    logger = logging.getLogger(f'process_mssb_{feed_date}{account_suffix}')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger


# ── batch cache loaders ────────────────────────────────────────────────────────

def _load_account_cache(cur, raw_rows: list[dict], account_id=None) -> dict[tuple, int]:
    """
    Batch-load broker_account rows for all (account, routing_code) pairs in raw_rows.
    Returns {(account, routing_code): account_id}.
    Pairs not found in the table will be absent from the dict.
    If account_id is given, only rows for that account_id are loaded — all others
    will be absent from the cache and naturally skipped by the caller.
    """
    accounts = list({r.get('account') or '' for r in raw_rows})
    if not accounts:
        return {}

    if account_id is not None:
        cur.execute(
            """
            SELECT account_id, broker_account, routing_code
            FROM broker_account
            WHERE broker = 'Morgan Stanley'
              AND broker_account = ANY(%s)
              AND account_id = %s
            """,
            (accounts, account_id),
        )
    else:
        cur.execute(
            """
            SELECT account_id, broker_account, routing_code
            FROM broker_account
            WHERE broker = 'Morgan Stanley'
              AND broker_account = ANY(%s)
            """,
            (accounts,),
        )
    return {(row[1], row[2]): row[0] for row in cur.fetchall()}


def _load_price_cache(cur, raw_rows: list[dict], feed_date) -> dict[str, tuple]:
    """
    Batch-load mssb_price rows for all CUSIPs in raw_rows for the given feed_date.
    Returns {cusip: (price2, price_last_date)}.
    """
    cusips = list({r.get('cusip') or '' for r in raw_rows if r.get('cusip')})
    if not cusips:
        return {}

    cur.execute(
        """
        SELECT cusip, price2, price_last_date
        FROM mssb_price
        WHERE feed_date = %s AND cusip = ANY(%s)
        """,
        (feed_date, cusips),
    )
    return {row[0]: (row[1], row[2]) for row in cur.fetchall()}


def _load_cash_security_map(cur) -> dict[str, str]:
    """
    Load SecurityID for cash securities keyed by Currency.
    Returns {currency: SecurityID} for all cash securities with DataSource='TRG CASH'.
    Used to resolve Money Market Fund (security_code='MF') positions by currency.
    """
    cur.execute(
        """
        SELECT "SecurityID", "Currency"
        FROM security_info
        WHERE "AssetClass" = 'Cash' AND "DataSource" = 'TRG CASH'
        """
    )
    return {row[1]: row[0] for row in cur.fetchall()}


def _load_asset_class_map(cur) -> dict[str, str]:
    """
    Load all broker_asset_class_map rows for broker='MSSB'.
    Returns {security_code: asset_class}.
    """
    cur.execute(
        "SELECT security_code, asset_class FROM broker_asset_class_map WHERE broker = 'MSSB'",
    )
    return {row[0]: row[1] for row in cur.fetchall()}


def _load_security_cache(cur, raw_rows: list[dict]) -> dict[tuple, str]:
    """
    Batch-load security_xref rows for all ISINs and CUSIPs in raw_rows.
    Returns {('ISIN', isin): security_id, ('CUSIP', cusip): security_id, ...}.
    """
    isins  = list({r.get('isin')  or '' for r in raw_rows if r.get('isin')})
    cusips = list({r.get('cusip') or '' for r in raw_rows if r.get('cusip')})

    cache: dict[tuple, str] = {}

    if isins:
        cur.execute(
            'SELECT "REF_ID", "SecurityID" FROM security_xref WHERE "REF_TYPE" = %s AND "REF_ID" = ANY(%s)',
            ('ISIN', isins),
        )
        for ref_id, sec_id in cur.fetchall():
            cache[('ISIN', ref_id)] = sec_id

    if cusips:
        cur.execute(
            'SELECT "REF_ID", "SecurityID" FROM security_xref WHERE "REF_TYPE" = %s AND "REF_ID" = ANY(%s)',
            ('CUSIP', cusips),
        )
        for ref_id, sec_id in cur.fetchall():
            # Don't overwrite if ISIN already resolved the same security
            cache.setdefault(('CUSIP', ref_id), sec_id)

    return cache


# ── security creation ──────────────────────────────────────────────────────────

def _create_security(
    cur,
    security_name: str,
    currency: str,
    security_code: str,
    asset_class_map: dict[str, str],
    logger: logging.Logger,
) -> str:
    """Insert a new row into security_info and return the generated SecurityID.
    Maps security_code → asset_class via asset_class_map; NULL if not found."""
    asset_class = asset_class_map.get(security_code)
    if asset_class is None and security_code:
        logger.warning(
            f"security_code '{security_code}' not found in broker_asset_class_map "
            f"(broker='MSSB') — AssetClass set to NULL in security_info"
        )
    cur.execute(
        """
        INSERT INTO security_info
            ("SecurityName", "Currency", "AssetClass", "AssetType", "DataSource")
        VALUES (%s, %s, %s, %s, 'MSSB')
        RETURNING id
        """,
        (security_name, currency, asset_class, ''),
    )
    new_id = cur.fetchone()[0]
    # SecurityID: "T1" + id zero-padded to 7 digits = 9 chars total, e.g. "T10000012"
    security_id = f'T1{str(new_id).zfill(7)}'
    cur.execute(
        'UPDATE security_info SET "SecurityID" = %s WHERE id = %s',
        (security_id, new_id),
    )
    return security_id


def _add_xref_if_missing(cur, security_id: str, ref_type: str, ref_id: str) -> None:
    """Insert a security_xref row if ref_id is non-empty and not already present."""
    if not ref_id or not ref_id.strip():
        return
    cur.execute(
        'SELECT 1 FROM security_xref WHERE "REF_TYPE" = %s AND "REF_ID" = %s',
        (ref_type, ref_id),
    )
    if cur.fetchone():
        return
    cur.execute(
        """
        INSERT INTO security_xref ("REF_ID", "REF_TYPE", "SecurityID", "DataSource")
        VALUES (%s, %s, %s, 'MSSB')
        """,
        (ref_id, ref_type, security_id),
    )


def _get_or_create_security_id(
    cur,
    security_cache: dict[tuple, str],
    isin: str | None,
    cusip: str | None,
    ticker: str | None,
    security_name: str,
    currency: str,
    security_code: str,
    asset_class_map: dict[str, str],
    cash_security_map: dict[str, str],
    logger: logging.Logger,
) -> str | None:
    """
    Look up SecurityID from the in-memory cache (ISIN first, then CUSIP).
    If not found and at least one identifier is present, create a new security + xrefs
    and update the cache so subsequent rows with the same identifiers skip the DB lookup.

    When both ISIN and CUSIP are missing:
      - security_code='MF': resolve by currency from cash_security_map (TRG CASH entries).
      - Any other code: do not create a new security.
    Returns None if the SecurityID cannot be resolved; caller should skip the row.
    """
    # ── Standard lookup: ISIN first, then CUSIP ──────────────────────────────
    if isin and ('ISIN', isin) in security_cache:
        return security_cache[('ISIN', isin)]

    if cusip and ('CUSIP', cusip) in security_cache:
        return security_cache[('CUSIP', cusip)]

    # ── Both ISIN and CUSIP missing ───────────────────────────────────────────
    if not isin and not cusip:
        # MF (Money Market Fund): resolve by currency from TRG CASH map
        if security_code == 'MF':
            security_id = cash_security_map.get(currency)
            if security_id:
                return security_id

        # Could not resolve — log and return None; caller will skip the row
        logger.warning(
            f"Cannot resolve security_id: "
            f"security_name='{security_name}' currency='{currency}' security_code='{security_code}'"
        )
        return None

    # ── At least one identifier present but not in cache: create new security ─
    security_id = _create_security(cur, security_name or '', currency or '', security_code, asset_class_map, logger)
    _add_xref_if_missing(cur, security_id, 'ISIN',   isin   or '')
    _add_xref_if_missing(cur, security_id, 'CUSIP',  cusip  or '')
    _add_xref_if_missing(cur, security_id, 'Ticker', ticker or '')

    # Populate cache so later rows with the same ISIN/CUSIP resolve instantly
    if isin:
        security_cache[('ISIN', isin)]   = security_id
    if cusip:
        security_cache[('CUSIP', cusip)] = security_id

    logger.info(
        f"Created new security: name='{security_name}' isin='{isin}' cusip='{cusip}' → {security_id}"
    )
    return security_id


# ── archive helpers ───────────────────────────────────────────────────────────

def _archive_and_replace(cur, account_ids: list[int], feed_date, feed_source: str, logger: logging.Logger) -> None:
    """
    For each account_id in account_ids, manage proc_positions rows before the new insert.
    All operations are scoped to feed_source so other feed sources are not affected.

    - Rows with as_of_date < feed_date  → move to proc_positions_hist (overwrite any
                                          existing hist rows for the same account/date/feed_source).
    - Rows with as_of_date = feed_date  → delete (will be replaced by the new insert).
    """
    ids = list(set(account_ids))

    # ① Delete hist rows that are about to be re-archived from proc_positions.
    #   Scoped to dates that currently exist in proc_positions (not all history)
    #   so we don't wipe archived data that has already been removed from proc_positions.
    cur.execute(
        """
        DELETE FROM proc_positions_hist h
        WHERE h.account_id = ANY(%s)
          AND h.feed_source = %s
          AND EXISTS (
              SELECT 1 FROM proc_positions p
              WHERE p.account_id = h.account_id
                AND p.as_of_date  = h.as_of_date
                AND p.feed_source = h.feed_source
                AND p.as_of_date  < %s
          )
        """,
        (ids, feed_source, feed_date),
    )

    # ② Copy older rows from proc_positions into hist — scoped to feed_source
    cur.execute(
        """
        INSERT INTO proc_positions_hist
            (as_of_date, account_id, position_id, security_id, security_name,
             isin, cusip, ticker, quantity, market_value, asset_class, currency,
             broker_account, broker, last_price, last_price_date, feed_source, insert_time, archived_at)
        SELECT
            as_of_date, account_id, position_id, security_id, security_name,
            isin, cusip, ticker, quantity, market_value, asset_class, currency,
            broker_account, broker, last_price, last_price_date, feed_source, insert_time, NOW()
        FROM proc_positions
        WHERE account_id = ANY(%s) AND as_of_date < %s AND feed_source = %s
        """,
        (ids, feed_date, feed_source),
    )
    archived = cur.rowcount

    # ③ Remove those older rows from proc_positions — scoped to feed_source
    cur.execute(
        "DELETE FROM proc_positions WHERE account_id = ANY(%s) AND as_of_date < %s AND feed_source = %s",
        (ids, feed_date, feed_source),
    )

    # ④ Remove same-date rows from proc_positions (will be replaced by new insert) — scoped to feed_source
    cur.execute(
        "DELETE FROM proc_positions WHERE account_id = ANY(%s) AND as_of_date = %s AND feed_source = %s",
        (ids, feed_date, feed_source),
    )
    replaced = cur.rowcount

    if archived:
        logger.info(f"Archived {archived} rows to proc_positions_hist (as_of_date < {feed_date})")
    if replaced:
        logger.info(f"Deleted {replaced} existing proc_positions rows for as_of_date={feed_date} (to be replaced)")


# ── main ───────────────────────────────────────────────────────────────────────

def process_mssb_positions(feed_date, account_id=None) -> int:
    """
    Process mssb_posit rows for feed_date and insert into proc_positions.
    If account_id is given, only broker accounts belonging to that account_id
    are processed; all other rows are skipped.
    Rows whose account cannot be resolved in broker_account are skipped and logged.
    For each account_id in the feed, older proc_positions rows are moved to
    proc_positions_hist and same-date rows are replaced.
    Returns the number of rows inserted.
    """
    logger = _setup_logger(feed_date, account_id)
    if account_id is not None:
        logger.info(f"=== Start processing mssb_posit for feed_date={feed_date}, account_id={account_id} ===")
    else:
        logger.info(f"=== Start processing mssb_posit for feed_date={feed_date} ===")

    with pg_connection() as conn:
        # ── Step 1: fetch raw rows ──────────────────────────────────────────────
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM mssb_posit WHERE feed_date = %s",
                (feed_date,),
            )
            col_names = [desc[0] for desc in cur.description]
            raw_rows = [dict(zip(col_names, row)) for row in cur.fetchall()]

        if not raw_rows:
            logger.warning(f"No rows found in mssb_posit for feed_date={feed_date}")
            return 0

        logger.info(f"Found {len(raw_rows)} raw rows in mssb_posit")

        with conn.cursor() as cur:
            # ── Step 2: batch-load account cache ───────────────────────────────
            account_cache = _load_account_cache(cur, raw_rows, account_id)
            logger.info(f"Loaded {len(account_cache)} broker_account entries into cache")

            # ── Step 3: batch-load security cache ──────────────────────────────
            security_cache = _load_security_cache(cur, raw_rows)
            logger.info(f"Loaded {len(security_cache)} security_xref entries into cache")

            # ── Step 3b: batch-load price cache ────────────────────────────────
            price_cache = _load_price_cache(cur, raw_rows, feed_date)
            logger.info(f"Loaded {len(price_cache)} mssb_price entries into cache")

            # ── Step 4: load asset_class map ────────────────────────────────────
            asset_class_map = _load_asset_class_map(cur)
            logger.info(f"Loaded {len(asset_class_map)} broker_asset_class_map entries")

            # ── Step 4b: load cash security map (for MF resolution by currency) ─
            cash_security_map = _load_cash_security_map(cur)
            logger.info(f"Loaded {len(cash_security_map)} TRG CASH entries into cash_security_map")

            missing_accounts: set[tuple] = set()
            processed: list[dict] = []
            skipped = 0
            pos_idx = 1  # only incremented for accepted rows

            for r in raw_rows:
                account      = r.get('account')      or ''
                routing_code = r.get('routing_code') or ''
                isin         = r.get('isin')          or ''
                cusip        = r.get('cusip')         or ''
                symbol       = r.get('symbol')        or ''
                security_name = r.get('security_description') or ''
                currency     = r.get('currency')      or ''
                security_code = r.get('security_code') or ''
                asset_class   = security_code  # raw value stored in proc_positions

                # ── Step 2 (per row): resolve account_id — skip if not found ────
                account_id = account_cache.get((account, routing_code))

                if account_id is None:
                    cache_key = (account, routing_code)
                    if cache_key not in missing_accounts:
                        missing_accounts.add(cache_key)
                        logger.warning(
                            f"account not found in broker_account — "
                            f"broker='Morgan Stanley' account='{account}' routing_code='{routing_code}' — "
                            f"all rows for this account will be skipped"
                        )
                    skipped += 1
                    continue

                # ── Step 3 (per row): resolve security_id ───────────────────────
                security_id = _get_or_create_security_id(
                    cur,
                    security_cache,
                    isin   or None,
                    cusip  or None,
                    symbol or None,
                    security_name,
                    currency,
                    security_code,
                    asset_class_map,
                    cash_security_map,
                    logger,
                )
                if security_id is None:
                    skipped += 1
                    continue

                # ── Step 3b (per row): resolve price ────────────────────────────
                price_entry = price_cache.get(cusip)
                last_price      = price_entry[0] if price_entry else None
                last_price_date = price_entry[1] if price_entry else None

                # ── Step 4+5: build processed row (position_id = 1..n) ──────────
                processed.append({
                    'as_of_date':      r.get('feed_date'),
                    'account_id':      account_id,
                    'position_id':     str(pos_idx),
                    'security_id':     security_id,
                    'security_name':   security_name,
                    'isin':            isin,
                    'cusip':           cusip,
                    'ticker':          symbol,
                    'quantity':        r.get('quantity'),
                    'market_value':    r.get('market_base'),
                    'asset_class':     asset_class,
                    'currency':        currency,
                    'broker_account':  account,
                    'broker':          'Morgan Stanley',
                    'last_price':      last_price,
                    'last_price_date': last_price_date,
                    # ── HARDCODED: feed_source identifies this pipeline ──────
                    'feed_source':     'mssb',
                })
                pos_idx += 1

            if skipped:
                logger.warning(f"Skipped {skipped} rows due to unresolved account_id")

            # ── Step 6: archive older rows, replace same-date rows, then insert ──
            if processed:
                account_ids = list({r['account_id'] for r in processed})
                _archive_and_replace(cur, account_ids, feed_date, 'mssb', logger)

                insert_sql = """
                    INSERT INTO proc_positions
                        (as_of_date, account_id, position_id, security_id, security_name,
                         isin, cusip, ticker, quantity, market_value, asset_class, currency,
                         broker_account, broker, last_price, last_price_date, feed_source)
                    VALUES
                        (%(as_of_date)s, %(account_id)s, %(position_id)s, %(security_id)s,
                         %(security_name)s, %(isin)s, %(cusip)s, %(ticker)s, %(quantity)s,
                         %(market_value)s, %(asset_class)s, %(currency)s, %(broker_account)s,
                         %(broker)s, %(last_price)s, %(last_price_date)s, %(feed_source)s)
                """
                cur.executemany(insert_sql, processed)

        conn.commit()

    n = len(processed)
    logger.info(f"Inserted {n} rows into proc_positions for feed_date={feed_date}")
    logger.info("=== Done ===")
    return n


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Process MSSB positions into proc_positions')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--date', metavar='YYYY-MM-DD',
                       help='Process a specific feed_date (default: read from proc_asof_date table)')
    group.add_argument('--all', action='store_true',
                       help='Process all feed_dates found in mssb_posit')
    parser.add_argument('--account-id', metavar='ACCOUNT_ID', type=int,
                        help='Limit processing to broker accounts belonging to this account_id')
    args = parser.parse_args()

    _account_id = args.account_id  # None means process all accounts

    if args.all:
        with pg_connection() as _conn:
            with _conn.cursor() as _cur:
                _cur.execute('SELECT DISTINCT feed_date FROM mssb_posit ORDER BY feed_date')
                _feed_dates = [row[0].isoformat() for row in _cur.fetchall()]
        if not _feed_dates:
            print('No feed_dates found in mssb_posit.')
            sys.exit(0)
        print(f'Processing {len(_feed_dates)} feed_date(s): {_feed_dates[0]} → {_feed_dates[-1]}')
        for _feed_date in _feed_dates:
            process_mssb_positions(_feed_date, _account_id)
    else:
        _feed_date = args.date or get_proc_asof_date()
        process_mssb_positions(_feed_date, _account_id)
