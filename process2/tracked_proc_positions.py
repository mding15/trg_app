"""
tracked_proc_positions.py — Daily process: generate proc_positions for tracked portfolios.

Steps:
    1. Get as_of_date from proc_asof_date table (or --date CLI arg).
    2. Select all portfolios from portfolio_info where port_type = 'tracked'.
    3. For each account_id, keep only the portfolio with the latest upload_dt.
    4. Fetch position rows from port_positions for the selected port_ids.
    5. Map port_positions columns to proc_positions shape:
         position_id  = port_positions.ID
         asset_class  = port_positions.Class
         feed_source  = 'file_upload'
    6. Update last_price / last_price_date: bond_price (latest on/before as_of_date, for
       securities where security_info.AssetType IN ('Bond','Treasury')) wins over
       current_price (exact date). Fallback: implied price (MarketValue / Quantity).
    7. Recalculate market_value = quantity x last_price.
    8. Archive older rows to proc_positions_hist, delete same-date rows (feed_source='file_upload').
    9. Insert into proc_positions.

Usage:
    python process2/tracked_proc_positions.py
    python process2/tracked_proc_positions.py --date 2026-05-16
    python process2/tracked_proc_positions.py --account-id 7
    python process2/tracked_proc_positions.py --dry-run
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from database2 import pg_connection, get_proc_asof_date

FEED_SOURCE = 'file_upload'


# ── logging setup ──────────────────────────────────────────────────────────────

def _setup_logger(as_of_date, account_id=None) -> logging.Logger:
    log_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'log')
    os.makedirs(log_dir, exist_ok=True)
    account_suffix = f'_account{account_id}' if account_id is not None else ''
    log_file = os.path.join(
        log_dir,
        f'tracked_proc_positions_{as_of_date}{account_suffix}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    )
    logger = logging.getLogger(f'tracked_proc_positions_{as_of_date}{account_suffix}')
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


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _load_tracked_portfolios(cur, account_id=None) -> list[dict]:
    """
    Fetch portfolios where port_type='tracked', then keep only the latest
    upload_dt per account_id.  If account_id is given, restrict to that account.
    """
    if account_id is not None:
        cur.execute(
            """
            SELECT port_id, account_id, port_name, upload_dt
            FROM portfolio_info
            WHERE port_type = 'tracked' AND filename != 'auto feed' AND account_id = %s
            """,
            (account_id,),
        )
    else:
        cur.execute(
            """
            SELECT port_id, account_id, port_name, upload_dt
            FROM portfolio_info
            WHERE port_type = 'tracked' AND filename != 'auto feed'
            """
        )
    rows = [
        {'port_id': r[0], 'account_id': r[1], 'port_name': r[2], 'upload_dt': r[3]}
        for r in cur.fetchall()
    ]

    # Deduplicate: keep latest upload_dt per account_id
    latest: dict[int, dict] = {}
    for r in rows:
        acc = r['account_id']
        if acc not in latest or (r['upload_dt'] or datetime.min) > (latest[acc]['upload_dt'] or datetime.min):
            latest[acc] = r
    return list(latest.values())


def _load_positions(cur, port_ids: list[int]) -> list[dict]:
    """Fetch all port_positions rows for the given port_ids."""
    cur.execute(
        """
        SELECT port_id, "ID", "SecurityID", "SecurityName", "ISIN", "CUSIP",
               "Ticker", "Quantity", "MarketValue", "userAssetClass", "userCurrency",
               total_cost
        FROM port_positions
        WHERE port_id = ANY(%s)
        """,
        (port_ids,),
    )
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _load_price_cache(cur, security_ids: list[str], as_of_date) -> dict[str, tuple]:
    """
    Batch-fetch closing prices from current_price for the given SecurityIDs and date.
    Returns {SecurityID: (Close, Date)}.
    """
    if not security_ids:
        return {}
    cur.execute(
        """
        SELECT "SecurityID", "Close", "Date"
        FROM current_price
        WHERE "Date" = %s AND "SecurityID" = ANY(%s)
        """,
        (as_of_date, security_ids),
    )
    return {row[0]: (row[1], row[2]) for row in cur.fetchall()}


def _load_bond_price_cache(cur, security_ids: list[str], as_of_date) -> dict[str, tuple]:
    """
    Fetch latest bond prices from bond_price for securities where
    security_info.AssetType IN ('Bond', 'Treasury').
    Uses the most recent price on or before as_of_date.
    Returns {SecurityID: (price/100, price_date)} — price divided by 100 (par-relative).
    """
    if not security_ids:
        return {}
    cur.execute(
        'SELECT "SecurityID" FROM security_info '
        'WHERE "SecurityID" = ANY(%s) AND "AssetType" IN (%s, %s)',
        (security_ids, 'Bond', 'Treasury'),
    )
    bond_ids = [r[0] for r in cur.fetchall()]
    if not bond_ids:
        return {}
    cur.execute(
        'SELECT DISTINCT ON (security_id) security_id, price, price_date '
        'FROM bond_price '
        'WHERE security_id = ANY(%s) AND price_date <= %s '
        'ORDER BY security_id, price_date DESC',
        (bond_ids, as_of_date),
    )
    return {row[0]: (row[1] / 100, row[2]) for row in cur.fetchall()}


# ── archive helper (scoped to FEED_SOURCE) ────────────────────────────────────

def _archive_and_replace(cur, account_ids: list[int], feed_date, logger: logging.Logger) -> None:
    ids = list(set(account_ids))

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
        (ids, FEED_SOURCE, feed_date),
    )

    cur.execute(
        """
        INSERT INTO proc_positions_hist
            (as_of_date, account_id, position_id, security_id, security_name,
             isin, cusip, ticker, quantity, market_value, asset_class, currency,
             broker_account, broker, last_price, last_price_date, feed_source,
             insert_time, archived_at, total_cost)
        SELECT
            as_of_date, account_id, position_id, security_id, security_name,
            isin, cusip, ticker, quantity, market_value, asset_class, currency,
            broker_account, broker, last_price, last_price_date, feed_source,
            insert_time, NOW(), total_cost
        FROM proc_positions
        WHERE account_id = ANY(%s) AND as_of_date < %s AND feed_source = %s
        """,
        (ids, feed_date, FEED_SOURCE),
    )
    archived = cur.rowcount

    cur.execute(
        "DELETE FROM proc_positions WHERE account_id = ANY(%s) AND as_of_date < %s AND feed_source = %s",
        (ids, feed_date, FEED_SOURCE),
    )

    cur.execute(
        "DELETE FROM proc_positions WHERE account_id = ANY(%s) AND as_of_date = %s AND feed_source = %s",
        (ids, feed_date, FEED_SOURCE),
    )
    replaced = cur.rowcount

    if archived:
        logger.info(f"Archived {archived} rows to proc_positions_hist (as_of_date < {feed_date})")
    if replaced:
        logger.info(f"Deleted {replaced} existing proc_positions rows for as_of_date={feed_date} (to be replaced)")


# ── main ───────────────────────────────────────────────────────────────────────

def process_tracked_positions(as_of_date, account_id=None, dry_run=False) -> int:
    logger = _setup_logger(as_of_date, account_id)
    logger.info(
        f"=== Start tracked_proc_positions  as_of_date={as_of_date}"
        + (f"  account_id={account_id}" if account_id is not None else "")
        + (" [DRY RUN]" if dry_run else "")
        + " ==="
    )

    with pg_connection() as conn:
        with conn.cursor() as cur:

            # ── Steps 2+3: tracked portfolios, deduplicated by account_id ─────────
            portfolios = _load_tracked_portfolios(cur, account_id)
            if not portfolios:
                logger.warning("No tracked portfolios found in portfolio_info (port_type='tracked')")
                return 0
            logger.info(f"Selected {len(portfolios)} tracked portfolio(s) (one per account_id):")
            for p in portfolios:
                logger.info(
                    f"  account_id={p['account_id']}  port_id={p['port_id']}"
                    f"  port_name='{p['port_name']}'  upload_dt={p['upload_dt']}"
                )

            port_id_to_account = {p['port_id']: p['account_id'] for p in portfolios}
            port_ids = list(port_id_to_account.keys())

            # ── Step 4: fetch positions ───────────────────────────────────────────
            raw_positions = _load_positions(cur, port_ids)
            if not raw_positions:
                logger.warning(f"No rows found in port_positions for port_ids={port_ids}")
                return 0
            logger.info(f"Fetched {len(raw_positions)} rows from port_positions")

            # ── Step 6: batch-load prices from current_price and bond_price ─────
            sec_ids = list({r['SecurityID'] for r in raw_positions if r.get('SecurityID')})
            price_cache      = _load_price_cache(cur, sec_ids, as_of_date)
            bond_price_cache = _load_bond_price_cache(cur, sec_ids, as_of_date)
            logger.info(
                f"Loaded {len(price_cache)} price entries from current_price for {as_of_date}"
            )
            logger.info(
                f"Loaded {len(bond_price_cache)} price entries from bond_price for {as_of_date}"
            )

        # ── Steps 5+6+7: map columns, apply prices, recalculate market_value ──────
        processed: list[dict] = []
        bond_priced_count = 0
        priced_count      = 0
        implied_count     = 0
        no_price_count    = 0

        for r in raw_positions:
            acct_id     = port_id_to_account[r['port_id']]
            security_id = r.get('SecurityID') or ''
            quantity    = r.get('Quantity')
            orig_mv     = r.get('MarketValue')

            # Resolve price: bond_price (wins) → current_price → implied → None
            bond_entry  = bond_price_cache.get(security_id)
            price_entry = price_cache.get(security_id)
            if bond_entry:
                last_price      = bond_entry[0]
                last_price_date = bond_entry[1]
                bond_priced_count += 1
            elif price_entry:
                last_price      = price_entry[0]
                last_price_date = price_entry[1]
                priced_count += 1
            elif quantity and orig_mv and float(quantity) != 0:
                last_price      = float(orig_mv) / float(quantity)
                last_price_date = None
                implied_count += 1
            else:
                last_price      = None
                last_price_date = None
                no_price_count += 1

            # Recalculate market_value = quantity × last_price
            if quantity is not None and last_price is not None:
                market_value = float(quantity) * float(last_price)
            else:
                market_value = orig_mv

            processed.append({
                'as_of_date':      as_of_date,
                'account_id':      acct_id,
                'position_id':     str(r.get('ID') or ''),
                'security_id':     security_id,
                'security_name':   r.get('SecurityName') or '',
                'isin':            r.get('ISIN') or None,
                'cusip':           r.get('CUSIP') or None,
                'ticker':          r.get('Ticker') or None,
                'quantity':        quantity,
                'market_value':    market_value,
                'asset_class':     r.get('userAssetClass') or None,
                'currency':        r.get('userCurrency') or '',
                'broker_account':  '',
                'broker':          '',
                'last_price':      last_price,
                'last_price_date': last_price_date,
                'feed_source':     FEED_SOURCE,
                'total_cost':      r.get('total_cost'),
            })

        logger.info(f"Priced from bond_price    : {bond_priced_count}")
        logger.info(f"Priced from current_price : {priced_count}")
        logger.info(f"Implied price (MV / Qty)  : {implied_count}")
        logger.info(f"No price available        : {no_price_count}")

        if dry_run:
            logger.info("─" * 60)
            logger.info("DRY RUN — no data written to database")
            logger.info(f"  Would insert {len(processed)} rows into proc_positions")
            logger.info(f"  as_of_date  : {as_of_date}")
            logger.info(f"  feed_source : {FEED_SOURCE}")
            logger.info(f"  account_ids : {sorted({r['account_id'] for r in processed})}")
            logger.info("─" * 60)
            return 0

        # ── Steps 8+9: archive older rows, replace same-date rows, insert ─────────
        with conn.cursor() as cur:
            account_ids = list({r['account_id'] for r in processed})
            _archive_and_replace(cur, account_ids, as_of_date, logger)

            cur.executemany(
                """
                INSERT INTO proc_positions
                    (as_of_date, account_id, position_id, security_id, security_name,
                     isin, cusip, ticker, quantity, market_value, asset_class, currency,
                     broker_account, broker, last_price, last_price_date, feed_source,
                     total_cost)
                VALUES
                    (%(as_of_date)s, %(account_id)s, %(position_id)s, %(security_id)s,
                     %(security_name)s, %(isin)s, %(cusip)s, %(ticker)s, %(quantity)s,
                     %(market_value)s, %(asset_class)s, %(currency)s, %(broker_account)s,
                     %(broker)s, %(last_price)s, %(last_price_date)s, %(feed_source)s,
                     %(total_cost)s)
                """,
                processed,
            )
        conn.commit()

    n = len(processed)
    logger.info(f"Inserted {n} rows into proc_positions (feed_source='{FEED_SOURCE}', as_of_date={as_of_date})")
    logger.info("=== Done ===")
    return n


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Generate proc_positions for tracked portfolios')
    parser.add_argument('--date', default=None, metavar='YYYY-MM-DD',
                        help='As-of date (default: read from proc_asof_date table)')
    parser.add_argument('--account-id', metavar='ACCOUNT_ID', type=int,
                        help='Limit processing to this account_id only')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview what would be inserted without writing to DB')
    args = parser.parse_args()

    _as_of_date = args.date or get_proc_asof_date()
    process_tracked_positions(_as_of_date, args.account_id, args.dry_run)
