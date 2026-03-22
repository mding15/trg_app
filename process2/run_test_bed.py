"""
run_test_bed.py — Load test portfolio into proc_positions and run VaR.

Reads test_data/proc_positions.csv, fetches prices, recalculates market_value,
archives old rows (same logic as mssb pipeline), inserts into proc_positions
with feed_source='test', then runs calculate_var.

Usage:
    python run_test_bed.py 2026-03-21        # single date, prices from current_price table
    python run_test_bed.py --all             # all dates in test_data/price_history.csv
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from psycopg2.extras import execute_batch

from database2 import pg_connection
from mkt_data import mkt_timeseries
from process2.process_mssb_positions import _archive_and_replace
from process2.calculate_var import calculate_var


FEED_SOURCE       = 'test'
CSV_PATH          = os.path.join(os.path.dirname(__file__), 'test_data', 'proc_positions.csv')
PRICE_HISTORY_CSV = os.path.join(os.path.dirname(__file__), 'test_data', 'price_history.csv')


# ── logging ────────────────────────────────────────────────────────────────────

def _setup_logger(as_of_date: str) -> logging.Logger:
    log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir,
        f'run_test_bed_{as_of_date}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    )
    logger = logging.getLogger(f'test_bed_{as_of_date}')
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


# ── price fetch ────────────────────────────────────────────────────────────────

def _fetch_prices(positions: pd.DataFrame, as_of_date: str, logger: logging.Logger) -> pd.DataFrame:
    """
    Fetch last prices for each security from the market DB and recalculate market_value.
    - Cash positions: price = 1.
    - All others: use mkt_timeseries.get_last_prices; fallback to 1 if no price found.
    market_value is recalculated as quantity * last_price for all positions.
    Returns updated DataFrame with last_price, last_price_date, market_value set.
    """
    positions = positions.copy()

    sec_ids   = positions['security_id'].unique().tolist()
    price_df  = mkt_timeseries.get_last_prices(sec_ids, as_of_date)
    price_map = dict(zip(price_df['SecurityID'], zip(price_df['Price'], price_df['PriceDate'])))

    prices, price_dates, market_values = [], [], []

    for _, row in positions.iterrows():
        sec_id      = row['security_id']
        asset_class = row.get('asset_class', '')
        quantity    = pd.to_numeric(row.get('quantity'), errors='coerce')

        if asset_class == 'Cash':
            price      = 1.0
            price_date = as_of_date
        elif sec_id in price_map and price_map[sec_id][0] is not None:
            price, price_date = price_map[sec_id]
        else:
            logger.warning(f'No price found for security_id={sec_id} — using fallback price=1')
            price      = 1.0
            price_date = as_of_date

        prices.append(float(price))
        price_dates.append(price_date)
        market_values.append(float(quantity) * float(price) if pd.notna(quantity) else None)

    positions['last_price']      = prices
    positions['last_price_date'] = price_dates
    positions['market_value']    = market_values

    return positions


# ── price fetch from current_price table ──────────────────────────────────────

def _fetch_prices_from_db(positions: pd.DataFrame, as_of_date: str, logger: logging.Logger) -> pd.DataFrame:
    """
    Fetch last prices from the current_price table and recalculate market_value.
    - Cash positions: price = 1.
    - All others: look up by ticker, using the most recent row where Date <= as_of_date.
      Falls back to price=1 and logs an error if no price is found.
    market_value is recalculated as quantity * last_price for all positions.
    Returns updated DataFrame with last_price, last_price_date, market_value set.
    """
    positions = positions.copy()

    non_cash_tickers = [
        row['ticker'] for _, row in positions.iterrows()
        if row.get('asset_class', '') != 'Cash'
    ]

    price_map: dict = {}
    if non_cash_tickers:
        with pg_connection() as conn:
            with conn.cursor() as cur:
                # Single query: fetch all prices within ±100 days of as_of_date
                cur.execute(
                    """
                    SELECT "Ticker", "Close", "Date"
                    FROM current_price
                    WHERE "Ticker" = ANY(%s)
                      AND "Date" BETWEEN %s::date - INTERVAL '100 days'
                                     AND %s::date + INTERVAL '100 days'
                    """,
                    (non_cash_tickers, as_of_date, as_of_date),
                )
                rows = cur.fetchall()

        # Group by ticker: {ticker: [(close, date), ...]}
        from collections import defaultdict
        ticker_rows: dict = defaultdict(list)
        for ticker, close, date in rows:
            ticker_rows[ticker].append((close, date))

        # Per ticker: forward fill (Date >= as_of_date, closest), then backward fill
        import datetime
        as_of = datetime.date.fromisoformat(as_of_date)
        for ticker, entries in ticker_rows.items():
            forward  = [(c, d) for c, d in entries if d >= as_of]
            backward = [(c, d) for c, d in entries if d <  as_of]
            if forward:
                price_map[ticker] = min(forward,  key=lambda x: x[1])
            elif backward:
                price_map[ticker] = max(backward, key=lambda x: x[1])

    prices, price_dates, market_values = [], [], []

    for _, row in positions.iterrows():
        ticker      = row['ticker']
        asset_class = row.get('asset_class', '')
        quantity    = pd.to_numeric(row.get('quantity'), errors='coerce')

        if asset_class == 'Cash':
            price      = 1.0
            price_date = as_of_date
        elif ticker in price_map and price_map[ticker][0] is not None:
            price, price_date = price_map[ticker]
        else:
            logger.error(f'No price found in current_price for ticker={ticker!r} as_of_date={as_of_date} — using fallback price=1')
            price      = 1.0
            price_date = as_of_date

        prices.append(float(price))
        price_dates.append(price_date)
        market_values.append(float(quantity) * float(price) if pd.notna(quantity) else None)

    positions['last_price']      = prices
    positions['last_price_date'] = price_dates
    positions['market_value']    = market_values

    return positions


# ── price fetch from price_history CSV ────────────────────────────────────────

def _fetch_prices_from_csv(positions: pd.DataFrame, as_of_date: str, price_df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Fetch prices from the pre-loaded price_history DataFrame and recalculate market_value.
    - Cash positions: price = 1.
    - Others: look up by security_id in price_df columns for the exact as_of_date row.
      Falls back to price=1 with error log if security_id or date is not found.
    market_value is recalculated as quantity * last_price for all positions.
    Returns updated DataFrame with last_price, last_price_date, market_value set.
    """
    positions  = positions.copy()
    as_of_dt   = pd.to_datetime(as_of_date)
    prices, price_dates, market_values = [], [], []

    for _, row in positions.iterrows():
        sec_id      = row['security_id']
        asset_class = row.get('asset_class', '')
        quantity    = pd.to_numeric(row.get('quantity'), errors='coerce')

        if asset_class == 'Cash':
            price      = 1.0
            price_date = as_of_date
        elif sec_id in price_df.columns and as_of_dt in price_df.index:
            price      = float(price_df.loc[as_of_dt, sec_id])
            price_date = as_of_date
        else:
            logger.error(f'No price found in price_history for security_id={sec_id!r} as_of_date={as_of_date} — using fallback price=1')
            price      = 1.0
            price_date = as_of_date

        prices.append(price)
        price_dates.append(price_date)
        market_values.append(float(quantity) * price if pd.notna(quantity) else None)

    positions['last_price']      = prices
    positions['last_price_date'] = price_dates
    positions['market_value']    = market_values

    return positions


# ── load and insert ────────────────────────────────────────────────────────────

def _load_and_insert(as_of_date: str, logger: logging.Logger, price_df: pd.DataFrame | None = None) -> int:
    """
    Load test portfolio from CSV, update prices, archive old rows,
    and insert into proc_positions with feed_source='test'.
    If price_df is provided, prices are sourced from it (--all mode);
    otherwise prices are fetched from the current_price DB table.
    Returns number of rows inserted.
    """
    df = pd.read_csv(CSV_PATH)
    df.columns = df.columns.str.strip()   # strip any whitespace from column names

    df['as_of_date']  = as_of_date
    df['feed_source'] = FEED_SOURCE

    if price_df is not None:
        df = _fetch_prices_from_csv(df, as_of_date, price_df, logger)
    else:
        df = _fetch_prices_from_db(df, as_of_date, logger)
    logger.info(f'Loaded {len(df)} positions from CSV, prices updated for as_of_date={as_of_date}')

    rows        = df.to_dict('records')
    account_ids = list({r['account_id'] for r in rows})

    insert_sql = """
        INSERT INTO proc_positions
            (as_of_date, account_id, position_id, security_id, security_name,
             isin, cusip, ticker, quantity, market_value, asset_class, currency,
             broker_account, last_price, last_price_date, feed_source)
        VALUES
            (%(as_of_date)s, %(account_id)s, %(position_id)s, %(security_id)s,
             %(security_name)s, %(isin)s, %(cusip)s, %(ticker)s, %(quantity)s,
             %(market_value)s, %(asset_class)s, %(currency)s, %(broker_account)s,
             %(last_price)s, %(last_price_date)s, %(feed_source)s)
    """

    with pg_connection() as conn:
        with conn.cursor() as cur:
            _archive_and_replace(cur, account_ids, as_of_date, FEED_SOURCE, logger)
            execute_batch(cur, insert_sql, rows)
        conn.commit()

    logger.info(f'Inserted {len(rows)} rows into proc_positions')
    return len(rows)


# ── main ───────────────────────────────────────────────────────────────────────

def _run_one(as_of_date: str, logger: logging.Logger, price_df: pd.DataFrame | None = None) -> None:
    """Run the full pipeline for a single as_of_date."""
    logger.info(f'=== Test bed run  as_of_date={as_of_date} ===')

    logger.info('Step 1: load test portfolio into proc_positions')
    _load_and_insert(as_of_date, logger, price_df)

    logger.info(f'Step 2: calculate_var  feed_source={FEED_SOURCE}')
    calculate_var(FEED_SOURCE, as_of_date)

    logger.info(f'=== Completed  as_of_date={as_of_date} ===')


def main() -> None:
    if len(sys.argv) < 2:
        print('Usage: python run_test_bed.py <as_of_date>')
        print('       python run_test_bed.py --all')
        sys.exit(1)

    if sys.argv[1] == '--all':
        if not os.path.exists(PRICE_HISTORY_CSV):
            print(f'ERROR: price_history file not found: {PRICE_HISTORY_CSV}')
            sys.exit(1)

        # Load and normalize price_history once
        price_df = pd.read_csv(PRICE_HISTORY_CSV, index_col='Date')
        price_df.index = pd.to_datetime(price_df.index)
        price_df.index = price_df.index.normalize()              # strip time component
        price_df = price_df[~price_df.index.duplicated(keep='last')]  # ensure unique dates

        dates  = sorted(price_df.index.strftime('%Y-%m-%d').tolist())
        logger = _setup_logger('all')
        logger.info(f'=== --all mode: {len(dates)} dates  {dates[0]} → {dates[-1]} ===')

        for as_of_date in dates:
            _run_one(as_of_date, logger, price_df)

        logger.info(f'=== --all complete: {len(dates)} dates processed ===')

    else:
        as_of_date = sys.argv[1]
        logger     = _setup_logger(as_of_date)
        _run_one(as_of_date, logger)


if __name__ == '__main__':
    main()
