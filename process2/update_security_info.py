"""
update_security_info.py — Enrich port_positions with security data in memory.

Steps:
    1. Set excluded=True, exclude_reason='unknown security' where SecurityID is NULL.
    2. Set excluded=True, exclude_reason='not modeled' where security is not in the
       current risk model (risk_factor joined with current risk_model, union Treasury securities).
    3. Update AssetClass, AssetType from security_info.
    4. Update ExpectedReturn, Currency, Class, SC1, SC2, Country, Region, Sector,
       Industry, OptionType, PaymentFrequency, MaturityDate, OptionStrike,
       UnderlyingSecurityID, CouponRate from security_attribute.
    5. Set excluded=True, exclude_reason='matured' where MaturityDate < AsofDate.
    6. Set is_option = True where OptionType in ('Call', 'Put').
    7. Set UnderlyingID from UnderlyingSecurityID.

No database writes are performed. Returns enriched positions as a DataFrame.
"""
from __future__ import annotations

import functools
import logging
import os
import sys
from datetime import datetime

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from database2 import pg_connection


# ── logging setup ──────────────────────────────────────────────────────────────

def _setup_logger(port_id: int) -> logging.Logger:
    log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir,
        f'update_security_info_{port_id}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    )
    logger = logging.getLogger(f'update_security_info_{port_id}')
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


# ── data fetchers ──────────────────────────────────────────────────────────────

def _fetch_port_positions(cur, port_id: int) -> pd.DataFrame:
    cur.execute('SELECT * FROM port_positions WHERE port_id = %s', (port_id,))
    cols = [desc[0] for desc in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)


def _fetch_asof_date(cur, port_id: int):
    cur.execute('SELECT "AsofDate" FROM port_parameters WHERE port_id = %s', (port_id,))
    row = cur.fetchone()
    return pd.to_datetime(row[0]) if row else None



def _fetch_security_info(cur, security_ids: list) -> pd.DataFrame:
    if not security_ids:
        return pd.DataFrame(columns=['SecurityID', 'AssetClass', 'AssetType'])
    cur.execute(
        'SELECT "SecurityID", "AssetClass", "AssetType" FROM security_info WHERE "SecurityID" = ANY(%s)',
        (security_ids,),
    )
    cols = [desc[0] for desc in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)


def _fetch_security_attributes(cur, security_ids: list) -> pd.DataFrame:
    if not security_ids:
        return pd.DataFrame(columns=['security_id'])
    cur.execute(
        """
        SELECT security_id, expected_return, currency, "class", sc1, sc2,
               country, region, sector, industry, option_type, payment_frequency,
               maturity_date, option_strike, underlying_security_id, coupon_rate,
               ticker
        FROM security_attribute
        WHERE security_id = ANY(%s)
        """,
        (security_ids,),
    )
    cols = [desc[0] for desc in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)


@functools.cache
def _fetch_modeled_security_ids() -> set:
    """Return SecurityIDs in the current risk model, plus all Treasury securities.
    Result is cached for the lifetime of the process."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT rf."SecurityID"
                FROM risk_factor rf
                JOIN risk_model rm ON rf.model_id = rm.model_id
                WHERE rm.is_current = 1
                UNION
                SELECT "SecurityID" FROM security_info WHERE "AssetType" = 'Treasury'
                """
            )
            return frozenset(row[0] for row in cur.fetchall())


# ── main ───────────────────────────────────────────────────────────────────────

def fetch_positions(port_id: int) -> pd.DataFrame:
    """Fetch port_positions rows for the given port_id from the database."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            return _fetch_port_positions(cur, port_id)


def update_security_info(positions: pd.DataFrame, asof_date=None) -> pd.DataFrame:
    """
    Enrich the given positions DataFrame with security data fetched from the database.
    No database writes are performed. Returns the enriched DataFrame.

    asof_date: used to exclude positions where MaturityDate < asof_date.
               If None, the MaturityDate filter is skipped.
    """
    logger = logging.getLogger(__name__)

    logger.info('=== Start update_security_info ===')

    if positions.empty:
        logger.warning('Input positions DataFrame is empty')
        return positions

    logger.info(f'Processing {len(positions)} rows')

    asof_date = pd.to_datetime(asof_date) if asof_date is not None else None
    if asof_date is None:
        logger.warning('asof_date not provided; MaturityDate filter will be skipped')

    with pg_connection() as conn:
        with conn.cursor() as cur:

            positions = positions.copy()
            positions['excluded']       = False
            positions['exclude_reason'] = None

            # ── Step 1: exclude where SecurityID is NULL ─────────────────────
            still_null = positions['SecurityID'].isna()
            positions.loc[still_null, 'excluded']       = True
            positions.loc[still_null, 'exclude_reason'] = 'unknown security'
            if still_null.any():
                logger.warning(
                    f'{still_null.sum()} positions have unresolved SecurityID → excluded'
                )

            # ── Step 2: exclude where not in current risk model ───────────────
            modeled_ids = _fetch_modeled_security_ids()
            logger.info(f'Fetched {len(modeled_ids)} modeled SecurityIDs')

            not_modeled = positions['SecurityID'].notna() & ~positions['SecurityID'].isin(modeled_ids)
            positions.loc[not_modeled, 'excluded']       = True
            positions.loc[not_modeled, 'exclude_reason'] = 'not modeled'
            if not_modeled.any():
                logger.warning(
                    f'{not_modeled.sum()} positions not in current risk model → excluded'
                )

            # collect known SecurityIDs for batch fetching
            known_ids = positions.loc[positions['SecurityID'].notna(), 'SecurityID'].unique().tolist()

            # ── Step 3: update asset_class, asset_type from security_info ────
            positions['AssetClass'] = None
            positions['AssetType']  = None

            sec_info = _fetch_security_info(cur, known_ids)
            logger.info(f'Fetched {len(sec_info)} security_info rows')

            if not sec_info.empty:
                info_map = sec_info.set_index('SecurityID')
                idx = positions['SecurityID'].isin(info_map.index)
                positions.loc[idx, 'AssetClass'] = positions.loc[idx, 'SecurityID'].map(info_map['AssetClass'])
                positions.loc[idx, 'AssetType']  = positions.loc[idx, 'SecurityID'].map(info_map['AssetType'])

            # ── Step 4: update attributes from security_attribute ────────────
            col_mapping = {
                'ExpectedReturn':       'expected_return',
                'Currency':             'currency',
                'Class':                'class',
                'SC1':                  'sc1',
                'SC2':                  'sc2',
                'Country':              'country',
                'Region':               'region',
                'Sector':               'sector',
                'Industry':             'industry',
                'OptionType':           'option_type',
                'PaymentFrequency':     'payment_frequency',
                'MaturityDate':         'maturity_date',
                'OptionStrike':         'option_strike',
                'UnderlyingSecurityID': 'underlying_security_id',
                'CouponRate':           'coupon_rate',
            }
            for pos_col in col_mapping:
                positions[pos_col] = None

            sec_attr = _fetch_security_attributes(cur, known_ids)
            logger.info(f'Fetched {len(sec_attr)} security_attribute rows')

            if not sec_attr.empty:
                attr_map = sec_attr.set_index('security_id')
                idx = positions['SecurityID'].isin(attr_map.index)
                for pos_col, attr_col in col_mapping.items():
                    positions.loc[idx, pos_col] = positions.loc[idx, 'SecurityID'].map(attr_map[attr_col])

                # Special case: fill Ticker from security_attribute only where currently None/NaN
                if 'ticker' in attr_map.columns:
                    ticker_missing = positions['Ticker'].isna() | (positions['Ticker'].astype(str).str.strip() == '')
                    if ticker_missing.any():
                        fill_idx = ticker_missing & positions['SecurityID'].isin(attr_map.index)
                        positions.loc[fill_idx, 'Ticker'] = positions.loc[fill_idx, 'SecurityID'].map(attr_map['ticker'])
                        logger.info(f'Filled Ticker from security_attribute for {fill_idx.sum()} positions')

            # ── Step 5: exclude where MaturityDate < AsofDate ────────────────
            if asof_date is not None:
                positions['MaturityDate'] = pd.to_datetime(positions['MaturityDate'])
                expired = positions['MaturityDate'].notna() & (positions['MaturityDate'] < asof_date)
                positions.loc[expired, 'excluded']       = True
                positions.loc[expired, 'exclude_reason'] = 'matured'
                if expired.any():
                    logger.info(
                        f'{expired.sum()} positions excluded: MaturityDate < AsofDate ({asof_date.date()})'
                    )

            # ── Step 6: set is_option ────────────────────────────────────────
            positions['is_option'] = False
            positions.loc[positions['OptionType'].isin(['Call', 'Put']), 'is_option'] = True

            # ── Step 7: set UnderlyingID from UnderlyingSecurityID ───────────
            positions['UnderlyingID'] = positions.get('UnderlyingSecurityID')

    logger.info(f'=== Done. Returning {len(positions)} enriched rows ===')
    return positions


def test():
    port_id = 5344
    asof_date = '2026-03-02'

    positions = fetch_positions(port_id)
    result = update_security_info(positions, asof_date=asof_date)
    print(result)


if __name__ == '__main__':
    test()
