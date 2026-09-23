"""
check_port_positions.py — Validate uploaded port_positions data for a portfolio.

port_positions is populated from user-provided Excel uploads
(dashboard/process_uploaded_portfolio.py), so it can contain bad data. This
is the first of what will be several checks, so it's structured as a small
set of independent check_*() functions, each returning a list of flagged-row
dicts; run() combines them into one report. Adding a future check is just
adding another check_*() function and one more line in run().

Checks:
    1. check_missing_security_id — SecurityID could not be resolved.
    2. check_option_market_value — for option positions (OptionType in
       ('Call', 'Put')), reprices the option via Black-Scholes at a flat
       20% volatility (reusing process2/calc_options_pnl.py's own pricing
       approach: engine/eq_option_var.py::calc_price(), tenor from
       MaturityDate, risk-free rate from models/ust_curve.py::get_rate()),
       and flags the position if provided MarketValue / theoretical market
       value falls outside [0.5, 1.5].

       theoretical_market_value = BS_price_per_share * Quantity, with NO
       x100 contract multiplier — matching calc_options_pnl.py's own
       convention (its price = SUM(market_value) / SUM(quantity) is used
       directly as a per-share price against calc_price()'s per-share
       output, with no multiplier anywhere in that pipeline).

Read-only: never writes to the database. Flagged rows are logged and, if
any exist, written to a timestamped CSV in data/maintenance/CSV/.

Usage:
    python maintenance/check_port_positions.py --port-id 1234
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database2 import pg_connection
from engine import eq_option_var as opt
from models.ust_curve import get_rate
from mkt_data.price_timeseries import get_current_price
from _paths import CSV_DIR

OPTION_VOL = 0.20
RATIO_LOW  = 0.5
RATIO_HIGH = 1.5

_POSITION_COLS = [
    'ID', 'SecurityID', 'SecurityName', 'Quantity', 'MarketValue',
    'OptionType', 'OptionStrike', 'MaturityDate', 'UnderlyingSecurityID',
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger('check_port_positions')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s', '%H:%M:%S')
    )
    logger.addHandler(handler)
    return logger


def _fetch_positions(cur, port_id: int) -> pd.DataFrame:
    col_sql = ", ".join(f'"{c}"' for c in _POSITION_COLS)
    cur.execute(
        f'SELECT {col_sql} FROM port_positions WHERE port_id = %s',
        (port_id,),
    )
    cols = [d[0] for d in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)


def _fetch_asof_date(cur, port_id: int):
    cur.execute('SELECT "AsofDate" FROM port_parameters WHERE port_id = %s', (port_id,))
    row = cur.fetchone()
    return row[0] if row else None


def _flag(row: pd.Series, check: str, detail: str) -> dict:
    return {
        'ID':           row['ID'],
        'SecurityID':   row['SecurityID'],
        'SecurityName': row['SecurityName'],
        'check':        check,
        'detail':       detail,
    }


# ── Checks ────────────────────────────────────────────────────────────────────

def check_missing_security_id(positions: pd.DataFrame) -> list[dict]:
    missing = positions[positions['SecurityID'].isna() | (positions['SecurityID'] == '')]
    return [_flag(r, 'missing_security_id', 'SecurityID could not be resolved') for _, r in missing.iterrows()]


def check_option_market_value(positions: pd.DataFrame, as_of_date, log: logging.Logger) -> list[dict]:
    options = positions[positions['OptionType'].isin(['Call', 'Put'])]
    if options.empty:
        return []

    flags: list[dict] = []

    missing_inputs = options[
        options['OptionStrike'].isna() | options['MaturityDate'].isna()
        | options['UnderlyingSecurityID'].isna() | options['Quantity'].isna()
    ]
    flags += [
        _flag(r, 'option_missing_pricing_inputs',
              'OptionStrike/MaturityDate/UnderlyingSecurityID/Quantity not all present')
        for _, r in missing_inputs.iterrows()
    ]
    options = options.drop(missing_inputs.index)
    if options.empty:
        return flags

    tenor = (pd.to_datetime(options['MaturityDate']) - pd.Timestamp(as_of_date)).dt.days / 365
    matured = options[tenor <= 0]
    flags += [
        _flag(r, 'option_matured', f'Already matured as of {as_of_date}')
        for _, r in matured.iterrows()
    ]
    options = options.drop(matured.index)
    tenor = tenor.drop(matured.index)
    if options.empty:
        return flags

    und_ids = options['UnderlyingSecurityID'].unique().tolist()
    und_prices = {k: v[0] for k, v in get_current_price(und_ids, as_of_date).items()}

    for (idx, row), t in zip(options.iterrows(), tenor):
        S = und_prices.get(row['UnderlyingSecurityID'])
        if S is None:
            flags.append(_flag(row, 'option_no_underlying_price',
                                f"No current_price found for underlying {row['UnderlyingSecurityID']} on/before {as_of_date}"))
            continue

        try:
            r = get_rate(float(t), as_of_date)
        except ValueError as e:
            flags.append(_flag(row, 'option_no_risk_free_rate', str(e)))
            continue

        theo_price = opt.calc_price(row['OptionType'], S, float(row['OptionStrike']), float(t), r, OPTION_VOL)
        theo_mv = float(theo_price) * float(row['Quantity'])

        if not theo_mv or np.isnan(theo_mv):
            flags.append(_flag(row, 'option_zero_theoretical_mv',
                                f'theoretical_mv={theo_mv} (underlying={S}, strike={row["OptionStrike"]}, tenor={t:.3f})'))
            continue

        market_value = float(row['MarketValue']) if pd.notna(row['MarketValue']) else None
        if market_value is None:
            flags.append(_flag(row, 'option_missing_pricing_inputs', 'MarketValue is missing'))
            continue

        ratio = market_value / theo_mv
        if ratio < RATIO_LOW or ratio > RATIO_HIGH:
            flags.append(_flag(
                row, 'option_mv_ratio_out_of_range',
                f'ratio={ratio:.3f}  provided_mv={market_value:.2f}  theoretical_mv={theo_mv:.2f}  '
                f'(underlying={S}, strike={row["OptionStrike"]}, tenor={t:.3f}, rate={r:.4f}, vol={OPTION_VOL})'
            ))

    return flags


# ── Core logic ────────────────────────────────────────────────────────────────

def run(port_id: int) -> None:
    log = _setup_logger()

    with pg_connection() as conn:
        with conn.cursor() as cur:
            positions = _fetch_positions(cur, port_id)
            if positions.empty:
                log.info(f'No port_positions rows found for port_id={port_id}.')
                return

            as_of_date = _fetch_asof_date(cur, port_id)
            if as_of_date is None:
                log.warning(f'No port_parameters.AsofDate found for port_id={port_id} — '
                            'option pricing check will be skipped.')

    log.info(f'port_id={port_id}: {len(positions)} position(s), as_of_date={as_of_date}')

    flags: list[dict] = []
    flags += check_missing_security_id(positions)
    if as_of_date is not None:
        flags += check_option_market_value(positions, as_of_date, log)

    if not flags:
        log.info('No issues found.')
        return

    report = pd.DataFrame(flags)
    log.info('─' * 60)
    log.info(f'{len(report)} issue(s) found:')
    log.info(f'  By check:\n{report["check"].value_counts().to_string()}')
    for _, r in report.iterrows():
        log.info(f"  [{r['check']}] ID={r['ID']} SecurityID={r['SecurityID']} '{r['SecurityName']}' — {r['detail']}")

    CSV_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = CSV_DIR / f'check_port_positions_{port_id}_{timestamp}.csv'
    report.to_csv(out_path, index=False)
    log.info('─' * 60)
    log.info(f'Report written to {out_path}')


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Validate uploaded port_positions data for a portfolio.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='Example:\n  python maintenance/check_port_positions.py --port-id 1234\n',
    )
    parser.add_argument('--port-id', type=int, required=True, metavar='PORT_ID',
                         help='port_id whose port_positions rows should be checked')
    args = parser.parse_args()

    run(port_id=args.port_id)


if __name__ == '__main__':
    main()
