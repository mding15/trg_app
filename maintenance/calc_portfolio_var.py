"""
calc_portfolio_var.py — Run an upload-template portfolio file through the VaR
pipeline without touching the database, and write the results to Excel.

Wrapper around dashboard/process_uploaded_portfolio.py::compute_portfolio_var()
— the same steps an uploaded portfolio goes through (read file, resolve
SecurityID, enrich, price, VaR engine, beta) — minus all inserts/updates.
Results are shaped with dashboard/db_port_position_var.py::
to_port_position_var_frame(), so the Positions sheet matches exactly what an
upload would insert into port_position_var.

Read-only is enforced, not just assumed: every psycopg2 connection opened
during the run is forced into read-only mode (default_transaction_read_only),
so any attempted write fails instead of reaching the database.

Output: data/maintenance/Excel/<input_stem>_var_<timestamp>.xlsx
    Positions  — port_position_var rows (active + excluded), DB column names
    Parameters — parameters as read (AsofDate reflects --date if given)
    Summary    — counts (excluded broken down by exclude_reason), total
                 market value, portfolio std/VaR/ES
                 (portfolio figures = sum of per-position marginal values,
                 which add up exactly to the portfolio total)
    New Securities — input '1. Positions' rows (all original columns) whose
                 SecurityID could not be resolved; header-only if none

If any SecurityIDs are unresolved, also writes
data/maintenance/CSV/new_securities_<timestamp>.csv in the same format as
Step 1 of the new-security workflow (security/new_security.py), ready to
review and load with maintenance/insert_new_security.py (Step 2). It has
one extra column, occ_ticker (True if ticker is a valid OCC option symbol),
right after ticker; Step 2 ignores it.

If any positions are excluded, also writes
data/maintenance/CSV/<input_stem>_excluded_<timestamp>.csv — one row per
excluded position (sorted by exclude_reason, then pos_id):
    pos_id, as_of_date, security_id, security_name, ticker, isin, cusip,
    broker, broker_account, asset_class, asset_type, quantity, market_value,
    last_price, maturity_date, exclude_reason

If any positions are options (asset_class 'Derivative', asset_type 'Option'),
also writes data/maintenance/CSV/<input_stem>_options_<timestamp>.csv — input
for maintenance/calc_options.py, to check the options' market values:
    pos_id, as_of_date, security_id, security_name, quantity, market_value
        (from the Positions sheet),
    option_type, option_class, maturity, strike, underlying, underlying_sec_id
        (from option_info; blank if the position has no option_info row)

Usage:
    python maintenance/calc_portfolio_var.py                        # input_template.xlsx
    python maintenance/calc_portfolio_var.py --file my_port.xlsx    # relative to data/maintenance/Excel
    python maintenance/calc_portfolio_var.py --date 2026-09-25      # override the file's As of Date
"""
from __future__ import annotations

import argparse
import functools
import logging
import re
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _paths import CSV_DIR, EXCEL_DIR

DEFAULT_FILE = 'portfolio.xlsx'

_SUMMARY_METRICS = [
    ('Portfolio Std',    'mg_std'),
    ('Portfolio VaR 95', 'mg_var_95'),
    ('Portfolio VaR 99', 'mg_var_99'),
    ('Portfolio ES 95',  'mg_es_95'),
    ('Portfolio ES 99',  'mg_es_99'),
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger('calc_portfolio_var')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s', '%H:%M:%S')
    )
    logger.addHandler(handler)
    return logger


def _force_read_only_connections() -> None:
    """Make every psycopg2 connection read-only for the rest of this process.

    Must run before any module that connects (or builds a SQLAlchemy engine)
    is imported — they look up psycopg2.connect at call time.
    """
    _connect = psycopg2.connect

    @functools.wraps(_connect)
    def connect(*args, **kwargs):
        opts = kwargs.get('options') or ''
        kwargs['options'] = f'{opts} -c default_transaction_read_only=on'.strip()
        return _connect(*args, **kwargs)

    psycopg2.connect = connect


def _build_summary(file_path: Path, asof_date, positions: pd.DataFrame, rows: pd.DataFrame) -> pd.DataFrame:
    excluded = rows['excluded'].fillna(False).astype(bool) if 'excluded' in rows.columns \
        else pd.Series(False, index=rows.index)
    mv = pd.to_numeric(positions['MarketValue'], errors='coerce').sum()

    items = [
        ('Input File',           str(file_path)),
        ('As of Date',           asof_date),
        ('Positions',            len(rows)),
        ('SecurityID Resolved',  int(rows['security_id'].notna().sum())),
        ('Active',               int((~excluded).sum())),
        ('Excluded',             int(excluded.sum())),
    ]
    reasons = rows.loc[excluded, 'exclude_reason'].fillna('(blank)').value_counts()
    items += [(f'  Excluded: {reason}', int(n)) for reason, n in reasons.items()]
    items += [
        ('No VaR (active)',      int(rows.loc[~excluded, 'var_95'].isna().sum())),
        ('Total Market Value',   float(mv)),
    ]
    for label, col in _SUMMARY_METRICS:
        items.append((label, float(pd.to_numeric(rows[col], errors='coerce').sum())))
    return pd.DataFrame(items, columns=['Item', 'Value'])


def _new_securities(file_path: Path, rows: pd.DataFrame) -> pd.DataFrame:
    """Input-sheet rows (all original columns, as entered) whose SecurityID could not be resolved.

    Matched on ID: the pipeline carries the input ID through as pos_id (as a string).
    """
    from preprocess.read_portfolio import POSITIONS_TAB

    unresolved = set(rows.loc[rows['security_id'].isna(), 'pos_id'].astype(str))
    raw = pd.read_excel(file_path, sheet_name=POSITIONS_TAB)
    return raw[raw['ID'].astype(str).isin(unresolved)]


# Loose OCC option symbol check: root + YYMMDD + C/P + 8-digit strike
_OCC_RE = re.compile(r'^[A-Z.]+\d{6}[CP]\d{8}$')


def _new_securities_csv_frame(new_secs: pd.DataFrame, asof_date) -> pd.DataFrame:
    """Shape New Securities rows like Step 1's scan output (security/new_security.py)
    so maintenance/insert_new_security.py can load the CSV as-is.

    Reuses Step 1's own helpers; only the source differs (input file rows
    instead of position_var). Same defaults as Step 1's query — currency
    'USD' when blank, option_class 'Equity' — except option_class is 'VIX'
    for VIX options, which Step 1 would otherwise get wrong.
    """
    from security.new_security import (
        _normalize_ticker_col, _add_parsed_columns, _apply_option_defaults, _reorder_columns,
    )

    df = pd.DataFrame({
        'security_name': new_secs['SecurityName'].values,
        'ticker':        new_secs['Ticker'].values,
        'isin':          new_secs['ISIN'].values,
        'cusip':         new_secs['Cusip'].values,
        'broker':        new_secs['Broker Name'].values if 'Broker Name' in new_secs else None,
        'security_id':   None,
        'asset_class':   new_secs['Asset Class'].values,
        'asset_type':    None,
        'class':         None,
        'is_option':     None,   # parser detects options from a leading CALL/PUT in the name
        'as_of_date':    asof_date,
        'currency':      new_secs['Currency'].fillna('USD').values,
        'option_class':  'Equity',
        'reason':        'missing_security_id',
        'input_id':      new_secs['ID'].values,
    })
    df = _normalize_ticker_col(df)
    df = _add_parsed_columns(df)
    df = _apply_option_defaults(df)
    df.loc[df['underlying'] == 'VIX', 'option_class'] = 'VIX'
    df = _reorder_columns(df)

    # Informational only — Step 2 reads columns by name and ignores this one
    occ = df['ticker'].apply(lambda t: bool(isinstance(t, str) and _OCC_RE.match(t)))
    df.insert(df.columns.get_loc('ticker') + 1, 'occ_ticker', occ)
    return df


_EXCLUDED_COLS = ['pos_id', 'as_of_date', 'security_id', 'security_name', 'ticker', 'isin', 'cusip',
                  'broker', 'broker_account', 'asset_class', 'asset_type', 'quantity', 'market_value',
                  'last_price', 'maturity_date', 'exclude_reason']


def _excluded_positions(rows: pd.DataFrame) -> pd.DataFrame:
    """Excluded positions (excluded=True) with identifying columns and exclude_reason,
    sorted by reason then pos_id."""
    if 'excluded' not in rows.columns:
        return pd.DataFrame(columns=_EXCLUDED_COLS)
    excl = rows[rows['excluded'].fillna(False).astype(bool)]
    return excl[[c for c in _EXCLUDED_COLS if c in excl.columns]].sort_values(['exclude_reason', 'pos_id'])


_OPTION_POS_COLS = ['pos_id', 'as_of_date', 'security_id', 'security_name', 'quantity', 'market_value']
_OPTION_INFO_COLS = ['option_type', 'option_class', 'maturity', 'strike', 'underlying', 'underlying_sec_id']


def _option_positions(rows: pd.DataFrame) -> pd.DataFrame:
    """Option positions (asset_class 'Derivative', asset_type 'Option'; active +
    excluded) with contract details from option_info — input rows for
    maintenance/calc_options.py. Option-info columns are blank for positions
    with no option_info row (or no security_id).
    """
    from database2 import pg_connection

    pos = rows.loc[(rows['asset_class'] == 'Derivative') & (rows['asset_type'] == 'Option'),
                   _OPTION_POS_COLS].copy()
    pos['security_id'] = pos['security_id'].map(lambda s: str(s) if pd.notna(s) else None)

    info = pd.DataFrame(columns=['security_id'] + _OPTION_INFO_COLS)
    sec_ids = pos['security_id'].dropna().unique().tolist()
    if sec_ids:
        with pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT DISTINCT ON (security_id) security_id, {', '.join(_OPTION_INFO_COLS)}
                    FROM option_info
                    WHERE security_id = ANY(%s)
                    ORDER BY security_id, id DESC
                    """,
                    (sec_ids,),
                )
                info = pd.DataFrame(cur.fetchall(), columns=['security_id'] + _OPTION_INFO_COLS)

    return pos.merge(info, on='security_id', how='left')[_OPTION_POS_COLS + _OPTION_INFO_COLS]


# ── Core logic ────────────────────────────────────────────────────────────────

def run(file_name: str, asof_date: date | None) -> Path:
    log = _setup_logger()
    _force_read_only_connections()

    # Imported after the read-only patch so every connection goes through it
    from api import app
    from dashboard.process_uploaded_portfolio import compute_portfolio_var
    from dashboard.db_port_position_var import to_port_position_var_frame

    in_path = Path(file_name)
    if not in_path.is_absolute():
        in_path = EXCEL_DIR / in_path
    log.info(f'Input: {in_path}')

    with app.app_context():
        params, positions, result = compute_portfolio_var(in_path, asof_date)
    asof_date = params.get('AsofDate')

    rows = to_port_position_var_frame(result, None, asof_date).drop(columns=['port_id'])
    summary = _build_summary(in_path, asof_date, positions, rows)
    new_secs = _new_securities(in_path, rows)
    options = _option_positions(rows)
    params_df = pd.DataFrame(list(dict(params).items()), columns=['Parameter', 'Value'])

    log.info('─' * 60)
    for _, r in summary.iterrows():
        v = r['Value']
        log.info(f"  {r['Item']:<32} {v:,.2f}" if isinstance(v, float) else f"  {r['Item']:<32} {v}")
    if not new_secs.empty:
        log.warning(f'{len(new_secs)} position(s) with unresolved SecurityID — see "New Securities" tab.')

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = EXCEL_DIR / f'{in_path.stem}_var_{timestamp}.xlsx'
    with pd.ExcelWriter(out_path) as writer:
        rows.to_excel(writer, sheet_name='Positions', index=False)
        params_df.to_excel(writer, sheet_name='Parameters', index=False)
        summary.to_excel(writer, sheet_name='Summary', index=False)
        new_secs.to_excel(writer, sheet_name='New Securities', index=False)
    log.info('─' * 60)
    log.info(f'Results written to {out_path}')

    excluded = _excluded_positions(rows)
    if not excluded.empty:
        CSV_DIR.mkdir(parents=True, exist_ok=True)
        excl_path = CSV_DIR / f'{in_path.stem}_excluded_{timestamp}.csv'
        excluded.to_csv(excl_path, index=False)
        log.info(f'{len(excluded)} excluded position(s) written to {excl_path}')
    else:
        log.info('No excluded positions — excluded CSV not written.')

    if not options.empty:
        CSV_DIR.mkdir(parents=True, exist_ok=True)
        opt_path = CSV_DIR / f'{in_path.stem}_options_{timestamp}.csv'
        options.to_csv(opt_path, index=False)
        log.info(f'{len(options)} option position(s) written to {opt_path} (input for maintenance/calc_options.py)')
        missing = options[options['option_type'].isna()]
        if not missing.empty:
            log.warning(f'  {len(missing)} option position(s) have no option_info row — blank in the CSV:')
            for _, r in missing.iterrows():
                log.warning(f"    pos_id={r['pos_id']}  security_id={r['security_id']}  {r['security_name']}")
    else:
        log.info("No option positions (asset_class 'Derivative', asset_type 'Option') — options CSV not written.")

    if not new_secs.empty:
        from security.new_security import write_csv, _TYPE_HANDLERS
        csv_df = _new_securities_csv_frame(new_secs, asof_date)
        csv_path = write_csv(csv_df)
        log.info(f'New Securities CSV for Step 2 written to {csv_path}')
        for stype, n in csv_df['parsed_security_type'].fillna('(none)').value_counts().items():
            note = 'supported' if stype in _TYPE_HANDLERS else 'unsupported — Step 2 will skip'
            log.info(f'  {stype:<14} {n}  ({note})')
        n_no_type = int((csv_df['parsed_security_type'].isin(_TYPE_HANDLERS) & csv_df['asset_type'].isna()).sum())
        if n_no_type:
            log.warning(f'  {n_no_type} supported row(s) have blank asset_type — fill it in the CSV '
                        '(e.g. Stock/ETF/Fund, Cash/Fund, Private Credit/Private Equity/Real Estate) or Step 2 will skip them')
        for _, r in csv_df[csv_df['parsed_security_type'] == 'Option'].iterrows():
            if not r['occ_ticker']:
                log.warning(f"  Check ticker for '{r['security_name']}': '{r['ticker']}' "
                            'is not a valid OCC option symbol — fix it in the CSV before Step 2')
        log.info(f'  Review, then: python maintenance/insert_new_security.py --file {csv_path.name} --dry-run')

    return out_path


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Calculate VaR for an upload-template portfolio file (no database writes).',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='Example:\n  python maintenance/calc_portfolio_var.py --date 2026-09-25\n',
    )
    parser.add_argument('--file', default=DEFAULT_FILE,
                        help=f'input Excel file, relative to data/maintenance/Excel (default: {DEFAULT_FILE})')
    parser.add_argument('--date', type=date.fromisoformat, default=None, metavar='YYYY-MM-DD',
                        help="override the file's As of Date")
    args = parser.parse_args()

    run(file_name=args.file, asof_date=args.date)


if __name__ == '__main__':
    main()
