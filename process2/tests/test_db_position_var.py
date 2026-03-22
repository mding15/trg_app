"""
Tests for db_position_var.py

Unit tests (no DB required):
    - insert_results: idempotency, stale row removal, type coercion

Integration tests (require live DB, run with: pytest -m integration):
    - fetch_proc_positions: returns DataFrame, drops system columns
    - fetch_latest_as_of_date: returns ISO string
"""
import pytest
import pandas as pd
import numpy as np
from datetime import date, datetime
from unittest.mock import patch, MagicMock, call

from process2.db_position_var import insert_results, fetch_proc_positions, fetch_latest_as_of_date


# ── fixtures ───────────────────────────────────────────────────────────────────

def _make_results_df(account_id=1001, n=3):
    """Minimal results DataFrame with engine-named columns."""
    return pd.DataFrame([{
        'account_id':  account_id,
        'pos_id':      f'P{i:03d}',
        'SecurityID':  f'SEC{i}',
        'SecurityName': f'Security {i}',
        'Quantity':    float(100 * (i + 1)),
        'MarketValue': float(10000 * (i + 1)),
        'Currency':    'USD',
        'VaR':         float(500 * (i + 1)),
        'excluded':    False,
        'is_option':   False,
    } for i in range(n)])


def _mock_pg():
    """Return a mock pg_connection context manager."""
    mock_conn = MagicMock()
    mock_cur  = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__  = MagicMock(return_value=False)
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
    mock_conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
    return mock_conn, mock_cur


# ── insert_results: idempotency ────────────────────────────────────────────────

@patch('process2.db_position_var.execute_batch')
@patch('process2.db_position_var.pg_connection')
def test_insert_results_idempotent(mock_pg_conn, mock_execute_batch):
    """Running insert_results twice returns the same row count."""
    mock_conn, mock_cur = _mock_pg()
    mock_pg_conn.return_value = mock_conn

    df = _make_results_df()
    n1 = insert_results(df, '2026-01-01')
    n2 = insert_results(df, '2026-01-01')

    assert n1 == n2 == 3


@patch('process2.db_position_var.execute_batch')
@patch('process2.db_position_var.pg_connection')
def test_insert_results_delete_called_before_insert(mock_pg_conn, mock_execute_batch):
    """DELETE is always executed before INSERT to remove stale rows."""
    mock_conn, mock_cur = _mock_pg()
    mock_pg_conn.return_value = mock_conn

    insert_results(_make_results_df(account_id=1001), '2026-01-01')

    # cur.execute should be the DELETE call
    assert mock_cur.execute.call_count == 1
    delete_sql = mock_cur.execute.call_args[0][0]
    assert 'DELETE' in delete_sql.upper()
    assert 'position_var' in delete_sql

    # execute_batch should be the INSERT call
    assert mock_execute_batch.call_count == 1
    insert_sql = mock_execute_batch.call_args[0][1]
    assert 'INSERT' in insert_sql.upper()


@patch('process2.db_position_var.execute_batch')
@patch('process2.db_position_var.pg_connection')
def test_insert_results_delete_uses_correct_account_and_date(mock_pg_conn, mock_execute_batch):
    """DELETE is scoped to the correct (as_of_date, account_id)."""
    mock_conn, mock_cur = _mock_pg()
    mock_pg_conn.return_value = mock_conn

    insert_results(_make_results_df(account_id=9999), '2026-03-04')

    delete_args = mock_cur.execute.call_args[0][1]  # (as_of, account_ids)
    assert delete_args[0] == date(2026, 3, 4)
    assert 9999 in delete_args[1]


# ── insert_results: type coercion ──────────────────────────────────────────────

@patch('process2.db_position_var.execute_batch')
@patch('process2.db_position_var.pg_connection')
def test_insert_results_numeric_coercion(mock_pg_conn, mock_execute_batch):
    """Numeric columns stored as strings are coerced to float."""
    mock_conn, mock_cur = _mock_pg()
    mock_pg_conn.return_value = mock_conn

    df = _make_results_df(n=1)
    df['Quantity']    = '250.5'   # string
    df['MarketValue'] = '99999'   # string

    insert_results(df, '2026-01-01')

    rows = mock_execute_batch.call_args[0][2]
    assert isinstance(rows[0]['quantity'],     float)
    assert isinstance(rows[0]['market_value'], float)
    assert rows[0]['quantity']     == 250.5
    assert rows[0]['market_value'] == 99999.0


@patch('process2.db_position_var.execute_batch')
@patch('process2.db_position_var.pg_connection')
def test_insert_results_date_coercion(mock_pg_conn, mock_execute_batch):
    """Date columns are coerced to datetime.date for psycopg2."""
    mock_conn, mock_cur = _mock_pg()
    mock_pg_conn.return_value = mock_conn

    df = _make_results_df(n=1)
    df['LastPriceDate'] = '2026-01-15'   # string

    insert_results(df, '2026-01-01')

    rows = mock_execute_batch.call_args[0][2]
    assert isinstance(rows[0]['last_price_date'], date)
    assert rows[0]['last_price_date'] == date(2026, 1, 15)


@patch('process2.db_position_var.execute_batch')
@patch('process2.db_position_var.pg_connection')
def test_insert_results_bool_coercion(mock_pg_conn, mock_execute_batch):
    """Boolean columns coerced from int/numpy bool to Python bool."""
    mock_conn, mock_cur = _mock_pg()
    mock_pg_conn.return_value = mock_conn

    df = _make_results_df(n=2)
    df.loc[0, 'excluded']  = 1          # int
    df.loc[1, 'excluded']  = np.bool_(True)  # numpy bool

    insert_results(df, '2026-01-01')

    rows = mock_execute_batch.call_args[0][2]
    assert rows[0]['excluded'] is True
    assert rows[1]['excluded'] is True
    assert type(rows[0]['excluded']) is bool
    assert type(rows[1]['excluded']) is bool


@patch('process2.db_position_var.execute_batch')
@patch('process2.db_position_var.pg_connection')
def test_insert_results_nan_becomes_none(mock_pg_conn, mock_execute_batch):
    """NaN and NaT values are converted to None for psycopg2."""
    mock_conn, mock_cur = _mock_pg()
    mock_pg_conn.return_value = mock_conn

    df = _make_results_df(n=1)
    df['VaR']           = np.nan
    df['LastPriceDate'] = pd.NaT

    insert_results(df, '2026-01-01')

    rows = mock_execute_batch.call_args[0][2]
    assert rows[0]['var']             is None
    assert rows[0]['last_price_date'] is None


# ── integration tests (require live DB) ────────────────────────────────────────

@pytest.mark.integration
def test_fetch_proc_positions_returns_dataframe():
    """fetch_proc_positions returns a non-empty DataFrame for a known date and feed_source."""
    from process2.db_position_var import fetch_latest_as_of_date
    as_of_date = fetch_latest_as_of_date('mssb')
    df = fetch_proc_positions(as_of_date, 'mssb')
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


@pytest.mark.integration
def test_fetch_proc_positions_drops_system_columns():
    """id and insert_time are not present in the returned DataFrame."""
    from process2.db_position_var import fetch_latest_as_of_date
    as_of_date = fetch_latest_as_of_date('mssb')
    df = fetch_proc_positions(as_of_date, 'mssb')
    assert 'id'          not in df.columns
    assert 'insert_time' not in df.columns


@pytest.mark.integration
def test_fetch_proc_positions_empty_for_unknown_date():
    """fetch_proc_positions returns empty DataFrame (not an error) for a date with no data."""
    df = fetch_proc_positions('1900-01-01', 'mssb')
    assert isinstance(df, pd.DataFrame)
    assert df.empty


@pytest.mark.integration
def test_fetch_latest_as_of_date_returns_iso_string():
    """fetch_latest_as_of_date returns a valid ISO date string for feed_source=mssb."""
    result = fetch_latest_as_of_date('mssb')
    assert isinstance(result, str)
    datetime.strptime(result, '%Y-%m-%d')  # raises if not valid ISO date
