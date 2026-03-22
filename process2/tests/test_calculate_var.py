"""
Tests for calculate_var.py

Unit tests (no DB or engine required):
    - build_results: correct merge, no duplicate columns, NULL VaR for excluded rows

Integration tests (require live DB + VaR model, run with: pytest -m integration):
    - calculate_var: runs without error, row count matches proc_positions
"""
import pytest
import pandas as pd
import numpy as np

from process2.calculate_var import build_results


# ── fixtures ───────────────────────────────────────────────────────────────────

def _make_positions(n=3, account_id=1001):
    return pd.DataFrame([{
        'pos_id':     f'P{i:03d}',
        'account_id': account_id,
        'SecurityID': f'SEC{i}',
        'Quantity':   float(100 * (i + 1)),
        'excluded':   False,
    } for i in range(n)])


def _make_engine_positions(pos_ids):
    """Simulate DATA['Positions'] output from the VaR engine."""
    return pd.DataFrame([{
        'pos_id':         pid,
        'UnderlyingPrice': 100.0,
        'RiskFreeRate':    0.05,
    } for pid in pos_ids])


def _make_var_df(pos_ids):
    """Simulate DATA['VaR'] output from the VaR engine."""
    return pd.DataFrame([{
        'pos_id': pid,
        'VaR':    500.0,
        'tVaR':   600.0,
        'DELTA':  0.5,
    } for pid in pos_ids])


# ── build_results ──────────────────────────────────────────────────────────────

def test_build_results_adds_var_columns():
    """VaR columns from DATA['VaR'] are merged onto positions."""
    positions = _make_positions(n=2)
    pos_ids   = positions['pos_id'].tolist()
    DATA = {
        'Positions': _make_engine_positions(pos_ids),
        'VaR':       _make_var_df(pos_ids),
    }

    result = build_results(positions, DATA)

    assert 'VaR'   in result.columns
    assert 'tVaR'  in result.columns
    assert 'DELTA' in result.columns
    assert len(result) == len(positions)


def test_build_results_no_duplicate_columns():
    """Merge does not produce duplicate columns."""
    positions = _make_positions(n=3)
    pos_ids   = positions['pos_id'].tolist()
    DATA = {
        'Positions': _make_engine_positions(pos_ids),
        'VaR':       _make_var_df(pos_ids),
    }

    result = build_results(positions, DATA)

    assert result.columns.duplicated().sum() == 0


def test_build_results_stored_in_data():
    """build_results stores the result in DATA['Results']."""
    positions = _make_positions(n=2)
    pos_ids   = positions['pos_id'].tolist()
    DATA = {
        'Positions': _make_engine_positions(pos_ids),
        'VaR':       _make_var_df(pos_ids),
    }

    result = build_results(positions, DATA)

    assert 'Results' in DATA
    pd.testing.assert_frame_equal(DATA['Results'], result)


def test_build_results_missing_engine_data_does_not_raise():
    """build_results handles missing Positions or VaR keys gracefully."""
    positions = _make_positions(n=2)

    result = build_results(positions, DATA={})

    assert len(result) == len(positions)
    assert 'SecurityID' in result.columns


def test_build_results_existing_columns_not_overwritten():
    """Columns already in positions are not replaced by engine columns."""
    positions = _make_positions(n=2)
    positions['Quantity'] = 999.0   # set a known value

    # engine Positions also has 'Quantity' — should not overwrite
    engine_pos = _make_engine_positions(positions['pos_id'].tolist())
    engine_pos['Quantity'] = 1.0

    DATA = {'Positions': engine_pos, 'VaR': _make_var_df(positions['pos_id'].tolist())}
    result = build_results(positions, DATA)

    assert (result['Quantity'] == 999.0).all()


# ── excluded positions re-attachment (logic from calculate_var loop) ───────────

def test_excluded_positions_get_null_var_columns():
    """Excluded positions re-attached to results have NULL for all VaR columns."""
    active   = _make_positions(n=2)
    excluded = _make_positions(n=1, account_id=1001)
    excluded['pos_id']   = 'EXCL_01'
    excluded['excluded'] = True

    pos_ids = active['pos_id'].tolist()
    DATA = {
        'Positions': _make_engine_positions(pos_ids),
        'VaR':       _make_var_df(pos_ids),
    }

    result = build_results(active, DATA)

    # simulate the re-attachment logic in calculate_var
    new_cols     = [c for c in result.columns if c not in excluded.columns]
    excluded_out = excluded.reindex(columns=result.columns)
    for col in new_cols:
        excluded_out[col] = None
    combined = pd.concat([result, excluded_out], ignore_index=True)

    excl_row = combined[combined['pos_id'] == 'EXCL_01'].iloc[0]
    assert excl_row['VaR']   is None
    assert excl_row['tVaR']  is None
    assert excl_row['DELTA'] is None


def test_excluded_positions_preserve_position_data():
    """Excluded rows retain their original position columns after re-attachment."""
    active   = _make_positions(n=2)
    excluded = _make_positions(n=1)
    excluded['pos_id']    = 'EXCL_01'
    excluded['excluded']  = True
    excluded['Quantity']  = 777.0

    pos_ids = active['pos_id'].tolist()
    DATA = {
        'Positions': _make_engine_positions(pos_ids),
        'VaR':       _make_var_df(pos_ids),
    }

    result       = build_results(active, DATA)
    new_cols     = [c for c in result.columns if c not in excluded.columns]
    excluded_out = excluded.reindex(columns=result.columns)
    for col in new_cols:
        excluded_out[col] = None
    combined = pd.concat([result, excluded_out], ignore_index=True)

    excl_row = combined[combined['pos_id'] == 'EXCL_01'].iloc[0]
    assert excl_row['Quantity'] == 777.0


def test_total_row_count_includes_excluded():
    """Combined result contains both active and excluded positions."""
    active   = _make_positions(n=3)
    excluded = _make_positions(n=2)
    excluded['pos_id']   = [f'EXCL_{i}' for i in range(2)]
    excluded['excluded'] = True

    pos_ids = active['pos_id'].tolist()
    DATA = {
        'Positions': _make_engine_positions(pos_ids),
        'VaR':       _make_var_df(pos_ids),
    }

    result       = build_results(active, DATA)
    new_cols     = [c for c in result.columns if c not in excluded.columns]
    excluded_out = excluded.reindex(columns=result.columns)
    for col in new_cols:
        excluded_out[col] = None
    combined = pd.concat([result, excluded_out], ignore_index=True)

    assert len(combined) == len(active) + len(excluded)


# ── integration tests (require live DB + VaR model) ───────────────────────────

@pytest.mark.integration
def test_calculate_var_runs_without_error():
    """calculate_var completes without raising for the latest as_of_date for feed_source=mssb."""
    from process2.calculate_var import calculate_var
    calculate_var('mssb')   # uses latest date from proc_positions for feed_source=mssb


@pytest.mark.integration
def test_calculate_var_row_count_matches_proc_positions():
    """Rows inserted into position_var match the total in proc_positions for that date and feed_source."""
    import psycopg2
    from process2.calculate_var import calculate_var
    from process2.db_position_var import fetch_latest_as_of_date, fetch_proc_positions

    as_of_date = fetch_latest_as_of_date('mssb')
    expected   = len(fetch_proc_positions(as_of_date, 'mssb'))

    calculate_var('mssb', as_of_date)

    from database2 import pg_connection
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT COUNT(*) FROM position_var WHERE as_of_date = %s',
                (as_of_date,),
            )
            actual = cur.fetchone()[0]

    assert actual == expected
