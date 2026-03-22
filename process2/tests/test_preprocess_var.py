"""
Tests for preprocess_var.py

Unit tests (no DB required):
    - _map_columns: renames, drops, passthrough
    - build_params: correct keys and types

Integration tests (require live DB, run with: pytest -m integration):
    - preprocess_var: returns non-empty enriched positions
"""
import pytest
import pandas as pd

from process2.preprocess_var import _map_columns, _RENAME, _DROP, build_params


# ── _map_columns ───────────────────────────────────────────────────────────────

def test_map_columns_renames_all_expected():
    """All columns in _RENAME are renamed to their engine equivalents."""
    df = pd.DataFrame(columns=list(_RENAME.keys()))
    result = _map_columns(df)
    for engine_col in _RENAME.values():
        assert engine_col in result.columns, f'Expected renamed column: {engine_col}'


def test_map_columns_drops_expected():
    """Columns in _DROP are removed."""
    df = pd.DataFrame(columns=list(_RENAME.keys()) + _DROP)
    result = _map_columns(df)
    for col in _DROP:
        assert col not in result.columns, f'Expected dropped column: {col}'


def test_map_columns_passthrough_unknown():
    """Columns not in _RENAME or _DROP pass through unchanged."""
    df = pd.DataFrame(columns=['account_id', 'pos_id', 'excluded', 'broker_account'])
    result = _map_columns(df)
    for col in ['account_id', 'pos_id', 'excluded', 'broker_account']:
        assert col in result.columns


def test_map_columns_drop_missing_columns_ignored():
    """_DROP columns that don't exist in the DataFrame don't raise an error."""
    df = pd.DataFrame(columns=['account_id', 'pos_id'])
    result = _map_columns(df)   # should not raise
    assert 'account_id' in result.columns


# ── build_params ───────────────────────────────────────────────────────────────

_REQUIRED_PARAM_KEYS = [
    'AsofDate', 'ReportDate', 'RiskHorizon', 'TailMeasure',
    'ReturnFrequency', 'Benchmark', 'ExpectedReturn', 'BaseCurrency', 'PortfolioName',
]

def test_build_params_has_all_keys():
    """build_params returns a dict with all required VaR engine keys."""
    params = build_params('2025-09-30')
    for key in _REQUIRED_PARAM_KEYS:
        assert key in params, f'Missing param key: {key}'


def test_build_params_dates_are_timestamps():
    """AsofDate and ReportDate are pandas Timestamps."""
    params = build_params('2025-09-30')
    assert isinstance(params['AsofDate'],   pd.Timestamp)
    assert isinstance(params['ReportDate'], pd.Timestamp)


def test_build_params_dates_match_input():
    """AsofDate and ReportDate reflect the given as_of_date."""
    params = build_params('2025-09-30')
    assert params['AsofDate'].date().isoformat()   == '2025-09-30'
    assert params['ReportDate'].date().isoformat() == '2025-09-30'


def test_build_params_fixed_values():
    """Hardcoded params have the expected values."""
    params = build_params('2025-09-30')
    assert params['RiskHorizon']     == '1 Month'
    assert params['TailMeasure']     == '95% TailVaR'
    assert params['ReturnFrequency'] == 'Daily'
    assert params['BaseCurrency']    == 'USD'
    assert params['PortfolioName']   == 'MSSB'


# ── integration tests (require live DB) ────────────────────────────────────────

@pytest.mark.integration
def test_preprocess_var_returns_nonempty_positions():
    """preprocess_var returns a non-empty DataFrame for the latest date."""
    from process2.db_position_var import fetch_latest_as_of_date
    from process2.preprocess_var import preprocess_var
    as_of_date = fetch_latest_as_of_date()
    params, positions = preprocess_var(as_of_date)
    assert not positions.empty
    assert isinstance(params, dict)


@pytest.mark.integration
def test_preprocess_var_has_engine_columns():
    """Preprocessed positions have the engine-expected column names."""
    from process2.db_position_var import fetch_latest_as_of_date
    from process2.preprocess_var import preprocess_var
    as_of_date = fetch_latest_as_of_date()
    _, positions = preprocess_var(as_of_date)
    for col in ['SecurityID', 'Quantity', 'MarketValue', 'Currency']:
        assert col in positions.columns, f'Missing engine column: {col}'


@pytest.mark.integration
def test_preprocess_var_raises_for_unknown_date():
    """preprocess_var raises ValueError for a date with no data."""
    from process2.preprocess_var import preprocess_var
    with pytest.raises(ValueError, match='No proc_positions rows found'):
        preprocess_var('1900-01-01')
