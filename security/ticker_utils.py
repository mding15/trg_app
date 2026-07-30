"""
ticker_utils.py — Shared ticker-string helpers.
"""
from __future__ import annotations


def normalize_ticker(t) -> str | None:
    """Strip all whitespace from a raw broker ticker (e.g. an OCC option
    symbol like 'AAPL  270115C0035000' -> 'AAPL270115C0035000') so it's
    usable as a clean security_xref Ticker value."""
    if not isinstance(t, str) or not t.strip():
        return None
    return ''.join(t.split())
