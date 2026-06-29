# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date
from functools import lru_cache

import numpy as np

from database2 import pg_connection

YIELD_COLS = ['bc_1month', 'bc_2month', 'bc_3month', 'bc_6month',
              'bc_1year',  'bc_2year',  'bc_3year',  'bc_5year',
              'bc_7year',  'bc_10year', 'bc_20year', 'bc_30year']

YIELD_TENORS = [1/12, 2/12, 3/12, 0.5, 1, 2, 3, 5, 7, 10, 20, 30]


@lru_cache(maxsize=8)
def get_curve(as_of_date: date) -> tuple[list, list]:
    """Return (tenors, yields) for the most recent curve on or before as_of_date.

    Yields are in decimal form (e.g. 0.0432). NULL points are skipped.
    Returns ([], []) if no curve is available.
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT ' + ', '.join(YIELD_COLS) + ' '
                'FROM treasury_yield WHERE date <= %s ORDER BY date DESC LIMIT 1',
                (as_of_date,),
            )
            row = cur.fetchone()
    if row is None:
        return [], []
    tenors, yields = [], []
    for t, v in zip(YIELD_TENORS, row):
        if v is not None:
            tenors.append(t)
            yields.append(float(v) / 100)
    return tenors, yields


def get_rate(tenor: float, as_of_date: date) -> float:
    """Return the interpolated UST yield at the given tenor (in years).

    Raises ValueError if no curve is available for as_of_date.
    Tenors outside the curve range are clamped to the nearest endpoint.
    """
    tenors, yields = get_curve(as_of_date)
    if not tenors:
        raise ValueError(f'No treasury yield curve available for {as_of_date}')
    return float(np.interp(tenor, tenors, yields))
