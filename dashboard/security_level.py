"""
security_level.py — Security Level page query.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database2 import pg_connection


def get_security_level(account_id: int) -> list:
    """
    Return one row per position for the most recent as_of_date for the given account.

    Joins position_var with alternative_model (LEFT) for proxy/liquidity columns.
    sharpe_vol and sharpe_var are computed in-query.
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    pv.security_id,
                    pv.security_name,
                    pv."class",
                    pv.sc1,
                    pv.sc2,
                    pv.region,
                    pv.country,
                    pv.industry,
                    pv.currency,
                    pv.market_value,
                    pv.weight,
                    pv.vol,
                    pv.mg_var_95,
                    pv.expected_return,
                    CASE WHEN pv.vol IS NOT NULL AND pv.vol <> 0
                         THEN pv.expected_return / pv.vol
                         ELSE NULL END                                          AS sharpe_vol,
                    CASE WHEN pv.mg_var_95 IS NOT NULL AND pv.mg_var_95 <> 0
                          AND pv.market_value IS NOT NULL AND pv.market_value <> 0
                         THEN pv.expected_return / (pv.mg_var_95 / pv.market_value)
                         ELSE NULL END                                          AS sharpe_var,
                    pv.delta,
                    pv.iv,
                    pv.gamma,
                    pv.vega,
                    pv.duration,
                    pv.convexity,
                    pv.spread_duration,
                    pv.spread_convexity,
                    pv.pd,
                    pv.rating,
                    pv.delta_var,
                    pv.gamma_var,
                    pv.vega_var,
                    pv.ir_duration_var,
                    pv.ir_convexity_var,
                    pv.ir_var,
                    pv.sp_duration_var,
                    pv.sp_convexity_var,
                    pv.spread_var,
                    pv.default_var,
                    pv.skewness,
                    pv.kurtosis,
                    am.proxy_name,
                    am.proxy_correl,
                    am.liq_adj
                FROM position_var pv
                LEFT JOIN alternative_model am ON am.security_id = pv.security_id
                WHERE pv.account_id = %s
                  AND pv.as_of_date = (
                      SELECT MAX(as_of_date)
                      FROM position_var
                      WHERE account_id = %s
                  )
                ORDER BY pv.security_id
                """,
                (account_id, account_id),
            )
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()

    return [dict(zip(cols, row)) for row in rows]
