from database2 import pg_connection

PARAMETER_OPTIONS = {
    "risk_horizon": ["1D", "10D"],
    "risk_measure": ["VaR 95%", "VaR 99%", "ES 95%", "ES 99%"],
    "benchmark":    ["SP500", "MSCI World", "100/0 Blend", "90/10 Blend", "80/20 Blend", "70/30 Blend", "60/40 Blend", "50/50 Blend", "40/60 Blend", "30/70 Blend", "20/80 Blend", "10/90 Blend","0/100 Blend"],
    "base_currency": ["USD", "EUR", "GBP", "JPY", "AUD", "CHF"],
    "beta_key":     ["SP500-1Y", "SP500-3Y","MSCI World", "MSCI EM", "Bloomberg Agg", "Russell 2000"],
    "exp_return":   ["Historical", "CAPM", "Black-Litterman", "Factor", "Equilibrium", "MS-Capital-Market", "User-Defined"],
}


_COLS = list(PARAMETER_OPTIONS.keys())


def read_account_parameters(account_id):
    """Return the most recent row from account_parameters as a dict of the 6 parameter fields."""
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cols = ", ".join(_COLS)
            cur.execute(
                f"SELECT {cols} FROM account_parameters "
                "WHERE account_id = %s ORDER BY updated_at DESC LIMIT 1",
                (account_id,),
            )
            row = cur.fetchone()
    if not row:
        return {col: None for col in _COLS}
    return dict(zip(_COLS, row))


def write_account_parameters(account_id, values):
    """Update account_parameters, archiving the previous row to account_parameters_history first."""
    all_cols = _COLS + ["gauge_measure"]
    new_vals = [values.get(col) for col in _COLS] + [values.get("risk_measure")]

    hist_cols = all_cols + ["valid_from"]
    set_sql   = ", ".join(f"{col} = %s" for col in all_cols)

    with pg_connection() as conn:
        with conn.cursor() as cur:
            # Fetch current row before overwriting it
            cur.execute(
                "SELECT " + ", ".join(all_cols) + ", updated_at "
                "FROM account_parameters WHERE account_id = %s",
                (account_id,),
            )
            current = cur.fetchone()

            if current:
                # Archive existing row: parameter values + valid_from = its updated_at
                hist_col_sql  = ", ".join(["account_id"] + hist_cols)
                hist_ph       = ", ".join(["%s"] * (1 + len(hist_cols)))
                hist_vals     = [account_id] + list(current)  # current includes updated_at at end
                cur.execute(
                    f"INSERT INTO account_parameters_history ({hist_col_sql}) "
                    f"VALUES ({hist_ph})",
                    hist_vals,
                )
                cur.execute(
                    f"UPDATE account_parameters SET {set_sql}, updated_at = now() "
                    "WHERE account_id = %s",
                    new_vals + [account_id],
                )
            else:
                # First save — no history to write, just insert
                col_list     = ", ".join(["account_id"] + all_cols)
                placeholders = ", ".join(["%s"] * (1 + len(all_cols)))
                cur.execute(
                    f"INSERT INTO account_parameters ({col_list}) VALUES ({placeholders})",
                    [account_id] + new_vals,
                )
        conn.commit()
