-- public.whatif_portfolio_metrics source

CREATE OR REPLACE VIEW public.whatif_portfolio_metrics
AS WITH base AS (
         SELECT ppv.port_id,
            sum(ppv.market_value) AS mv,
            sum(ppv.marginal_std) AS std_dollar,
            sum(ppv.marginal_var) AS var_dollar,
            sum(ppv.market_value * ppv.expected_return) AS weighted_ret
           FROM port_position_var ppv
          GROUP BY ppv.port_id
        ), port_info AS (
         SELECT pi.port_id,
            pi.client_id,
            pi.account_id,
            pi.port_name,
            pp."AsofDate" AS as_of_date,
            COALESCE(rhd.days, 1) AS days
           FROM portfolio_info pi
             JOIN port_parameters pp ON pi.port_id = pp.port_id AND pi.status::text = 'Success'::text
             LEFT JOIN risk_horizon_days rhd ON rhd.risk_horizon::text = pp."RiskHorizon"::text
        )
 SELECT p.port_id,
    p.client_id,
    p.account_id,
    p.port_name,
    p.as_of_date,
    b.mv,
    b.weighted_ret / NULLIF(b.mv, 0::numeric) AS exp_ret,
    (b.std_dollar / NULLIF(b.mv, 0::numeric))::double precision * sqrt((252 / p.days)::double precision) * 100::double precision AS vol_pct,
    b.var_dollar::double precision / sqrt(p.days::double precision) AS var_1d_95,
    (b.weighted_ret / NULLIF(b.mv, 0::numeric))::double precision / NULLIF((b.std_dollar / NULLIF(b.mv, 0::numeric))::double precision * sqrt((252 / p.days)::double precision), 0::double precision) AS sharpe_vol,
    (b.weighted_ret / NULLIF(b.mv, 0::numeric))::double precision / NULLIF((b.var_dollar / NULLIF(b.mv, 0::numeric))::double precision * sqrt((252 / p.days)::double precision), 0::double precision) AS sharpe_var
   FROM base b
     JOIN port_info p ON b.port_id = p.port_id
UNION
 SELECT pi.port_id,
    pi.client_id,
    pi.account_id,
    pi.port_name,
    ps.as_of_date,
    ps.aum AS mv,
    0 AS exp_ret,
    ps.volatility AS vol_pct,
    ps.var_1d_95,
    ps.sharpe_vol,
    ps.sharpe_var
   FROM db_portfolio_summary ps,
    portfolio_info pi
  WHERE ps.account_id = pi.account_id AND ps.as_of_date = (( SELECT max(ps2.as_of_date) AS max
           FROM db_portfolio_summary ps2
          WHERE ps2.account_id = ps.account_id));