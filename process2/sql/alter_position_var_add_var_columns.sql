-- Add dual-CL VaR columns and backfill from existing single-CL columns.
-- Old columns (var, tvar, marginal_var, marginal_tvar, marginal_std) are kept.

ALTER TABLE position_var
    ADD COLUMN IF NOT EXISTS mg_std      NUMERIC,
    ADD COLUMN IF NOT EXISTS var_95      NUMERIC,
    ADD COLUMN IF NOT EXISTS var_99      NUMERIC,
    ADD COLUMN IF NOT EXISTS es_95       NUMERIC,
    ADD COLUMN IF NOT EXISTS es_99       NUMERIC,
    ADD COLUMN IF NOT EXISTS mg_var_95   NUMERIC,
    ADD COLUMN IF NOT EXISTS mg_var_99   NUMERIC,
    ADD COLUMN IF NOT EXISTS mg_es_95    NUMERIC,
    ADD COLUMN IF NOT EXISTS mg_es_99    NUMERIC;

UPDATE position_var SET
    mg_std    = marginal_std,
    var_95    = var,
    es_95     = tvar,
    mg_var_95 = marginal_var,
    mg_es_95  = marginal_tvar
WHERE var_95 IS NULL;
