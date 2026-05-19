-- Risk preset table: DDL + seed data
-- Run once to create the table and populate initial preset values.

CREATE TABLE IF NOT EXISTS risk_preset (
    preset_name    VARCHAR(50)   NOT NULL,
    limit_category VARCHAR(100)  NOT NULL,
    limit_value    NUMERIC(10,4) NOT NULL,
    PRIMARY KEY (preset_name, limit_category)
);

-- ── Conservative ──────────────────────────────────────────────────────────────
INSERT INTO risk_preset (preset_name, limit_category, limit_value) VALUES
  -- concentration
  ('conservative', 'con_limit_asset_pct',    50),
  ('conservative', 'con_limit_region_pct',   40),
  ('conservative', 'con_limit_currency_pct', 50),
  ('conservative', 'con_limit_industry_pct', 30),
  ('conservative', 'con_limit_name_pct',      5),
  ('conservative', 'var_con_asset_pct',      50),
  ('conservative', 'var_con_region_pct',     40),
  ('conservative', 'var_con_currency_pct',   50),
  ('conservative', 'var_con_industry_pct',   30),
  ('conservative', 'var_con_name_pct',        5),
  -- risk
  ('conservative', 'var_limit_pct',           4),
  ('conservative', 'vol_limit_pct',           6),
  -- alloc
  ('conservative', 'alloc_equity_pct',       35),
  ('conservative', 'alloc_fixedincome_pct',  60),
  ('conservative', 'alloc_alternative_pct',   5),
  ('conservative', 'alloc_multiasset_pct',    5),
  ('conservative', 'alloc_cash_pct',          5),
  ('conservative', 'var_alloc_equity_pct',   40),
  ('conservative', 'var_alloc_fixedincome_pct', 50),
  ('conservative', 'var_alloc_alternative_pct',  5),
  ('conservative', 'var_alloc_multiasset_pct',   5),
  ('conservative', 'var_alloc_cash_pct',         0);

-- ── Moderate ──────────────────────────────────────────────────────────────────
INSERT INTO risk_preset (preset_name, limit_category, limit_value) VALUES
  -- concentration
  ('moderate', 'con_limit_asset_pct',    60),
  ('moderate', 'con_limit_region_pct',   50),
  ('moderate', 'con_limit_currency_pct', 70),
  ('moderate', 'con_limit_industry_pct', 40),
  ('moderate', 'con_limit_name_pct',     10),
  ('moderate', 'var_con_asset_pct',      60),
  ('moderate', 'var_con_region_pct',     50),
  ('moderate', 'var_con_currency_pct',   70),
  ('moderate', 'var_con_industry_pct',   40),
  ('moderate', 'var_con_name_pct',       10),
  -- risk
  ('moderate', 'var_limit_pct',           5),
  ('moderate', 'vol_limit_pct',          15),
  -- alloc
  ('moderate', 'alloc_equity_pct',       50),
  ('moderate', 'alloc_fixedincome_pct',  35),
  ('moderate', 'alloc_alternative_pct',   5),
  ('moderate', 'alloc_multiasset_pct',    5),
  ('moderate', 'alloc_cash_pct',          5),
  ('moderate', 'var_alloc_equity_pct',   60),
  ('moderate', 'var_alloc_fixedincome_pct', 25),
  ('moderate', 'var_alloc_alternative_pct', 10),
  ('moderate', 'var_alloc_multiasset_pct',   5),
  ('moderate', 'var_alloc_cash_pct',         0);

-- ── Aggressive ────────────────────────────────────────────────────────────────
INSERT INTO risk_preset (preset_name, limit_category, limit_value) VALUES
  -- concentration
  ('aggressive', 'con_limit_asset_pct',    80),
  ('aggressive', 'con_limit_region_pct',   80),
  ('aggressive', 'con_limit_currency_pct', 100),
  ('aggressive', 'con_limit_industry_pct', 80),
  ('aggressive', 'con_limit_name_pct',     20),
  ('aggressive', 'var_con_asset_pct',      80),
  ('aggressive', 'var_con_region_pct',     80),
  ('aggressive', 'var_con_currency_pct',  100),
  ('aggressive', 'var_con_industry_pct',   80),
  ('aggressive', 'var_con_name_pct',       20),
  -- risk
  ('aggressive', 'var_limit_pct',           8),
  ('aggressive', 'vol_limit_pct',          30),
  -- alloc
  ('aggressive', 'alloc_equity_pct',       70),
  ('aggressive', 'alloc_fixedincome_pct',  10),
  ('aggressive', 'alloc_alternative_pct',  10),
  ('aggressive', 'alloc_multiasset_pct',    5),
  ('aggressive', 'alloc_cash_pct',          5),
  ('aggressive', 'var_alloc_equity_pct',   85),
  ('aggressive', 'var_alloc_fixedincome_pct',  5),
  ('aggressive', 'var_alloc_alternative_pct',  5),
  ('aggressive', 'var_alloc_multiasset_pct',   5),
  ('aggressive', 'var_alloc_cash_pct',         0);
