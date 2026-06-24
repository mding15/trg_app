CREATE TABLE public."user" (
	user_id int4 DEFAULT nextval('user_id_seq'::regclass) NOT NULL,
	username varchar(120) NOT NULL,
	email varchar(120) NOT NULL,
	"password" varchar(60) NOT NULL,
	approval int4 NOT NULL,
	phone varchar(20) NULL,
	client_id int4 NOT NULL,
	"role" varchar(20) NULL,
	create_date date NOT NULL,
	firstname varchar(100) NULL,
	lastname varchar(100) NULL,
	activation_completed bool NULL,
	webdashboard_login varchar(120) NULL,
	CONSTRAINT user_email_key UNIQUE (email),
	CONSTRAINT user_pkey PRIMARY KEY (user_id),
	CONSTRAINT user_username_key UNIQUE (username),
	CONSTRAINT user_approval_fk FOREIGN KEY (approval) REFERENCES public.approval(id)
);

CREATE TABLE public.client (
	client_id serial4 NOT NULL,
	client_name varchar(100) NOT NULL,
	address varchar(200) NULL,
	contact_person varchar(100) NULL,
	contact_phone varchar(20) NULL,
	create_date date NOT NULL,
	aum varchar(50) NULL,
	primary_interest varchar(100) NULL,
	CONSTRAINT client_client_name_key UNIQUE (client_name),
	CONSTRAINT client_pkey PRIMARY KEY (client_id)
);

CREATE TABLE public.account_sec_attribute (
	account_id int4 NOT NULL,
	security_id varchar(20) NOT NULL,
	security_name varchar(1000) NULL,
	expected_return float4 NULL,
	currency varchar(20) NULL,
	"class" varchar(20) NULL,
	sc1 varchar(20) NULL,
	sc2 varchar(20) NULL,
	country varchar(50) NULL,
	region varchar(50) NULL,
	sector varchar(50) NULL,
	industry varchar(50) NULL,
	option_type varchar(20) NULL,
	payment_frequency int4 NULL,
	maturity_date date NULL,
	option_strike float4 NULL,
	underlying_security_id varchar(20) NULL,
	coupon_rate float4 NULL,
	isin varchar(200) NULL,
	cusip varchar(200) NULL,
	ticker varchar(200) NULL,
	CONSTRAINT acct_sec_attribute_pkey PRIMARY KEY (account_id, security_id)
);

CREATE TABLE public.account_parameters (
	id serial4 NOT NULL,
	account_id int4 NOT NULL,
	risk_horizon varchar(20) null,
	risk_measure varchar(20) null,
	base_currency varchar(20) null,
	beta_key varchar(20) null,
	benchmark varchar(20) null,
	exp_return varchar(20) null,
	gauge_measure varchar(20) null,
	updated_at timestamp DEFAULT now() NOT null
);

CREATE TABLE public.account_parameters_history (
	id serial4 NOT NULL,
	account_id int4 NOT NULL,
	risk_horizon varchar(20) null,
	risk_measure varchar(20) null,
	base_currency varchar(20) null,
	beta_key varchar(20) null,
	benchmark varchar(20) null,
	exp_return varchar(20) null,
	gauge_measure varchar(20) null,
	valid_from timestamp NOT NULL,
	archived_at timestamp DEFAULT now() NOT NULL
);

CREATE TABLE public.account_limit (
	account_id     int4          NOT NULL,
	limit_category varchar(100)  NOT NULL,
	limit_value    numeric       NULL,
	CONSTRAINT account_limit_pkey PRIMARY KEY (account_id, limit_category)
);

CREATE TABLE public.account_limit_history (
	id serial4 NOT NULL,
	account_id int4 NOT NULL,
	limit_category varchar(50) NOT NULL,
	limit_value numeric NULL,
	valid_from timestamp NOT NULL,
	archived_at timestamp DEFAULT now() NOT NULL
);

CREATE TABLE public.portfolio_info (
	port_id serial4 NOT NULL,
	port_name varchar(100) NOT NULL,
	filename varchar(100) NOT NULL,
	status varchar(20) NULL,
	report_id varchar(20) NULL,
	created_by varchar(50) NULL,
	create_date date NOT NULL,
	update_date date NOT NULL,
	message TEXT NULL,
	port_group_id int4 NULL,                      -- made nullable 2026-04-26
	as_of_date date NULL,
	market_value numeric NULL,
	tail_measure varchar(20) NULL,
	risk_horizon varchar(20) NULL,
	benchmark varchar(100) NULL,
	created_user_id int4 NULL,
	is_batch bool NULL,
	account_id int4 NULL,
	upload_dt timestamp NULL,                     -- added 2026-04-26
	client_id int4 NULL,                          -- added 2026-04-26
	port_type varchar(20) NULL,                   -- added 2026-05-16: 'tracked' | 'adhoc' | NULL (legacy)
	description text NULL,                        -- added 2026-05-16
	CONSTRAINT portfolio_info_pkey PRIMARY KEY (port_id),
	CONSTRAINT portfolio_info_client_id_fkey FOREIGN KEY (client_id) REFERENCES public.client(client_id)
	-- port_group_id FK removed when column made nullable (2026-04-26)
);

-- Migration (run once against live DB):
-- ALTER TABLE portfolio_info ADD COLUMN upload_dt TIMESTAMP NULL;
-- ALTER TABLE portfolio_info ALTER COLUMN port_group_id DROP NOT NULL;
-- ALTER TABLE portfolio_info DROP CONSTRAINT portfolio_info_port_group_id_fkey;
-- ALTER TABLE portfolio_info ADD COLUMN client_id INT4 NULL REFERENCES public.client(client_id);
-- UPDATE portfolio_info pi SET upload_dt = create_date::timestamp WHERE upload_dt IS NULL;
-- UPDATE portfolio_info pi SET client_id = (SELECT u.client_id FROM "user" u WHERE u.user_id = pi.created_user_id) WHERE client_id IS NULL;
-- ALTER TABLE portfolio_info ALTER COLUMN message TYPE TEXT;
-- ALTER TABLE portfolio_info ADD COLUMN IF NOT EXISTS port_type VARCHAR(20) NULL;   -- 2026-05-16
-- ALTER TABLE portfolio_info ADD COLUMN IF NOT EXISTS description TEXT NULL;         -- 2026-05-16

CREATE TABLE public.port_positions (
	port_id int4 NOT NULL,
	"ID" varchar(50) NOT NULL,
	"SecurityID" varchar(20) NULL,
	"SecurityName" varchar(200) NULL,
	"ISIN" varchar(20) NULL,
	"CUSIP" varchar(20) NULL,
	"Ticker" varchar(20) NULL,
	"Quantity" numeric NULL,
	"MarketValue" numeric NULL,
	"userAssetClass" varchar(50) NULL,
	"userCurrency" varchar(20) NULL,
	"ExpectedReturn" numeric NULL,
	"Currency" varchar(20) NULL,
	"Class" varchar(20) NULL,
	"SC1" varchar(20) NULL,
	"SC2" varchar(20) NULL,
	"Country" varchar(50) NULL,
	"Region" varchar(50) NULL,
	"Sector" varchar(50) NULL,
	"Industry" varchar(50) NULL,
	"OptionType" varchar(20) NULL,
	"PaymentFrequency" int4 NULL,
	"MaturityDate" date NULL,
	"OptionStrike" float4 NULL,
	"UnderlyingSecurityID" varchar(20) NULL,
	"CouponRate" numeric NULL,
	"LastPrice" numeric NULL,
	"LastPriceDate" date NULL,
	is_option bool NULL,
	"UnderlyingID" varchar(20) NULL,
	unknown_security bool DEFAULT false NOT NULL,
	asset_class varchar(50) NULL,
	asset_type varchar(50) NULL,
	total_cost numeric NULL,
	broker_name text NULL,
	broker_account text NULL
);
CREATE INDEX idx_port_positions_port_id ON public.port_positions USING btree (port_id);

CREATE TABLE public.port_parameters (
	port_id int4 NOT NULL,
	"PortfolioName" varchar(100) NULL,
	"AsofDate" date NOT NULL,
	"ReportDate" date NULL,
	"RiskHorizon" varchar(20) NULL,
	"TailMeasure" varchar(20) NULL,
	"ReturnFrequency" varchar(20) NULL,
	"Benchmark" varchar(50) NULL,
	"ExpectedReturn" varchar(20) NULL,
	"BaseCurrency" varchar(20) NULL
);
CREATE INDEX idx_port_id ON public.port_parameters USING btree (port_id);

CREATE TABLE public.limit_category (
	limit_category varchar(100) NULL,
	category_label varchar(100) NULL
);

CREATE TABLE public.asset_class_map (
	asset_class varchar(100) NULL,
	class_code varchar(10) NULL
);

CREATE TABLE public.risk_preset (
	preset_name    varchar(50)   NOT NULL,
	limit_category varchar(100)  NOT NULL,
	limit_value    numeric(10,4) NOT NULL,
	CONSTRAINT risk_preset_pkey PRIMARY KEY (preset_name, limit_category)
);

CREATE TABLE public.bond_info (
	"SecurityID" varchar(50) NULL,
	"Name" varchar(128) NULL,
	"ISIN" varchar(50) NULL,
	"CUSIP" varchar(50) NULL,
	"BB_Global" varchar(50) NULL,
	"BB_UNIQUE" varchar(50) NULL,
	"MaturityDate" date NULL,
	"IssuedCurrency" varchar(50) NULL,
	"IssuerTicker" varchar(50) NULL,
	"Rating" varchar(50) NULL,
	"Sector" varchar(50) NULL,
	"Country" varchar(50) NULL,
	"CouponRate" float4 NULL,
	"CouponType" varchar(50) NULL,
	"PaymentFrequency" int4 NULL,
	"Callable" varchar(50) NULL,
	"CallDate" date NULL,
	"Formula" varchar(50) NULL,
	"Putable" varchar(50) NULL,
	"DayCountBasis" varchar(50) NULL,
	"DatedDate" date NULL,
	"FirstInterestPayment" date NULL,
	"AddDate" date NULL,
	"UpdateDate" date NULL
);

CREATE TABLE public.bond_price (
	security_id varchar(20) NOT NULL,
	price_date date NOT NULL,
	price float8 NOT NULL
);

CREATE TABLE public.ir_curves (
	"CurveID" varchar(50) NULL,
	"SecurityID" varchar(50) NULL,
	"Ticker" varchar(50) NULL,
	"Tenor" float4 NULL
);

CREATE TABLE public.modeled_security (
	"SecurityID" varchar(20) NOT NULL,
	"SecurityName" varchar(200) NULL,
	"Currency" varchar(20) NULL,
	"AssetClass" varchar(20) NOT NULL,
	"AssetType" varchar(20) NOT NULL,
	active bool DEFAULT true NULL,
	add_at timestamp DEFAULT now() NOT NULL,
	CONSTRAINT modeled_security_pkey PRIMARY KEY ("SecurityID")
);

CREATE TABLE public.treasury_yield (
	date         DATE        PRIMARY KEY,

	bc_1month    NUMERIC,
	bc_2month    NUMERIC,
	bc_3month    NUMERIC,
	bc_6month    NUMERIC,
	bc_1year     NUMERIC,
	bc_2year     NUMERIC,
	bc_3year     NUMERIC,
	bc_5year     NUMERIC,
	bc_7year     NUMERIC,
	bc_10year    NUMERIC,
	bc_20year    NUMERIC,
	bc_30year    NUMERIC,

	insert_time  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- Populated by detl/tr_extract.py (run weekdays).
-- Values in percentage points (e.g. 4.32 = 4.32%).

CREATE TABLE public.current_security (
	"SecurityID" varchar(20) NOT NULL,
	"SecurityName" varchar(200) NULL,
	"Currency" varchar(20) NULL,
	"AssetClass" varchar(20) NOT NULL,
	"AssetType" varchar(20) NOT NULL,
	"ISIN" varchar(20) NULL,
	"CUSIP" varchar(20) NULL,
	"BB_UNIQUE" varchar(20) NULL,
	"BB_GLOBAL" varchar(20) NULL,
	"Ticker" varchar(100) NULL,
	"DataSource" varchar(20) NULL,
	"insert_time" TIMESTAMP NULL DEFAULT NOW(),
	CONSTRAINT current_security_pkey PRIMARY KEY ("SecurityID")
);

-- ---------------------------------------------------------------
-- st_corefactor
-- ---------------------------------------------------------------
CREATE TABLE st_corefactor (
  id              INTEGER         NOT NULL,
  symbol          VARCHAR(50)     NOT NULL,
  factor_name     VARCHAR(255)    NOT NULL,
  factor_sec_id   VARCHAR(50)     NOT NULL,
  asset_class     VARCHAR(100)    NOT NULL,
  PRIMARY KEY (id)
);

-- ---------------------------------------------------------------
-- st_model
-- ---------------------------------------------------------------
CREATE TABLE st_model (
  model_id    INTEGER         NOT NULL,
  model_name  VARCHAR(255)    NOT NULL,
  model_type  VARCHAR(50)     NOT NULL,
  f1          VARCHAR(50)     NULL,
  f2          VARCHAR(50)     NULL,
  f3          VARCHAR(50)     NULL,
  f4          VARCHAR(50)     NULL,
  f5          VARCHAR(50)     NULL,
  f6          VARCHAR(50)     NULL,
  f7          VARCHAR(50)     NULL,
  f8          VARCHAR(50)     NULL,
  f9          VARCHAR(50)     NULL,
  f10         VARCHAR(50)     NULL,
  PRIMARY KEY (model_id)
);

-- ---------------------------------------------------------------
-- st_shock  (29 rows: scenario × factor shocks)
-- ---------------------------------------------------------------
CREATE TABLE st_shock (
  scenario_id     INTEGER         NOT NULL,
  factor_symbol   VARCHAR(50)     NOT NULL,
  factor_sec_id   VARCHAR(50)     NOT NULL,
  shock           FLOAT           NOT NULL,   -- e.g. -50 means −50 %
  unit            VARCHAR(50)     NOT NULL,   -- 'percentage'
  PRIMARY KEY (scenario_id, factor_symbol)
);

-- ---------------------------------------------------------------
-- st_model_beta  (1 165 rows: factor loadings per security)
-- ---------------------------------------------------------------
CREATE TABLE st_model_beta (
  id          INTEGER         NOT NULL,
  model_id    INTEGER         NOT NULL,
  security_id VARCHAR(50)     NOT NULL,
  b1          FLOAT  NULL,
  b2          FLOAT  NULL,
  b3          FLOAT  NULL,
  b4          FLOAT  NULL,
  b5          FLOAT  NULL,
  b6          FLOAT  NULL,
  b7          FLOAT  NULL,
  b8          FLOAT  NULL,
  b9          FLOAT  NULL,
  b10         FLOAT  NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (model_id) REFERENCES st_model (model_id)
);

-- ---------------------------------------------------------------
-- st_security_pnl  (stress test pnl per scenario_id, security)
-- ---------------------------------------------------------------
CREATE TABLE st_security_pnl (
  id          INTEGER         GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  scenario_id INTEGER         NOT NULL,
  security_id VARCHAR(50)     NOT NULL,
  pnl         FLOAT           NULL,

  FOREIGN KEY (scenario_id) REFERENCES st_scenarios (scenario_id),
  CONSTRAINT uq_security_pnl UNIQUE (scenario_id, security_id)
);

-- ---------------------------------------------------------------
-- st_scenarios  (5 rows: scenario definitions)
-- ---------------------------------------------------------------
CREATE TABLE public.st_scenarios (
	scenario_id int4 DEFAULT nextval('scenario_definition_scenario_id_seq'::regclass) NOT NULL,
	"name" varchar(128) NOT NULL,
	"period" varchar(64) NULL,
	severity varchar(16) NOT NULL,
	type varchar(20) NULL,   -- 'Historical' | 'Hypothetical'
	is_active bool DEFAULT true NOT NULL,
	CONSTRAINT scenario_definition_pkey PRIMARY KEY (scenario_id)
);
-- Migration (run once against live DB):
-- ALTER TABLE public.st_scenarios ADD COLUMN IF NOT EXISTS type VARCHAR(20) NULL;
-- UPDATE public.st_scenarios SET type = 'Hypothetical' WHERE period = 'Hypothetical';
-- UPDATE public.st_scenarios SET type = 'Historical'   WHERE period != 'Hypothetical' AND type IS NULL;

-- ---------------------------------------------------------------
-- st_account_summary  (stress test P&L per account × scenario)
-- ---------------------------------------------------------------
CREATE TABLE public.st_account_summary (
  id          INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  account_id  INTEGER NOT NULL,
  as_of_date  DATE    NOT NULL,
  scenario_id INTEGER NOT NULL,
  st_pnl      FLOAT   NULL,
  CONSTRAINT uq_st_account_summary UNIQUE (account_id, as_of_date, scenario_id),
  FOREIGN KEY (scenario_id) REFERENCES public.st_scenarios (scenario_id)
);

-- ---------------------------------------------------------------
-- benchmark_metrics  (daily risk metrics per benchmark)
-- Populated by process2/calc_benchmark.py (run_metrics).
-- ---------------------------------------------------------------
CREATE TABLE public.benchmark_metrics (
    benchmark_id  INTEGER   NOT NULL,
    date          DATE      NOT NULL,
    volatility    NUMERIC,          -- annualised (sqrt(252) * daily std)
    var_1d_95     NUMERIC,          -- 1-day 95% VaR, positive loss
    es_1d_95      NUMERIC,          -- 1-day 95% ES (CVaR), positive loss
    var_1d_99     NUMERIC,          -- 1-day 99% VaR, positive loss
    es_1d_99      NUMERIC,          -- 1-day 99% ES (CVaR), positive loss
    sharpe_vol    NUMERIC,          -- (expect_return - rf) / volatility
    sharpe_var    NUMERIC,          -- (expect_return - rf) / (var_1d_95 * sqrt(252))
    updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT benchmark_metrics_pkey PRIMARY KEY (benchmark_id, date)
);

CREATE TABLE public.stat_static_data (
	"Name" varchar(50) NULL,
	"Value" float4 NULL
);

-- ---------------------------------------------------------------
-- alternative_model  (illiquidity / proxy model per alternative security)
-- Sourced from work/Alternative.xlsx sheet "model".
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.alternative_model (
    security_id    VARCHAR(50)  NOT NULL,
    security_name  VARCHAR(500) NULL,
    asset_subclass VARCHAR(100) NULL,
    proxy_name     VARCHAR(200) NULL,
    proxy_id       VARCHAR(50)  NULL,
    proxy_correl   FLOAT        NULL,
    unadj_vol      FLOAT        NULL,
    adj_vol        FLOAT        NULL,
    liq_adj        FLOAT        NULL,    -- liquidity adjustment factor (adj_vol / unadj_vol)
    proxy_vol      FLOAT        NULL,
    beta           FLOAT        NULL,
    sigma          FLOAT        NULL,    -- idiosyncratic vol
    r_sq           FLOAT        NULL,   -- R-squared; source column name was "r-sq"
    updated_at     TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT alternative_model_pkey PRIMARY KEY (security_id)
);

-- ---------------------------------------------------------------
-- alternative_var  (VaR metrics per alternative position × account × date)
-- Populated by process2/calc_alternative_var.py.
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.alternative_var (
    account_id      INTEGER      NOT NULL,
    as_of_date      DATE         NOT NULL,
    pos_id          VARCHAR(50)  NOT NULL,
    security_id     VARCHAR(50)  NOT NULL,
    market_value    FLOAT        NULL,
    -- liquidity-adjusted metrics
    std             FLOAT        NULL,
    mg_std          FLOAT        NULL,
    var_95          FLOAT        NULL,
    var_99          FLOAT        NULL,
    es_95           FLOAT        NULL,
    es_99           FLOAT        NULL,
    mg_var_95       FLOAT        NULL,
    mg_var_99       FLOAT        NULL,
    mg_es_95        FLOAT        NULL,
    mg_es_99        FLOAT        NULL,
    -- unadjusted metrics
    unadj_std       FLOAT        NULL,
    unadj_mg_std    FLOAT        NULL,
    unadj_var_95    FLOAT        NULL,
    unadj_var_99    FLOAT        NULL,
    unadj_es_95     FLOAT        NULL,
    unadj_es_99     FLOAT        NULL,
    unadj_mg_var_95 FLOAT        NULL,
    unadj_mg_var_99 FLOAT        NULL,
    unadj_mg_es_95  FLOAT        NULL,
    unadj_mg_es_99  FLOAT        NULL,
    updated_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT alternative_var_pkey PRIMARY KEY (account_id, as_of_date, pos_id)
);
-- Migration (run once against live DB):
-- ALTER TABLE alternative_var ADD COLUMN IF NOT EXISTS market_value    FLOAT NULL;
-- ALTER TABLE alternative_var ADD COLUMN IF NOT EXISTS unadj_std       FLOAT NULL;
-- ALTER TABLE alternative_var ADD COLUMN IF NOT EXISTS unadj_mg_std    FLOAT NULL;
-- ALTER TABLE alternative_var ADD COLUMN IF NOT EXISTS unadj_var_95    FLOAT NULL;
-- ALTER TABLE alternative_var ADD COLUMN IF NOT EXISTS unadj_var_99    FLOAT NULL;
-- ALTER TABLE alternative_var ADD COLUMN IF NOT EXISTS unadj_es_95     FLOAT NULL;
-- ALTER TABLE alternative_var ADD COLUMN IF NOT EXISTS unadj_es_99     FLOAT NULL;
-- ALTER TABLE alternative_var ADD COLUMN IF NOT EXISTS unadj_mg_var_95 FLOAT NULL;
-- ALTER TABLE alternative_var ADD COLUMN IF NOT EXISTS unadj_mg_var_99 FLOAT NULL;
-- ALTER TABLE alternative_var ADD COLUMN IF NOT EXISTS unadj_mg_es_95  FLOAT NULL;
-- ALTER TABLE alternative_var ADD COLUMN IF NOT EXISTS unadj_mg_es_99  FLOAT NULL;


CREATE TABLE public.position_var (
	as_of_date date NOT NULL,
	account_id int4 NOT NULL,
	pos_id text NOT NULL,
	security_id text NULL,
	security_name text NULL,
	isin text NULL,
	cusip text NULL,
	ticker text NULL,
	broker_account text NULL,
	quantity numeric NULL,
	market_value numeric NULL,
	weight numeric NULL,
	currency text NULL,
	last_price numeric NULL,
	last_price_date date NULL,
	asset_class text NULL,
	asset_type text NULL,
	"class" text NULL,
	sc1 text NULL,
	sc2 text NULL,
	country text NULL,
	region text NULL,
	sector text NULL,
	industry text NULL,
	rating text NULL,
	pd numeric NULL,
	expected_return numeric NULL,
	coupon_rate numeric NULL,
	option_type text NULL,
	option_strike numeric NULL,
	payment_frequency text NULL,
	maturity_date date NULL,
	underlying_security_id text NULL,
	underlying_id text NULL,
	underlying_price numeric NULL,
	is_option bool NULL,
	excluded bool NULL,
	exclude_reason text NULL,
	risk_free_rate numeric NULL,
	tenor numeric NULL,
	delta numeric NULL,
	gamma numeric NULL,
	vega numeric NULL,
	iv numeric NULL,
	ir_tenor numeric NULL,
	yield numeric NULL,
	duration numeric NULL,
	convexity numeric NULL,
	ir_pv01 numeric NULL,
	sp_pv01 numeric NULL,
	spread_duration numeric NULL,
	spread_convexity numeric NULL,
	delta_var numeric NULL,
	ir_var numeric NULL,
	spread_var numeric NULL,
	gamma_var numeric NULL,
	vega_var numeric NULL,
	ir_duration_var numeric NULL,
	ir_convexity_var numeric NULL,
	sp_duration_var numeric NULL,
	sp_convexity_var numeric NULL,
	default_var numeric NULL,
	skewness numeric NULL,
	kurtosis numeric NULL,
	vol numeric NULL,
	std numeric NULL,
	insert_time timestamptz DEFAULT now() NOT NULL,
	broker text NULL,
	beta float8 NULL,
	total_cost numeric NULL,
	mg_std numeric NULL,
	var_95 numeric NULL,
	var_99 numeric NULL,
	es_95 numeric NULL,
	es_99 numeric NULL,
	mg_var_95 numeric NULL,
	mg_var_99 numeric NULL,
	mg_es_95 numeric NULL,
	mg_es_99 numeric NULL,
	mg_delta_var        numeric NULL,
	mg_ir_var           numeric NULL,
	mg_spread_var       numeric NULL,
	mg_ir_duration_var  numeric NULL,
	mg_ir_convexity_var numeric NULL,
	mg_sp_duration_var  numeric NULL,
	mg_sp_convexity_var numeric NULL,
	CONSTRAINT position_var_pkey PRIMARY KEY (as_of_date, account_id, pos_id)
);
-- Migration (run once against live DB):
-- ALTER TABLE position_var ADD COLUMN IF NOT EXISTS mg_delta_var        NUMERIC NULL;
-- ALTER TABLE position_var ADD COLUMN IF NOT EXISTS mg_ir_var           NUMERIC NULL;
-- ALTER TABLE position_var ADD COLUMN IF NOT EXISTS mg_spread_var       NUMERIC NULL;
-- ALTER TABLE position_var ADD COLUMN IF NOT EXISTS mg_ir_duration_var  NUMERIC NULL;
-- ALTER TABLE position_var ADD COLUMN IF NOT EXISTS mg_ir_convexity_var NUMERIC NULL;
-- ALTER TABLE position_var ADD COLUMN IF NOT EXISTS mg_sp_duration_var  NUMERIC NULL;
-- ALTER TABLE position_var ADD COLUMN IF NOT EXISTS mg_sp_convexity_var NUMERIC NULL;

CREATE TABLE public.port_position_var (
	port_id int4 NOT NULL,
	pos_id text NOT NULL,
	as_of_date date NULL,
	security_id text NULL,
	security_name text NULL,
	isin text NULL,
	cusip text NULL,
	ticker text NULL,
	broker_account text NULL,
	quantity numeric NULL,
	market_value numeric NULL,
	weight numeric NULL,
	currency text NULL,
	last_price numeric NULL,
	last_price_date date NULL,
	asset_class text NULL,
	asset_type text NULL,
	"class" text NULL,
	sc1 text NULL,
	sc2 text NULL,
	country text NULL,
	region text NULL,
	sector text NULL,
	industry text NULL,
	rating text NULL,
	pd numeric NULL,
	expected_return numeric NULL,
	coupon_rate numeric NULL,
	option_type text NULL,
	option_strike numeric NULL,
	payment_frequency text NULL,
	maturity_date date NULL,
	underlying_security_id text NULL,
	underlying_id text NULL,
	underlying_price numeric NULL,
	is_option bool NULL,
	excluded bool NULL,
	exclude_reason text NULL,
	risk_free_rate numeric NULL,
	tenor numeric NULL,
	delta numeric NULL,
	gamma numeric NULL,
	vega numeric NULL,
	iv numeric NULL,
	ir_tenor numeric NULL,
	yield numeric NULL,
	duration numeric NULL,
	convexity numeric NULL,
	ir_pv01 numeric NULL,
	sp_pv01 numeric NULL,
	spread_duration numeric NULL,
	spread_convexity numeric NULL,
	delta_var numeric NULL,
	ir_var numeric NULL,
	spread_var numeric NULL,
	gamma_var numeric NULL,
	vega_var numeric NULL,
	ir_duration_var numeric NULL,
	ir_convexity_var numeric NULL,
	sp_duration_var numeric NULL,
	sp_convexity_var numeric NULL,
	default_var numeric NULL,
	skewness numeric NULL,
	kurtosis numeric NULL,
	vol numeric NULL,
	std numeric NULL,
	beta numeric NULL,
	insert_time timestamptz DEFAULT now() NOT NULL,
	total_cost numeric NULL,
	mg_std numeric NULL,
	var_95 numeric NULL,
	es_95 numeric NULL,
	mg_var_95 numeric NULL,
	mg_es_95 numeric NULL,
	var_99 numeric NULL,
	es_99 numeric NULL,
	mg_var_99 numeric NULL,
	mg_es_99 numeric NULL,
	CONSTRAINT port_position_var_pkey PRIMARY KEY (port_id, pos_id),
	CONSTRAINT port_position_var_port_id_fkey FOREIGN KEY (port_id) REFERENCES public.portfolio_info(port_id)
);
-- ---------------------------------------------------------------
-- security_sensitivity  (security-level sensitivities per as_of_date)
-- Populated by the VaR engine after calc_*_pnl runs.
-- ---------------------------------------------------------------
CREATE TABLE public.security_sensitivity (
    as_of_date       DATE         NOT NULL,
    security_id      VARCHAR(50)  NOT NULL,
    tenor            FLOAT      NULL,
    delta            FLOAT      NULL,
    gamma            FLOAT      NULL,
    vega             FLOAT      NULL,
    iv               FLOAT      NULL,
    ir_tenor         FLOAT      NULL,
    yield            FLOAT      NULL,
    duration         FLOAT      NULL,
    convexity        FLOAT      NULL,
    spread_duration  FLOAT        NULL,
    spread_convexity FLOAT        NULL,
    skewness         FLOAT      NULL,
    kurtosis         FLOAT      NULL,
    insert_time      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT security_sensitivity_pkey PRIMARY KEY (as_of_date, security_id)
);
-- Migration (run once against live DB):
-- ALTER TABLE security_sensitivity ADD COLUMN IF NOT EXISTS skewness FLOAT NULL;
-- ALTER TABLE security_sensitivity ADD COLUMN IF NOT EXISTS kurtosis FLOAT NULL;

-- ---------------------------------------------------------------
-- security_pnl_stat  (P&L distribution statistics per security × date × type)
-- Populated by process2/db_pnl_stat.py  (called by calc_*_pnl).
-- ---------------------------------------------------------------
CREATE TABLE security_pnl_stat (
    as_of_date  DATE         NOT NULL,
    security_id VARCHAR(32)  NOT NULL,
    pnl_type    VARCHAR(16)  NOT NULL,
    min         FLOAT,
    max         FLOAT,
    mean        FLOAT,
    std         FLOAT,
    q_1pct      FLOAT,
    q_5pct      FLOAT,
    q_50pct     FLOAT,
    q_95pct     FLOAT,
    q_99pct     FLOAT,
    es_5pct     FLOAT,
    es_1pct     FLOAT,
    insert_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (as_of_date, security_id, pnl_type)
);

-- ---------------------------------------------------------------
-- model_security_stat  (distribution statistics per model × security)
-- Populated by models/collect_model_securities.py.
-- ---------------------------------------------------------------
CREATE TABLE public.model_security_stat (
    model_id    VARCHAR(50)  NOT NULL,
    model       VARCHAR(50)  NOT NULL,
    category    VARCHAR(20)  NOT NULL,
    folder      VARCHAR(200) NOT NULL,
    security_id VARCHAR(50)  NOT NULL,
    min         FLOAT        NULL,
    max         FLOAT        NULL,
    mean        FLOAT        NULL,
    std         FLOAT        NULL,
    q_1pct      FLOAT        NULL,
    q_5pct      FLOAT        NULL,
    q_50pct     FLOAT        NULL,
    q_95pct     FLOAT        NULL,
    q_99pct     FLOAT        NULL,
    es_5pct     FLOAT        NULL,
    es_1pct     FLOAT        NULL,
    insert_time TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT model_security_stat_pkey PRIMARY KEY (model_id, model, security_id)
);

CREATE TABLE public.security_xref (
	id serial4 NOT NULL,
	"REF_ID" varchar(200) NOT NULL,
	"REF_TYPE" varchar(20) NOT NULL,
	"SecurityID" varchar(20) NULL,
	"DataSource" varchar(100) NULL,
	"DateAdded" date DEFAULT CURRENT_DATE NOT NULL,
	CONSTRAINT security_xref_pkey PRIMARY KEY (id)
);

CREATE TABLE public.security_info (
	id serial4 NOT NULL,
	"SecurityID" varchar(20) NULL,
	"SecurityName" varchar(1000) NOT NULL,
	"Currency" varchar(20) NULL,
	"AssetClass" varchar(20) NULL,
	"AssetType" varchar(20) NULL,
	"DataSource" varchar(100) NULL,
	"DateAdded" date DEFAULT CURRENT_DATE NULL,
	CONSTRAINT security_info_pkey PRIMARY KEY (id)
);

