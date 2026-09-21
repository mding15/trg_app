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
	risk_level varchar(20) DEFAULT 'custom' null,
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
	risk_level varchar(20) null,
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
	"SecurityID" text NULL,
	"SecurityName" text NULL,
	"ISIN" text NULL,
	"CUSIP" text NULL,
	"Ticker" text NULL,
	"Quantity" numeric NULL,
	"MarketValue" numeric NULL,
	"userAssetClass" text NULL,
	"userCurrency" text NULL,
	"ExpectedReturn" numeric NULL,
	"Currency" text NULL,
	"Class" text NULL,
	"SC1" text NULL,
	"SC2" text NULL,
	"Country" text NULL,
	"Region" text NULL,
	"Sector" text NULL,
	"Industry" text NULL,
	"OptionType" text NULL,
	"PaymentFrequency" int4 NULL,
	"MaturityDate" date NULL,
	"OptionStrike" float4 NULL,
	"UnderlyingSecurityID" text NULL,
	"CouponRate" numeric NULL,
	"LastPrice" numeric NULL,
	"LastPriceDate" date NULL,
	is_option bool NULL,
	"UnderlyingID" text NULL,
	unknown_security bool DEFAULT false NOT NULL,
	asset_class text NULL,
	asset_type text NULL,
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
    theta            FLOAT      NULL,
    insert_time      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT security_sensitivity_pkey PRIMARY KEY (as_of_date, security_id)
);
-- Migration (run once against live DB):
-- ALTER TABLE security_sensitivity ADD COLUMN IF NOT EXISTS skewness FLOAT NULL;
-- ALTER TABLE security_sensitivity ADD COLUMN IF NOT EXISTS kurtosis FLOAT NULL;
-- ALTER TABLE security_sensitivity ADD COLUMN IF NOT EXISTS theta FLOAT NULL;

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
	"reviewed" bool DEFAULT false NOT NULL,
	CONSTRAINT security_info_pkey PRIMARY KEY (id)
);

CREATE TABLE public.option_info (
	id serial4 NOT NULL,
	security_id varchar(20) NULL,
	option_type varchar(20) NULL,
	option_class varchar(20) NULL,
	maturity date NULL,
	strike numeric NULL,
	underlying varchar(100) NULL,
	underlying_sec_id varchar(20) NULL,
	CONSTRAINT option_info_pkey PRIMARY KEY (id)
);

-- ---------------------------------------------------------------
-- Tables migrated from create_tables.py (2026-07-06)
-- ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.account (
    account_id        SERIAL       PRIMARY KEY,
    account_name      VARCHAR(120) NOT NULL,
    short_name        VARCHAR(20)  NULL,
    owner_id          INT          NOT NULL,
    client_id         INT          NOT NULL,
    parent_account_id INT          DEFAULT NULL REFERENCES public.account(account_id),
    create_time       TIMESTAMP    DEFAULT NOW(),
    next_run_time     TIMESTAMP    NULL
);

CREATE TABLE IF NOT EXISTS public.db_mv_history (
    id             SERIAL PRIMARY KEY,
    account_id     INT    NOT NULL,
    as_of_date     DATE   NOT NULL,
    security_id    TEXT   NOT NULL,
    broker         TEXT   NULL,
    broker_account TEXT   NULL,
    market_value   FLOAT,
    UNIQUE (account_id, as_of_date, security_id, broker, broker_account)
);

CREATE TABLE IF NOT EXISTS public.db_portfolio_summary (
    id              SERIAL    PRIMARY KEY,
    account_id      INT       NOT NULL,
    as_of_date      DATE      NOT NULL,
    aum             FLOAT,
    num_positions   INT,
    day_pnl         FLOAT,
    day_return      FLOAT,
    mtd_return      FLOAT,
    ytd_return      FLOAT,
    one_year_return FLOAT,
    unrealized_gain  FLOAT,
    var_1d_95        FLOAT,
    var_1d_99        FLOAT,
    var_10d_99       FLOAT,
    es_1d_95         FLOAT,
    es_1d_99         FLOAT,
    volatility       FLOAT,
    sharpe_vol       FLOAT,
    sharpe_var       FLOAT,
    sharpe_es        FLOAT,
    beta             FLOAT,
    max_drawdown      FLOAT,
    top_five_conc     FLOAT,
    three_year_return FLOAT,
    si_return         FLOAT,
    expected_return   NUMERIC,
    updated_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, as_of_date)
);

CREATE TABLE IF NOT EXISTS public.db_positions (
    id              SERIAL    PRIMARY KEY,
    account_id      INT       NOT NULL,
    as_of_date      DATE      NOT NULL,
    security_id     TEXT      NOT NULL,
    ticker          TEXT      NULL,
    name            TEXT      NULL,
    asset_class     TEXT      NULL,
    currency        TEXT      NULL,
    market_value    FLOAT,
    weight          FLOAT,
    day_pnl         FLOAT,
    day_return      FLOAT,
    mtd_return      FLOAT,
    ytd_return      FLOAT,
    one_year_return FLOAT,
    var_contrib     FLOAT,
    unrealized_gain FLOAT     NULL,
    broker          TEXT      NULL,
    broker_account  TEXT      NULL,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, as_of_date, security_id, broker, broker_account)
);

CREATE TABLE IF NOT EXISTS public.db_concentrations (
    id            SERIAL    PRIMARY KEY,
    account_id    INT       NOT NULL,
    as_of_date    DATE      NOT NULL,
    category      TEXT      NOT NULL,
    category_name TEXT      NULL,
    max_weight    FLOAT     NULL,
    limit_value   FLOAT     NULL,
    ratio         FLOAT     NULL,
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, as_of_date, category)
);

CREATE TABLE IF NOT EXISTS public.mssb_secty_map (
    id           SERIAL       PRIMARY KEY,
    trg_sec_id   INT          NOT NULL,
    sec_cusip    VARCHAR(20)  NOT NULL,
    sec_isin     VARCHAR(20)  NOT NULL,
    sec_sedol    VARCHAR(20)  NOT NULL,
    sec_symbol   VARCHAR(20)  NOT NULL,
    description  TEXT         NULL,
    updated_at   TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.proc_positions (
    id              SERIAL    PRIMARY KEY,
    as_of_date      DATE      NOT NULL,
    account_id      INT       NOT NULL,
    position_id     TEXT      NOT NULL,
    security_id     TEXT      NOT NULL,
    security_name   TEXT      NOT NULL,
    isin            TEXT      NOT NULL,
    cusip           TEXT      NOT NULL,
    ticker          TEXT      NOT NULL,
    quantity        FLOAT     NULL,
    market_value    FLOAT     NULL,
    asset_class     TEXT      NULL,
    currency        TEXT      NOT NULL,
    broker_account  TEXT      NOT NULL,
    broker          TEXT      NULL,
    insert_time     TIMESTAMP NOT NULL DEFAULT NOW(),
    last_price      NUMERIC   NULL,
    last_price_date DATE      NULL,
    feed_source     TEXT      NULL,
    total_cost      NUMERIC   NULL
);

CREATE TABLE IF NOT EXISTS public.mssb_posit (
    feed_date                    DATE     NULL,
    routing_code                 TEXT     NULL,
    account                      TEXT     NULL,
    cusip                        TEXT     NULL,
    security_description         TEXT     NULL,
    quantity                     NUMERIC  NULL,
    market_base                  NUMERIC  NULL,
    market_local                 NUMERIC  NULL,
    coupon_rate                  NUMERIC  NULL,
    issue_date                   DATE     NULL,
    maturity_date                DATE     NULL,
    original_face                NUMERIC  NULL,
    factor                       NUMERIC  NULL,
    currency                     TEXT     NULL,
    symbol                       TEXT     NULL,
    total_cost                   NUMERIC  NULL,
    exchange                     TEXT     NULL,
    account_type                 TEXT     NULL,
    security_code                TEXT     NULL,
    security_no                  TEXT     NULL,
    sedol                        TEXT     NULL,
    isin                         TEXT     NULL,
    settlement_quantity          NUMERIC  NULL,
    market_base_sd               NUMERIC  NULL,
    product_id                   TEXT     NULL,
    restricted_sec_flag          TEXT     NULL,
    restricted_qnty              NUMERIC  NULL,
    long_short_indicator         TEXT     NULL,
    blank_1                      TEXT     NULL,
    quantity_1                   NUMERIC  NULL,
    symbol_cusip                 TEXT     NULL,
    position_as_of_date          DATE     NULL,
    alternate_security_indicator TEXT     NULL,
    wash_sales_indicator         TEXT     NULL,
    partial_call_quantity        NUMERIC  NULL,
    blank_2                      TEXT     NULL,
    accrued_interest             NUMERIC  NULL
);

CREATE TABLE IF NOT EXISTS public.proc_positions_hist (
    id              SERIAL    PRIMARY KEY,
    as_of_date      DATE      NOT NULL,
    account_id      INT       NOT NULL,
    position_id     TEXT      NOT NULL,
    security_id     TEXT      NOT NULL,
    security_name   TEXT      NOT NULL,
    isin            TEXT      NOT NULL,
    cusip           TEXT      NOT NULL,
    ticker          TEXT      NOT NULL,
    quantity        FLOAT     NULL,
    market_value    FLOAT     NULL,
    asset_class     TEXT      NULL,
    currency        TEXT      NOT NULL,
    broker_account  TEXT      NOT NULL,
    broker          TEXT      NULL,
    insert_time     TIMESTAMP NOT NULL DEFAULT NOW(),
    archived_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    last_price      NUMERIC   NULL,
    last_price_date DATE      NULL,
    feed_source     TEXT      NULL,
    total_cost      NUMERIC   NULL
);

CREATE TABLE IF NOT EXISTS public.broker_account (
    id             SERIAL    PRIMARY KEY,
    account_id     INT       NOT NULL,
    broker_account TEXT      NOT NULL,
    broker         TEXT      NOT NULL,
    routing_code   TEXT      NULL,
    name           TEXT      NULL,
    setup_user_id  INT       NULL REFERENCES public."user"(user_id),
    auth_expiry    DATE      NULL,
    status         TEXT      NOT NULL DEFAULT 'Pending',
    updated_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.broker_feed_summary (
    id                SERIAL    PRIMARY KEY,
    broker_account_id INT       NOT NULL REFERENCES public.broker_account(id),
    feed_date         DATE      NOT NULL,
    mv                NUMERIC   NULL,
    positions         INT       NULL,
    securities        INT       NULL,
    transactions      INT       NULL,
    tax_lots          INT       NULL,
    last_feed         TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (broker_account_id, feed_date)
);

CREATE TABLE IF NOT EXISTS public.broker_account_hist (
    id                SERIAL    PRIMARY KEY,
    broker_account_id INT       NOT NULL,
    account_id        INT       NOT NULL,
    broker_account    TEXT      NOT NULL,
    broker            TEXT      NOT NULL,
    routing_code      TEXT      NULL,
    name              TEXT      NULL,
    setup_user_id     INT       NULL,
    auth_expiry       DATE      NULL,
    status            TEXT      NOT NULL,
    updated_at        TIMESTAMP NOT NULL,
    deleted_by        TEXT      NULL,
    archived_at       TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.account_access (
    id         SERIAL    PRIMARY KEY,
    account_id INT       NOT NULL,
    user_id    INT       NOT NULL,
    is_default BOOLEAN   NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_account_access_default
    ON public.account_access (user_id)
    WHERE is_default = TRUE;

CREATE TABLE IF NOT EXISTS public.db_asset_allocation (
    id            SERIAL       PRIMARY KEY,
    account_id    INT          NOT NULL,
    as_of_date    DATE         NOT NULL,
    asset_class   VARCHAR(64)  NOT NULL,
    market_value  FLOAT,
    weight        FLOAT,
    bmk_weight    FLOAT,
    period_return FLOAT,
    var_contrib   FLOAT,
    updated_at    TIMESTAMP    NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, as_of_date, asset_class)
);

CREATE TABLE IF NOT EXISTS public.db_portfolio_breakdown (
    id             SERIAL      PRIMARY KEY,
    account_id     INT         NOT NULL,
    as_of_date     DATE        NOT NULL,
    breakdown_type VARCHAR(32) NOT NULL,
    category       VARCHAR(64) NOT NULL,
    weight         FLOAT,
    var_contrib    FLOAT,
    updated_at     TIMESTAMP   NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, as_of_date, breakdown_type, category)
);

CREATE TABLE IF NOT EXISTS public.account_scenario (
    account_id  INT NOT NULL,
    scenario_id INT NOT NULL REFERENCES public.st_scenarios(scenario_id),
    PRIMARY KEY (account_id, scenario_id)
);

CREATE TABLE IF NOT EXISTS public.db_stress_results (
    id          SERIAL    PRIMARY KEY,
    account_id  INT       NOT NULL,
    as_of_date  DATE      NOT NULL,
    scenario_id INT       NOT NULL REFERENCES public.st_scenarios(scenario_id),
    pnl_usd     FLOAT,
    pnl_pct     FLOAT,
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, as_of_date, scenario_id)
);

CREATE TABLE IF NOT EXISTS public.db_risk_alerts (
    id         SERIAL      PRIMARY KEY,
    account_id INT         NOT NULL,
    as_of_date DATE        NOT NULL,
    seq        SMALLINT    NOT NULL,
    msg        TEXT        NOT NULL,
    level      VARCHAR(16) NOT NULL,
    updated_at TIMESTAMP   NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, as_of_date, seq)
);

CREATE TABLE IF NOT EXISTS public.proc_asof_date (
    as_of_date DATE      NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.demo_request (
    id          SERIAL       PRIMARY KEY,
    first_name  VARCHAR(100) NOT NULL,
    last_name   VARCHAR(100) NOT NULL,
    email       VARCHAR(120) NOT NULL,
    company     VARCHAR(100) NOT NULL,
    aum         VARCHAR(50)  NULL,
    interest    VARCHAR(100) NULL,
    message     TEXT         NULL,
    status      VARCHAR(20)  NOT NULL DEFAULT 'new',
    create_date TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.benchmark (
    benchmark_id          SERIAL       PRIMARY KEY,
    benchmark_name        VARCHAR(100) NOT NULL,
    description           TEXT,
    source_provider       VARCHAR(100),
    currency              VARCHAR(10),
    rebalancing_frequency VARCHAR(20),
    is_custom             BOOLEAN      NOT NULL DEFAULT FALSE,
    create_date           DATE         NOT NULL DEFAULT current_date,
    security_id           VARCHAR(20)  NULL,
    is_active             BOOLEAN      NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS public.benchmark_weights (
    id           SERIAL       PRIMARY KEY,
    benchmark_id INT          NOT NULL REFERENCES public.benchmark(benchmark_id) ON DELETE CASCADE,
    asset_class  VARCHAR(50)  NOT NULL,
    weight       NUMERIC(5,4) NOT NULL,
    security_id  VARCHAR(20)  NULL,
    CONSTRAINT chk_weight_range         CHECK (weight >= 0 AND weight <= 1),
    CONSTRAINT uq_benchmark_asset_class UNIQUE (benchmark_id, asset_class)
);

CREATE TABLE IF NOT EXISTS public.account_benchmark (
    id            SERIAL PRIMARY KEY,
    account_id    INT    NOT NULL REFERENCES public.account(account_id),
    benchmark_id  INT    NOT NULL REFERENCES public.benchmark(benchmark_id),
    assigned_date DATE   NOT NULL DEFAULT current_date,
    CONSTRAINT uq_account_benchmark UNIQUE (account_id)
);

CREATE TABLE IF NOT EXISTS public.benchmark_hist (
    benchmark_id INT   NOT NULL REFERENCES public.benchmark(benchmark_id),
    date         DATE  NOT NULL,
    value        FLOAT,
    CONSTRAINT uq_benchmark_hist UNIQUE (benchmark_id, date)
);

CREATE TABLE IF NOT EXISTS public.beta_definition (
    beta_key       VARCHAR(50)  PRIMARY KEY,
    benchmark_id   VARCHAR(20)  NOT NULL,
    benchmark_name VARCHAR(100) NOT NULL,
    lookback_days  INT          NOT NULL DEFAULT 252,
    return_type    VARCHAR(10)  NOT NULL DEFAULT 'SIMPLE',
    min_obs        INT          NOT NULL DEFAULT 100,
    description    TEXT
);

CREATE TABLE IF NOT EXISTS public.sec_beta (
    security_id  VARCHAR(20) NOT NULL,
    beta_key     VARCHAR(50) NOT NULL REFERENCES public.beta_definition(beta_key),
    benchmark_id VARCHAR(20) NOT NULL,
    beta         FLOAT,
    r_squared    FLOAT,
    vol          FLOAT,
    obs_count    INT,
    start_date   DATE,
    end_date     DATE,
    calc_date    DATE,
    PRIMARY KEY (security_id, beta_key)
);

CREATE TABLE IF NOT EXISTS public.illiquid_model_parameters (
    security_id    VARCHAR(20) PRIMARY KEY,
    category       VARCHAR(50) NULL,
    historical_vol NUMERIC     NULL,
    liquidity_adj  NUMERIC     NULL,
    correlation    NUMERIC     NULL,
    beta           NUMERIC     NULL,
    sigma          NUMERIC     NULL
);

CREATE TABLE IF NOT EXISTS public.sn_replica (
    security_id           VARCHAR(20) NOT NULL,
    position_id           VARCHAR(20) NOT NULL,
    security_type         VARCHAR(50) NOT NULL,
    note_notional         NUMERIC     NOT NULL,
    notional              NUMERIC     NOT NULL,
    quantity              NUMERIC     NOT NULL,
    maturity              DATE        NOT NULL,
    coupon_rate           NUMERIC     NULL,
    trigger_level         NUMERIC     NULL,
    strike                NUMERIC     NULL,
    underlying            VARCHAR(20) NOT NULL,
    underlying_sec_id     VARCHAR(20) NOT NULL,
    init_underlying_price NUMERIC     NULL,
    init_price_date       DATE        NULL,
    PRIMARY KEY (security_id, position_id)
);

CREATE TABLE IF NOT EXISTS public.sn_replica_calc (
    id               SERIAL      PRIMARY KEY,
    security_id      VARCHAR(20) NOT NULL,
    as_of_date       DATE        NOT NULL,
    position_id      VARCHAR(20) NOT NULL,
    underlying_price NUMERIC     NULL,
    tenor            NUMERIC     NULL,
    risk_free_rate   NUMERIC     NULL,
    iv               NUMERIC     NULL,
    value            NUMERIC     NULL,
    delta            NUMERIC     NULL,
    gamma            NUMERIC     NULL,
    UNIQUE (security_id, as_of_date, position_id)
);

CREATE TABLE IF NOT EXISTS public.slide_shocks (
    id         SERIAL      PRIMARY KEY,
    slide_name VARCHAR(50) NOT NULL,
    shock      NUMERIC     NOT NULL,
    UNIQUE (slide_name, shock)
);

-- ---------------------------------------------------------------
-- Tables migrated from database/postgres_tables.sql (2026-09-21)
-- Tables already superseded by newer definitions above (client, account,
-- portfolio_info, port_positions, port_parameters, current_security) or
-- renamed/redesigned (security_attribute, limit_var, limit_concentration,
-- private_equity, account_run_parameters) were intentionally left out.
-- ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.account_positions (
	id serial4 NOT NULL,
	account_id int4 NOT NULL,
	position_id varchar(20) NOT NULL,
	security_name varchar(200) NULL,
	isin varchar(20) NULL,
	cusip varchar(20) NULL,
	ticker varchar(20) NULL,
	quantity numeric NOT NULL,
	market_value numeric NOT NULL,
	asset_class varchar(50) NULL,
	currency varchar(20) NULL,
	insert_time date NULL DEFAULT CURRENT_DATE,
	CONSTRAINT account_positions_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.upload_security (
	upload_id SERIAL PRIMARY KEY,
	upload_name varchar(100) NOT NULL,
	filename varchar(100) NULL,
	result_filename varchar(100) NULL,
	err_filename varchar(100) NULL,
	status varchar(20) NULL,
	message varchar(200) NULL,
	created_by varchar(50) NULL,
	created_user_id int4 NULL,
	create_date TIMESTAMP default NOW()
);

CREATE TABLE IF NOT EXISTS public.yh_stock_profile (
	ticker varchar(20) NULL,
	address1 varchar(500) NULL,
	city varchar(50) NULL,
	"state" varchar(50) NULL,
	zip varchar(50) NULL,
	country varchar(50) NULL,
	phone varchar(20) NULL,
	website varchar(500) NULL,
	industry varchar(500) NULL,
	industrykey varchar(500) NULL,
	industrydisp varchar(500) NULL,
	sector varchar(500) NULL,
	sectorkey varchar(500) NULL,
	sectordisp varchar(500) NULL,
	longbusinesssummary varchar(10000) NULL,
	fulltimeemployees numeric NULL,
	auditrisk numeric NULL,
	boardrisk numeric NULL,
	compensationrisk numeric NULL,
	shareholderrightsrisk numeric NULL,
	governanceepochdate numeric NULL,
	compensationasofepochdate numeric NULL,
	irwebsite varchar(500) NULL,
	maxage numeric NULL,
	overallrisk numeric NULL
);

CREATE TABLE IF NOT EXISTS public.yh_stock_price (
	ticker varchar(20) NULL,
	"date" date NULL,
	"open" numeric NULL,
	high numeric NULL,
	low numeric NULL,
	"close" numeric NULL,
	volume numeric NULL
);

CREATE TABLE IF NOT EXISTS public.yh_stock_dividend (
    symbol               varchar(100) NOT NULL,
    "companyName"        varchar(100) NULL,
    "dividend_Ex_Date"   date NOT NULL,
    "payment_Date"       date NULL,
    "record_Date"        date NULL,
    "dividend_Rate"      numeric NOT NULL,
    "indicated_Annual_Dividend"   numeric NULL,
    "announcement_Date"  date NULL,
    CONSTRAINT unique_symbol_date UNIQUE (symbol, "dividend_Ex_Date")
);

CREATE TABLE IF NOT EXISTS public.current_price (
	"SecurityID"	 varchar(20) NOT NULL,
	"Ticker"         varchar(20) NOT NULL,
	"Date"           date NOT NULL,
	"Open"           numeric NULL,
	"High"           numeric NULL,
	"Low"            numeric NULL,
	"Close"          numeric NULL,
	"Volume"         numeric NULL,
	"PriceTime"      timestamp NOT NULL,
	CONSTRAINT unique_price_entry UNIQUE ("SecurityID", "Date")
);

CREATE TABLE IF NOT EXISTS public.risk_model (
    model_id         SERIAL PRIMARY KEY,
	model_name       varchar(50) NOT NULL,
	description      varchar(200) NOT NULL,
	is_current       INTEGER DEFAULT 0,
	create_date      timestamp default NOW()
);

CREATE TABLE IF NOT EXISTS public.risk_factor (
    model_id         INTEGER NOT NULL,
	"SecurityID"	 varchar(20) NOT NULL,
	"Category"       varchar(20) NOT NULL,
	"RF_ID"          varchar(20) NOT NULL,
	"Sensitivity"    numeric NULL,
	CONSTRAINT unique_entry UNIQUE (model_id, "SecurityID", "Category")
);

CREATE TABLE IF NOT EXISTS public.parameters (
    param_id SERIAL PRIMARY KEY,
	param_name   varchar(100) NOT NULL,
	str_value    varchar(100) NULL,
	date_value   date NULL,
	float_value  float NULL
);

-- Note: source file defined mkt_data_price twice, the second copy missing
-- a table name (syntax error) -- only the valid definition is kept here.
CREATE TABLE IF NOT EXISTS public.mkt_data_price (
	security_id varchar(20) NOT NULL,
	price_date  date NOT NULL,
	price       numeric    NULL,
    CONSTRAINT unique_mkt_data_price UNIQUE (security_id, price_date)
);
CREATE INDEX IF NOT EXISTS idx_security_id ON public.mkt_data_price (security_id);

-- Note: source file had a stray double comma after "Source" (syntax error), fixed here.
CREATE TABLE IF NOT EXISTS public.mkt_data_source (
    id               SERIAL PRIMARY KEY,
    "SecurityID"	 varchar(20)	NOT NULL,
    "SecurityName"	 varchar(200)	NOT NULL,
    "Source"	     varchar(20)	NOT NULL,
    "SourceID"	     varchar(50)	NOT NULL,
    is_active	     int          	NOT NULL,
    update_time      timestamp default NOW()
);

CREATE TABLE IF NOT EXISTS public.dividend (
    id           SERIAL PRIMARY KEY,
    ticker     	 varchar(20)	NOT NULL,
    ex_date      date NOT NULL,
    amount       float NULL
);

CREATE TABLE IF NOT EXISTS public.risk_limit_level (
	id serial4 NOT NULL,
	risk_type varchar(20) NOT NULL,
	category varchar(20) NULL,
	low float4 NULL,
	mid float4 NULL,
	high float4 NULL
);

-- ---------------------------------------------------------------
-- Remaining tables that exist in the live postgres database
-- (trg-input-database) but had no definition in this file.
-- Pulled from information_schema on 2026-09-21. Includes legacy
-- tables (account_run_parameters, limit_var, limit_concentration,
-- security_attribute, private_equity, stat_private_equity) that
-- are still present live alongside their newer replacements.
-- ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.account_run_parameters (
    id int4 DEFAULT nextval('account_run_parameters_id_seq'::regclass) NOT NULL,
    account_id int4 NOT NULL,
    "PortfolioName" varchar(100) NOT NULL,
    "RiskHorizon" varchar(20) NULL,
    "TailMeasure" varchar(20) NULL,
    "ReturnFrequency" varchar(20) NULL,
    "Benchmark" varchar(50) NULL,
    "ExpectedReturn" varchar(20) NULL,
    "BaseCurrency" varchar(20) NULL,
    insert_date date DEFAULT CURRENT_DATE NULL,
    "AsofDate" date NULL,
    "ReportDate" date NULL,
    CONSTRAINT account_run_parameters_pkey PRIMARY KEY (id),
    CONSTRAINT unique_account_id UNIQUE (account_id)
);

CREATE TABLE IF NOT EXISTS public.adhoc_temp (
    id int4 DEFAULT nextval('adhoc_temp_id_seq'::regclass) NOT NULL,
    batch varchar(50) NULL,
    char_value varchar(200) NULL,
    float_value float8 NULL,
    date_value date NULL,
    CONSTRAINT adhoc_temp_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.approval (
    id int4 NOT NULL,
    status varchar(20) NULL,
    CONSTRAINT approval_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.broker_asset_class_map (
    broker text NOT NULL,
    security_code text NOT NULL,
    asset_class text NULL,
    CONSTRAINT broker_asset_class_map_pkey PRIMARY KEY (broker, security_code)
);

CREATE TABLE IF NOT EXISTS public.class_expect_return (
    "Class" varchar(50) NULL,
    "SC1" varchar(50) NULL,
    "ExpectedReturn" float4 NULL
);

CREATE TABLE IF NOT EXISTS public.class_taxonomy (
    "Class" varchar(50) NULL,
    "SC1" varchar(50) NULL,
    "SC2" varchar(50) NULL
);

CREATE TABLE IF NOT EXISTS public.country_region (
    "Country" varchar(50) NULL,
    "Region" varchar(50) NULL,
    "UpdateDate" date NULL
);

CREATE TABLE IF NOT EXISTS public.fiae (
    "ID" varchar(50) NULL,
    "SecurityName" varchar(64) NULL,
    "ISIN" varchar(50) NULL,
    "Cusip" varchar(50) NULL,
    "Ticker" varchar(64) NULL,
    "Quantity" numeric NULL,
    "Market Value" numeric NULL,
    "Asset Class" varchar(50) NULL,
    "Currency" varchar(50) NULL,
    "Column10" varchar(50) NULL,
    "Column11" varchar(50) NULL
);

CREATE TABLE IF NOT EXISTS public.fiae_portfolio (
    "SecurityID" varchar(50) NULL,
    "SecurityName" varchar(64) NULL,
    "Account" varchar(50) NULL,
    "Broker" varchar(50) NULL,
    "FIAE_ID" varchar(50) NULL,
    "ISIN" varchar(50) NULL,
    "Ticker" varchar(64) NULL,
    "Quantity" numeric NULL,
    "MarketValue" varchar(50) NULL,
    "Weight" varchar(50) NULL,
    "LastPrice" varchar(50) NULL,
    "LastPriceDate" varchar(50) NULL,
    "AssetReturnClass" varchar(50) NULL,
    "ExpectedReturn" varchar(50) NULL,
    "Class" varchar(50) NULL,
    "SC1" varchar(50) NULL,
    "SC2" varchar(50) NULL,
    "Country" varchar(50) NULL,
    "Region" varchar(50) NULL,
    "Sector" varchar(50) NULL,
    "Industry" varchar(50) NULL,
    "Currency" varchar(50) NULL,
    "Option Type" varchar(50) NULL,
    "Coupon Rate" varchar(50) NULL,
    "Maturity Date" varchar(50) NULL,
    "CUSIP" varchar(50) NULL,
    "Underlying Security ID" varchar(50) NULL,
    "Frequency Months" varchar(50) NULL,
    "OptionStrike" varchar(50) NULL
);

CREATE TABLE IF NOT EXISTS public.figi_lookup (
    id int4 DEFAULT nextval('figi_lookup_id_seq'::regclass) NOT NULL,
    security_id varchar(20) NULL,
    name varchar(255) NULL,
    ticker varchar(50) NULL,
    exch varchar(10) NULL,
    isin varchar(12) NULL,
    cusip varchar(9) NULL,
    sedol varchar(7) NULL,
    figi varchar(12) NULL,
    comp_figi varchar(12) NULL,
    shareclass_figi varchar(12) NULL,
    sectype varchar(100) NULL,
    sectype2 varchar(100) NULL,
    mkt_sector varchar(50) NULL,
    created_at timestamptz DEFAULT now() NULL,
    update_at timestamptz DEFAULT now() NULL,
    CONSTRAINT figi_lookup_pkey PRIMARY KEY (id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uix_figi_lookup_figi ON public.figi_lookup USING btree (figi);

CREATE TABLE IF NOT EXISTS public.fund_maturity (
    "SecurityID" varchar(50) NULL,
    "Maturity" varchar(50) NULL,
    "Weight" float4 NULL,
    "UpdateDate" date NULL
);

CREATE TABLE IF NOT EXISTS public.fund_rating (
    "SecurityID" varchar(50) NULL,
    "Rating" varchar(50) NULL,
    "Weight" float4 NULL,
    "UpdateDate" date NULL
);

CREATE TABLE IF NOT EXISTS public.fund_regions (
    "SecurityID" varchar(50) NULL,
    "Region" varchar(50) NULL,
    "Weight" float4 NULL,
    "UpdateDate" date NULL
);

CREATE TABLE IF NOT EXISTS public.fund_sectors (
    "SecurityID" varchar(50) NULL,
    "Sector" varchar(50) NULL,
    "Weight" float4 NULL,
    "UpdateDate" date NULL
);

CREATE TABLE IF NOT EXISTS public.limit_concentration (
    port_group_id int4 NOT NULL,
    category varchar(20) NOT NULL,
    limit_value float4 NULL
);

CREATE TABLE IF NOT EXISTS public.limit_var (
    port_group_id int4 NOT NULL,
    risk_type varchar(20) NOT NULL,
    low float4 NULL,
    mid1 float4 NULL,
    mid2 float4 NULL,
    high float4 NULL
);

CREATE TABLE IF NOT EXISTS public.mkt_data_info (
    id int4 DEFAULT nextval('mkt_data_info_id_seq'::regclass) NOT NULL,
    "SecurityID" varchar(20) NULL,
    "Category" varchar(20) NULL,
    "SecurityName" varchar(1000) NULL,
    "AssetClass" varchar(20) NULL,
    "AssetType" varchar(20) NULL,
    "DataSource" varchar(20) NULL,
    "StartDate" date NOT NULL,
    "EndDate" date NOT NULL,
    "Length" int4 NULL,
    "MaxValue" float8 NULL,
    "MinValue" float8 NULL,
    "AverageValue" float8 NULL,
    "StdValue" float8 NULL,
    "LastUpdate" date DEFAULT CURRENT_DATE NULL,
    CONSTRAINT mkt_data_info_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.mssb_price (
    feed_date date NOT NULL,
    routing_code text NULL,
    product_id text NULL,
    symbol_cusip text NULL,
    price1 numeric NULL,
    option_symbol text NULL,
    price_last_date date NULL,
    cusip text NULL,
    price2 numeric NULL,
    blank text NULL
);

CREATE TABLE IF NOT EXISTS public.mssb_sec_code (
    security_code text NOT NULL,
    asset_class text NULL,
    asset_type text NULL,
    updated_at timestamp DEFAULT now() NOT NULL
);

CREATE TABLE IF NOT EXISTS public.mssb_secty (
    feed_date date NOT NULL,
    sec_routing_code text NULL,
    sec_cusip text NULL,
    sec_symbol text NULL,
    sec_description text NULL,
    sec_underly_cusip text NULL,
    sec_sedol text NULL,
    sec_isin text NULL,
    sec_sp_rating text NULL,
    sec_moody_rating text NULL,
    sec_close_price numeric NULL,
    sec_current_face numeric NULL,
    sec_original_face numeric NULL,
    sec_curr_mbs_factor numeric NULL,
    sec_curr_mbs_factor_date date NULL,
    sec_currency text NULL,
    sec_issue_date date NULL,
    sec_dated_date date NULL,
    sec_pay_date date NULL,
    sec_mat_exp_date date NULL,
    sec_day_count text NULL,
    sec_pay_frequency text NULL,
    sec_state_code text NULL,
    sec_country_code text NULL,
    sec_exchange_code text NULL,
    sec_tax_code text NULL,
    sec_maturity_type text NULL,
    sec_ex_div_date date NULL,
    sec_div_record_date date NULL,
    sec_cde_security text NULL,
    sec_interest_rate numeric NULL,
    sec_share_multiplier numeric NULL,
    sec_product_id text NULL,
    sec_last_price_date date NULL,
    sec_retrict_flag text NULL,
    sec_inst_type text NULL,
    bond_type text NULL,
    bond_status_ind text NULL,
    alt_sec_ind text NULL
);

CREATE TABLE IF NOT EXISTS public.mssb_taxlot (
    feed_date date NOT NULL,
    routing_code text NULL,
    account text NULL,
    cusip text NULL,
    symbol text NULL,
    security_type text NULL,
    security_description text NULL,
    tax_lot_qty numeric NULL,
    tax_lot_price numeric NULL,
    total_cost numeric NULL,
    trade_date date NULL,
    settle_date date NULL,
    adjusted_trade_date date NULL,
    buy_sell_indicator text NULL,
    isin text NULL,
    filler text NULL,
    tax_lot_cost numeric NULL,
    cost_adjusted numeric NULL,
    original_face numeric NULL,
    open_sequence_number text NULL,
    source_indicator text NULL,
    unreal_gain_loss numeric NULL,
    close_method text NULL,
    product_id text NULL,
    underlying_shares numeric NULL,
    alternate_security_indicator text NULL,
    wash_adjusted_indicator text NULL,
    inheritance_gifted_indicator text NULL,
    irs_covered_indicator text NULL,
    adjusted_trade_date_due_to_wash date NULL,
    adjusted_cost_after_wash numeric NULL,
    wash_adjusted_amount numeric NULL
);

CREATE TABLE IF NOT EXISTS public.mssb_trans (
    feed_date date NOT NULL,
    routing_code text NULL,
    account text NULL,
    cusip text NULL,
    security_description text NULL,
    tran_code text NULL,
    tran_date date NULL,
    trade_date date NULL,
    settle_date date NULL,
    quantity numeric NULL,
    price numeric NULL,
    accrued_interest numeric NULL,
    other_fee_base numeric NULL,
    other_fee_local numeric NULL,
    comm_base numeric NULL,
    comm_local numeric NULL,
    total_amount numeric NULL,
    broker text NULL,
    fx_rate numeric NULL,
    secondary_fee_base numeric NULL,
    secondary_fee_local numeric NULL,
    original_face numeric NULL,
    factor numeric NULL,
    coupon numeric NULL,
    issue_date date NULL,
    maturity_date date NULL,
    current_order text NULL,
    previous_order text NULL,
    cancel_indicator text NULL,
    buy_sell_indicator text NULL,
    symbol text NULL,
    exchange text NULL,
    security_code text NULL,
    security_no text NULL,
    postage_amount numeric NULL,
    foreign_tax numeric NULL,
    fc_number text NULL,
    broker_code1_4 text NULL,
    broker_code_5_6 text NULL,
    broker_code_7 text NULL,
    sedol text NULL,
    isin text NULL,
    principal numeric NULL,
    confirm_trailer text NULL,
    filler text NULL,
    vsp_code text NULL,
    alternate_transaction_code text NULL,
    trade_date_1 date NULL,
    sb_alpha_tran_code text NULL,
    sb_source_destination text NULL,
    solicited_or_non_solicited text NULL,
    sub_category_code text NULL,
    blank_1 text NULL,
    new_symbol_field text NULL,
    new_security_type text NULL,
    vsp_date_2 text NULL,
    vsp_price_2 numeric NULL,
    vsp_qnty_2 numeric NULL,
    alternate_security_indicator text NULL,
    blank_2 text NULL
);

CREATE TABLE IF NOT EXISTS public.pbi_current_report_url (
    id int4 DEFAULT nextval('pbi_current_report_url_id_seq'::regclass) NOT NULL,
    report_url varchar(200) NOT NULL,
    version varchar(20) NULL,
    is_active int4 NULL,
    create_date date NOT NULL,
    CONSTRAINT pbi_current_report_url_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.pbi_report_url (
    id int4 DEFAULT nextval('pbi_report_url_id_seq'::regclass) NOT NULL,
    client_id int4 NOT NULL,
    pgroup_id int4 NULL,
    report_url varchar(200) NOT NULL,
    is_active int4 NULL,
    create_date date NULL,
    CONSTRAINT pbi_report_url_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.port_limit (
    limit_category varchar(100) NULL,
    limit_value numeric NULL,
    port_id int4 NULL
);

CREATE TABLE IF NOT EXISTS public.portfolio_group (
    pgroup_id int4 DEFAULT nextval('portfolio_group_pgroup_id_seq'::regclass) NOT NULL,
    client_id int4 NOT NULL,
    group_name varchar(100) NULL,
    create_date date NOT NULL,
    CONSTRAINT portfolio_group_pkey PRIMARY KEY (pgroup_id),
    CONSTRAINT portfolio_group_client_id_fkey FOREIGN KEY (client_id) REFERENCES public.client(client_id)
);

CREATE TABLE IF NOT EXISTS public.private_equity (
    model_id varchar(20) NOT NULL,
    security_id varchar(20) NOT NULL,
    security_name varchar(200) NULL,
    benchmark varchar(200) NULL,
    proxy varchar(200) NULL,
    proxy_id varchar(20) NULL,
    correlation numeric NULL,
    beta numeric NULL,
    simga numeric NULL,
    proxy_vol numeric NULL,
    hist_vol numeric NULL,
    adj_vol numeric NULL,
    liquidity_factor numeric NULL,
    tail_shock numeric NULL
);
CREATE INDEX IF NOT EXISTS idx_private_equity_id ON public.private_equity USING btree (model_id, security_id);

CREATE TABLE IF NOT EXISTS public.risk_horizon_days (
    risk_horizon varchar(20) NOT NULL,
    days int4 NOT NULL
);

CREATE TABLE IF NOT EXISTS public.roles (
    id int4 NOT NULL,
    role varchar(20) NULL,
    CONSTRAINT roles_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.sch_processes (
    id varchar(64) NOT NULL,
    name varchar(255) NOT NULL,
    description text NULL,
    script_path text NOT NULL,
    script_type varchar(16) NOT NULL,
    schedule_time varchar(5) NULL,
    dependencies jsonb DEFAULT '[]'::jsonb NULL,
    max_retries int4 DEFAULT 2 NULL,
    retry_delay_seconds int4 DEFAULT 60 NULL,
    enabled bool DEFAULT true NULL,
    created_at timestamptz DEFAULT now() NULL,
    updated_at timestamptz DEFAULT now() NULL,
    venv_path varchar(512) NULL,
    CONSTRAINT sch_processes_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.sch_config (
    key varchar(64) NOT NULL,
    value text NOT NULL,
    updated_at timestamptz DEFAULT now() NULL,
    CONSTRAINT sch_config_pkey PRIMARY KEY (key)
);

CREATE TABLE IF NOT EXISTS public.sch_daily_runs (
    id int4 DEFAULT nextval('sch_daily_runs_id_seq'::regclass) NOT NULL,
    process_id varchar(64) NOT NULL,
    run_date date NOT NULL,
    status varchar(16) DEFAULT 'waiting'::character varying NOT NULL,
    attempts int4 DEFAULT 0 NULL,
    blocked_by jsonb DEFAULT '[]'::jsonb NULL,
    start_time timestamptz NULL,
    end_time timestamptz NULL,
    CONSTRAINT sch_daily_runs_pkey PRIMARY KEY (id),
    CONSTRAINT sch_daily_runs_process_id_run_date_key UNIQUE (process_id, run_date),
    CONSTRAINT sch_daily_runs_process_id_fkey FOREIGN KEY (process_id) REFERENCES public.sch_processes(id)
);
CREATE INDEX IF NOT EXISTS idx_daily_runs_date ON public.sch_daily_runs USING btree (run_date);
CREATE INDEX IF NOT EXISTS idx_daily_runs_status ON public.sch_daily_runs USING btree (status);

CREATE TABLE IF NOT EXISTS public.sch_run_attempts (
    id int4 DEFAULT nextval('sch_run_attempts_id_seq'::regclass) NOT NULL,
    process_id varchar(64) NOT NULL,
    run_date date NOT NULL,
    attempt_number int4 NOT NULL,
    triggered_by varchar(16) DEFAULT 'scheduler'::character varying NULL,
    start_time timestamptz NOT NULL,
    end_time timestamptz NULL,
    status varchar(16) NOT NULL,
    exit_code int4 NULL,
    stdout text NULL,
    stderr text NULL,
    CONSTRAINT sch_run_attempts_pkey PRIMARY KEY (id),
    CONSTRAINT sch_run_attempts_process_id_fkey FOREIGN KEY (process_id) REFERENCES public.sch_processes(id)
);
CREATE INDEX IF NOT EXISTS idx_run_attempts_proc_date ON public.sch_run_attempts USING btree (process_id, run_date);

CREATE TABLE IF NOT EXISTS public.security_attribute (
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
    CONSTRAINT security_attribute_pkey PRIMARY KEY (security_id)
);

CREATE TABLE IF NOT EXISTS public.security_sectors (
    "SecurityID" varchar(50) NULL,
    "Sector" varchar(50) NULL,
    "Industry" varchar(50) NULL,
    "Country" varchar(50) NULL,
    "UpdateDate" date NULL
);

CREATE TABLE IF NOT EXISTS public.security_xref_deleted (
    id int4 DEFAULT nextval('security_xref_deleted_id_seq'::regclass) NOT NULL,
    "REF_ID" varchar(200) NOT NULL,
    "REF_TYPE" varchar(20) NOT NULL,
    "SecurityID" varchar(20) NULL,
    "DataSource" varchar(100) NULL,
    "DateDeleted" date DEFAULT CURRENT_DATE NOT NULL,
    "ExchCode" varchar(10) NULL,
    CONSTRAINT security_xref_deleted_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.stat_private_equity (
    "SecurityID" varchar(50) NULL,
    "SecurityName" varchar(64) NULL,
    "Ticker" varchar(50) NULL,
    "SEDOL" varchar(50) NULL,
    "CUSIP" varchar(50) NULL,
    "ISIN" varchar(50) NULL,
    "Currency" varchar(50) NULL,
    "AssetClass" varchar(50) NULL,
    "AssetType" varchar(50) NULL,
    "Benchmark" varchar(50) NULL,
    "Proxy" varchar(50) NULL,
    "ProxySymbol" varchar(50) NULL,
    "ProxyCorr" float4 NULL,
    "MonthlyVol" float4 NULL,
    "ProxyBeta" float4 NULL,
    "ResVol" float4 NULL,
    hist_vol float4 NULL,
    proxy_vol float4 NULL,
    hist_beta float4 NULL,
    hist_r_sq float4 NULL,
    sim_vol float4 NULL,
    "Liquidity Adjusted" float4 NULL,
    "Tail_Shock" float4 NULL,
    report_id int4 NULL
);

CREATE TABLE IF NOT EXISTS public.user_entitilement (
    id int4 DEFAULT nextval('user_entitilement_id_seq'::regclass) NOT NULL,
    user_id int4 NOT NULL,
    port_group_id int4 NOT NULL,
    permission varchar(20) NULL,
    update_date date NOT NULL,
    CONSTRAINT user_entitilement_pkey PRIMARY KEY (id),
    CONSTRAINT user_entitilement_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.user(user_id),
    CONSTRAINT user_entitilement_port_group_id_fkey FOREIGN KEY (port_group_id) REFERENCES public.portfolio_group(pgroup_id)
);

CREATE TABLE IF NOT EXISTS public.user_report_mapping_table (
    id int4 DEFAULT nextval('user_report_mapping_table_id_seq'::regclass) NOT NULL,
    client_id int4 NULL,
    pgroup_id int4 NULL,
    report_id varchar(20) NOT NULL,
    email varchar(120) NOT NULL,
    report_name varchar(200) NULL,
    CONSTRAINT user_report_mapping_table_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.user_temp (
    user_id int4 NULL,
    username varchar(120) NULL,
    email varchar(120) NULL,
    password varchar(60) NULL,
    approval int4 NULL,
    phone varchar(20) NULL,
    client_id int4 NULL,
    role varchar(20) NULL,
    create_date date NULL,
    firstname varchar(100) NULL,
    lastname varchar(100) NULL,
    activation_completed bool NULL
);

CREATE TABLE IF NOT EXISTS public.yh_price_stats (
    ticker varchar(20) NULL,
    min_date date NULL,
    max_date date NULL,
    count int8 NULL
);

CREATE TABLE IF NOT EXISTS public.yh_tickers (
    ticker varchar(20) NOT NULL,
    securitytype varchar(20) NOT NULL,
    securityname varchar(500) NULL,
    CONSTRAINT yh_tickers_pkey PRIMARY KEY (ticker)
);

CREATE TABLE IF NOT EXISTS public.yh_tickers_ids (
    ticker text NOT NULL,
    cusip text NULL,
    isin text NULL,
    cik text NULL,
    found_in_filing text NULL,
    filing_date date NULL,
    updated_at timestamptz DEFAULT now() NULL,
    CONSTRAINT yh_tickers_ids_pkey PRIMARY KEY (ticker)
);
