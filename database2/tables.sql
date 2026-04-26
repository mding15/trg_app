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

CREATE TABLE public.account_limit_history (
	id serial4 NOT NULL,
	account_id int4 NOT NULL,
	limit_category varchar(50) NOT NULL,
	limit_value numeric NULL,
	valid_from timestamp NOT NULL,
	archived_at timestamp DEFAULT now() NOT NULL
);

