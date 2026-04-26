CREATE TABLE public.account_parameters (
	id serial4 NOT NULL,
	account_id int4 NOT NULL,
	risk_horizon varchar(20) NULL,
	risk_measure varchar(20) NULL,
	base_currency varchar(20) NULL,
	beta_key varchar(20) NULL,
	updated_at timestamp DEFAULT now() NOT NULL,
	benchmark varchar(20) NULL,
	exp_return varchar(20) NULL,
	gauge_measure varchar(20) NULL
);
