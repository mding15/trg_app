--
-- account
--

CREATE TABLE public.account  (
	account_id SERIAL PRIMARY KEY,
	account_name varchar(120) NOT NULL,
	owner_id int not null,
	client_id int not null,
	create_time TIMESTAMP default NOW(),
	-- Scheduler fields for track over time
	next_run_time timestamp NULL
);

--
-- account_positions
--

CREATE TABLE public.account_positions (
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

--
-- account_run_parameters
--

CREATE TABLE public.account_run_parameters (
	id SERIAL PRIMARY KEY,
	account_id	int not null,
	"PortfolioName" varchar(100) NOT NULL,
	"RiskHorizon" varchar(20) NULL,
	"TailMeasure" varchar(20) NULL,
	"ReturnFrequency" varchar(20) NULL,
	"Benchmark" varchar(50) NULL,
	"ExpectedReturn" varchar(20) NULL,
	"BaseCurrency" varchar(20) null,
	insert_date date DEFAULT current_date,
	"AsofDate" date NULL,
	"ReportDate" date NULL,
	
	CONSTRAINT unique_account_id UNIQUE (account_id)
	
);	

--
-- portfolio_info
--
CREATE TABLE public.portfolio_info (
	port_id serial4 NOT NULL,
	port_name varchar(100) NOT NULL,
	filename varchar(100) NOT NULL,
	status varchar(20) NULL,
	report_id varchar(20) NULL,
	created_by varchar(50) NULL,
	create_date date NOT NULL,
	update_date date NOT NULL,
	message varchar(200) NULL,
	port_group_id int4 NOT NULL,
	as_of_date date NULL,
	market_value numeric NULL,
	tail_measure varchar(20) NULL,
	risk_horizon varchar(20) NULL,
	benchmark varchar(100) NULL,
	created_user_id int4 NULL,
	CONSTRAINT portfolio_info_pkey PRIMARY KEY (port_id),
	CONSTRAINT portfolio_info_port_group_id_fkey FOREIGN KEY (port_group_id) REFERENCES public.portfolio_group(pgroup_id)
);

--
-- upload_security
--
CREATE TABLE public.upload_security (
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


--
-- yh_stock_profile
--

CREATE TABLE public.yh_stock_profile (
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

--
-- yh_stock_price
--

CREATE TABLE public.yh_stock_price (
	ticker varchar(20) NULL,
	"date" date NULL,
	"open" numeric NULL,
	high numeric NULL,
	low numeric NULL,
	"close" numeric NULL,
	volume numeric NULL
);

--
-- yh_stock_dividend
--

CREATE TABLE public.yh_stock_dividend (
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


--
-- current_security
--

CREATE TABLE public.current_security (

    "SecurityID"	 varchar(20)	PRIMARY KEY,
    "SecurityName"	 varchar(200)	NULL,
    "Currency"	     varchar(20)	NULL,
    "AssetClass"	 varchar(20)	NOT NULL,
    "AssetType"	     varchar(20)	NOT NULL,
    "ISIN"	         varchar(20)	NULL	,
    "CUSIP"	         varchar(20)	NULL	,
    "BB_UNIQUE"	     varchar(20)	NULL	,
    "BB_GLOBAL"	     varchar(20)	NULL	,
    "Ticker"	     varchar(20)	NULL	,
    "DataSource"     varchar(20)	NULL	
);


--
-- current_price
--

CREATE TABLE public.current_price (
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


--
-- risk_model
--
CREATE TABLE public.risk_model (
    model_id         SERIAL PRIMARY KEY,
	model_name       varchar(50) NOT NULL,
	description      varchar(200) NOT NULL,
	is_current       INTEGER DEFAULT 0,
	create_date      timestamp default NOW()
)


--
-- risk_factor
--

CREATE TABLE public.risk_factor (
    model_id         INTEGER NOT NULL,
	"SecurityID"	 varchar(20) NOT NULL,
	"Category"       varchar(20) NOT NULL,
	"RF_ID"          varchar(20) NOT NULL,
	"Sensitivity"    numeric NULL,
	
	CONSTRAINT unique_entry UNIQUE (model_id, "SecurityID", "Category")
);

--
-- proc_dates
--

CREATE TABLE public.parameters (
    param_id SERIAL PRIMARY KEY,
	param_name   varchar(100) NOT NULL,
	str_value    varchar(100) NULL,
	date_value   date NULL,
	float_value  float NULL
);

--
-- port_parameters
--
CREATE TABLE port_parameters (
	port_id				int		        NOT NULL	,
	"PortfolioName"		varchar(100)	NOT NULL	,
	"AsofDate"			date			NOT NULL	,
	"ReportDate"		date		NULL	,
	"RiskHorizon"		varchar(20)	NULL	,
	"TailMeasure"		varchar(20)	NULL	,
	"ReturnFrequency"	varchar(20)	NULL	,
	"Benchmark"			varchar(50)	NULL	,
	"ExpectedReturn"	varchar(20)	NULL	,
	"BaseCurrency"		varchar(20)	NULL	
	
);
CREATE INDEX idx_port_id ON port_parameters ("port_id")
--
-- port_positions
--

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
	asset_type varchar(50) NULL
);
CREATE INDEX idx_port_positions_port_id ON public.port_positions USING btree (port_id);

--
-- security_attribute
--

-- DROP TABLE public.security_attribute;

CREATE TABLE public.security_attribute (
	security_id varchar(20) NULL,
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


-- DROP TABLE public.mkt_data_price;

CREATE TABLE mkt_data_price (
	security_id varchar(20) NOT NULL,
	price_date  date NOT NULL,
	price       numeric    NULL,
	
    CONSTRAINT unique_mkt_data_price UNIQUE (security_id, price_date)
);
create index idx_security_id on mkt_data_price (security_id)

CREATE TABLE  (
	security_id varchar(20) NOT NULL,
	price_date  date NOT NULL,
	price       numeric    NULL,
	
    CONSTRAINT unique_mkt_data_price UNIQUE (security_id, price_date)
);
create index idx_security_id on mkt_data_price (security_id)



CREATE TABLE public.limit_var (
	port_group_id int NOT NULL,
	risk_type varchar(20) NOT NULL,
	low float4 NULL,
	mid1 float4 NULL,
	mid2 float4 NULL,
	high float4 NULL
);
-- default value
insert into limit_var values (0, 'VaR',	0.0, 0.17, 0.34, 0.51)
insert into limit_var values (0, 'Vol',	0.0, 0.06, 0.12, 0.18)


CREATE TABLE public.limit_concentration (
	port_group_id int NOT NULL,
	category varchar(20) NOT NULL,
	limit_value float4 NULL
);
-- default values
insert into limit_concentration (port_group_id, category, limit_value) values (0, 'Single Name', 0.1);
insert into limit_concentration (port_group_id, category, limit_value) values (0, 'Region', 0.35);
insert into limit_concentration (port_group_id, category, limit_value) values (0, 'Asset Class', 0.4);
insert into limit_concentration (port_group_id, category, limit_value) values (0, 'Industry', 0.5);
insert into limit_concentration (port_group_id, category, limit_value) values (0, 'Currency', 0.7);



--
-- mkt_data_source
--

CREATE TABLE public.mkt_data_source (

    id               SERIAL PRIMARY KEY,
    "SecurityID"	 varchar(20)	NOT NULL,
    "SecurityName"	 varchar(200)	NOT NULL,
    "Source"	     varchar(20)	NOT NULL,,
    "SourceID"	     varchar(50)	NOT NULL,
    is_active	     int          	NOT NULL,
    update_time      timestamp default NOW()
);


--
-- dividend
--
CREATE TABLE public.dividend (
    id           SERIAL PRIMARY KEY,
    ticker     	 varchar(20)	NOT NULL,
    ex_date      date NOT NULL,
    amount       float NULL
);


--
-- private_equity
--
-- DROP TABLE public.private_equity;

CREATE TABLE public.private_equity (
    model_id varchar(20) not NULL,
	security_id varchar(20) not NULL,
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
CREATE INDEX idx_private_equity_id ON public.private_equity USING btree (model_id, security_id);

									

---------------------------------------------------------------------------------------

-- update mkt_data_price from yh_stock_price
with start_date as (
	select cs."SecurityID", max(mp.price_date) as max_date
	from mkt_data_price mp, current_security cs 
	where cs."SecurityID" = mp.security_id 
	and cs."DataSource" = 'YH'
	group by cs."SecurityID" )
insert into mkt_data_price (security_id, price_date, price)
select cs."SecurityID", yp."date", yp."close"  
from current_security cs, start_date sd, yh_stock_price yp
where cs."DataSource" = 'YH' and cs."Ticker" = yp.ticker 
and cs."SecurityID"  = sd."SecurityID"
and yp."date" > sd.max_date

-- update mkt_data_price from current_price
with start_date as (
	select cs."SecurityID", max(mp.price_date) as max_date
	from mkt_data_price mp, current_security cs 
	where cs."SecurityID" = mp.security_id 
	and cs."DataSource" = 'YH'
	group by cs."SecurityID" )
insert into mkt_data_price (security_id, price_date, price)
select cs."SecurityID", cp."Date", cp."Close"  
from current_security cs, start_date sd, current_price cp 
where cs."DataSource" = 'YH' and cs."Ticker" = cp."Ticker"  
and cs."SecurityID"  = sd."SecurityID"
and cp."Date" > sd.max_date

------------------------------------------------------------------------------------------
--
-- risk_limit_level
--
CREATE TABLE public.risk_limit_level (
	id serial4 NOT NULL,
	risk_type varchar(20) NOT NULL,
	category varchar(20) NULL, 
	low float4 NULL,
	mid float4 NULL,
	high float4 NULL
);