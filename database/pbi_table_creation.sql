CREATE TABLE dm_report_id (
	report_id int NOT NULL,
	creator nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	report_description nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	create_date date DEFAULT getdate() NULL,
	insert_time datetime DEFAULT getdate() NULL,
	CONSTRAINT PK__dm_repo_report_id PRIMARY KEY (report_id)
);


-------------------------------------------------------------------------------
-- DROP TABLE tailriskglobal.dbo.dm_dim_d_Region;

CREATE TABLE tailriskglobal.dbo.dm_dim_d_Broker (
	broker varchar(50) Not NULL,
	report_id int Not NULL,
	insert_time datetime DEFAULT getdate() NULL,
);

-------------------------------------------------------------------------------
CREATE TABLE ods_Positions (
    Error varchar(50),
    SecurityName varchar(50),
    ISIN varchar(50),
    CUSIP varchar(50),
    Ticker varchar(50),
    Quantity float,
    MarketValue float,
    Weight float,
    ExpectedReturn float,
    AssetClass varchar(50),
    AssetType varchar(50),
    LastPrice float,
    LastPriceDate date,
    Currency varchar(50),
    Country varchar(50),
    Region varchar(50),
    Sector varchar(50),
    Industry varchar(50),
    OptionType varchar(50),
    FrequencyMonths float,
    MaturityDate date,
    OptionStrike float,
    UnderlyingSecurityID varchar(50),
    CouponRate float,
    Class varchar(50),
    SC1 varchar(50),
    SC2 varchar(50),
    report_id int,
    bdp_id varchar(128)
)

CREATE TABLE ods_Class2 (
    Class varchar(50),
    VaR float,
    Mg_VaR_to_Top_D float,
    Mg_VaR_to_Top_P float,
    Annual_Volatility float,
    Mg_Vol_to_Top_P float,
    report_id int,
    bdp_id varchar(128)
)

CREATE TABLE ods_Country (
    Country varchar(50),
    Region varchar(50)
)

CREATE TABLE ods_Currency (
    Currency varchar(50)
)

CREATE TABLE ods_DB_Risk (
    Type varchar(50),
    Vol_P float,
    VaR_P float,
    Exp_Ret float,
    SR_Vol float,
    SR_VaR float,
    VaR_D float,
    report_id int,
    bdp_id varchar(128)
)

CREATE TABLE ods_DimPositions (
    Security_ID varchar(50),
    Class varchar(50),
    SC1 varchar(50),
    SC2 varchar(50),
    Currency varchar(50),
    Country varchar(50),
    Region varchar(50),
    Industry varchar(50),
    report_id int,
    bdp_id varchar(128)
)

CREATE TABLE ods_Fact_Bmk (
    Date date,
    Portfolio float,
    Benchmark float,
    report_id int,
    bdp_id varchar(128)
)

CREATE TABLE ods_Fact_MgPositions (
    Security_ID varchar(50),
    VaR float,
    Mg_VaR_to_Top_D float,
    Mg_VaR_to_Top_P float,
    Annual_Volatility float,
    Mg_Vol_to_Top_P float,
    MV_Weight float,
    Ticker varchar(50),
    report_id int,
    bdp_id varchar(128)
)

CREATE TABLE ods_GICS (
    Sector varchar(50),
    Industry varchar(50)
)

CREATE TABLE ods_Region (
    Region varchar(50)
)

CREATE TABLE ods_Sector (
    Sector varchar(50)
)

CREATE TABLE ods_Top_Hedges (
    Security_ID varchar(50),
    Ticker varchar(50),
    MV_Weight float,
    Volatility float,
    VaR float,
    Exp_Ret float,
    Vol_Contrib float,
    VaR_Contrib float,
    Return_Contrib float,
    report_id int,
    bdp_id varchar(128)
)

CREATE TABLE ods_Top_Risks (
    Security_ID varchar(50),
    Ticker varchar(50),
    MV_Weight float,
    Volatility float,
    VaR float,
    Exp_Ret float,
    Vol_Contrib float,
    VaR_Contrib float,
    Return_Contrib float,
    report_id int,
    bdp_id varchar(128),
    securityid varchar(100)
)

CREATE TABLE ods_SC1 (
    Class varchar(50),
    SC1 varchar(50)
)

CREATE TABLE ods_SC2 (
    Class varchar(50),
    SC1 varchar(50),
    SC2 varchar(50)
)

CREATE TABLE ods_Fact_Positions (
    Security_ID varchar(50),
    Security_Name varchar(50),
    Market_Value float,
    MV_Weight float,
    Vol float,
    VaR float,
    Exp_Ret float,
    SR_Vol float,
    SR_VaR float,
    Class varchar(50),
    SC1 varchar(50),
    SC2 varchar(50),
    Currency varchar(50),
    Country varchar(50),
    Region varchar(50),
    Industry varchar(50),
    IR_Duration float,
    IR_Convexity float,
    Spread_Duration float,
    Spread_Convexity float,
    Delta float,
    Gamma float,
    Vega float,
    Implied_Vol float,
    PD varchar(50),
    Delta_VaR float,
    Gamma_VaR float,
    Vega_VaR float,
    IR_Duration_VaR float,
    IR_Convexity_VaR varchar(50),
    Spread_Duration_VaR float,
    Spread_Convexity_VaR varchar(50),
    Default_VaR varchar(50),
    Rating varchar(50),
    Skewness varchar(50),
    Tail_Fatness varchar(50),
    Proxy varchar(50),
    Proxy_Corr float,
    Liquidity_Adjusted float,
    Ticker varchar(50),
    report_id int,
    bdp_id varchar(128)
)

CREATE TABLE ods_SC12 (
    Class varchar(50),
    SC1 varchar(50),
    VaR float,
    Mg_VaR_to_Top_D float,
    Mg_VaR_to_Top_P float,
    Annual_Volatility float,
    Mg_Vol_to_Top_P float,
    report_id int,
    bdp_id varchar(128)
)

CREATE TABLE ods_SC22 (
    Class varchar(50),
    SC1 varchar(50),
    SC2 varchar(50),
    VaR float,
    Mg_VaR_to_Top_D float,
    Mg_VaR_to_Top_P float,
    Annual_Volatility float,
    Mg_Vol_to_Top_P float,
    report_id int,
    bdp_id varchar(128)
)

CREATE TABLE ods_Fact_Parameters (
	Portfolio_Name varchar(50),
	Portfolio_Size_mm float,
	Report_Date date,
	As_of_Date date,
	Var_Vol_Window varchar(50),
	Return_Frequency varchar(50),
	Tail_Measure varchar(50),
	Benchmarks varchar(50),
	Expected_Returns varchar(50),
	report_id int,
	bdp_id varchar(128)
)

CREATE TABLE tailriskglobal.dbo.dm_port_consolidated (
	security_name varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	security_id varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	isin varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	cusip varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	ticker varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	class varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	sc1 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	sc2 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	country varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	region varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	sector varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	industry varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	last_amount numeric(38,0) NULL,
	as_of_date date NULL,
	quantity float NULL,
	market_value float NULL,
	change_percentage float NULL,
	change_amount float NULL,
	total_cost float NULL,
	adjusted_cost float NULL,
	unrealized_gain_loss_percentage float NULL,
	unrealized_gain_loss_amount float NULL,
	report_id int NULL,
	insert_time date DEFAULT getdate() NULL,
	bdp_id varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
);

CREATE   TRIGGER update_positions_id ON tailriskglobal.dbo.dm_d_Positions
AFTER INSERT
AS
begin
    SET NOCOUNT ON;
    UPDATE P
    SET 
        P.bdp_id = CONCAT(P.Security_ID, P.report_id),
        P.SC2_bdp_id = CONCAT(P.Class, P.SC1, P.SC2, P.report_id),
        P.Currency_bdp_id = CONCAT(P.Currency, P.report_id),
        P.Industry_bdp_id = CONCAT(P.Industry, P.report_id),
        P.Country_bdp_id = CONCAT(P.Country, P.Region, P.report_id)
    FROM dm_d_Positions P
    WHERE P.report_id IN (SELECT DISTINCT report_id FROM INSERTED);
end;

CREATE   TRIGGER update_factdpositions_bdpid ON tailriskglobal.dbo.dm_fact_d_Positions
AFTER INSERT
AS
begin
	SET NOCOUNT ON;

	DECLARE @report_id INT;
	SELECT TOP 1 @report_id = report_id FROM INSERTED;

	UPDATE dm_fact_d_Positions
	set bdp_id = CONCAT(Security_ID, report_id)
	WHERE report_id = @report_id
end;

CREATE   TRIGGER update_factdmgpositions_bdpid ON tailriskglobal.dbo.dm_fact_d_MgPositions
AFTER INSERT
AS
begin
	SET NOCOUNT ON;
	DECLARE @report_id INT;
	SELECT TOP 1 @report_id = report_id FROM INSERTED;

	update dm_fact_d_MgPositions set bdp_id = CONCAT(Security_ID,report_id) WHERE report_id = @report_id
end
;

--
-- delete Report Data
--
CREATE PROCEDURE DeleteReportData
    @ReportId INT
AS
BEGIN
    SET NOCOUNT ON;
   
	DELETE FROM dm_fact_d_Positions WHERE report_id = @ReportId;
	DELETE FROM dm_fact_d_MgPositions WHERE report_id = @ReportId;
	DELETE FROM dm_fact_d_Parameters WHERE report_id = @ReportId;
	DELETE FROM dm_fact_d_AggTable WHERE report_id = @ReportId;
	DELETE FROM dm_d_Positions WHERE report_id = @ReportId;
	DELETE FROM dm_d_Top_Risks_and_Hedges WHERE report_id = @ReportId;
	DELETE FROM dm_d_Lims_Concentration WHERE report_id = @ReportId;
	DELETE FROM dm_d_Lims_VaR WHERE report_id = @ReportId;
	DELETE FROM dm_d_Lims_Vol WHERE report_id = @ReportId;
	DELETE FROM dm_d_TS WHERE report_id = @ReportId;
	DELETE FROM dm_dim_d_Classes WHERE report_id = @ReportId;
	DELETE FROM dm_dim_d_Country WHERE report_id = @ReportId;
	DELETE FROM dm_dim_d_Currency WHERE report_id = @ReportId;
	DELETE FROM dm_dim_d_GICS WHERE report_id = @ReportId;
	DELETE FROM dm_dim_d_Industry WHERE report_id = @ReportId;
	DELETE FROM dm_dim_d_Region WHERE report_id = @ReportId;
	DELETE FROM dm_dim_d_SC1 WHERE report_id = @ReportId;
	DELETE FROM dm_dim_d_SC2 WHERE report_id = @ReportId;
	DELETE FROM dm_dim_d_SC3 WHERE report_id = @ReportId;
	DELETE FROM dm_dim_d_Sectors WHERE report_id = @ReportId;
	DELETE FROM dm_d_Unknown_Position WHERE report_id = @ReportId;    
	DELETE FROM dm_report_id WHERE report_id = @ReportId;
    DELETE FROM dm_d_concentration WHERE report_id = @ReportId;
    DELETE FROM dm_d_concentration_2 WHERE report_id = @ReportId;
    DELETE FROM dm_port_consolidated WHERE report_id = @ReportId;
    DELETE FROM dm_port_hist_value WHERE report_id = @ReportId;
    DELETE FROM dm_port_return WHERE report_id = @ReportId;


	PRINT 'Rows deleted successfully.';
END;

