-- DROP PROCEDURE public.updatesecurityinfo(int4);

CREATE OR REPLACE PROCEDURE public.updatesecurityinfo(IN in_port_id integer)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Update SecurityID using ISIN
    UPDATE port_positions pp
    SET "SecurityID" = sx."SecurityID"
    FROM security_xref sx
    WHERE pp.port_id = in_port_id
    AND pp."SecurityID" IS NULL
    AND pp."ISIN" = sx."REF_ID" 
    AND sx."REF_TYPE" = 'ISIN';

    -- Update SecurityID using CUSIP (where SecurityID is NULL)
    UPDATE port_positions pp
    SET "SecurityID" = sx."SecurityID"
    FROM security_xref sx
    WHERE pp.port_id = in_port_id 
    AND pp."SecurityID" IS NULL
    AND pp."CUSIP" = sx."REF_ID" 
    AND sx."REF_TYPE" = 'CUSIP';

    -- Update SecurityID using Ticker (where SecurityID is NULL)
    UPDATE port_positions pp
    SET "SecurityID" = sx."SecurityID"
    FROM security_xref sx
    WHERE pp.port_id = in_port_id 
    AND pp."SecurityID" IS NULL
    AND pp."Ticker" = sx."REF_ID" 
    AND sx."REF_TYPE" = 'Ticker';


	-- update unknown_security
	update port_positions
	set unknown_security =  true 
	where port_id = in_port_id and "SecurityID" is null;

	-- check if security is modeled
	update port_positions pp
	set unknown_security = true
	where pp.port_id = in_port_id and pp."SecurityID" not in 
	(	select rf."SecurityID" from risk_factor rf, risk_model rm where rf.model_id = rm.model_id and rm.is_current=1
		union 
		select si."SecurityID" from security_info si where si."AssetType" = 'Treasury'
	);

	-- update asset_class, asset_type
	update port_positions pp
	set asset_class = si."AssetClass", asset_type = si."AssetType"  
	from security_info si 
	where pp.port_id = in_port_id 
	and pp."SecurityID" = si."SecurityID" ;


	-- update other security attributes
	update port_positions pp
	set "ExpectedReturn" = sa.expected_return	,
		"Currency" = sa.currency	,
		"Class" = sa."class"	,
		"SC1" = sa.sc1	,
		"SC2" = sa.sc2	,
		"Country" = sa.country	,
		"Region" = sa.region	,
		"Sector" = sa.sector	,
		"Industry" = sa.industry	,
		"OptionType" = sa.option_type	,
		"PaymentFrequency" = sa.payment_frequency	,
		"MaturityDate" = sa.maturity_date	,
		"OptionStrike" = sa.option_strike	,
		"UnderlyingSecurityID" = sa.underlying_security_id	,
		"CouponRate" = sa.coupon_rate
	from security_attribute sa 
	where pp.port_id = in_port_id 
	and pp."SecurityID" = sa.security_id ;

	-- exclude maturity < AsOfDate
	UPDATE port_positions pp
	set unknown_security =  true 
	from port_parameters pm
	where pp.port_id = in_port_id 
	and pp.port_id = pm.port_id 
	and pp."MaturityDate" < pm."AsofDate";


	-- update options
	update port_positions
	set is_option = true 
	where port_id = in_port_id
	and "OptionType" in ('Call', 'Put');

END;
$procedure$
;
