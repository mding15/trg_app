---------------------------------------------------------------------------------------------------------
--
-- public.user_report_mapping_view source

CREATE OR REPLACE VIEW public.user_report_mapping_view
AS WITH reports AS (
         SELECT pi.report_id,
            pi.port_group_id AS pgroup_id,
            pg.client_id,
            concat(pi.report_id, ' - ', pi.port_name) AS report_name
           FROM portfolio_info pi,
            portfolio_group pg
          WHERE pi.report_id::text <> ''::text AND pi.port_group_id = pg.pgroup_id
        )
 SELECT DISTINCT r.client_id,
    r.pgroup_id,
    r.report_id,
    u.webdashboard_login AS email,
    r.report_name
   FROM reports r,
    user_entitilement ue,
    "user" u
  WHERE r.pgroup_id = ue.port_group_id AND ue.user_id = u.user_id AND ue.permission::text = 'view'::text
UNION
 SELECT r.client_id,
    r.pgroup_id,
    r.report_id,
    u.email,
    r.report_name
   FROM reports r,
    "user" u
  WHERE r.client_id = u.client_id AND u.role::text = 'admin'::text
UNION
 SELECT r.client_id,
    r.pgroup_id,
    r.report_id,
    u.email,
    r.report_name
   FROM reports r,
    "user" u
  WHERE u.role::text = 'superadmin'::text
UNION
 SELECT 0 AS client_id,
    0 AS pgroup_id,
    '100'::character varying AS report_id,
    u.email,
    'Demo'::text AS report_name
   FROM "user" u;
  
-------------------------------------------------------------------------------------------------------------------- 
--
-- security_info_view
--

CREATE OR REPLACE VIEW security_info_view
as
select si."SecurityID", si."SecurityName", si."Currency", si."AssetClass", si."AssetType", 
	x1."REF_ID" as "ISIN", x2."REF_ID" as "CUSIP", x4."REF_ID" as "BB_UNIQUE", x5."REF_ID" as "BB_GLOBAL", x3."REF_ID" as "Ticker" 
from security_info si
	left join security_xref x1 on si."SecurityID" = x1."SecurityID" and x1."REF_TYPE" = 'ISIN'
	left join security_xref x2 on si."SecurityID" = x2."SecurityID" and x2."REF_TYPE" = 'CUSIP' 
	left join security_xref x3 on si."SecurityID" = x3."SecurityID" and x3."REF_TYPE" = 'Ticker'
	left join security_xref x4 on si."SecurityID" = x4."SecurityID" and x4."REF_TYPE" = 'BB_UNIQUE'
	left join security_xref x5 on si."SecurityID" = x5."SecurityID" and x5."REF_TYPE" = 'BB_GLOBAL'


