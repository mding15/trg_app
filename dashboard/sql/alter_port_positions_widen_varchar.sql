-- Migration: widen varchar columns in port_positions to text
-- Matches position_var where all equivalent columns are text.
ALTER TABLE public.port_positions
    ALTER COLUMN "SecurityID"         TYPE text,
    ALTER COLUMN "SecurityName"       TYPE text,
    ALTER COLUMN "ISIN"               TYPE text,
    ALTER COLUMN "CUSIP"              TYPE text,
    ALTER COLUMN "Ticker"             TYPE text,
    ALTER COLUMN "userAssetClass"     TYPE text,
    ALTER COLUMN "userCurrency"       TYPE text,
    ALTER COLUMN "Currency"           TYPE text,
    ALTER COLUMN "Class"              TYPE text,
    ALTER COLUMN "SC1"                TYPE text,
    ALTER COLUMN "SC2"                TYPE text,
    ALTER COLUMN "Country"            TYPE text,
    ALTER COLUMN "Region"             TYPE text,
    ALTER COLUMN "Sector"             TYPE text,
    ALTER COLUMN "Industry"           TYPE text,
    ALTER COLUMN "OptionType"         TYPE text,
    ALTER COLUMN "UnderlyingSecurityID" TYPE text,
    ALTER COLUMN "UnderlyingID"       TYPE text,
    ALTER COLUMN asset_class          TYPE text,
    ALTER COLUMN asset_type           TYPE text;
