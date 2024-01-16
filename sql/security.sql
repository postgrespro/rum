-- Check security CVE-2020-14350
CREATE FUNCTION rum_numeric_cmp(numeric, numeric) RETURNS int4 AS $$ SELECT 0 $$ LANGUAGE SQL;
CREATE EXTENSION rum;
DROP FUNCTION rum_numeric_cmp(numeric, numeric);

