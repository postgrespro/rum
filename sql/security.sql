-- Check security CVE-2020-14350
CREATE FUNCTION rum_anyarray_similar(anyarray,anyarray) RETURNS bool AS $$ SELECT false $$ LANGUAGE SQL;
CREATE EXTENSION rum;
DROP FUNCTION rum_anyarray_similar(anyarray,anyarray);

