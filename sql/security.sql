/*
 * ------------------------------------
 *  NOTE: This test behaves differenly
 * ------------------------------------
 *
 * Since 774171c4f64 (>=18) reporting of errors in extension script files
 * is more detailed.
 *
 * security.out - test output for PostgreSQL (<18)
 * security_1.out - test output for PostgreSQL (>=18)
 *
 */

-- Check security CVE-2020-14350
CREATE FUNCTION rum_anyarray_similar(anyarray,anyarray) RETURNS bool AS $$ SELECT false $$ LANGUAGE SQL;
CREATE EXTENSION rum;
DROP FUNCTION rum_anyarray_similar(anyarray,anyarray);

