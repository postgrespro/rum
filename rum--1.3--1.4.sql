/*
 * RUM version 1.4
 */
DROP OPERATOR CLASS rum_anyarray_addon_ops USING rum CASCADE; 
DROP OPERATOR CLASS rum_anyarray_ops USING rum CASCADE; 
DROP FUNCTION rum_anyarray_ordering(internal,smallint,anyarray,int,internal,internal,internal,internal,internal);
DROP FUNCTION rum_anyarray_consistent(internal, smallint, anyarray, integer, internal, internal, internal, internal);
DROP FUNCTION rum_extract_anyarray_query(anyarray,internal,smallint,internal,internal,internal,internal);
DROP FUNCTION rum_extract_anyarray(anyarray,internal,internal,internal,internal);
DROP OPERATOR <=> (anyarray,anyarray) CASCADE;
DROP FUNCTION rum_anyarray_distance(anyarray,anyarray);
DROP OPERATOR % (anyarray,anyarray) CASCADE;
DROP FUNCTION rum_anyarray_similar(anyarray,anyarray);
DROP FUNCTION rum_anyarray_config(internal);
