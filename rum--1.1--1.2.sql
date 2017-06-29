/*
 * RUM version 1.2
 */

/*--------------------anyarray-----------------------*/

CREATE FUNCTION rum_anyarray_config(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION rum_anyarray_similar(anyarray,anyarray)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT STABLE;

CREATE OPERATOR % (
	PROCEDURE = rum_anyarray_similar,
	LEFTARG = anyarray,
	RIGHTARG = anyarray,
	COMMUTATOR = '%',
	RESTRICT = contsel,
	JOIN = contjoinsel
);


CREATE OR REPLACE FUNCTION rum_anyarray_distance(anyarray,anyarray)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT STABLE;

CREATE OPERATOR <=> (
	PROCEDURE = rum_anyarray_distance,
	LEFTARG = anyarray,
	RIGHTARG = anyarray,
	COMMUTATOR = '<=>'
);


CREATE FUNCTION rum_extract_anyarray(anyarray,internal,internal,internal,internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION rum_extract_anyarray_query(anyarray,internal,smallint,internal,internal,internal,internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION rum_anyarray_consistent(internal, smallint, anyarray, integer, internal, internal, internal, internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION rum_anyarray_ordering(internal,smallint,anyarray,int,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


CREATE OPERATOR CLASS rum_anyarray_ops
DEFAULT FOR TYPE anyarray USING rum
AS
	OPERATOR	1	&&  (anyarray, anyarray),
	OPERATOR	2	@>  (anyarray, anyarray),
	OPERATOR	3	<@  (anyarray, anyarray),
	OPERATOR	4	=   (anyarray, anyarray),
	OPERATOR	5	%   (anyarray, anyarray),
	OPERATOR	20	<=> (anyarray, anyarray) FOR ORDER BY pg_catalog.float_ops,
	--dispatch function 1 for concrete type
	FUNCTION	2	rum_extract_anyarray(anyarray,internal,internal,internal,internal),
	FUNCTION	3	rum_extract_anyarray_query(anyarray,internal,smallint,internal,internal,internal,internal),
	FUNCTION	4	rum_anyarray_consistent(internal,smallint,anyarray,integer,internal,internal,internal,internal),
	FUNCTION	6	rum_anyarray_config(internal),
	FUNCTION	8	rum_anyarray_ordering(internal,smallint,anyarray,int,internal,internal,internal,internal,internal),
	STORAGE anyelement;

CREATE OPERATOR CLASS rum_anyarray_addon_ops
FOR TYPE anyarray USING rum
AS
	OPERATOR	1	&& (anyarray, anyarray),
	OPERATOR	2	@> (anyarray, anyarray),
	OPERATOR	3	<@ (anyarray, anyarray),
	OPERATOR	4	=  (anyarray, anyarray),
	--dispatch function 1 for concrete type
	FUNCTION	2	ginarrayextract(anyarray,internal,internal),
	FUNCTION	3	ginqueryarrayextract(anyarray,internal,smallint,internal,internal,internal,internal),
	FUNCTION	4	ginarrayconsistent(internal,smallint,anyarray,integer,internal,internal,internal,internal),
	STORAGE anyelement;

/*--------------------int2-----------------------*/

CREATE FUNCTION rum_int2_key_distance(int2, int2, smallint)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_int2_ops USING rum ADD
	FUNCTION	8	(int2,int2) rum_int2_key_distance(int2, int2, smallint);

/*--------------------int4-----------------------*/

CREATE FUNCTION rum_int4_key_distance(int4, int4, smallint)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_int4_ops USING rum ADD
	FUNCTION	8	(int4,int4) rum_int4_key_distance(int4, int4, smallint);

/*--------------------int8-----------------------*/

CREATE FUNCTION rum_int8_key_distance(int8, int8, smallint)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_int8_ops USING rum ADD
	FUNCTION	8	(int8,int8) rum_int8_key_distance(int8, int8, smallint);

/*--------------------float4-----------------------*/

CREATE FUNCTION rum_float4_key_distance(float4, float4, smallint)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_float4_ops USING rum ADD
	FUNCTION	8	(float4,float4) rum_float4_key_distance(float4, float4, smallint);

/*--------------------float8-----------------------*/

CREATE FUNCTION rum_float8_key_distance(float8, float8, smallint)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_float8_ops USING rum ADD
	FUNCTION	8	(float8,float8) rum_float8_key_distance(float8, float8, smallint);

/*--------------------money-----------------------*/

CREATE FUNCTION rum_money_key_distance(money, money, smallint)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_money_ops USING rum ADD
	FUNCTION	8	(money,money) rum_money_key_distance(money, money, smallint);

/*--------------------oid-----------------------*/

CREATE FUNCTION rum_oid_key_distance(oid, oid, smallint)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_oid_ops USING rum ADD
	FUNCTION	8	(oid,oid) rum_oid_key_distance(oid, oid, smallint);

/*--------------------timestamp-----------------------*/

CREATE FUNCTION rum_timestamp_key_distance(timestamp, timestamp, smallint)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_timestamp_ops USING rum ADD
	FUNCTION	8	(timestamp,timestamp) rum_timestamp_key_distance(timestamp, timestamp, smallint);

/*--------------------timestamptz-----------------------*/

CREATE FUNCTION rum_timestamptz_key_distance(timestamptz, timestamptz, smallint)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_timestamptz_ops USING rum ADD
	FUNCTION	8	(timestamptz,timestamptz) rum_timestamptz_key_distance(timestamptz, timestamptz, smallint);

