/*
 * RUM version 1.2
 */

ALTER OPERATOR FAMILY rum_tsvector_ops USING rum DROP
	FUNCTION	8	(internal,smallint,tsvector,int,internal,internal,internal,internal,internal);

ALTER OPERATOR FAMILY rum_tsvector_hash_ops USING rum DROP
	FUNCTION	8	(internal,smallint,tsvector,int,internal,internal,internal,internal,internal);

CREATE OR REPLACE FUNCTION rum_tsquery_distance(internal,smallint,internal,smallint,tsvector,int,internal,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

ALTER OPERATOR FAMILY rum_tsvector_ops USING rum ADD
	FUNCTION	8	rum_tsquery_distance(internal,smallint,internal,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

ALTER OPERATOR FAMILY rum_tsvector_hash_ops USING rum ADD
	FUNCTION	8	rum_tsquery_distance(internal,smallint,internal,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

/*--------------------int2-----------------------*/

CREATE FUNCTION rum_int2_key_distance(internal,smallint,int2,smallint,tsvector,int,internal,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_int2_ops USING rum ADD
	FUNCTION	8	rum_int2_key_distance(internal,smallint,int2,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

/*--------------------int4-----------------------*/

CREATE FUNCTION rum_int4_key_distance(internal,smallint,int4,smallint,tsvector,int,internal,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_int4_ops USING rum ADD
	FUNCTION	8	rum_int4_key_distance(internal,smallint,int4,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

/*--------------------int8-----------------------*/

CREATE FUNCTION rum_int8_key_distance(internal,smallint,int8,smallint,tsvector,int,internal,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_int8_ops USING rum ADD
	FUNCTION	8	rum_int8_key_distance(internal,smallint,int8,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

/*--------------------float4-----------------------*/

CREATE FUNCTION rum_float4_key_distance(internal,smallint,float4,smallint,tsvector,int,internal,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_float4_ops USING rum ADD
	FUNCTION	8	rum_float4_key_distance(internal,smallint,float4,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

/*--------------------float8-----------------------*/

CREATE FUNCTION rum_float8_key_distance(internal,smallint,float8,smallint,tsvector,int,internal,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_float8_ops USING rum ADD
	FUNCTION	8	rum_float8_key_distance(internal,smallint,float8,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

/*--------------------money-----------------------*/

CREATE FUNCTION rum_money_key_distance(internal,smallint,money,smallint,tsvector,int,internal,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_money_ops USING rum ADD
	FUNCTION	8	rum_money_key_distance(internal,smallint,money,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

/*--------------------oid-----------------------*/

CREATE FUNCTION rum_oid_key_distance(internal,smallint,oid,smallint,tsvector,int,internal,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_oid_ops USING rum ADD
	FUNCTION	8	rum_oid_key_distance(internal,smallint,oid,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

