/*
 * RUM version 1.2
 */

/*--------------------tsvector-----------------------*/

DROP OPERATOR CLASS rum_tsvector_ops USING rum;
DROP OPERATOR CLASS rum_tsvector_hash_ops USING rum;

CREATE OR REPLACE FUNCTION rum_tsquery_distance(internal,smallint,internal,smallint,tsvector,int,internal,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR CLASS rum_tsvector_ops
DEFAULT FOR TYPE tsvector USING rum
AS
        OPERATOR        1       @@ (tsvector, tsquery),
        OPERATOR        2       <=> (tsvector, tsquery) FOR ORDER BY pg_catalog.float_ops,
        FUNCTION        1       gin_cmp_tslexeme(text, text),
        FUNCTION        2       rum_extract_tsvector(tsvector,internal,internal,internal,internal),
        FUNCTION        3       rum_extract_tsquery(tsquery,internal,smallint,internal,internal,internal,internal),
        FUNCTION        4       rum_tsquery_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
        FUNCTION        5       gin_cmp_prefix(text,text,smallint,internal),
        FUNCTION        6       rum_tsvector_config(internal),
        FUNCTION        7       rum_tsquery_pre_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
        FUNCTION        8       rum_tsquery_distance(internal,smallint,internal,smallint,tsvector,int,internal,internal,internal,internal,internal,internal),
        FUNCTION        10      rum_ts_join_pos(internal, internal),
        STORAGE         text;

CREATE OPERATOR CLASS rum_tsvector_hash_ops
FOR TYPE tsvector USING rum
AS
        OPERATOR        1       @@ (tsvector, tsquery),
        OPERATOR        2       <=> (tsvector, tsquery) FOR ORDER BY pg_catalog.float_ops,
        FUNCTION        1       btint4cmp(integer, integer),
        FUNCTION        2       rum_extract_tsvector_hash(tsvector,internal,internal,internal,internal),
        FUNCTION        3       rum_extract_tsquery_hash(tsquery,internal,smallint,internal,internal,internal,internal),
        FUNCTION        4       rum_tsquery_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
        FUNCTION        6       rum_tsvector_config(internal),
        FUNCTION        7       rum_tsquery_pre_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
        FUNCTION        8       rum_tsquery_distance(internal,smallint,internal,smallint,tsvector,int,internal,internal,internal,internal,internal,internal),
        FUNCTION        10      rum_ts_join_pos(internal, internal),
        STORAGE         integer;

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

CREATE FUNCTION rum_anyarray_ordering(internal,smallint,internal,smallint,tsvector,int,internal,internal,internal,internal,internal)
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
	FUNCTION	8	rum_anyarray_ordering(internal,smallint,internal,smallint,tsvector,int,internal,internal,internal,internal,internal),
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

CREATE FUNCTION rum_int2_key_distance(internal,smallint,int2,smallint,tsvector,int,internal,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_int2_ops USING rum ADD
	FUNCTION	8	(int2,int2) rum_int2_key_distance(internal,smallint,int2,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

/*--------------------int4-----------------------*/

CREATE FUNCTION rum_int4_key_distance(internal,smallint,int4,smallint,tsvector,int,internal,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_int4_ops USING rum ADD
	FUNCTION	8	(int4,int4) rum_int4_key_distance(internal,smallint,int4,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

/*--------------------int8-----------------------*/

CREATE FUNCTION rum_int8_key_distance(internal,smallint,int8,smallint,tsvector,int,internal,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_int8_ops USING rum ADD
	FUNCTION	8	(int8,int8) rum_int8_key_distance(internal,smallint,int8,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

/*--------------------float4-----------------------*/

CREATE FUNCTION rum_float4_key_distance(internal,smallint,float4,smallint,tsvector,int,internal,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_float4_ops USING rum ADD
	FUNCTION	8	(float4,float4) rum_float4_key_distance(internal,smallint,float4,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

/*--------------------float8-----------------------*/

CREATE FUNCTION rum_float8_key_distance(internal,smallint,float8,smallint,tsvector,int,internal,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_float8_ops USING rum ADD
	FUNCTION	8	(float8,float8) rum_float8_key_distance(internal,smallint,float8,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

/*--------------------money-----------------------*/

CREATE FUNCTION rum_money_key_distance(internal,smallint,money,smallint,tsvector,int,internal,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_money_ops USING rum ADD
	FUNCTION	8	(money,money) rum_money_key_distance(internal,smallint,money,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

/*--------------------oid-----------------------*/

CREATE FUNCTION rum_oid_key_distance(internal,smallint,oid,smallint,tsvector,int,internal,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


ALTER OPERATOR FAMILY rum_oid_ops USING rum ADD
	FUNCTION	8	(oid,oid) rum_oid_key_distance(internal,smallint,oid,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

