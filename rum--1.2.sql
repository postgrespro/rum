CREATE OR REPLACE FUNCTION rumhandler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

/*
 * RUM access method
 */

CREATE ACCESS METHOD rum TYPE INDEX HANDLER rumhandler;

/*
 * RUM built-in types, operators and functions
 */

-- Type used in distance calculations with normalization argument
CREATE TYPE rum_distance_query AS (query tsquery, method int);

CREATE FUNCTION tsquery_to_distance_query(tsquery)
RETURNS rum_distance_query
AS 'MODULE_PATHNAME', 'tsquery_to_distance_query'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (tsquery AS rum_distance_query)
        WITH FUNCTION tsquery_to_distance_query(tsquery) AS IMPLICIT;

CREATE FUNCTION rum_ts_distance(tsvector,tsquery)
RETURNS float4
AS 'MODULE_PATHNAME', 'rum_ts_distance_tt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION rum_ts_distance(tsvector,tsquery,int)
RETURNS float4
AS 'MODULE_PATHNAME', 'rum_ts_distance_ttf'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION rum_ts_distance(tsvector,rum_distance_query)
RETURNS float4
AS 'MODULE_PATHNAME', 'rum_ts_distance_td'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=> (
        LEFTARG = tsvector,
        RIGHTARG = tsquery,
        PROCEDURE = rum_ts_distance
);

CREATE OPERATOR <=> (
        LEFTARG = tsvector,
        RIGHTARG = rum_distance_query,
        PROCEDURE = rum_ts_distance
);

CREATE FUNCTION rum_timestamp_distance(timestamp, timestamp)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=> (
        PROCEDURE = rum_timestamp_distance,
        LEFTARG = timestamp,
        RIGHTARG = timestamp,
        COMMUTATOR = <=>
);

CREATE FUNCTION rum_timestamp_left_distance(timestamp, timestamp)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=| (
        PROCEDURE = rum_timestamp_left_distance,
        LEFTARG = timestamp,
        RIGHTARG = timestamp,
        COMMUTATOR = |=>
);

CREATE FUNCTION rum_timestamp_right_distance(timestamp, timestamp)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR |=> (
        PROCEDURE = rum_timestamp_right_distance,
        LEFTARG = timestamp,
        RIGHTARG = timestamp,
        COMMUTATOR = <=|
);

/*
 * rum_tsvector_ops operator class
 */

CREATE FUNCTION rum_extract_tsvector(tsvector,internal,internal,internal,internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION rum_extract_tsquery(tsquery,internal,smallint,internal,internal,internal,internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION rum_tsvector_config(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION rum_tsquery_pre_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION rum_tsquery_consistent(internal, smallint, tsvector, integer, internal, internal, internal, internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION rum_tsquery_distance(internal,smallint,tsvector,int,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

-- To prevent calling from SQL
CREATE FUNCTION rum_ts_join_pos(internal, internal)
RETURNS bytea
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
        FUNCTION        8       rum_tsquery_distance(internal,smallint,tsvector,int,internal,internal,internal,internal,internal),
        FUNCTION        10      rum_ts_join_pos(internal, internal),
        STORAGE         text;

/*
 * rum_tsvector_hash_ops operator class.
 *
 * Stores hash of entries as keys in index.
 */

CREATE FUNCTION rum_extract_tsvector_hash(tsvector,internal,internal,internal,internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION rum_extract_tsquery_hash(tsquery,internal,smallint,internal,internal,internal,internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

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
        FUNCTION        8       rum_tsquery_distance(internal,smallint,tsvector,int,internal,internal,internal,internal,internal),
        FUNCTION        10      rum_ts_join_pos(internal, internal),
        STORAGE         integer;

/*
 * rum_timestamp_ops operator class
 */

-- timestamp operator class

CREATE FUNCTION rum_timestamp_extract_value(timestamp,internal,internal,internal,internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_timestamp_compare_prefix(timestamp,timestamp,smallint,internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_timestamp_config(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION rum_timestamp_extract_query(timestamp,internal,smallint,internal,internal,internal,internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_timestamp_consistent(internal,smallint,timestamp,int,internal,internal,internal,internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_timestamp_outer_distance(timestamp, timestamp, smallint)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR CLASS rum_timestamp_ops
DEFAULT FOR TYPE timestamp USING rum
AS
        OPERATOR        1       <,
        OPERATOR        2       <=,
        OPERATOR        3       =,
        OPERATOR        4       >=,
        OPERATOR        5       >,
        --support
        FUNCTION        1       timestamp_cmp(timestamp,timestamp),
        FUNCTION        2       rum_timestamp_extract_value(timestamp,internal,internal,internal,internal),
        FUNCTION        3       rum_timestamp_extract_query(timestamp,internal,smallint,internal,internal,internal,internal),
        FUNCTION        4       rum_timestamp_consistent(internal,smallint,timestamp,int,internal,internal,internal,internal),
        FUNCTION        5       rum_timestamp_compare_prefix(timestamp,timestamp,smallint,internal),
        FUNCTION        6       rum_timestamp_config(internal),
        -- support to timestamp distance in rum_tsvector_timestamp_ops
        FUNCTION        9       rum_timestamp_outer_distance(timestamp, timestamp, smallint),
        OPERATOR        20      <=> (timestamp,timestamp) FOR ORDER BY pg_catalog.float_ops,
        OPERATOR        21      <=| (timestamp,timestamp) FOR ORDER BY pg_catalog.float_ops,
        OPERATOR        22      |=> (timestamp,timestamp) FOR ORDER BY pg_catalog.float_ops,
STORAGE         timestamp;

/*
 * rum_tsvector_timestamp_ops operator class.
 *
 * Stores timestamp with tsvector.
 */

CREATE FUNCTION rum_tsquery_timestamp_consistent(internal, smallint, tsvector, integer, internal, internal, internal, internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

/*
 *	!!!deprecated, use rum_tsvector_addon_ops!!!
 */
CREATE OPERATOR CLASS rum_tsvector_timestamp_ops
FOR TYPE tsvector USING rum
AS
        OPERATOR        1       @@ (tsvector, tsquery),
        --support function
        FUNCTION        1       gin_cmp_tslexeme(text, text),
        FUNCTION        2       rum_extract_tsvector(tsvector,internal,internal,internal,internal),
        FUNCTION        3       rum_extract_tsquery(tsquery,internal,smallint,internal,internal,internal,internal),
        FUNCTION        4       rum_tsquery_timestamp_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
        FUNCTION        5       gin_cmp_prefix(text,text,smallint,internal),
        FUNCTION        7       rum_tsquery_pre_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
        STORAGE         text;

/*
 * rum_tsvector_hash_timestamp_ops operator class
 *	!!!deprecated, use rum_tsvector_hash_addon_ops!!!
 */

CREATE OPERATOR CLASS rum_tsvector_hash_timestamp_ops
FOR TYPE tsvector USING rum
AS
        OPERATOR        1       @@ (tsvector, tsquery),
        --support function
        FUNCTION        1       btint4cmp(integer, integer),
        FUNCTION        2       rum_extract_tsvector_hash(tsvector,internal,internal,internal,internal),
        FUNCTION        3       rum_extract_tsquery_hash(tsquery,internal,smallint,internal,internal,internal,internal),
        FUNCTION        4       rum_tsquery_timestamp_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
        FUNCTION        7       rum_tsquery_pre_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
        STORAGE         integer;

/*
 * rum_timestamptz_ops operator class
 */

CREATE FUNCTION rum_timestamptz_distance(timestamptz, timestamptz)
RETURNS float8
AS 'MODULE_PATHNAME', 'rum_timestamp_distance'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=> (
        PROCEDURE = rum_timestamptz_distance,
        LEFTARG = timestamptz,
        RIGHTARG = timestamptz,
        COMMUTATOR = <=>
);

CREATE FUNCTION rum_timestamptz_left_distance(timestamptz, timestamptz)
RETURNS float8
AS 'MODULE_PATHNAME', 'rum_timestamp_left_distance'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=| (
        PROCEDURE = rum_timestamptz_left_distance,
        LEFTARG = timestamptz,
        RIGHTARG = timestamptz,
        COMMUTATOR = |=>
);

CREATE FUNCTION rum_timestamptz_right_distance(timestamptz, timestamptz)
RETURNS float8
AS 'MODULE_PATHNAME', 'rum_timestamp_right_distance'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR |=> (
        PROCEDURE = rum_timestamptz_right_distance,
        LEFTARG = timestamptz,
        RIGHTARG = timestamptz,
        COMMUTATOR = <=|
);

CREATE OPERATOR CLASS rum_timestamptz_ops
DEFAULT FOR TYPE timestamptz USING rum
AS
        OPERATOR        1       <,
        OPERATOR        2       <=,
        OPERATOR        3       =,
        OPERATOR        4       >=,
        OPERATOR        5       >,
        --support
        FUNCTION        1       timestamptz_cmp(timestamptz,timestamptz),
        FUNCTION        2       rum_timestamp_extract_value(timestamp,internal,internal,internal,internal),
        FUNCTION        3       rum_timestamp_extract_query(timestamp,internal,smallint,internal,internal,internal,internal),
        FUNCTION        4       rum_timestamp_consistent(internal,smallint,timestamp,int,internal,internal,internal,internal),
        FUNCTION        5       rum_timestamp_compare_prefix(timestamp,timestamp,smallint,internal),
        FUNCTION        6       rum_timestamp_config(internal),
        -- support to timestamptz distance in rum_tsvector_timestamptz_ops
        FUNCTION        9       rum_timestamp_outer_distance(timestamp, timestamp, smallint),
        OPERATOR        20      <=> (timestamptz,timestamptz) FOR ORDER BY pg_catalog.float_ops,
        OPERATOR        21      <=| (timestamptz,timestamptz) FOR ORDER BY pg_catalog.float_ops,
        OPERATOR        22      |=> (timestamptz,timestamptz) FOR ORDER BY pg_catalog.float_ops,
STORAGE         timestamptz;

/*
 * rum_tsvector_timestamptz_ops operator class.
 *
 * Stores tsvector with timestamptz.
 */

CREATE OPERATOR CLASS rum_tsvector_timestamptz_ops
FOR TYPE tsvector USING rum
AS
        OPERATOR        1       @@ (tsvector, tsquery),
        --support function
        FUNCTION        1       gin_cmp_tslexeme(text, text),
        FUNCTION        2       rum_extract_tsvector(tsvector,internal,internal,internal,internal),
        FUNCTION        3       rum_extract_tsquery(tsquery,internal,smallint,internal,internal,internal,internal),
        FUNCTION        4       rum_tsquery_timestamp_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
        FUNCTION        5       gin_cmp_prefix(text,text,smallint,internal),
        FUNCTION        7       rum_tsquery_pre_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
        STORAGE         text;

/*
 * rum_tsvector_hash_timestamptz_ops operator class
 */

CREATE OPERATOR CLASS rum_tsvector_hash_timestamptz_ops
FOR TYPE tsvector USING rum
AS
        OPERATOR        1       @@ (tsvector, tsquery),
        --support function
        FUNCTION        1       btint4cmp(integer, integer),
        FUNCTION        2       rum_extract_tsvector_hash(tsvector,internal,internal,internal,internal),
        FUNCTION        3       rum_extract_tsquery_hash(tsquery,internal,smallint,internal,internal,internal,internal),
        FUNCTION        4       rum_tsquery_timestamp_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
        FUNCTION        7       rum_tsquery_pre_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
        STORAGE         integer;

/*
 * rum_tsquery_ops operator class.
 *
 * Used for inversed text search.
 */

CREATE FUNCTION ruminv_extract_tsquery(tsquery,internal,internal,internal,internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ruminv_extract_tsvector(tsvector,internal,smallint,internal,internal,internal,internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ruminv_tsvector_consistent(internal, smallint, tsvector, integer, internal, internal, internal, internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ruminv_tsquery_config(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR CLASS rum_tsquery_ops
DEFAULT FOR TYPE tsquery USING rum
AS
        OPERATOR        1       @@ (tsquery, tsvector),
        FUNCTION        1       gin_cmp_tslexeme(text, text),
        FUNCTION        2       ruminv_extract_tsquery(tsquery,internal,internal,internal,internal),
        FUNCTION        3       ruminv_extract_tsvector(tsvector,internal,smallint,internal,internal,internal,internal),
        FUNCTION        4       ruminv_tsvector_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
        FUNCTION        6       ruminv_tsquery_config(internal),
        STORAGE         text;
/*
 * RUM version 1.1
 */
 
CREATE FUNCTION rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

ALTER FUNCTION
	rum_tsquery_timestamp_consistent (internal,smallint,tsvector,int,internal,internal,internal,internal)
	RENAME TO rum_tsquery_addon_consistent;

CREATE FUNCTION rum_numeric_cmp(numeric, numeric)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR CLASS rum_tsvector_addon_ops
FOR TYPE tsvector USING rum
AS
	OPERATOR	1	@@ (tsvector, tsquery),
	--support function
	FUNCTION	1	gin_cmp_tslexeme(text, text),
	FUNCTION	2	rum_extract_tsvector(tsvector,internal,internal,internal,internal),
	FUNCTION	3	rum_extract_tsquery(tsquery,internal,smallint,internal,internal,internal,internal),
	FUNCTION	4	rum_tsquery_addon_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
	FUNCTION	5	gin_cmp_prefix(text,text,smallint,internal),
	FUNCTION	7	rum_tsquery_pre_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
	STORAGE	 text;

CREATE OPERATOR CLASS rum_tsvector_hash_addon_ops
FOR TYPE tsvector USING rum
AS
	OPERATOR	1	@@ (tsvector, tsquery),
	--support function
	FUNCTION	1	btint4cmp(integer, integer),
	FUNCTION	2	rum_extract_tsvector_hash(tsvector,internal,internal,internal,internal),
	FUNCTION	3	rum_extract_tsquery_hash(tsquery,internal,smallint,internal,internal,internal,internal),
	FUNCTION	4	rum_tsquery_addon_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
	FUNCTION	7	rum_tsquery_pre_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
	STORAGE	 integer;

/*--------------------int2-----------------------*/

CREATE FUNCTION rum_int2_extract_value(int2, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_int2_compare_prefix(int2, int2, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_int2_extract_query(int2, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;



CREATE FUNCTION rum_int2_distance(int2, int2)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=> (
	PROCEDURE = rum_int2_distance,
	LEFTARG = int2,
	RIGHTARG = int2,
	COMMUTATOR = <=>
);

CREATE FUNCTION rum_int2_left_distance(int2, int2)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=| (
	PROCEDURE = rum_int2_left_distance,
	LEFTARG = int2,
	RIGHTARG = int2,
	COMMUTATOR = |=>
);

CREATE FUNCTION rum_int2_right_distance(int2, int2)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR |=> (
	PROCEDURE = rum_int2_right_distance,
	LEFTARG = int2,
	RIGHTARG = int2,
	COMMUTATOR = <=|
);

CREATE FUNCTION rum_int2_outer_distance(int2, int2, smallint)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_int2_config(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;



CREATE OPERATOR CLASS rum_int2_ops
DEFAULT FOR TYPE int2 USING rum
AS
	OPERATOR	1	<	,
	OPERATOR	2	<=	,
	OPERATOR	3	=	,
	OPERATOR	4	>=	,
	OPERATOR	5	>	,
	OPERATOR	20	<=> (int2,int2) FOR ORDER BY pg_catalog.float_ops,
	OPERATOR	21	<=| (int2,int2) FOR ORDER BY pg_catalog.float_ops,
	OPERATOR	22	|=> (int2,int2) FOR ORDER BY pg_catalog.float_ops,
	FUNCTION	1	btint2cmp(int2,int2),
	FUNCTION	2	rum_int2_extract_value(int2, internal),
	FUNCTION	3	rum_int2_extract_query(int2, internal, int2, internal, internal),
	FUNCTION	4	rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	rum_int2_compare_prefix(int2,int2,int2, internal),
	-- support to int2 distance in rum_tsvector_addon_ops
	FUNCTION	6	rum_int2_config(internal),
	FUNCTION	9	rum_int2_outer_distance(int2, int2, smallint),
STORAGE		 int2;

/*--------------------int4-----------------------*/

CREATE FUNCTION rum_int4_extract_value(int4, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_int4_compare_prefix(int4, int4, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_int4_extract_query(int4, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;



CREATE FUNCTION rum_int4_distance(int4, int4)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=> (
	PROCEDURE = rum_int4_distance,
	LEFTARG = int4,
	RIGHTARG = int4,
	COMMUTATOR = <=>
);

CREATE FUNCTION rum_int4_left_distance(int4, int4)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=| (
	PROCEDURE = rum_int4_left_distance,
	LEFTARG = int4,
	RIGHTARG = int4,
	COMMUTATOR = |=>
);

CREATE FUNCTION rum_int4_right_distance(int4, int4)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR |=> (
	PROCEDURE = rum_int4_right_distance,
	LEFTARG = int4,
	RIGHTARG = int4,
	COMMUTATOR = <=|
);

CREATE FUNCTION rum_int4_outer_distance(int4, int4, smallint)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_int4_config(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;



CREATE OPERATOR CLASS rum_int4_ops
DEFAULT FOR TYPE int4 USING rum
AS
	OPERATOR	1	<	,
	OPERATOR	2	<=	,
	OPERATOR	3	=	,
	OPERATOR	4	>=	,
	OPERATOR	5	>	,
	OPERATOR	20	<=> (int4,int4) FOR ORDER BY pg_catalog.float_ops,
	OPERATOR	21	<=| (int4,int4) FOR ORDER BY pg_catalog.float_ops,
	OPERATOR	22	|=> (int4,int4) FOR ORDER BY pg_catalog.float_ops,
	FUNCTION	1	btint4cmp(int4,int4),
	FUNCTION	2	rum_int4_extract_value(int4, internal),
	FUNCTION	3	rum_int4_extract_query(int4, internal, int2, internal, internal),
	FUNCTION	4	rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	rum_int4_compare_prefix(int4,int4,int2, internal),
	-- support to int4 distance in rum_tsvector_addon_ops
	FUNCTION	6	rum_int4_config(internal),
	FUNCTION	9	rum_int4_outer_distance(int4, int4, smallint),
STORAGE		 int4;

/*--------------------int8-----------------------*/

CREATE FUNCTION rum_int8_extract_value(int8, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_int8_compare_prefix(int8, int8, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_int8_extract_query(int8, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;



CREATE FUNCTION rum_int8_distance(int8, int8)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=> (
	PROCEDURE = rum_int8_distance,
	LEFTARG = int8,
	RIGHTARG = int8,
	COMMUTATOR = <=>
);

CREATE FUNCTION rum_int8_left_distance(int8, int8)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=| (
	PROCEDURE = rum_int8_left_distance,
	LEFTARG = int8,
	RIGHTARG = int8,
	COMMUTATOR = |=>
);

CREATE FUNCTION rum_int8_right_distance(int8, int8)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR |=> (
	PROCEDURE = rum_int8_right_distance,
	LEFTARG = int8,
	RIGHTARG = int8,
	COMMUTATOR = <=|
);

CREATE FUNCTION rum_int8_outer_distance(int8, int8, smallint)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_int8_config(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;



CREATE OPERATOR CLASS rum_int8_ops
DEFAULT FOR TYPE int8 USING rum
AS
	OPERATOR	1	<	,
	OPERATOR	2	<=	,
	OPERATOR	3	=	,
	OPERATOR	4	>=	,
	OPERATOR	5	>	,
	OPERATOR	20	<=> (int8,int8) FOR ORDER BY pg_catalog.float_ops,
	OPERATOR	21	<=| (int8,int8) FOR ORDER BY pg_catalog.float_ops,
	OPERATOR	22	|=> (int8,int8) FOR ORDER BY pg_catalog.float_ops,
	FUNCTION	1	btint8cmp(int8,int8),
	FUNCTION	2	rum_int8_extract_value(int8, internal),
	FUNCTION	3	rum_int8_extract_query(int8, internal, int2, internal, internal),
	FUNCTION	4	rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	rum_int8_compare_prefix(int8,int8,int2, internal),
	-- support to int8 distance in rum_tsvector_addon_ops
	FUNCTION	6	rum_int8_config(internal),
	FUNCTION	9	rum_int8_outer_distance(int8, int8, smallint),
STORAGE		 int8;

/*--------------------float4-----------------------*/

CREATE FUNCTION rum_float4_extract_value(float4, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_float4_compare_prefix(float4, float4, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_float4_extract_query(float4, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;



CREATE FUNCTION rum_float4_distance(float4, float4)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=> (
	PROCEDURE = rum_float4_distance,
	LEFTARG = float4,
	RIGHTARG = float4,
	COMMUTATOR = <=>
);

CREATE FUNCTION rum_float4_left_distance(float4, float4)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=| (
	PROCEDURE = rum_float4_left_distance,
	LEFTARG = float4,
	RIGHTARG = float4,
	COMMUTATOR = |=>
);

CREATE FUNCTION rum_float4_right_distance(float4, float4)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR |=> (
	PROCEDURE = rum_float4_right_distance,
	LEFTARG = float4,
	RIGHTARG = float4,
	COMMUTATOR = <=|
);

CREATE FUNCTION rum_float4_outer_distance(float4, float4, smallint)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_float4_config(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;



CREATE OPERATOR CLASS rum_float4_ops
DEFAULT FOR TYPE float4 USING rum
AS
	OPERATOR	1	<	,
	OPERATOR	2	<=	,
	OPERATOR	3	=	,
	OPERATOR	4	>=	,
	OPERATOR	5	>	,
	OPERATOR	20	<=> (float4,float4) FOR ORDER BY pg_catalog.float_ops,
	OPERATOR	21	<=| (float4,float4) FOR ORDER BY pg_catalog.float_ops,
	OPERATOR	22	|=> (float4,float4) FOR ORDER BY pg_catalog.float_ops,
	FUNCTION	1	btfloat4cmp(float4,float4),
	FUNCTION	2	rum_float4_extract_value(float4, internal),
	FUNCTION	3	rum_float4_extract_query(float4, internal, int2, internal, internal),
	FUNCTION	4	rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	rum_float4_compare_prefix(float4,float4,int2, internal),
	-- support to float4 distance in rum_tsvector_addon_ops
	FUNCTION	6	rum_float4_config(internal),
	FUNCTION	9	rum_float4_outer_distance(float4, float4, smallint),
STORAGE		 float4;

/*--------------------float8-----------------------*/

CREATE FUNCTION rum_float8_extract_value(float8, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_float8_compare_prefix(float8, float8, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_float8_extract_query(float8, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;



CREATE FUNCTION rum_float8_distance(float8, float8)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=> (
	PROCEDURE = rum_float8_distance,
	LEFTARG = float8,
	RIGHTARG = float8,
	COMMUTATOR = <=>
);

CREATE FUNCTION rum_float8_left_distance(float8, float8)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=| (
	PROCEDURE = rum_float8_left_distance,
	LEFTARG = float8,
	RIGHTARG = float8,
	COMMUTATOR = |=>
);

CREATE FUNCTION rum_float8_right_distance(float8, float8)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR |=> (
	PROCEDURE = rum_float8_right_distance,
	LEFTARG = float8,
	RIGHTARG = float8,
	COMMUTATOR = <=|
);

CREATE FUNCTION rum_float8_outer_distance(float8, float8, smallint)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_float8_config(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;



CREATE OPERATOR CLASS rum_float8_ops
DEFAULT FOR TYPE float8 USING rum
AS
	OPERATOR	1	<	,
	OPERATOR	2	<=	,
	OPERATOR	3	=	,
	OPERATOR	4	>=	,
	OPERATOR	5	>	,
	OPERATOR	20	<=> (float8,float8) FOR ORDER BY pg_catalog.float_ops,
	OPERATOR	21	<=| (float8,float8) FOR ORDER BY pg_catalog.float_ops,
	OPERATOR	22	|=> (float8,float8) FOR ORDER BY pg_catalog.float_ops,
	FUNCTION	1	btfloat8cmp(float8,float8),
	FUNCTION	2	rum_float8_extract_value(float8, internal),
	FUNCTION	3	rum_float8_extract_query(float8, internal, int2, internal, internal),
	FUNCTION	4	rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	rum_float8_compare_prefix(float8,float8,int2, internal),
	-- support to float8 distance in rum_tsvector_addon_ops
	FUNCTION	6	rum_float8_config(internal),
	FUNCTION	9	rum_float8_outer_distance(float8, float8, smallint),
STORAGE		 float8;

/*--------------------money-----------------------*/

CREATE FUNCTION rum_money_extract_value(money, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_money_compare_prefix(money, money, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_money_extract_query(money, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;



CREATE FUNCTION rum_money_distance(money, money)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=> (
	PROCEDURE = rum_money_distance,
	LEFTARG = money,
	RIGHTARG = money,
	COMMUTATOR = <=>
);

CREATE FUNCTION rum_money_left_distance(money, money)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=| (
	PROCEDURE = rum_money_left_distance,
	LEFTARG = money,
	RIGHTARG = money,
	COMMUTATOR = |=>
);

CREATE FUNCTION rum_money_right_distance(money, money)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR |=> (
	PROCEDURE = rum_money_right_distance,
	LEFTARG = money,
	RIGHTARG = money,
	COMMUTATOR = <=|
);

CREATE FUNCTION rum_money_outer_distance(money, money, smallint)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_money_config(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;



CREATE OPERATOR CLASS rum_money_ops
DEFAULT FOR TYPE money USING rum
AS
	OPERATOR	1	<	,
	OPERATOR	2	<=	,
	OPERATOR	3	=	,
	OPERATOR	4	>=	,
	OPERATOR	5	>	,
	OPERATOR	20	<=> (money,money) FOR ORDER BY pg_catalog.float_ops,
	OPERATOR	21	<=| (money,money) FOR ORDER BY pg_catalog.float_ops,
	OPERATOR	22	|=> (money,money) FOR ORDER BY pg_catalog.float_ops,
	FUNCTION	1	cash_cmp(money,money),
	FUNCTION	2	rum_money_extract_value(money, internal),
	FUNCTION	3	rum_money_extract_query(money, internal, int2, internal, internal),
	FUNCTION	4	rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	rum_money_compare_prefix(money,money,int2, internal),
	-- support to money distance in rum_tsvector_addon_ops
	FUNCTION	6	rum_money_config(internal),
	FUNCTION	9	rum_money_outer_distance(money, money, smallint),
STORAGE		 money;

/*--------------------oid-----------------------*/

CREATE FUNCTION rum_oid_extract_value(oid, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_oid_compare_prefix(oid, oid, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_oid_extract_query(oid, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;



CREATE FUNCTION rum_oid_distance(oid, oid)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=> (
	PROCEDURE = rum_oid_distance,
	LEFTARG = oid,
	RIGHTARG = oid,
	COMMUTATOR = <=>
);

CREATE FUNCTION rum_oid_left_distance(oid, oid)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=| (
	PROCEDURE = rum_oid_left_distance,
	LEFTARG = oid,
	RIGHTARG = oid,
	COMMUTATOR = |=>
);

CREATE FUNCTION rum_oid_right_distance(oid, oid)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR |=> (
	PROCEDURE = rum_oid_right_distance,
	LEFTARG = oid,
	RIGHTARG = oid,
	COMMUTATOR = <=|
);

CREATE FUNCTION rum_oid_outer_distance(oid, oid, smallint)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_oid_config(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;



CREATE OPERATOR CLASS rum_oid_ops
DEFAULT FOR TYPE oid USING rum
AS
	OPERATOR	1	<	,
	OPERATOR	2	<=	,
	OPERATOR	3	=	,
	OPERATOR	4	>=	,
	OPERATOR	5	>	,
	OPERATOR	20	<=> (oid,oid) FOR ORDER BY pg_catalog.float_ops,
	OPERATOR	21	<=| (oid,oid) FOR ORDER BY pg_catalog.float_ops,
	OPERATOR	22	|=> (oid,oid) FOR ORDER BY pg_catalog.float_ops,
	FUNCTION	1	btoidcmp(oid,oid),
	FUNCTION	2	rum_oid_extract_value(oid, internal),
	FUNCTION	3	rum_oid_extract_query(oid, internal, int2, internal, internal),
	FUNCTION	4	rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	rum_oid_compare_prefix(oid,oid,int2, internal),
	-- support to oid distance in rum_tsvector_addon_ops
	FUNCTION	6	rum_oid_config(internal),
	FUNCTION	9	rum_oid_outer_distance(oid, oid, smallint),
STORAGE		 oid;

/*--------------------time-----------------------*/

CREATE FUNCTION rum_time_extract_value(time, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_time_compare_prefix(time, time, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_time_extract_query(time, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;


CREATE OPERATOR CLASS rum_time_ops
DEFAULT FOR TYPE time USING rum
AS
	OPERATOR	1	  <		,
	OPERATOR	2	  <=	,
	OPERATOR	3	  =		,
	OPERATOR	4	  >=	,
	OPERATOR	5	  >		,
	FUNCTION	1	  time_cmp(time,time),
	FUNCTION	2	  rum_time_extract_value(time, internal),
	FUNCTION	3	  rum_time_extract_query(time, internal, int2, internal, internal),
	FUNCTION	4	  rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	  rum_time_compare_prefix(time,time,int2, internal),
STORAGE		 time;

/*--------------------timetz-----------------------*/

CREATE FUNCTION rum_timetz_extract_value(timetz, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_timetz_compare_prefix(timetz, timetz, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_timetz_extract_query(timetz, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;


CREATE OPERATOR CLASS rum_timetz_ops
DEFAULT FOR TYPE timetz USING rum
AS
	OPERATOR	1	  <		,
	OPERATOR	2	  <=	,
	OPERATOR	3	  =		,
	OPERATOR	4	  >=	,
	OPERATOR	5	  >		,
	FUNCTION	1	  timetz_cmp(timetz,timetz),
	FUNCTION	2	  rum_timetz_extract_value(timetz, internal),
	FUNCTION	3	  rum_timetz_extract_query(timetz, internal, int2, internal, internal),
	FUNCTION	4	  rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	  rum_timetz_compare_prefix(timetz,timetz,int2, internal),
STORAGE		 timetz;

/*--------------------date-----------------------*/

CREATE FUNCTION rum_date_extract_value(date, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_date_compare_prefix(date, date, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_date_extract_query(date, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;


CREATE OPERATOR CLASS rum_date_ops
DEFAULT FOR TYPE date USING rum
AS
	OPERATOR	1	  <		,
	OPERATOR	2	  <=	,
	OPERATOR	3	  =		,
	OPERATOR	4	  >=	,
	OPERATOR	5	  >		,
	FUNCTION	1	  date_cmp(date,date),
	FUNCTION	2	  rum_date_extract_value(date, internal),
	FUNCTION	3	  rum_date_extract_query(date, internal, int2, internal, internal),
	FUNCTION	4	  rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	  rum_date_compare_prefix(date,date,int2, internal),
STORAGE		 date;

/*--------------------interval-----------------------*/

CREATE FUNCTION rum_interval_extract_value(interval, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_interval_compare_prefix(interval, interval, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_interval_extract_query(interval, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;


CREATE OPERATOR CLASS rum_interval_ops
DEFAULT FOR TYPE interval USING rum
AS
	OPERATOR	1	  <		,
	OPERATOR	2	  <=	,
	OPERATOR	3	  =		,
	OPERATOR	4	  >=	,
	OPERATOR	5	  >		,
	FUNCTION	1	  interval_cmp(interval,interval),
	FUNCTION	2	  rum_interval_extract_value(interval, internal),
	FUNCTION	3	  rum_interval_extract_query(interval, internal, int2, internal, internal),
	FUNCTION	4	  rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	  rum_interval_compare_prefix(interval,interval,int2, internal),
STORAGE		 interval;

/*--------------------macaddr-----------------------*/

CREATE FUNCTION rum_macaddr_extract_value(macaddr, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_macaddr_compare_prefix(macaddr, macaddr, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_macaddr_extract_query(macaddr, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;


CREATE OPERATOR CLASS rum_macaddr_ops
DEFAULT FOR TYPE macaddr USING rum
AS
	OPERATOR	1	  <		,
	OPERATOR	2	  <=	,
	OPERATOR	3	  =		,
	OPERATOR	4	  >=	,
	OPERATOR	5	  >		,
	FUNCTION	1	  macaddr_cmp(macaddr,macaddr),
	FUNCTION	2	  rum_macaddr_extract_value(macaddr, internal),
	FUNCTION	3	  rum_macaddr_extract_query(macaddr, internal, int2, internal, internal),
	FUNCTION	4	  rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	  rum_macaddr_compare_prefix(macaddr,macaddr,int2, internal),
STORAGE		 macaddr;

/*--------------------inet-----------------------*/

CREATE FUNCTION rum_inet_extract_value(inet, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_inet_compare_prefix(inet, inet, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_inet_extract_query(inet, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;


CREATE OPERATOR CLASS rum_inet_ops
DEFAULT FOR TYPE inet USING rum
AS
	OPERATOR	1	  <		,
	OPERATOR	2	  <=	,
	OPERATOR	3	  =		,
	OPERATOR	4	  >=	,
	OPERATOR	5	  >		,
	FUNCTION	1	  network_cmp(inet,inet),
	FUNCTION	2	  rum_inet_extract_value(inet, internal),
	FUNCTION	3	  rum_inet_extract_query(inet, internal, int2, internal, internal),
	FUNCTION	4	  rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	  rum_inet_compare_prefix(inet,inet,int2, internal),
STORAGE		 inet;

/*--------------------cidr-----------------------*/

CREATE FUNCTION rum_cidr_extract_value(cidr, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_cidr_compare_prefix(cidr, cidr, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_cidr_extract_query(cidr, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;


CREATE OPERATOR CLASS rum_cidr_ops
DEFAULT FOR TYPE cidr USING rum
AS
	OPERATOR	1	  <		(inet, inet),
	OPERATOR	2	  <=	(inet, inet),
	OPERATOR	3	  =		(inet, inet),
	OPERATOR	4	  >=	(inet, inet),
	OPERATOR	5	  >		(inet, inet),
	FUNCTION	1	  network_cmp(inet,inet),
	FUNCTION	2	  rum_cidr_extract_value(cidr, internal),
	FUNCTION	3	  rum_cidr_extract_query(cidr, internal, int2, internal, internal),
	FUNCTION	4	  rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	  rum_cidr_compare_prefix(cidr,cidr,int2, internal),
STORAGE		 cidr;

/*--------------------text-----------------------*/

CREATE FUNCTION rum_text_extract_value(text, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_text_compare_prefix(text, text, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_text_extract_query(text, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;


CREATE OPERATOR CLASS rum_text_ops
DEFAULT FOR TYPE text USING rum
AS
	OPERATOR	1	  <		,
	OPERATOR	2	  <=	,
	OPERATOR	3	  =		,
	OPERATOR	4	  >=	,
	OPERATOR	5	  >		,
	FUNCTION	1	  bttextcmp(text,text),
	FUNCTION	2	  rum_text_extract_value(text, internal),
	FUNCTION	3	  rum_text_extract_query(text, internal, int2, internal, internal),
	FUNCTION	4	  rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	  rum_text_compare_prefix(text,text,int2, internal),
STORAGE		 text;

/*--------------------varchar-----------------------*/


CREATE OPERATOR CLASS rum_varchar_ops
DEFAULT FOR TYPE varchar USING rum
AS
	OPERATOR	1	  <		(text, text),
	OPERATOR	2	  <=	(text, text),
	OPERATOR	3	  =		(text, text),
	OPERATOR	4	  >=	(text, text),
	OPERATOR	5	  >		(text, text),
	FUNCTION	1	  bttextcmp(text,text),
	FUNCTION	2	  rum_text_extract_value(text, internal),
	FUNCTION	3	  rum_text_extract_query(text, internal, int2, internal, internal),
	FUNCTION	4	  rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	  rum_text_compare_prefix(text,text,int2, internal),
STORAGE		 varchar;

/*--------------------"char"-----------------------*/

CREATE FUNCTION rum_char_extract_value("char", internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_char_compare_prefix("char", "char", int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_char_extract_query("char", internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;


CREATE OPERATOR CLASS rum_char_ops
DEFAULT FOR TYPE "char" USING rum
AS
	OPERATOR	1	  <		,
	OPERATOR	2	  <=	,
	OPERATOR	3	  =		,
	OPERATOR	4	  >=	,
	OPERATOR	5	  >		,
	FUNCTION	1	  btcharcmp("char","char"),
	FUNCTION	2	  rum_char_extract_value("char", internal),
	FUNCTION	3	  rum_char_extract_query("char", internal, int2, internal, internal),
	FUNCTION	4	  rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	  rum_char_compare_prefix("char","char",int2, internal),
STORAGE		 "char";

/*--------------------bytea-----------------------*/

CREATE FUNCTION rum_bytea_extract_value(bytea, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_bytea_compare_prefix(bytea, bytea, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_bytea_extract_query(bytea, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;


CREATE OPERATOR CLASS rum_bytea_ops
DEFAULT FOR TYPE bytea USING rum
AS
	OPERATOR	1	  <		,
	OPERATOR	2	  <=	,
	OPERATOR	3	  =		,
	OPERATOR	4	  >=	,
	OPERATOR	5	  >		,
	FUNCTION	1	  byteacmp(bytea,bytea),
	FUNCTION	2	  rum_bytea_extract_value(bytea, internal),
	FUNCTION	3	  rum_bytea_extract_query(bytea, internal, int2, internal, internal),
	FUNCTION	4	  rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	  rum_bytea_compare_prefix(bytea,bytea,int2, internal),
STORAGE		 bytea;

/*--------------------bit-----------------------*/

CREATE FUNCTION rum_bit_extract_value(bit, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_bit_compare_prefix(bit, bit, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_bit_extract_query(bit, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;


CREATE OPERATOR CLASS rum_bit_ops
DEFAULT FOR TYPE bit USING rum
AS
	OPERATOR	1	  <		,
	OPERATOR	2	  <=	,
	OPERATOR	3	  =		,
	OPERATOR	4	  >=	,
	OPERATOR	5	  >		,
	FUNCTION	1	  bitcmp(bit,bit),
	FUNCTION	2	  rum_bit_extract_value(bit, internal),
	FUNCTION	3	  rum_bit_extract_query(bit, internal, int2, internal, internal),
	FUNCTION	4	  rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	  rum_bit_compare_prefix(bit,bit,int2, internal),
STORAGE		 bit;

/*--------------------varbit-----------------------*/

CREATE FUNCTION rum_varbit_extract_value(varbit, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_varbit_compare_prefix(varbit, varbit, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_varbit_extract_query(varbit, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;


CREATE OPERATOR CLASS rum_varbit_ops
DEFAULT FOR TYPE varbit USING rum
AS
	OPERATOR	1	  <		,
	OPERATOR	2	  <=	,
	OPERATOR	3	  =		,
	OPERATOR	4	  >=	,
	OPERATOR	5	  >		,
	FUNCTION	1	  varbitcmp(varbit,varbit),
	FUNCTION	2	  rum_varbit_extract_value(varbit, internal),
	FUNCTION	3	  rum_varbit_extract_query(varbit, internal, int2, internal, internal),
	FUNCTION	4	  rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	  rum_varbit_compare_prefix(varbit,varbit,int2, internal),
STORAGE		 varbit;

/*--------------------numeric-----------------------*/

CREATE FUNCTION rum_numeric_extract_value(numeric, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_numeric_compare_prefix(numeric, numeric, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_numeric_extract_query(numeric, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;


CREATE OPERATOR CLASS rum_numeric_ops
DEFAULT FOR TYPE numeric USING rum
AS
	OPERATOR	1	  <		,
	OPERATOR	2	  <=	,
	OPERATOR	3	  =		,
	OPERATOR	4	  >=	,
	OPERATOR	5	  >		,
	FUNCTION	1	  rum_numeric_cmp(numeric,numeric),
	FUNCTION	2	  rum_numeric_extract_value(numeric, internal),
	FUNCTION	3	  rum_numeric_extract_query(numeric, internal, int2, internal, internal),
	FUNCTION	4	  rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	  rum_numeric_compare_prefix(numeric,numeric,int2, internal),
STORAGE		 numeric;

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

