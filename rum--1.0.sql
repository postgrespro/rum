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
