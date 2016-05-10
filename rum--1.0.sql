CREATE OR REPLACE FUNCTION rumhandler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Access method
CREATE ACCESS METHOD rum TYPE INDEX HANDLER rumhandler;

-- Opclasses
CREATE FUNCTION rum_ts_distance(tsvector,tsquery)
RETURNS float4
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR >< (
        LEFTARG = tsvector,
        RIGHTARG = tsquery,
        PROCEDURE = rum_ts_distance,
        COMMUTATOR = '><'
);

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

CREATE OPERATOR CLASS rum_tsvector_ops
FOR TYPE tsvector USING rum
AS
        OPERATOR        1       @@ (tsvector, tsquery),
        OPERATOR        2       >< (tsvector, tsquery) FOR ORDER BY pg_catalog.float_ops,
        FUNCTION        1       gin_cmp_tslexeme(text, text),
        FUNCTION        2       rum_extract_tsvector(tsvector,internal,internal,internal,internal),
        FUNCTION        3       rum_extract_tsquery(tsquery,internal,smallint,internal,internal,internal,internal),
        FUNCTION        4       rum_tsquery_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
        FUNCTION        5       gin_cmp_prefix(text,text,smallint,internal),
        FUNCTION        6       gin_tsquery_triconsistent(internal,smallint,tsvector,int,internal,internal,internal),
        FUNCTION        7       rum_tsvector_config(internal),
        FUNCTION        8       rum_tsquery_pre_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
        FUNCTION        9       rum_tsquery_distance(internal,smallint,tsvector,int,internal,internal,internal,internal,internal),
        STORAGE         text;


-- timestamp

CREATE FUNCTION rum_timestamp_extract_value(timestamp,internal,internal,internal,internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_timestamp_compare_prefix(timestamp,timestamp,smallint,internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_timestamp_extract_query(timestamp,internal,smallint,internal,internal,internal,internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_timestamp_consistent(internal,smallint,timestamp,int,internal,internal,internal,internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR CLASS timestamp_ops
DEFAULT FOR TYPE timestamp USING rum
AS
    OPERATOR        1       <,
	OPERATOR        2       <=,
	OPERATOR        3       =,
	OPERATOR        4       >=,
	OPERATOR        5       >,
	FUNCTION        1 timestamp_cmp(timestamp,timestamp),
	FUNCTION        2 rum_timestamp_extract_value(timestamp,internal,internal,internal,internal),
	FUNCTION        3 rum_timestamp_extract_query(timestamp,internal,smallint,internal,internal,internal,internal),
	FUNCTION        4 rum_timestamp_consistent(internal,smallint,timestamp,int,internal,internal,internal,internal),
	FUNCTION        5 rum_timestamp_compare_prefix(timestamp,timestamp,smallint,internal),
STORAGE         timestamp;

