CREATE OR REPLACE FUNCTION rumhandler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Access method
CREATE ACCESS METHOD rum TYPE INDEX HANDLER rumhandler;

-- Opclasses
CREATE FUNCTION gin_tsvector_config(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gin_tsquery_pre_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gin_tsquery_distance(internal,smallint,tsvector,int,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR CLASS rum_tsvector_ops
FOR TYPE tsvector USING rum
AS
        OPERATOR        1       @@ (tsvector, tsquery),
        OPERATOR        2       @@@ (tsvector, tsquery),
        FUNCTION        1       bttextcmp(text, text),
        FUNCTION        2       gin_extract_tsvector(tsvector,internal,internal),
        FUNCTION        3       gin_extract_tsquery(tsvector,internal,smallint,internal,internal,internal,internal),
        FUNCTION        4       gin_tsquery_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
        FUNCTION        5       gin_cmp_prefix(text,text,smallint,internal),
        FUNCTION        6       gin_tsquery_triconsistent(internal,smallint,tsvector,int,internal,internal,internal),
        FUNCTION        7       gin_tsvector_config(internal),
        FUNCTION        8       gin_tsquery_pre_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
        FUNCTION        9       gin_tsquery_distance(internal,smallint,tsvector,int,internal,internal,internal,internal,internal),
        STORAGE         text;
