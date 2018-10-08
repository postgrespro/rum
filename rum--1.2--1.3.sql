/*
 * RUM version 1.3
 */

CREATE FUNCTION rum_ts_score(tsvector,tsquery)
RETURNS float4
AS 'MODULE_PATHNAME', 'rum_ts_score_tt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION rum_ts_score(tsvector,tsquery,int)
RETURNS float4
AS 'MODULE_PATHNAME', 'rum_ts_score_ttf'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION rum_ts_score(tsvector,rum_distance_query)
RETURNS float4
AS 'MODULE_PATHNAME', 'rum_ts_score_td'
LANGUAGE C IMMUTABLE STRICT;

