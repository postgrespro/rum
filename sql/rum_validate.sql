--
-- Various sanity tests
--

-- First validate operator classes
SELECT opcname, amvalidate(opc.oid)
FROM pg_opclass opc JOIN pg_am am ON am.oid = opcmethod
WHERE amname = 'rum'
ORDER BY opcname;

--
-- Test access method and 'rumidx' index properties
--

-- Access method properties
SELECT a.amname, p.name, pg_indexam_has_property(a.oid,p.name)
FROM pg_am a, unnest(array['can_order','can_unique','can_multi_col','can_exclude']) p(name)
WHERE a.amname = 'rum' ORDER BY a.amname;

-- Index properties
SELECT p.name, pg_index_has_property('rumidx'::regclass,p.name)
FROM unnest(array['clusterable','index_scan','bitmap_scan','backward_scan']) p(name);

-- Index column properties
SELECT p.name, pg_index_column_has_property('rumidx'::regclass,1,p.name)
FROM unnest(array['asc','desc','nulls_first','nulls_last','orderable','distance_orderable','returnable','search_array','search_nulls']) p(name);

--
-- Check incorrect operator class
--

DROP INDEX rumidx;

-- Check incorrect operator class
CREATE OPERATOR CLASS rum_tsvector_norm_ops
FOR TYPE tsvector USING rum
AS
	OPERATOR        1       @@ (tsvector, tsquery),
	OPERATOR        2       <=> (tsvector, rum_distance_query) FOR ORDER BY pg_catalog.float_ops,
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

CREATE INDEX rum_norm_idx ON test_rum USING rum(a rum_tsvector_norm_ops);

SET enable_seqscan=off;
SET enable_bitmapscan=off;
SET enable_indexscan=on;

SELECT a
	FROM test_rum
	WHERE a @@ to_tsquery('pg_catalog.english', 'bar')
	ORDER BY a <=> (to_tsquery('pg_catalog.english', 'bar'),0)