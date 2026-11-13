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

SET enable_seqscan=off;
SET enable_bitmapscan=off;
SET enable_indexscan=on;


-- PGPRO-1175: NULL ordering key must not lead to empty scan
SELECT a
	FROM test_rum
	WHERE a @@ to_tsquery('pg_catalog.english', 'bar')
	ORDER BY a <=> (SELECT NULL::tsquery); -- to avoid cut the NULL during plan time

-- PGPRO-1175: an ordering key without entries must not lead to empty scan
EXPLAIN (COSTS OFF) -- to ensure that ordering will be done by rum index itself
SELECT a
	FROM test_rum
	WHERE a @@ to_tsquery('pg_catalog.english', 'bar')
	ORDER BY a <=> to_tsquery('pg_catalog.english', 'the'); -- will be empty since 'the' is the stop word
SELECT a
	FROM test_rum
	WHERE a @@ to_tsquery('pg_catalog.english', 'bar')
	ORDER BY a <=> to_tsquery('pg_catalog.english', 'the');


-- PGPRO-9026: column and attached column cannot be the same
CREATE TABLE test_array (i int2[]);
CREATE INDEX idx_array ON test_array USING rum (i rum_anyarray_addon_ops) WITH (attach = 'i', to = 'i');
SELECT * FROM test_array WHERE i && '{1}';
DROP TABLE test_array;
