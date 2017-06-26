set enable_seqscan=off;
SET enable_bitmapscan=OFF;

CREATE TABLE test_anyarray(
	i int[]
);
INSERT INTO test_anyarray
SELECT ARRAY[1, x, x + 1] FROM generate_series(1, 10000) x;

CREATE INDEX test_anyarray_idx ON test_anyarray USING rum (i);

EXPLAIN (costs off)
SELECT *, i <=> '{1}' FROM test_anyarray WHERE i % '{1}' ORDER BY i <=> '{1}' LIMIT 5;
SELECT *, i <=> '{1}' FROM test_anyarray WHERE i % '{1}' ORDER BY i <=> '{1}' LIMIT 5;

EXPLAIN (costs off)
SELECT *, i <=> '{1}' FROM test_anyarray WHERE i @> '{1}' ORDER BY i <=> '{1}' LIMIT 5;
SELECT *, i <=> '{1}' FROM test_anyarray WHERE i @> '{1}' ORDER BY i <=> '{1}' LIMIT 5;

EXPLAIN (costs off)
SELECT *, i <=> '{1}' FROM test_anyarray WHERE i % '{1}' ORDER BY i <=> '{1}' LIMIT 5000;

CREATE TABLE test_anyarray_ts(
	i int[],
	d timestamp
);
INSERT INTO test_anyarray_ts VALUES
	( '{1,2,3}', '2004-10-26 03:55:08' ),
	( '{2,3,4}', '2004-10-26 04:55:08' ),
	( '{3,4,5}', '2004-10-26 05:55:08' ),
	( '{4,5,6}', '2004-10-26 08:55:08' ),
	( '{5,6,7}', '2004-10-26 09:55:08' ),
	( '{6,7,8}', '2004-10-26 10:55:08' )
;

CREATE INDEX test_anyarray_ts_idx ON test_anyarray_ts USING rum
	(i rum_anyarray_addon_ops, d)
	WITH (attach = 'd', to = 'i');

EXPLAIN (costs off)
SELECT *, d <=> '2004-10-26 08:00:00' FROM test_anyarray_ts WHERE i % '{3}' ORDER BY d <=> '2004-10-26 08:00:00' LIMIT 5;
SELECT *, d <=> '2004-10-26 08:00:00' FROM test_anyarray_ts WHERE i % '{3}' ORDER BY d <=> '2004-10-26 08:00:00' LIMIT 5;

EXPLAIN (costs off)
SELECT *, d <=> '2004-10-26 08:00:00' FROM test_anyarray_ts WHERE i @> '{3}' ORDER BY d <=> '2004-10-26 08:00:00' LIMIT 5;
SELECT *, d <=> '2004-10-26 08:00:00' FROM test_anyarray_ts WHERE i @> '{3}' ORDER BY d <=> '2004-10-26 08:00:00' LIMIT 5;
