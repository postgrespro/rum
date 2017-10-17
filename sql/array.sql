set enable_seqscan=off;


/*
 * Complete checks for int2[].
 */

CREATE TABLE test_array (
	i int2[]
);

INSERT INTO test_array VALUES ('{}'), ('{0}'), ('{1,2,3,4}'), ('{1,2,3}'), ('{1,2}'), ('{1}');

CREATE INDEX idx_array ON test_array USING rum (i);

SELECT * FROM test_array WHERE i = '{NULL}';
SELECT * FROM test_array WHERE i = '{1,2,3,NULL}';
SELECT * FROM test_array WHERE i = '{{1,2},{3,4}}';

EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i = '{}';
SELECT * FROM test_array WHERE i = '{}';
SELECT * FROM test_array WHERE i = '{0}';
SELECT * FROM test_array WHERE i = '{1}';
SELECT * FROM test_array WHERE i = '{1,2}';
SELECT * FROM test_array WHERE i = '{2,1}';
SELECT * FROM test_array WHERE i = '{1,2,3,3}';
SELECT * FROM test_array WHERE i = '{0,0}';
SELECT * FROM test_array WHERE i = '{100}';

EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i && '{}';
SELECT * FROM test_array WHERE i && '{}';
SELECT * FROM test_array WHERE i && '{1}';
SELECT * FROM test_array WHERE i && '{2}';
SELECT * FROM test_array WHERE i && '{3}';
SELECT * FROM test_array WHERE i && '{4}';
SELECT * FROM test_array WHERE i && '{1,2}';
SELECT * FROM test_array WHERE i && '{1,2,3}';
SELECT * FROM test_array WHERE i && '{1,2,3,4}';
SELECT * FROM test_array WHERE i && '{4,3,2,1}';
SELECT * FROM test_array WHERE i && '{0,0}';
SELECT * FROM test_array WHERE i && '{100}';

EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i @> '{}';
SELECT * FROM test_array WHERE i @> '{}';
SELECT * FROM test_array WHERE i @> '{1}';
SELECT * FROM test_array WHERE i @> '{2}';
SELECT * FROM test_array WHERE i @> '{3}';
SELECT * FROM test_array WHERE i @> '{4}';
SELECT * FROM test_array WHERE i @> '{1,2,4}';
SELECT * FROM test_array WHERE i @> '{1,2,3,4}';
SELECT * FROM test_array WHERE i @> '{4,3,2,1}';
SELECT * FROM test_array WHERE i @> '{0,0}';
SELECT * FROM test_array WHERE i @> '{100}';

EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i <@ '{}';
SELECT * FROM test_array WHERE i <@ '{}';
SELECT * FROM test_array WHERE i <@ '{1}';
SELECT * FROM test_array WHERE i <@ '{2}';
SELECT * FROM test_array WHERE i <@ '{1,2,4}';
SELECT * FROM test_array WHERE i <@ '{1,2,3,4}';
SELECT * FROM test_array WHERE i <@ '{4,3,2,1}';
SELECT * FROM test_array WHERE i <@ '{0,0}';
SELECT * FROM test_array WHERE i <@ '{100}';

EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i % '{}';
SELECT * FROM test_array WHERE i % '{}';
SELECT * FROM test_array WHERE i % '{1}';
SELECT * FROM test_array WHERE i % '{2}';
SELECT * FROM test_array WHERE i % '{1,2}';
SELECT * FROM test_array WHERE i % '{1,2,4}';
SELECT * FROM test_array WHERE i % '{1,2,3,4}';
SELECT * FROM test_array WHERE i % '{4,3,2,1}';
SELECT * FROM test_array WHERE i % '{1,2,3,4,5}';
SELECT * FROM test_array WHERE i % '{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15}';
SELECT * FROM test_array WHERE i % '{1,10,20,30,40,50}';
SELECT * FROM test_array WHERE i % '{1,10,20,30}';
SELECT * FROM test_array WHERE i % '{1,1,1,1,1}';
SELECT * FROM test_array WHERE i % '{0,0}';
SELECT * FROM test_array WHERE i % '{100}';


/*
 * Sanity checks for popular array types.
 */

ALTER TABLE test_array ALTER COLUMN i TYPE int4[];
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i = '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i && '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i @> '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i <@ '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i % '{}';

ALTER TABLE test_array ALTER COLUMN i TYPE int8[];
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i = '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i && '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i @> '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i <@ '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i % '{}';

ALTER TABLE test_array ALTER COLUMN i TYPE text[];
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i = '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i && '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i @> '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i <@ '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i % '{}';

ALTER TABLE test_array ALTER COLUMN i TYPE varchar[];
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i = '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i && '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i @> '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i <@ '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i % '{}';

ALTER TABLE test_array ALTER COLUMN i TYPE char[];
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i = '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i && '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i @> '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i <@ '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i % '{}';

ALTER TABLE test_array ALTER COLUMN i TYPE numeric[] USING i::numeric[];
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i = '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i && '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i @> '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i <@ '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i % '{}';

ALTER TABLE test_array ALTER COLUMN i TYPE float4[] USING i::float4[];
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i = '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i && '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i @> '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i <@ '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i % '{}';

ALTER TABLE test_array ALTER COLUMN i TYPE float8[] USING i::float8[];
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i = '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i && '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i @> '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i <@ '{}';
EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i % '{}';

