set enable_seqscan=off;


CREATE TABLE test_array (
	i int2[]
);

INSERT INTO test_array VALUES ('{}'), ('{0}'), ('{1,2,3,4}'), ('{1,2,3}'), ('{1,2}'), ('{1}');

CREATE INDEX idx_array ON test_array USING rum (i);

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

