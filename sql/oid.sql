set enable_seqscan=off;

CREATE TABLE test_oid (
	i oid
);

INSERT INTO test_oid VALUES (0),(1),(2),(3),(4),(5);

CREATE INDEX idx_oid ON test_oid USING rum (i);

SELECT * FROM test_oid WHERE i<3::oid ORDER BY i;
SELECT * FROM test_oid WHERE i<=3::oid ORDER BY i;
SELECT * FROM test_oid WHERE i=3::oid ORDER BY i;
SELECT * FROM test_oid WHERE i>=3::oid ORDER BY i;
SELECT * FROM test_oid WHERE i>3::oid ORDER BY i;

EXPLAIN (costs off)
SELECT *, i <=> 0::oid FROM test_oid ORDER BY i <=> 0::oid;
SELECT *, i <=> 0::oid FROM test_oid ORDER BY i <=> 0::oid;

EXPLAIN (costs off)
SELECT *, i <=> 1::oid FROM test_oid WHERE i<1::oid ORDER BY i <=> 1::oid;
SELECT *, i <=> 1::oid FROM test_oid WHERE i<1::oid ORDER BY i <=> 1::oid;
