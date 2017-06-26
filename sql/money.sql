set enable_seqscan=off;

CREATE TABLE test_money (
	i money
);

INSERT INTO test_money VALUES ('-2'),('-1'),('0'),('1'),('2'),('3');

CREATE INDEX idx_money ON test_money USING rum (i);

SELECT * FROM test_money WHERE i<'1'::money ORDER BY i;
SELECT * FROM test_money WHERE i<='1'::money ORDER BY i;
SELECT * FROM test_money WHERE i='1'::money ORDER BY i;
SELECT * FROM test_money WHERE i>='1'::money ORDER BY i;
SELECT * FROM test_money WHERE i>'1'::money ORDER BY i;

EXPLAIN (costs off)
SELECT *, i <=> 0::money FROM test_money ORDER BY i <=> 0::money;
SELECT *, i <=> 0::money FROM test_money ORDER BY i <=> 0::money;

EXPLAIN (costs off)
SELECT *, i <=> 1::money FROM test_money WHERE i<1::money ORDER BY i <=> 1::money;
SELECT *, i <=> 1::money FROM test_money WHERE i<1::money ORDER BY i <=> 1::money;
