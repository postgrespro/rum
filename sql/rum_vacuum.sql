-- Test RUM index scan correctness after concurrent VACUUM removes all
-- posting tree entry items.

SET enable_seqscan TO off;
SET enable_indexscan TO off;
SET enable_bitmapscan TO on;

CREATE TABLE test_rum_vacuum (id int, body tsvector);
ALTER TABLE test_rum_vacuum SET (autovacuum_enabled = false);

INSERT INTO test_rum_vacuum SELECT i, to_tsvector('great ann') FROM generate_series(1, 6) i;
INSERT INTO test_rum_vacuum SELECT i, to_tsvector('great john') FROM generate_series(7, 10000) i;
INSERT INTO test_rum_vacuum SELECT i, to_tsvector('great james') FROM generate_series(10001, 10003) i;

CREATE INDEX ON test_rum_vacuum USING rum (body rum_tsvector_ops);

DELETE FROM test_rum_vacuum WHERE body @@ 'ann'::tsquery AND id <= 5;
DELETE FROM test_rum_vacuum WHERE body @@ 'john'::tsquery AND id <= 9999;

-- test normal result
SELECT * FROM test_rum_vacuum WHERE body @@ to_tsquery('ann');
SELECT * FROM test_rum_vacuum WHERE body @@ to_tsquery('john');
SELECT id, body FROM (SELECT id, body, body <=> to_tsquery('ann') AS distance FROM test_rum_vacuum ORDER BY body <=> to_tsquery('ann')) AS sub ORDER BY distance ASC, id ASC;
SELECT id, body FROM (SELECT id, body, body <=> to_tsquery('john') AS distance FROM test_rum_vacuum ORDER BY body <=> to_tsquery('john')) AS sub ORDER BY distance ASC, id ASC;

VACUUM test_rum_vacuum;

-- this shouldn't cause a core dump
SELECT * FROM test_rum_vacuum WHERE body @@ to_tsquery('ann');
SELECT * FROM test_rum_vacuum WHERE body @@ to_tsquery('john');
SELECT id, body FROM (SELECT id, body, body <=> to_tsquery('ann') AS distance FROM test_rum_vacuum ORDER BY body <=> to_tsquery('ann')) AS sub ORDER BY distance ASC, id ASC;
SELECT id, body FROM (SELECT id, body, body <=> to_tsquery('john') AS distance FROM test_rum_vacuum ORDER BY body <=> to_tsquery('john')) AS sub ORDER BY distance ASC, id ASC;

-- test that data can still be found after reinsertion
INSERT INTO test_rum_vacuum SELECT i, to_tsvector('great john') FROM generate_series(10004, 20000) i;
SELECT count(*) FROM test_rum_vacuum WHERE body @@ to_tsquery('john');
DELETE FROM test_rum_vacuum WHERE body @@ 'john'::tsquery AND id <= 19999;

VACUUM test_rum_vacuum;

SELECT * FROM test_rum_vacuum WHERE body @@ to_tsquery('ann');
SELECT * FROM test_rum_vacuum WHERE body @@ to_tsquery('john');
SELECT id, body FROM (SELECT id, body, body <=> to_tsquery('ann') AS distance FROM test_rum_vacuum ORDER BY body <=> to_tsquery('ann')) AS sub ORDER BY distance ASC, id ASC;
SELECT id, body FROM (SELECT id, body, body <=> to_tsquery('john') AS distance FROM test_rum_vacuum ORDER BY body <=> to_tsquery('john')) AS sub ORDER BY distance ASC, id ASC;

-- test if do while loop works when an entry has no non-empty posting tree pages
INSERT INTO test_rum_vacuum SELECT i, to_tsvector('great john') FROM generate_series(7, 10000) i;
DELETE FROM test_rum_vacuum WHERE body @@ 'john'::tsquery;
VACUUM test_rum_vacuum;

SELECT * FROM test_rum_vacuum WHERE body @@ to_tsquery('ann');
SELECT * FROM test_rum_vacuum WHERE body @@ to_tsquery('john');
SELECT id, body FROM (SELECT id, body, body <=> to_tsquery('ann') AS distance FROM test_rum_vacuum ORDER BY body <=> to_tsquery('ann')) AS sub ORDER BY distance ASC, id ASC;
SELECT id, body FROM (SELECT id, body, body <=> to_tsquery('john') AS distance FROM test_rum_vacuum ORDER BY body <=> to_tsquery('john')) AS sub ORDER BY distance ASC, id ASC;
