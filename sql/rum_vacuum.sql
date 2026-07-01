-- The test verifies that the scan in different directions in the RUM index
-- correctly handles empty posting lists and empty pages of the posting tree
-- that have been vacuumed.

SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = on;

-- Prepare the data. Creating a posting list for ann and a posting tree for john.
CREATE TABLE test_rum_vacuum (id int, body tsvector);
ALTER TABLE test_rum_vacuum SET (autovacuum_enabled = false);
INSERT INTO test_rum_vacuum SELECT i, to_tsvector('ann') FROM generate_series(1, 5) i;
INSERT INTO test_rum_vacuum SELECT i, to_tsvector('john') FROM generate_series(6, 10000) i;
CREATE INDEX ON test_rum_vacuum USING rum (body rum_tsvector_ops);

-- Delete all the items from the posting list and all but one item from the
-- posting tree.
DELETE FROM test_rum_vacuum WHERE body @@ 'ann'::tsquery;
DELETE FROM test_rum_vacuum WHERE body @@ 'john'::tsquery AND id <= 9999;

-- Check the scan before vacuum.

-- Scan with the extraction of results from the posting list (RumFastScan mode).
EXPLAIN (costs off)
SELECT * FROM test_rum_vacuum WHERE body @@ to_tsquery('ann');
SELECT * FROM test_rum_vacuum WHERE body @@ to_tsquery('ann');

-- Scan with the extraction of results from the posting tree (RumFastScan mode).
EXPLAIN (costs off)
SELECT * FROM test_rum_vacuum WHERE body @@ to_tsquery('john');
SELECT * FROM test_rum_vacuum WHERE body @@ to_tsquery('john');

-- Full-pass index scan (RumFullScan mode).
EXPLAIN (costs off)
SELECT id, body, body <=> to_tsquery('john') AS distance FROM test_rum_vacuum ORDER BY distance;
SELECT id, body, body <=> to_tsquery('john') AS distance FROM test_rum_vacuum ORDER BY distance;

-- Remove the elements from the posting tree and the posting list and make the
-- same checks.
VACUUM test_rum_vacuum;

-- Scan of an empty posting list.
EXPLAIN (costs off)
SELECT * FROM test_rum_vacuum WHERE body @@ to_tsquery('ann');
SELECT * FROM test_rum_vacuum WHERE body @@ to_tsquery('ann');

-- After the vacuum, the left and right leaf pages will remain in the posting
-- tree (because the outermost pages are not deleted). The left page is empty,
-- and the right page has one element. Check that the scan skips an left empty
-- page and receives an item from the right non-empty page.
EXPLAIN (costs off)
SELECT * FROM test_rum_vacuum WHERE body @@ to_tsquery('john');
SELECT * FROM test_rum_vacuum WHERE body @@ to_tsquery('john');

-- Check that in RumFullScan mode, the scan correctly switches from an empty
-- posting list to a non-empty posting tree and receives 1 element.
EXPLAIN (costs off)
SELECT id, body, body <=> to_tsquery('john') AS distance FROM test_rum_vacuum ORDER BY distance;
SELECT id, body, body <=> to_tsquery('john') AS distance FROM test_rum_vacuum ORDER BY distance;

-- Delete the last item from the posting tree.
DELETE FROM test_rum_vacuum WHERE body @@ 'john'::tsquery;
VACUUM test_rum_vacuum;

-- Check that the scan correctly skips all empty posting tree pages, reaches
-- the far right and ends.
EXPLAIN (costs off)
SELECT * FROM test_rum_vacuum WHERE body @@ to_tsquery('john');
SELECT * FROM test_rum_vacuum WHERE body @@ to_tsquery('john');

-- Check that in RumFullScan mode, the scan correctly skips an empty posting
-- list and an empty posting tree.
EXPLAIN (costs off)
SELECT id, body, body <=> to_tsquery('john') AS distance FROM test_rum_vacuum ORDER BY distance;
SELECT id, body, body <=> to_tsquery('john') AS distance FROM test_rum_vacuum ORDER BY distance;

-- Check that after the reinsertion, the new data can be found in the posting
-- list and posting tree.
INSERT INTO test_rum_vacuum SELECT i, to_tsvector('ann') FROM generate_series(10001, 10005) i;
INSERT INTO test_rum_vacuum SELECT i, to_tsvector('john') FROM generate_series(10006, 20000) i;

EXPLAIN (costs off)
SELECT count(*) FROM test_rum_vacuum WHERE body @@ to_tsquery('ann');
SELECT count(*) FROM test_rum_vacuum WHERE body @@ to_tsquery('ann');

EXPLAIN (costs off)
SELECT count(*) FROM test_rum_vacuum WHERE body @@ to_tsquery('john');
SELECT count(*) FROM test_rum_vacuum WHERE body @@ to_tsquery('john');

DROP TABLE test_rum_vacuum;

-- Check the backward scan direction.

SET enable_bitmapscan = off;
SET enable_indexscan = on;

-- Prepare the data. Creating a posting list for ann and a posting tree for john.
CREATE TABLE test_rum_vacuum_backward (id int, body tsvector);
ALTER TABLE test_rum_vacuum_backward SET (autovacuum_enabled = false);
INSERT INTO test_rum_vacuum_backward SELECT i, to_tsvector('ann') FROM generate_series(1, 5) i;
INSERT INTO test_rum_vacuum_backward SELECT i, to_tsvector('john') FROM generate_series(6, 10000) i;
CREATE INDEX ON test_rum_vacuum_backward USING rum (body rum_tsvector_addon_ops, id) WITH (attach='id', to='body', order_by_attach='true');

-- Delete all the items from the posting list and all but one item from the
-- posting tree.
DELETE FROM test_rum_vacuum_backward WHERE body @@ 'ann'::tsquery;
DELETE FROM test_rum_vacuum_backward WHERE body @@ 'john'::tsquery AND id <= 9999;

-- Check the backward scan before vacuum.

-- A scan with the results extracted from the posting list (in the backward direction).
EXPLAIN (costs off)
SELECT *, id <=| 2 AS distance FROM test_rum_vacuum_backward WHERE body @@ 'ann'::tsquery ORDER BY distance;
SELECT *, id <=| 2 AS distance FROM test_rum_vacuum_backward WHERE body @@ 'ann'::tsquery ORDER BY distance;

-- A scan with the results extracted from the posting tree (in the backward direction).
EXPLAIN (costs off)
SELECT *, id <=| 2 AS distance FROM test_rum_vacuum_backward WHERE body @@ 'john'::tsquery ORDER BY distance;
SELECT *, id <=| 2 AS distance FROM test_rum_vacuum_backward WHERE body @@ 'john'::tsquery ORDER BY distance;

-- Remove the elements from the posting tree and the posting list and make the
-- same checks.
VACUUM test_rum_vacuum_backward;

EXPLAIN (costs off)
SELECT *, id <=| 2 AS distance FROM test_rum_vacuum_backward WHERE body @@ 'ann'::tsquery ORDER BY distance;
SELECT *, id <=| 2 AS distance FROM test_rum_vacuum_backward WHERE body @@ 'ann'::tsquery ORDER BY distance;

EXPLAIN (costs off)
SELECT *, id <=| 2 AS distance FROM test_rum_vacuum_backward WHERE body @@ 'john'::tsquery ORDER BY distance;
SELECT *, id <=| 2 AS distance FROM test_rum_vacuum_backward WHERE body @@ 'john'::tsquery ORDER BY distance;

-- Delete the last item from the posting tree and make the same check.
DELETE FROM test_rum_vacuum_backward WHERE body @@ 'john'::tsquery;
VACUUM test_rum_vacuum_backward;

EXPLAIN (costs off)
SELECT *, id <=| 2 AS distance FROM test_rum_vacuum_backward WHERE body @@ 'john'::tsquery ORDER BY distance;
SELECT *, id <=| 2 AS distance FROM test_rum_vacuum_backward WHERE body @@ 'john'::tsquery ORDER BY distance;

-- Check that after the reinsertion, the new data can be found in the posting
-- list and posting tree.
INSERT INTO test_rum_vacuum_backward SELECT i, to_tsvector('ann') FROM generate_series(10001, 10005) i;
INSERT INTO test_rum_vacuum_backward SELECT i, to_tsvector('john') FROM generate_series(10006, 20000) i;

EXPLAIN (costs off)
SELECT count(*) FROM (SELECT * FROM test_rum_vacuum_backward WHERE body @@ 'ann'::tsquery ORDER BY id <=| 2);
SELECT count(*) FROM (SELECT * FROM test_rum_vacuum_backward WHERE body @@ 'ann'::tsquery ORDER BY id <=| 2);

EXPLAIN (costs off)
SELECT count(*) FROM (SELECT * FROM test_rum_vacuum_backward WHERE body @@ 'john'::tsquery ORDER BY id <=| 2);
SELECT count(*) FROM (SELECT * FROM test_rum_vacuum_backward WHERE body @@ 'john'::tsquery ORDER BY id <=| 2);

DROP TABLE test_rum_vacuum_backward;
