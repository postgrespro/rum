-- Check we can put and query lexemes of maximum size 2046 bytes
-- with maximum posting list size.
CREATE TABLE limits_test (v tsvector);
INSERT INTO limits_test (SELECT (SELECT (repeat(chr(65 + num % 26), 2046) || ':' || string_agg(i::text, ','))::tsvector FROM generate_series(1,1024) i) FROM generate_series(1,1000) num);
CREATE INDEX limits_test_idx ON limits_test USING rum (v);

SET enable_seqscan = off;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('A', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('B', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('C', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('D', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('E', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('F', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('G', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('H', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('I', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('J', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('K', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('L', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('M', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('N', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('O', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('P', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('Q', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('R', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('S', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('T', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('U', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('V', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('W', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('X', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('Y', 2046)::tsquery;
SELECT COUNT(*) FROM limits_test WHERE v @@ repeat('Z', 2046)::tsquery;
