set enable_seqscan=off;

CREATE TABLE test_int4 (
	i int4
);

INSERT INTO test_int4 VALUES (-2),(-1),(0),(1),(2),(3);

CREATE INDEX idx_int4 ON test_int4 USING rum (i);

SELECT * FROM test_int4 WHERE i<1::int4 ORDER BY i;
SELECT * FROM test_int4 WHERE i<=1::int4 ORDER BY i;
SELECT * FROM test_int4 WHERE i=1::int4 ORDER BY i;
SELECT * FROM test_int4 WHERE i>=1::int4 ORDER BY i;
SELECT * FROM test_int4 WHERE i>1::int4 ORDER BY i;

CREATE TABLE test_int4_o AS SELECT id::int4, t FROM tsts;

CREATE INDEX test_int4_o_idx ON test_int4_o USING rum
	(t rum_tsvector_addon_ops, id)
	WITH (attach = 'id', to = 't');

RESET enable_seqscan;
SET enable_indexscan=OFF;
SET enable_indexonlyscan=OFF;
SET enable_bitmapscan=OFF;
SELECT id, id <=> 400 FROM test_int4_o WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
SELECT id, id <=| 400 FROM test_int4_o WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
SELECT id, id |=> 400 FROM test_int4_o WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;
SELECT id FROM test_int4_o WHERE  t @@ 'wr&qh' AND id <= 400 ORDER BY id;
SELECT id FROM test_int4_o WHERE  t @@ 'wr&qh' AND id >= 400 ORDER BY id;

RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_bitmapscan;
SET enable_seqscan = off;

EXPLAIN (costs off)
SELECT id, id <=> 400 FROM test_int4_o WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
SELECT id, id <=> 400 FROM test_int4_o WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
EXPLAIN (costs off)
SELECT id, id <=| 400 FROM test_int4_o WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
SELECT id, id <=| 400 FROM test_int4_o WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
EXPLAIN (costs off)
SELECT id, id |=> 400 FROM test_int4_o WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;
SELECT id, id |=> 400 FROM test_int4_o WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;

EXPLAIN (costs off)
SELECT id FROM test_int4_o WHERE  t @@ 'wr&qh' AND id <= 400 ORDER BY id;
SELECT id FROM test_int4_o WHERE  t @@ 'wr&qh' AND id <= 400 ORDER BY id;
EXPLAIN (costs off)
SELECT id FROM test_int4_o WHERE  t @@ 'wr&qh' AND id >= 400 ORDER BY id;
SELECT id FROM test_int4_o WHERE  t @@ 'wr&qh' AND id >= 400 ORDER BY id;

CREATE TABLE test_int4_a AS SELECT id::int4, t FROM tsts;

CREATE INDEX test_int4_a_idx ON test_int4_a USING rum
	(t rum_tsvector_addon_ops, id)
	WITH (attach = 'id', to = 't', order_by_attach='t');

SET enable_bitmapscan=OFF;

EXPLAIN (costs off)
SELECT count(*) FROM test_int4_a WHERE id < 400;
SELECT count(*) FROM test_int4_a WHERE id < 400;

EXPLAIN (costs off)
SELECT id, id <=> 400 FROM test_int4_a WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
SELECT id, id <=> 400 FROM test_int4_a WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
EXPLAIN (costs off)
SELECT id, id <=| 400 FROM test_int4_a WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
SELECT id, id <=| 400 FROM test_int4_a WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
EXPLAIN (costs off)
SELECT id, id |=> 400 FROM test_int4_a WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;
SELECT id, id |=> 400 FROM test_int4_a WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;

EXPLAIN (costs off)
SELECT id FROM test_int4_a WHERE  t @@ 'wr&qh' AND id <= 400 ORDER BY id;
SELECT id FROM test_int4_a WHERE  t @@ 'wr&qh' AND id <= 400 ORDER BY id;
EXPLAIN (costs off)
SELECT id FROM test_int4_a WHERE  t @@ 'wr&qh' AND id >= 400 ORDER BY id;
SELECT id FROM test_int4_a WHERE  t @@ 'wr&qh' AND id >= 400 ORDER BY id;

CREATE TABLE test_int4_h_o AS SELECT id::int4, t FROM tsts;

CREATE INDEX test_int4_h_o_idx ON test_int4_h_o USING rum
	(t rum_tsvector_hash_addon_ops, id)
	WITH (attach = 'id', to = 't');

RESET enable_seqscan;
SET enable_indexscan=OFF;
SET enable_indexonlyscan=OFF;
SET enable_bitmapscan=OFF;
SELECT id, id <=> 400 FROM test_int4_h_o WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
SELECT id, id <=| 400 FROM test_int4_h_o WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
SELECT id, id |=> 400 FROM test_int4_h_o WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;
SELECT id FROM test_int4_h_o WHERE  t @@ 'wr&qh' AND id <= 400 ORDER BY id;
SELECT id FROM test_int4_h_o WHERE  t @@ 'wr&qh' AND id >= 400 ORDER BY id;

RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_bitmapscan;
SET enable_seqscan = off;

EXPLAIN (costs off)
SELECT id, id <=> 400 FROM test_int4_h_o WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
SELECT id, id <=> 400 FROM test_int4_h_o WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
EXPLAIN (costs off)
SELECT id, id <=| 400 FROM test_int4_h_o WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
SELECT id, id <=| 400 FROM test_int4_h_o WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
EXPLAIN (costs off)
SELECT id, id |=> 400 FROM test_int4_h_o WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;
SELECT id, id |=> 400 FROM test_int4_h_o WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;

EXPLAIN (costs off)
SELECT id FROM test_int4_h_o WHERE  t @@ 'wr&qh' AND id <= 400 ORDER BY id;
SELECT id FROM test_int4_h_o WHERE  t @@ 'wr&qh' AND id <= 400 ORDER BY id;
EXPLAIN (costs off)
SELECT id FROM test_int4_h_o WHERE  t @@ 'wr&qh' AND id >= 400 ORDER BY id;
SELECT id FROM test_int4_h_o WHERE  t @@ 'wr&qh' AND id >= 400 ORDER BY id;

CREATE TABLE test_int4_h_a AS SELECT id::int4, t FROM tsts;

CREATE INDEX test_int4_h_a_idx ON test_int4_h_a USING rum
	(t rum_tsvector_hash_addon_ops, id)
	WITH (attach = 'id', to = 't', order_by_attach='t');

SET enable_bitmapscan=OFF;

EXPLAIN (costs off)
SELECT count(*) FROM test_int4_h_a WHERE id < 400;
SELECT count(*) FROM test_int4_h_a WHERE id < 400;

EXPLAIN (costs off)
SELECT id, id <=> 400 FROM test_int4_h_a WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
SELECT id, id <=> 400 FROM test_int4_h_a WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
EXPLAIN (costs off)
SELECT id, id <=| 400 FROM test_int4_h_a WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
SELECT id, id <=| 400 FROM test_int4_h_a WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
EXPLAIN (costs off)
SELECT id, id |=> 400 FROM test_int4_h_a WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;
SELECT id, id |=> 400 FROM test_int4_h_a WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;

EXPLAIN (costs off)
SELECT id FROM test_int4_h_a WHERE  t @@ 'wr&qh' AND id <= 400 ORDER BY id;
SELECT id FROM test_int4_h_a WHERE  t @@ 'wr&qh' AND id <= 400 ORDER BY id;
EXPLAIN (costs off)
SELECT id FROM test_int4_h_a WHERE  t @@ 'wr&qh' AND id >= 400 ORDER BY id;
SELECT id FROM test_int4_h_a WHERE  t @@ 'wr&qh' AND id >= 400 ORDER BY id;

