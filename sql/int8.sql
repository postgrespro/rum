/*
 * ------------------------------------
 *  NOTE: This test behaves differenly
 * ------------------------------------
 *
 * int8.out - test output for 64-bit systems and
 * int8_1.out - test output for 32-bit systems.
 *
 * Since c01743aa486 (>=18) EXPLAIN output was changed,
 * now it includes the number of disabled nodes
 *
 * int8_2.out - test output for 32-bit systems (>=18)
 *
 */


set enable_seqscan=off;

CREATE TABLE test_int8 (
	i int8
);

INSERT INTO test_int8 VALUES (-2),(-1),(0),(1),(2),(3);

CREATE INDEX idx_int8 ON test_int8 USING rum (i);

SELECT * FROM test_int8 WHERE i<1::int8 ORDER BY i;
SELECT * FROM test_int8 WHERE i<=1::int8 ORDER BY i;
SELECT * FROM test_int8 WHERE i=1::int8 ORDER BY i;
SELECT * FROM test_int8 WHERE i>=1::int8 ORDER BY i;
SELECT * FROM test_int8 WHERE i>1::int8 ORDER BY i;

EXPLAIN (costs off)
SELECT *, i <=> 0::int8 FROM test_int8 ORDER BY i <=> 0::int8;
SELECT *, i <=> 0::int8 FROM test_int8 ORDER BY i <=> 0::int8;

EXPLAIN (costs off)
SELECT *, i <=> 1::int8 FROM test_int8 WHERE i<1::int8 ORDER BY i <=> 1::int8;
SELECT *, i <=> 1::int8 FROM test_int8 WHERE i<1::int8 ORDER BY i <=> 1::int8;

CREATE TABLE test_int8_o AS SELECT id::int8, t FROM tsts;

CREATE INDEX test_int8_o_idx ON test_int8_o USING rum
	(t rum_tsvector_addon_ops, id)
	WITH (attach = 'id', to = 't');

RESET enable_seqscan;
SET enable_indexscan=OFF;
SET enable_indexonlyscan=OFF;
SET enable_bitmapscan=OFF;
SELECT id, id <=> 400 FROM test_int8_o WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
SELECT id, id <=| 400 FROM test_int8_o WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
SELECT id, id |=> 400 FROM test_int8_o WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;
SELECT id FROM test_int8_o WHERE  t @@ 'wr&qh' AND id <= 400::int8 ORDER BY id;
SELECT id FROM test_int8_o WHERE  t @@ 'wr&qh' AND id >= 400::int8 ORDER BY id;

RESET enable_indexscan;
RESET enable_indexonlyscan;
SET enable_seqscan = off;

EXPLAIN (costs off)
SELECT id, id <=> 400 FROM test_int8_o WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
SELECT id, id <=> 400 FROM test_int8_o WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
EXPLAIN (costs off)
SELECT id, id <=| 400 FROM test_int8_o WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
SELECT id, id <=| 400 FROM test_int8_o WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
EXPLAIN (costs off)
SELECT id, id |=> 400 FROM test_int8_o WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;
SELECT id, id |=> 400 FROM test_int8_o WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;

EXPLAIN (costs off)
SELECT id FROM test_int8_o WHERE  t @@ 'wr&qh' AND id <= 400::int8 ORDER BY id;
SELECT id FROM test_int8_o WHERE  t @@ 'wr&qh' AND id <= 400::int8 ORDER BY id;
EXPLAIN (costs off)
SELECT id FROM test_int8_o WHERE  t @@ 'wr&qh' AND id >= 400::int8 ORDER BY id;
SELECT id FROM test_int8_o WHERE  t @@ 'wr&qh' AND id >= 400::int8 ORDER BY id;

CREATE TABLE test_int8_a AS SELECT id::int8, t FROM tsts;

CREATE INDEX test_int8_a_idx ON test_int8_a USING rum
	(t rum_tsvector_addon_ops, id)
	WITH (attach = 'id', to = 't', order_by_attach='t');

EXPLAIN (costs off)
SELECT count(*) FROM test_int8_a WHERE id < 400::int8;
SELECT count(*) FROM test_int8_a WHERE id < 400::int8;

EXPLAIN (costs off)
SELECT id, id <=> 400 FROM test_int8_a WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
SELECT id, id <=> 400 FROM test_int8_a WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
EXPLAIN (costs off)
SELECT id, id <=| 400 FROM test_int8_a WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
SELECT id, id <=| 400 FROM test_int8_a WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
EXPLAIN (costs off)
SELECT id, id |=> 400 FROM test_int8_a WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;
SELECT id, id |=> 400 FROM test_int8_a WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;

EXPLAIN (costs off)
SELECT id FROM test_int8_a WHERE  t @@ 'wr&qh' AND id <= 400::int8 ORDER BY id;
SELECT id FROM test_int8_a WHERE  t @@ 'wr&qh' AND id <= 400::int8 ORDER BY id;
EXPLAIN (costs off)
SELECT id FROM test_int8_a WHERE  t @@ 'wr&qh' AND id >= 400::int8 ORDER BY id;
SELECT id FROM test_int8_a WHERE  t @@ 'wr&qh' AND id >= 400::int8 ORDER BY id;

CREATE TABLE test_int8_h_o AS SELECT id::int8, t FROM tsts;

CREATE INDEX test_int8_h_o_idx ON test_int8_h_o USING rum
	(t rum_tsvector_hash_addon_ops, id)
	WITH (attach = 'id', to = 't');

RESET enable_seqscan;
SET enable_indexscan=OFF;
SET enable_indexonlyscan=OFF;
SET enable_bitmapscan=OFF;
SELECT id, id <=> 400 FROM test_int8_h_o WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
SELECT id, id <=| 400 FROM test_int8_h_o WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
SELECT id, id |=> 400 FROM test_int8_h_o WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;
SELECT id FROM test_int8_h_o WHERE  t @@ 'wr&qh' AND id <= 400::int8 ORDER BY id;
SELECT id FROM test_int8_h_o WHERE  t @@ 'wr&qh' AND id >= 400::int8 ORDER BY id;

RESET enable_indexscan;
RESET enable_indexonlyscan;
SET enable_seqscan = off;

EXPLAIN (costs off)
SELECT id, id <=> 400 FROM test_int8_h_o WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
SELECT id, id <=> 400 FROM test_int8_h_o WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
EXPLAIN (costs off)
SELECT id, id <=| 400 FROM test_int8_h_o WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
SELECT id, id <=| 400 FROM test_int8_h_o WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
EXPLAIN (costs off)
SELECT id, id |=> 400 FROM test_int8_h_o WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;
SELECT id, id |=> 400 FROM test_int8_h_o WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;


EXPLAIN (costs off)
SELECT id FROM test_int8_h_o WHERE  t @@ 'wr&qh' AND id <= 400::int8 ORDER BY id;
SELECT id FROM test_int8_h_o WHERE  t @@ 'wr&qh' AND id <= 400::int8 ORDER BY id;
EXPLAIN (costs off)
SELECT id FROM test_int8_h_o WHERE  t @@ 'wr&qh' AND id >= 400::int8 ORDER BY id;
SELECT id FROM test_int8_h_o WHERE  t @@ 'wr&qh' AND id >= 400::int8 ORDER BY id;

CREATE TABLE test_int8_h_a AS SELECT id::int8, t FROM tsts;

CREATE INDEX test_int8_h_a_idx ON test_int8_h_a USING rum
	(t rum_tsvector_hash_addon_ops, id)
	WITH (attach = 'id', to = 't', order_by_attach='t');

EXPLAIN (costs off)
SELECT count(*) FROM test_int8_h_a WHERE id < 400::int8;
SELECT count(*) FROM test_int8_h_a WHERE id < 400::int8;

EXPLAIN (costs off)
SELECT id, id <=> 400 FROM test_int8_h_a WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
SELECT id, id <=> 400 FROM test_int8_h_a WHERE t @@ 'wr&qh' ORDER BY id <=> 400 LIMIT 5;
EXPLAIN (costs off)
SELECT id, id <=| 400 FROM test_int8_h_a WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
SELECT id, id <=| 400 FROM test_int8_h_a WHERE t @@ 'wr&qh' ORDER BY id <=| 400 LIMIT 5;
EXPLAIN (costs off)
SELECT id, id |=> 400 FROM test_int8_h_a WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;
SELECT id, id |=> 400 FROM test_int8_h_a WHERE t @@ 'wr&qh' ORDER BY id |=> 400 LIMIT 5;

EXPLAIN (costs off)
SELECT id FROM test_int8_h_a WHERE  t @@ 'wr&qh' AND id <= 400::int8 ORDER BY id;
SELECT id FROM test_int8_h_a WHERE  t @@ 'wr&qh' AND id <= 400::int8 ORDER BY id;
EXPLAIN (costs off)
SELECT id FROM test_int8_h_a WHERE  t @@ 'wr&qh' AND id >= 400::int8 ORDER BY id;
SELECT id FROM test_int8_h_a WHERE  t @@ 'wr&qh' AND id >= 400::int8 ORDER BY id;

CREATE TABLE test_int8_id_t AS SELECT id::int8, t FROM tsts;

CREATE INDEX test_int8_id_t_idx ON test_int8_o USING rum
	(t rum_tsvector_ops, id);

EXPLAIN (costs off)
SELECT id FROM test_int8_h_a WHERE  t @@ 'wr&qh' AND id <= 400::int8 ORDER BY id <=> 400::int8;
SELECT id FROM test_int8_h_a WHERE  t @@ 'wr&qh' AND id <= 400::int8 ORDER BY id <=> 400::int8;
