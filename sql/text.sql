/*
 * ------------------------------------
 *  NOTE: This test behaves differenly
 * ------------------------------------
 *
 * Since c01743aa486 (>=18) EXPLAIN output was changed,
 * now it includes the number of disabled nodes
 *
 * text.out - test output for PostgreSQL (<18)
 * text_1.out - for PostgreSQL (>=18)
 *
 */
set enable_seqscan=off;

CREATE TABLE test_text (
	i text
);

INSERT INTO test_text VALUES ('a'),('ab'),('abc'),('abb'),('axy'),('xyz');

CREATE INDEX idx_text ON test_text USING rum (i);

SELECT * FROM test_text WHERE i<'abc' ORDER BY i;
SELECT * FROM test_text WHERE i<='abc' ORDER BY i;
SELECT * FROM test_text WHERE i='abc' ORDER BY i;
SELECT * FROM test_text WHERE i>='abc' ORDER BY i;
SELECT * FROM test_text WHERE i>'abc' ORDER BY i;

CREATE TABLE test_text_o AS SELECT id::text, t FROM tsts;

SELECT id FROM test_text_o WHERE  t @@ 'wr&qh' AND id <= '400' ORDER BY id;
SELECT id FROM test_text_o WHERE  t @@ 'wr&qh' AND id >= '400' ORDER BY id;

CREATE INDEX test_text_o_idx ON test_text_o USING rum
    (t rum_tsvector_addon_ops, id)
    WITH (attach = 'id', to = 't');

RESET enable_indexscan;
RESET enable_indexonlyscan;
SET enable_bitmapscan=OFF;
SET enable_seqscan = off;

EXPLAIN (costs off)
SELECT id FROM test_text_o WHERE  t @@ 'wr&qh' AND id <= '400' ORDER BY id;
SELECT id FROM test_text_o WHERE  t @@ 'wr&qh' AND id <= '400' ORDER BY id;
EXPLAIN (costs off)
SELECT id FROM test_text_o WHERE  t @@ 'wr&qh' AND id >= '400' ORDER BY id;
SELECT id FROM test_text_o WHERE  t @@ 'wr&qh' AND id >= '400' ORDER BY id;

CREATE TABLE test_text_a AS SELECT id::text, t FROM tsts;

-- Should fail, temporarly it isn't allowed to order an index over pass-by-reference column
CREATE INDEX test_text_a_idx ON test_text_a USING rum
	(t rum_tsvector_addon_ops, id)
	WITH (attach = 'id', to = 't', order_by_attach='t');

EXPLAIN (costs off)
SELECT count(*) FROM test_text_a WHERE id < '400';
SELECT count(*) FROM test_text_a WHERE id < '400';

EXPLAIN (costs off)
SELECT id FROM test_text_a WHERE  t @@ 'wr&qh' AND id <= '400' ORDER BY id;
SELECT id FROM test_text_a WHERE  t @@ 'wr&qh' AND id <= '400' ORDER BY id;
EXPLAIN (costs off)
SELECT id FROM test_text_a WHERE  t @@ 'wr&qh' AND id >= '400' ORDER BY id;
SELECT id FROM test_text_a WHERE  t @@ 'wr&qh' AND id >= '400' ORDER BY id;

CREATE TABLE test_text_h_o AS SELECT id::text, t FROM tsts;

CREATE INDEX test_text_h_o_idx ON test_text_h_o USING rum
	(t rum_tsvector_hash_addon_ops, id)
	WITH (attach = 'id', to = 't');

EXPLAIN (costs off)
SELECT id FROM test_text_h_o WHERE  t @@ 'wr&qh' AND id <= '400' ORDER BY id;
SELECT id FROM test_text_h_o WHERE  t @@ 'wr&qh' AND id <= '400' ORDER BY id;
EXPLAIN (costs off)
SELECT id FROM test_text_h_o WHERE  t @@ 'wr&qh' AND id >= '400' ORDER BY id;
SELECT id FROM test_text_h_o WHERE  t @@ 'wr&qh' AND id >= '400' ORDER BY id;

CREATE TABLE test_text_h_a AS SELECT id::text, t FROM tsts;

-- Should fail, temporarly it isn't allowed to order an index over pass-by-reference column
CREATE INDEX test_text_h_a_idx ON test_text_h_a USING rum
	(t rum_tsvector_hash_addon_ops, id)
	WITH (attach = 'id', to = 't', order_by_attach='t');

EXPLAIN (costs off)
SELECT count(*) FROM test_text_h_a WHERE id < '400';
SELECT count(*) FROM test_text_h_a WHERE id < '400';

EXPLAIN (costs off)
SELECT id FROM test_text_h_a WHERE  t @@ 'wr&qh' AND id <= '400' ORDER BY id;
SELECT id FROM test_text_h_a WHERE  t @@ 'wr&qh' AND id <= '400' ORDER BY id;
EXPLAIN (costs off)
SELECT id FROM test_text_h_a WHERE  t @@ 'wr&qh' AND id >= '400' ORDER BY id;
SELECT id FROM test_text_h_a WHERE  t @@ 'wr&qh' AND id >= '400' ORDER BY id;

