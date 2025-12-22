/*
 * ------------------------------------
 *  NOTE: This test behaves differenly
 * ------------------------------------
 *
 * altorder.out - test output for 64-bit systems
 * altorder_1.out - test output for 32-bit systems
 *
 * Since c01743aa486 and 161320b4b96 (>=18) EXPLAIN output was changed,
 * now it shows whether nodes are disabled.
 *
 * altorder_2.out - test output for 32-bit systems (>=18)
 *
 */
CREATE TABLE atsts (id int, t tsvector, d timestamp);

\copy atsts from 'data/tsts.data'
-- PGPRO-2537: We need more data to test rumsort.c with logtape.c
\copy atsts from 'data/tsts.data'
\copy atsts from 'data/tsts.data'
\copy atsts from 'data/tsts.data'

CREATE INDEX atsts_idx ON atsts USING rum (t rum_tsvector_addon_ops, d)
	WITH (attach = 'd', to = 't', order_by_attach='t');


INSERT INTO atsts VALUES (-1, 't1 t2',  '2016-05-02 02:24:22.326724');
INSERT INTO atsts VALUES (-2, 't1 t2 t3',  '2016-05-02 02:26:22.326724');


SELECT count(*) FROM atsts WHERE t @@ 'wr|qh';
SELECT count(*) FROM atsts WHERE t @@ 'wr&qh';
SELECT count(*) FROM atsts WHERE t @@ 'eq&yt';
SELECT count(*) FROM atsts WHERE t @@ 'eq|yt';
SELECT count(*) FROM atsts WHERE t @@ '(eq&yt)|(wr&qh)';
SELECT count(*) FROM atsts WHERE t @@ '(eq|yt)&(wr|qh)';

SET enable_indexscan=OFF;
SET enable_indexonlyscan=OFF;
SET enable_bitmapscan=OFF;
SELECT id, d, d <=> '2016-05-16 14:21:25' FROM atsts WHERE t @@ 'wr&qh' ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d, d <=| '2016-05-16 14:21:25' FROM atsts WHERE t @@ 'wr&qh' ORDER BY d <=| '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d, d |=> '2016-05-16 14:21:25' FROM atsts WHERE t @@ 'wr&qh' ORDER BY d |=> '2016-05-16 14:21:25' LIMIT 5;

SELECT count(*) FROM atsts WHERE d < '2016-05-16 14:21:25';
SELECT count(*) FROM atsts WHERE d > '2016-05-16 14:21:25';

SELECT id, d FROM atsts WHERE  t @@ 'wr&qh' AND d <= '2016-05-16 14:21:25' ORDER BY d;
SELECT id, d FROM atsts WHERE  t @@ 'wr&qh' AND d >= '2016-05-16 14:21:25' ORDER BY d;

-- Test bitmap index scan
SET enable_bitmapscan=on;
SET enable_seqscan = off;

EXPLAIN (costs off)
SELECT count(*) FROM atsts WHERE t @@ 'wr|qh';
SELECT count(*) FROM atsts WHERE t @@ 'wr|qh';
SELECT count(*) FROM atsts WHERE t @@ 'wr&qh';
SELECT count(*) FROM atsts WHERE t @@ 'eq&yt';
SELECT count(*) FROM atsts WHERE t @@ 'eq|yt';
SELECT count(*) FROM atsts WHERE t @@ '(eq&yt)|(wr&qh)';
SELECT count(*) FROM atsts WHERE t @@ '(eq|yt)&(wr|qh)';


EXPLAIN (costs off)
SELECT count(*) FROM atsts WHERE d < '2016-05-16 14:21:25';
SELECT count(*) FROM atsts WHERE d < '2016-05-16 14:21:25';
EXPLAIN (costs off)
SELECT count(*) FROM atsts WHERE d > '2016-05-16 14:21:25';
SELECT count(*) FROM atsts WHERE d > '2016-05-16 14:21:25';

-- Test index scan
SET enable_indexscan=on;
SET enable_indexonlyscan=on;
SET enable_bitmapscan=off;

EXPLAIN (costs off)
SELECT id, d, d <=> '2016-05-16 14:21:25' FROM atsts WHERE t @@ 'wr&qh' ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d, d <=> '2016-05-16 14:21:25' FROM atsts WHERE t @@ 'wr&qh' ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;
EXPLAIN (costs off)
SELECT id, d, d <=| '2016-05-16 14:21:25' FROM atsts WHERE t @@ 'wr&qh' ORDER BY d <=| '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d, d <=| '2016-05-16 14:21:25' FROM atsts WHERE t @@ 'wr&qh' ORDER BY d <=| '2016-05-16 14:21:25' LIMIT 5;
EXPLAIN (costs off)
SELECT id, d, d |=> '2016-05-16 14:21:25' FROM atsts WHERE t @@ 'wr&qh' ORDER BY d |=> '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d, d |=> '2016-05-16 14:21:25' FROM atsts WHERE t @@ 'wr&qh' ORDER BY d |=> '2016-05-16 14:21:25' LIMIT 5;

EXPLAIN (costs off)
SELECT id, d, d <=> '2016-05-16 14:21:25' FROM atsts ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d, d <=> '2016-05-16 14:21:25' FROM atsts ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;

EXPLAIN (costs off)
SELECT id, d FROM atsts WHERE  t @@ 'wr&qh' AND d <= '2016-05-16 14:21:25' ORDER BY d;
SELECT id, d FROM atsts WHERE  t @@ 'wr&qh' AND d <= '2016-05-16 14:21:25' ORDER BY d;
EXPLAIN (costs off)
SELECT id, d FROM atsts WHERE  t @@ 'wr&qh' AND d >= '2016-05-16 14:21:25' ORDER BY d;
SELECT id, d FROM atsts WHERE  t @@ 'wr&qh' AND d >= '2016-05-16 14:21:25' ORDER BY d;

EXPLAIN (costs off)
SELECT id, d FROM atsts WHERE  t @@ 'wr&q:*' AND d >= '2016-05-16 14:21:25' ORDER BY d;
SELECT id, d FROM atsts WHERE  t @@ 'wr&q:*' AND d >= '2016-05-16 14:21:25' ORDER BY d;

CREATE TABLE test_table (id bigint, folder bigint, time bigint, tsv tsvector);
CREATE INDEX test_idx ON test_table USING rum(folder, tsv rum_tsvector_addon_ops, time) with (attach = 'time', to = 'tsv', order_by_attach=TRUE);

INSERT INTO test_table (id, folder, time, tsv) VALUES
	(1, 10, 100, to_tsvector('wordA')),
	(2, 20, 200, to_tsvector('wordB')),
	(3, 10, 300, to_tsvector('wordA')),
	(4, 20, 400, to_tsvector('wordB')),
	(5, 20, 60,  to_tsvector('wordB')),
	(6, 10, 40,  to_tsvector('wordA')),
	(7, 20, 50,  to_tsvector('wordB')),
	(8, 10, 30,  to_tsvector('wordA'));

EXPLAIN (costs off)
SELECT * FROM test_table WHERE tsv @@ (to_tsquery('wordA')) AND (folder = 10::bigint);
SELECT * FROM test_table WHERE tsv @@ (to_tsquery('wordA')) AND (folder = 10::bigint);

EXPLAIN (costs off)
SELECT * FROM test_table WHERE tsv @@ (to_tsquery('wordA')) AND (folder = 10::bigint) ORDER BY time <=| 500::bigint;
SELECT * FROM test_table WHERE tsv @@ (to_tsquery('wordA')) AND (folder = 10::bigint) ORDER BY time <=| 500::bigint;

