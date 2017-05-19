CREATE TABLE tstsh (id int, t tsvector, d timestamp);

\copy tstsh from 'data/tsts.data'

CREATE INDEX tstsh_idx ON tstsh USING rum (t rum_tsvector_hash_addon_ops, d)
	WITH (attach = 'd', to = 't');


INSERT INTO tstsh VALUES (-1, 't1 t2',  '2016-05-02 02:24:22.326724'); 
INSERT INTO tstsh VALUES (-2, 't1 t2 t3',  '2016-05-02 02:26:22.326724'); 


SELECT count(*) FROM tstsh WHERE t @@ 'wr|qh';
SELECT count(*) FROM tstsh WHERE t @@ 'wr&qh';
SELECT count(*) FROM tstsh WHERE t @@ 'eq&yt';
SELECT count(*) FROM tstsh WHERE t @@ 'eq|yt';
SELECT count(*) FROM tstsh WHERE t @@ '(eq&yt)|(wr&qh)';
SELECT count(*) FROM tstsh WHERE t @@ '(eq|yt)&(wr|qh)';

SET enable_indexscan=OFF;
SET enable_indexonlyscan=OFF;
SET enable_bitmapscan=OFF;
SELECT id, d, d <=> '2016-05-16 14:21:25' FROM tstsh WHERE t @@ 'wr&qh' ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d, d <=| '2016-05-16 14:21:25' FROM tstsh WHERE t @@ 'wr&qh' ORDER BY d <=| '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d, d |=> '2016-05-16 14:21:25' FROM tstsh WHERE t @@ 'wr&qh' ORDER BY d |=> '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d FROM tstsh WHERE  t @@ 'wr&qh' AND d <= '2016-05-16 14:21:25' ORDER BY d;
SELECT id, d FROM tstsh WHERE  t @@ 'wr&qh' AND d >= '2016-05-16 14:21:25' ORDER BY d;


RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_bitmapscan;
SET enable_seqscan = off;

EXPLAIN (costs off)
SELECT count(*) FROM tstsh WHERE t @@ 'wr|qh';
SELECT count(*) FROM tstsh WHERE t @@ 'wr|qh';
SELECT count(*) FROM tstsh WHERE t @@ 'wr&qh';
SELECT count(*) FROM tstsh WHERE t @@ 'eq&yt';
SELECT count(*) FROM tstsh WHERE t @@ 'eq|yt';
SELECT count(*) FROM tstsh WHERE t @@ '(eq&yt)|(wr&qh)';
SELECT count(*) FROM tstsh WHERE t @@ '(eq|yt)&(wr|qh)';

EXPLAIN (costs off)
SELECT id, d, d <=> '2016-05-16 14:21:25' FROM tstsh WHERE t @@ 'wr&qh' ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d, d <=> '2016-05-16 14:21:25' FROM tstsh WHERE t @@ 'wr&qh' ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;
EXPLAIN (costs off)
SELECT id, d, d <=| '2016-05-16 14:21:25' FROM tstsh WHERE t @@ 'wr&qh' ORDER BY d <=| '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d, d <=| '2016-05-16 14:21:25' FROM tstsh WHERE t @@ 'wr&qh' ORDER BY d <=| '2016-05-16 14:21:25' LIMIT 5;
EXPLAIN (costs off)
SELECT id, d, d |=> '2016-05-16 14:21:25' FROM tstsh WHERE t @@ 'wr&qh' ORDER BY d |=> '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d, d |=> '2016-05-16 14:21:25' FROM tstsh WHERE t @@ 'wr&qh' ORDER BY d |=> '2016-05-16 14:21:25' LIMIT 5;

EXPLAIN (costs off)
SELECT id, d, d <=> '2016-05-16 14:21:25' FROM tstsh ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d, d <=> '2016-05-16 14:21:25' FROM tstsh ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;

EXPLAIN (costs off)
SELECT id, d FROM tstsh WHERE  t @@ 'wr&qh' AND d <= '2016-05-16 14:21:25' ORDER BY d;
SELECT id, d FROM tstsh WHERE  t @@ 'wr&qh' AND d <= '2016-05-16 14:21:25' ORDER BY d;
EXPLAIN (costs off)
SELECT id, d FROM tstsh WHERE  t @@ 'wr&qh' AND d >= '2016-05-16 14:21:25' ORDER BY d;
SELECT id, d FROM tstsh WHERE  t @@ 'wr&qh' AND d >= '2016-05-16 14:21:25' ORDER BY d;

SET enable_bitmapscan=OFF;

EXPLAIN (costs off)
SELECT id, d FROM tstsh WHERE  t @@ 'wr&qh' AND d <= '2016-05-16 14:21:25' ORDER BY d;
SELECT id, d FROM tstsh WHERE  t @@ 'wr&qh' AND d <= '2016-05-16 14:21:25' ORDER BY d;
EXPLAIN (costs off)
SELECT id, d FROM tstsh WHERE  t @@ 'wr&qh' AND d >= '2016-05-16 14:21:25' ORDER BY d;
SELECT id, d FROM tstsh WHERE  t @@ 'wr&qh' AND d >= '2016-05-16 14:21:25' ORDER BY d;


SELECT id, d FROM tstsh WHERE  t @@ 'wr&qh' AND d <= '2016-05-16 14:21:25' ORDER BY d ASC LIMIT 3;
SELECT id, d FROM tstsh WHERE  t @@ 'wr&qh' AND d <= '2016-05-16 14:21:25' ORDER BY d DESC LIMIT 3;
SELECT id, d FROM tstsh WHERE  t @@ 'wr&qh' AND d >= '2016-05-16 14:21:25' ORDER BY d ASC LIMIT 3;
SELECT id, d FROM tstsh WHERE  t @@ 'wr&qh' AND d >= '2016-05-16 14:21:25' ORDER BY d DESC LIMIT 3;

-- Test multicolumn index

RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_bitmapscan;
SET enable_seqscan = off;

DROP INDEX tstsh_idx;

CREATE INDEX tstsh_id_idx ON tsts USING rum (t rum_tsvector_addon_ops, id, d)
	WITH (attach = 'd', to = 't');

EXPLAIN (costs off)
SELECT id, d FROM tsts WHERE t @@ 'wr&qh' AND id = 1::int ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d FROM tsts WHERE t @@ 'wr&qh' AND id = 1::int ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;

EXPLAIN (costs off)
SELECT id, d FROM tsts WHERE t @@ 'wr&qh' AND id = 355::int ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d FROM tsts WHERE t @@ 'wr&qh' AND id = 355::int ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;

EXPLAIN (costs off)
SELECT id, d FROM tsts WHERE t @@ 'wr&qh' AND d = '2016-05-11 11:21:22.326724'::timestamp ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d FROM tsts WHERE t @@ 'wr&qh' AND d = '2016-05-11 11:21:22.326724'::timestamp ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;

EXPLAIN (costs off)
SELECT id, d FROM tsts WHERE t @@ 'wr&qh' AND d = '2000-05-01'::timestamp ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d FROM tsts WHERE t @@ 'wr&qh' AND d = '2000-05-01'::timestamp ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;
