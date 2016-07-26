CREATE TABLE tsts (id int, t tsvector, d timestamp);

\copy tsts from 'data/tsts.data'

CREATE INDEX tsts_idx ON tsts USING rum (t rum_tsvector_timestamp_ops, d)
	WITH (attach = 'd', to = 't');


INSERT INTO tsts VALUES (-1, 't1 t2',  '2016-05-02 02:24:22.326724'); 
INSERT INTO tsts VALUES (-2, 't1 t2 t3',  '2016-05-02 02:26:22.326724'); 


SELECT count(*) FROM tsts WHERE t @@ 'wr|qh';
SELECT count(*) FROM tsts WHERE t @@ 'wr&qh';
SELECT count(*) FROM tsts WHERE t @@ 'eq&yt';
SELECT count(*) FROM tsts WHERE t @@ 'eq|yt';
SELECT count(*) FROM tsts WHERE t @@ '(eq&yt)|(wr&qh)';
SELECT count(*) FROM tsts WHERE t @@ '(eq|yt)&(wr|qh)';

SET enable_indexscan=OFF;
SET enable_indexonlyscan=OFF;
SET enable_bitmapscan=OFF;
SELECT id, d, d <=> '2016-05-16 14:21:25' FROM tsts WHERE t @@ 'wr&qh' ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d, d <=| '2016-05-16 14:21:25' FROM tsts WHERE t @@ 'wr&qh' ORDER BY d <=| '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d, d |=> '2016-05-16 14:21:25' FROM tsts WHERE t @@ 'wr&qh' ORDER BY d |=> '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d FROM tsts WHERE  t @@ 'wr&qh' AND d <= '2016-05-16 14:21:25' ORDER BY d;
SELECT id, d FROM tsts WHERE  t @@ 'wr&qh' AND d >= '2016-05-16 14:21:25' ORDER BY d;


RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_bitmapscan;
SET enable_seqscan = off;

EXPLAIN (costs off)
SELECT count(*) FROM tsts WHERE t @@ 'wr|qh';
SELECT count(*) FROM tsts WHERE t @@ 'wr|qh';
SELECT count(*) FROM tsts WHERE t @@ 'wr&qh';
SELECT count(*) FROM tsts WHERE t @@ 'eq&yt';
SELECT count(*) FROM tsts WHERE t @@ 'eq|yt';
SELECT count(*) FROM tsts WHERE t @@ '(eq&yt)|(wr&qh)';
SELECT count(*) FROM tsts WHERE t @@ '(eq|yt)&(wr|qh)';

EXPLAIN (costs off)
SELECT id, d, d <=> '2016-05-16 14:21:25' FROM tsts WHERE t @@ 'wr&qh' ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d, d <=> '2016-05-16 14:21:25' FROM tsts WHERE t @@ 'wr&qh' ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;
EXPLAIN (costs off)
SELECT id, d, d <=| '2016-05-16 14:21:25' FROM tsts WHERE t @@ 'wr&qh' ORDER BY d <=| '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d, d <=| '2016-05-16 14:21:25' FROM tsts WHERE t @@ 'wr&qh' ORDER BY d <=| '2016-05-16 14:21:25' LIMIT 5;
EXPLAIN (costs off)
SELECT id, d, d |=> '2016-05-16 14:21:25' FROM tsts WHERE t @@ 'wr&qh' ORDER BY d |=> '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d, d |=> '2016-05-16 14:21:25' FROM tsts WHERE t @@ 'wr&qh' ORDER BY d |=> '2016-05-16 14:21:25' LIMIT 5;

EXPLAIN (costs off)
SELECT id, d, d <=> '2016-05-16 14:21:25' FROM tsts ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;
SELECT id, d, d <=> '2016-05-16 14:21:25' FROM tsts ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;

EXPLAIN (costs off)
SELECT id, d FROM tsts WHERE  t @@ 'wr&qh' AND d <= '2016-05-16 14:21:25' ORDER BY d;
SELECT id, d FROM tsts WHERE  t @@ 'wr&qh' AND d <= '2016-05-16 14:21:25' ORDER BY d;
EXPLAIN (costs off)
SELECT id, d FROM tsts WHERE  t @@ 'wr&qh' AND d >= '2016-05-16 14:21:25' ORDER BY d;
SELECT id, d FROM tsts WHERE  t @@ 'wr&qh' AND d >= '2016-05-16 14:21:25' ORDER BY d;

SET enable_bitmapscan=OFF;

EXPLAIN (costs off)
SELECT id, d FROM tsts WHERE  t @@ 'wr&qh' AND d <= '2016-05-16 14:21:25' ORDER BY d;
SELECT id, d FROM tsts WHERE  t @@ 'wr&qh' AND d <= '2016-05-16 14:21:25' ORDER BY d;
EXPLAIN (costs off)
SELECT id, d FROM tsts WHERE  t @@ 'wr&qh' AND d >= '2016-05-16 14:21:25' ORDER BY d;
SELECT id, d FROM tsts WHERE  t @@ 'wr&qh' AND d >= '2016-05-16 14:21:25' ORDER BY d;


SELECT id, d FROM tsts WHERE  t @@ 'wr&qh' AND d <= '2016-05-16 14:21:25' ORDER BY d ASC LIMIT 3;
SELECT id, d FROM tsts WHERE  t @@ 'wr&qh' AND d <= '2016-05-16 14:21:25' ORDER BY d DESC LIMIT 3;
SELECT id, d FROM tsts WHERE  t @@ 'wr&qh' AND d >= '2016-05-16 14:21:25' ORDER BY d ASC LIMIT 3;
SELECT id, d FROM tsts WHERE  t @@ 'wr&qh' AND d >= '2016-05-16 14:21:25' ORDER BY d DESC LIMIT 3;
