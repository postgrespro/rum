CREATE TABLE atsts (id int, t tsvector, d timestamp);

\copy atsts from 'data/tsts.data'

CREATE INDEX atsts_idx ON atsts USING rum (t rum_tsvector_hash_timestamp_ops, d)
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

RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_bitmapscan;
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

DROP TABLE atsts CASCADE;
