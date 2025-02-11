CREATE EXTENSION rum;

CREATE TABLE test_rum( t text, a tsvector );

CREATE TRIGGER tsvectorupdate
BEFORE UPDATE OR INSERT ON test_rum
FOR EACH ROW EXECUTE PROCEDURE tsvector_update_trigger('a', 'pg_catalog.english', 't');
CREATE INDEX rumidx ON test_rum USING rum (a rum_tsvector_ops);

-- Check empty table using index scan
SELECT
	a <=> to_tsquery('pg_catalog.english', 'way & (go | half)'),
	rum_ts_distance(a, to_tsquery('pg_catalog.english', 'way & (go | half)')),
	rum_ts_score(a, to_tsquery('pg_catalog.english', 'way & (go | half)')),
	*
	FROM test_rum
	ORDER BY a <=> to_tsquery('pg_catalog.english', 'way & (go | half)') limit 2;

-- Fill the table with data
\copy test_rum(t) from 'data/rum.data';

CREATE INDEX failed_rumidx ON test_rum USING rum (a rum_tsvector_addon_ops);

CREATE TABLE tsts (id int, t tsvector, d timestamp);

\copy tsts from 'data/tsts.data'

CREATE INDEX tsts_idx ON tsts USING rum (t rum_tsvector_addon_ops, d)
	WITH (attach = 'd', to = 't');


INSERT INTO tsts VALUES (-1, 't1 t2',  '2016-05-02 02:24:22.326724');
INSERT INTO tsts VALUES (-2, 't1 t2 t3',  '2016-05-02 02:26:22.326724');

INSERT INTO test_rum (t) VALUES ('foo bar foo the over foo qq bar');
INSERT INTO test_rum (t) VALUES ('345 qwerty copyright');
INSERT INTO test_rum (t) VALUES ('345 qwerty');
INSERT INTO test_rum (t) VALUES ('A fat cat has just eaten a rat.');


SET enable_seqscan=off;
SET enable_indexscan=off;
