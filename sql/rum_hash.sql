CREATE TABLE test_rum( t text, a tsvector );

CREATE TRIGGER tsvectorupdate
BEFORE UPDATE OR INSERT ON test_rum
FOR EACH ROW EXECUTE PROCEDURE tsvector_update_trigger('a', 'pg_catalog.english', 't');
CREATE INDEX rumidx ON test_rum USING rum (a rum_tsvector_hash_ops);

\copy test_rum(t) from 'data/rum.data';

CREATE INDEX failed_rumidx ON test_rum USING rum (a rum_tsvector_timestamp_ops);

SET enable_seqscan=off;

explain (costs off)
SELECT count(*) FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english', 'ever|wrote');
explain (costs off)
SELECT * FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english', 'ever|wrote')
ORDER BY a <=> to_tsquery('pg_catalog.english', 'ever|wrote');
explain (costs off)
SELECT count(*) FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english',
													'def <-> fgr');

SELECT count(*) FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english', 'ever|wrote');
SELECT count(*) FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english', 'have&wish');
SELECT count(*) FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english', 'knew&brain');
SELECT count(*) FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english', 'among');
SELECT count(*) FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english', 'structure&ancient');
SELECT count(*) FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english', '(complimentary|sight)&(sending|heart)');
SELECT count(*) FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english', '(gave | half) <-> way');
SELECT count(*) FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english', '(gave | !half) <-> way');
SELECT count(*) FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english', '!gave & way');
SELECT count(*) FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english', '!gave & wooded & !look');
SELECT count(*) FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english',
													'def <-> fgr');
SELECT count(*) FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english',
													'def <2> fgr');
SELECT rum_ts_distance(a, to_tsquery('pg_catalog.english', 'way')), *
	FROM test_rum
	WHERE a @@ to_tsquery('pg_catalog.english', 'way')
	ORDER BY a <=> to_tsquery('pg_catalog.english', 'way');
SELECT rum_ts_distance(a, to_tsquery('pg_catalog.english', 'way & (go | half)')), *
	FROM test_rum
	WHERE a @@ to_tsquery('pg_catalog.english', 'way & (go | half)')
	ORDER BY a <=> to_tsquery('pg_catalog.english', 'way & (go | half)');
SELECT
	a <=> to_tsquery('pg_catalog.english', 'way & (go | half)'), 
	rum_ts_distance(a, to_tsquery('pg_catalog.english', 'way & (go | half)')),
	*
	FROM test_rum
	ORDER BY a <=> to_tsquery('pg_catalog.english', 'way & (go | half)') limit 2;

-- Check ranking normalization
SELECT rum_ts_distance(a, to_tsquery('pg_catalog.english', 'way'), 0), *
	FROM test_rum
	WHERE a @@ to_tsquery('pg_catalog.english', 'way')
	ORDER BY a <=> to_tsquery('pg_catalog.english', 'way');
SELECT rum_ts_distance(a, row(to_tsquery('pg_catalog.english', 'way & (go | half)'), 0)::rum_distance_query), *
	FROM test_rum
	WHERE a @@ to_tsquery('pg_catalog.english', 'way & (go | half)')
	ORDER BY a <=> to_tsquery('pg_catalog.english', 'way & (go | half)');

INSERT INTO test_rum (t) VALUES ('foo bar foo the over foo qq bar');
INSERT INTO test_rum (t) VALUES ('345 qwerty copyright');
INSERT INTO test_rum (t) VALUES ('345 qwerty');
INSERT INTO test_rum (t) VALUES ('A fat cat has just eaten a rat.');

SELECT count(*) FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english', 'bar');
SELECT count(*) FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english', 'qwerty&345');
SELECT count(*) FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english', '345');
SELECT count(*) FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english', 'rat');

SELECT a FROM test_rum WHERE a @@ to_tsquery('pg_catalog.english', 'bar') ORDER BY a;

-- Check full-index scan with order by
SELECT a <=> to_tsquery('pg_catalog.english', 'ever|wrote') FROM test_rum ORDER BY a <=> to_tsquery('pg_catalog.english', 'ever|wrote');

CREATE TABLE tst (i int4, t tsvector);
INSERT INTO tst SELECT i%10, to_tsvector('simple', substr(md5(i::text), 1, 1)) FROM generate_series(1,100000) i;
CREATE INDEX tstidx ON tst USING rum (t rum_tsvector_hash_ops);

DELETE FROM tst WHERE i = 1;
VACUUM tst;
INSERT INTO tst SELECT i%10, to_tsvector('simple', substr(md5(i::text), 1, 1)) FROM generate_series(10001,11000) i;

DELETE FROM tst WHERE i = 2;
VACUUM tst;
INSERT INTO tst SELECT i%10, to_tsvector('simple', substr(md5(i::text), 1, 1)) FROM generate_series(11001,12000) i;

DELETE FROM tst WHERE i = 3;
VACUUM tst;
INSERT INTO tst SELECT i%10, to_tsvector('simple', substr(md5(i::text), 1, 1)) FROM generate_series(12001,13000) i;

DELETE FROM tst WHERE i = 4;
VACUUM tst;
INSERT INTO tst SELECT i%10, to_tsvector('simple', substr(md5(i::text), 1, 1)) FROM generate_series(13001,14000) i;

DELETE FROM tst WHERE i = 5;
VACUUM tst;
INSERT INTO tst SELECT i%10, to_tsvector('simple', substr(md5(i::text), 1, 1)) FROM generate_series(14001,15000) i;

set enable_bitmapscan=off;
explain (costs off)
SELECT a <=> to_tsquery('pg_catalog.english', 'w:*'), *
	FROM test_rum
	WHERE a @@ to_tsquery('pg_catalog.english', 'w:*')
	ORDER BY a <=> to_tsquery('pg_catalog.english', 'w:*');
SELECT a <=> to_tsquery('pg_catalog.english', 'w:*'), *
	FROM test_rum
	WHERE a @@ to_tsquery('pg_catalog.english', 'w:*')
	ORDER BY a <=> to_tsquery('pg_catalog.english', 'w:*');

DROP TABLE test_rum CASCADE;
DROP TABLE tst CASCADE;
