CREATE TABLE test_invrum(q tsquery);

INSERT INTO test_invrum VALUES ('a|b'::tsquery);
INSERT INTO test_invrum VALUES ('a&b'::tsquery);
INSERT INTO test_invrum VALUES ('!(a|b)'::tsquery);
INSERT INTO test_invrum VALUES ('!(a&b)'::tsquery);
INSERT INTO test_invrum VALUES ('!a|b'::tsquery);
INSERT INTO test_invrum VALUES ('a&!b'::tsquery);
INSERT INTO test_invrum VALUES ('(a|b)&c'::tsquery);
INSERT INTO test_invrum VALUES ('(!(a|b))&c'::tsquery);
INSERT INTO test_invrum VALUES ('(a|b)&(c|d)'::tsquery);
INSERT INTO test_invrum VALUES ('!a'::tsquery);
INSERT INTO test_invrum VALUES ('(a|a1|a2|a3|a4|a5)&(b|b1|b2|b3|b4|b5|b6)&!(c|c1|c2|c3)'::tsquery);

SELECT * FROM test_invrum WHERE q @@ ''::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'a'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'b'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'a b'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'c'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'a c'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'b c'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'a b c'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'd'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'a d'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'b d'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'a b d'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'c d'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'a c d'::tsvector;

CREATE INDEX test_invrum_idx ON test_invrum USING rum(q);
SET enable_seqscan = OFF;

SELECT * FROM test_invrum WHERE q @@ ''::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'a'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'b'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'a b'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'c'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'a c'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'b c'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'a b c'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'd'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'a d'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'b d'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'a b d'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'c d'::tsvector;
SELECT * FROM test_invrum WHERE q @@ 'a c d'::tsvector;

INSERT INTO test_invrum VALUES ('a:*'::tsquery);
INSERT INTO test_invrum VALUES ('a <-> b'::tsquery);
