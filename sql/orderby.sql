CREATE TABLE tsts (id int, t tsvector, d timestamp);

\copy tsts from 'data/tsts.data'

CREATE INDEX tsts_idx ON tsts USING rum (t rum_tsvector_ops, d) WITH (orderby = 'd');

