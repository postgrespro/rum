CREATE TABLE documents (
    en text not null,
    score float not null,
    textsearch_index_en_col tsvector
);

INSERT INTO documents VALUES ('the pet cat is in the shed', 56, to_tsvector('english', 'the pet cat is in the shed'));

CREATE INDEX textsearch_index_en ON documents
    USING rum (textsearch_index_en_col rum_tsvector_addon_ops, score)
    WITH (attach = 'score', to = 'textsearch_index_en_col');

SET enable_seqscan=off;
-- should be 1 row
SELECT * FROM documents WHERE textsearch_index_en_col @@ ('pet'::tsquery <-> ('dog'::tsquery || 'cat'::tsquery));

SET enable_seqscan=on;
-- 1 row
SELECT * FROM documents WHERE textsearch_index_en_col @@ ('pet'::tsquery <-> ('dog'::tsquery || 'cat'::tsquery));

DROP TABLE documents;
