CREATE TABLE test_debug_table (id int, t tsvector, d timestamp);

\copy test_debug_table from 'data/tsts.data'
\copy test_debug_table from 'data/tsts.data'
\copy test_debug_table from 'data/tsts.data'
\copy test_debug_table from 'data/tsts.data'
\copy test_debug_table from 'data/tsts.data'

-- It is necessary to create all types of pages in the Posting Tree
DO $$
BEGIN
    FOR i IN 1..5000 LOOP
      INSERT INTO test_debug_table(id, t, d) 
      VALUES(i, 'b9', '2016-05-02 00:21:22.326724');
    END LOOP;
END;
$$;

-- Testing on 32-bit and 64-bit machine on the index without additional information
CREATE INDEX test_without_addinfo_idx ON test_debug_table
USING rum (t, d);

SELECT * FROM rum_metapage_info('test_without_addinfo_idx', 0) \gx

SELECT * FROM rum_page_opaque_info('test_without_addinfo_idx', 1);
SELECT * FROM rum_internal_entry_page_items('test_without_addinfo_idx', 1);

SELECT * FROM rum_page_opaque_info('test_without_addinfo_idx', 10);
SELECT * FROM rum_leaf_entry_page_items('test_without_addinfo_idx', 10);

SELECT * FROM rum_page_opaque_info('test_without_addinfo_idx', 7);
SELECT * FROM rum_internal_data_page_items('test_without_addinfo_idx', 7);

SELECT * FROM rum_page_opaque_info('test_without_addinfo_idx', 9);
SELECT * FROM rum_leaf_data_page_items('test_without_addinfo_idx', 9);

-- Testing on the index with additional information (order_by_attach = false)
CREATE INDEX test_with_addinfo_idx_false ON test_debug_table 
USING rum (t rum_tsvector_addon_ops, d) 
WITH (attach = 'd', to = 't', order_by_attach='f');

SELECT * FROM rum_metapage_info('test_with_addinfo_idx_false', 0) \gx

SELECT * FROM rum_page_opaque_info('test_with_addinfo_idx_false', 1);
SELECT * FROM rum_internal_entry_page_items('test_with_addinfo_idx_false', 1);

-- 64-bit machine
SELECT * FROM rum_page_opaque_info('test_with_addinfo_idx_false', 28);
SELECT * FROM rum_leaf_entry_page_items('test_with_addinfo_idx_false', 28);

SELECT * FROM rum_page_opaque_info('test_with_addinfo_idx_false', 19);
SELECT * FROM rum_internal_data_page_items('test_with_addinfo_idx_false', 19);

SELECT * FROM rum_page_opaque_info('test_with_addinfo_idx_false', 20);
SELECT * FROM rum_leaf_data_page_items('test_with_addinfo_idx_false', 20);

-- 32-bit machine
SELECT * FROM rum_page_opaque_info('test_with_addinfo_idx_false', 32);
SELECT * FROM rum_leaf_entry_page_items('test_with_addinfo_idx_false', 32);

SELECT * FROM rum_page_opaque_info('test_with_addinfo_idx_false', 22);
SELECT * FROM rum_internal_data_page_items('test_with_addinfo_idx_false', 22);

SELECT * FROM rum_page_opaque_info('test_with_addinfo_idx_false', 27);
SELECT * FROM rum_leaf_data_page_items('test_with_addinfo_idx_false', 27);

-- Testing on 32-bit and 64-bit on the index with additional information (order_by_attach = true)
CREATE INDEX test_with_addinfo_idx_true ON test_debug_table 
USING rum (t rum_tsvector_addon_ops, id) 
WITH (attach = 'id', to = 't', order_by_attach='t');

SELECT * FROM rum_metapage_info('test_with_addinfo_idx_true', 0) \gx

SELECT * FROM rum_page_opaque_info('test_with_addinfo_idx_true', 1);
SELECT * FROM rum_internal_entry_page_items('test_with_addinfo_idx_true', 1);

SELECT * FROM rum_page_opaque_info('test_with_addinfo_idx_true', 27);
SELECT * FROM rum_leaf_entry_page_items('test_with_addinfo_idx_true', 27);

SELECT * FROM rum_page_opaque_info('test_with_addinfo_idx_true', 19);
SELECT * FROM rum_internal_data_page_items('test_with_addinfo_idx_true', 19);

SELECT * FROM rum_page_opaque_info('test_with_addinfo_idx_true', 22);
SELECT * FROM rum_leaf_data_page_items('test_with_addinfo_idx_true', 22);

DROP TABLE test_debug_table;

-- Проверяем с позициями лексем
CREATE TABLE test_debug_table_with_weight(t text, a tsvector, r text);

CREATE FUNCTION fill_test_debug_weight_trigger() RETURNS trigger AS $$
begin
  new.a :=
     setweight(to_tsvector('pg_catalog.english', coalesce(new.r,'')), 'A') ||
     setweight(to_tsvector('pg_catalog.english', coalesce(new.t,'')), 'D');
  return new;
end
$$ LANGUAGE plpgsql;

CREATE TRIGGER test_debug_weight_trigger
BEFORE INSERT OR UPDATE ON test_debug_table_with_weight
FOR EACH ROW EXECUTE PROCEDURE fill_test_debug_weight_trigger();

\copy test_debug_table_with_weight(t,r) FROM 'data/rum_weight.data' DELIMITER '|';
\copy test_debug_table_with_weight(t,r) FROM 'data/rum_weight.data' DELIMITER '|';
\copy test_debug_table_with_weight(t,r) FROM 'data/rum_weight.data' DELIMITER '|';
\copy test_debug_table_with_weight(t,r) FROM 'data/rum_weight.data' DELIMITER '|';
\copy test_debug_table_with_weight(t,r) FROM 'data/rum_weight.data' DELIMITER '|';

DO $$
BEGIN
    FOR i IN 1..5000 LOOP
      INSERT INTO test_debug_table_with_weight(t,r) 
      VALUES('As a reward for your reformation I write to you on this precious sheet.', 'write');
    END LOOP;
END;
$$;

CREATE INDEX test_with_weight_idx ON test_debug_table_with_weight 
USING rum (a rum_tsvector_ops);

SELECT * FROM rum_metapage_info('test_with_weight_idx', 0) \gx

SELECT * FROM rum_page_opaque_info('test_with_weight_idx', 1);
SELECT * FROM rum_internal_entry_page_items('test_with_weight_idx', 1);

-- 64-bit machine
SELECT * FROM rum_page_opaque_info('test_with_weight_idx', 20);
SELECT * FROM rum_leaf_entry_page_items('test_with_weight_idx', 20);

SELECT * FROM rum_page_opaque_info('test_with_weight_idx', 21);
SELECT * FROM rum_internal_data_page_items('test_with_weight_idx', 21);

SELECT * FROM rum_page_opaque_info('test_with_weight_idx', 22);
SELECT * FROM rum_leaf_data_page_items('test_with_weight_idx', 22);

-- 32-bit machine
SELECT * FROM rum_page_opaque_info('test_with_weight_idx', 10);
SELECT * FROM rum_leaf_entry_page_items('test_with_weight_idx', 10);

SELECT * FROM rum_page_opaque_info('test_with_weight_idx', 12);
SELECT * FROM rum_internal_data_page_items('test_with_weight_idx', 12);

SELECT * FROM rum_page_opaque_info('test_with_weight_idx', 14);
SELECT * FROM rum_leaf_data_page_items('test_with_weight_idx', 14);

DROP TABLE test_debug_table_with_weight;
DROP FUNCTION fill_test_debug_weight_trigger;
