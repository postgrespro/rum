/*
 * RUM version 1.4
 */

/*--------------------RUM debug functions-----------------------*/

CREATE FUNCTION rum_metapage_info(
    IN rel_name text,
    IN blk_num int4,
    OUT pending_head bigint,
    OUT pending_tail bigint,
    OUT tail_free_size int4,
    OUT n_pending_pages bigint,
    OUT n_pending_tuples bigint,
    OUT n_total_pages bigint,
    OUT n_entry_pages bigint,
    OUT n_data_pages bigint,
    OUT n_entries bigint,
    OUT version varchar)
AS 'MODULE_PATHNAME', 'rum_metapage_info'
LANGUAGE C STRICT PARALLEL SAFE;

CREATE FUNCTION rum_page_opaque_info(
    IN rel_name text,
    IN blk_num int4,
    OUT leftlink bigint,
    OUT rightlink bigint,
    OUT maxoff int4,
    OUT freespace int4,
    OUT flags text[])
AS 'MODULE_PATHNAME', 'rum_page_opaque_info'
LANGUAGE C STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION
rum_page_items_info(rel_name text, blk_num int4, page_type int4)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'rum_page_items_info'
LANGUAGE C STRICT;

CREATE FUNCTION rum_leaf_data_page_items(
    rel_name text,
    blk_num int4
)
RETURNS TABLE(
  is_high_key bool,
  tuple_id tid,
  add_info_is_null bool,
  add_info varchar
)
AS $$
    SELECT *
    FROM rum_page_items_info(rel_name, blk_num, 0)
        AS rum_page_items_info(
            is_high_key bool,
            tuple_id tid,
            add_info_is_null bool,
            add_info varchar
        );
$$ LANGUAGE sql;

CREATE FUNCTION rum_internal_data_page_items(
    rel_name text,
    blk_num int4
)
RETURNS TABLE(
    is_high_key bool,
    block_number int4,
    tuple_id tid,
    add_info_is_null bool,
    add_info varchar
)
AS $$
    SELECT *
    FROM rum_page_items_info(rel_name, blk_num, 1)
        AS rum_page_items_info(
            is_high_key bool,
            block_number int4,
            tuple_id tid,
            add_info_is_null bool,
            add_info varchar
        );
$$ LANGUAGE sql;

CREATE FUNCTION rum_leaf_entry_page_items(
    rel_name text,
    blk_num int4
)
RETURNS TABLE(
    key varchar,
    attrnum int4,
    category varchar,
    tuple_id tid,
    add_info_is_null bool,
    add_info varchar,
    is_postring_tree bool,
    postring_tree_root int4
)
AS $$
  SELECT *
  FROM rum_page_items_info(rel_name, blk_num, 2)
      AS rum_page_items_info(
          key varchar,
          attrnum int4,
          category varchar,
          tuple_id tid,
          add_info_is_null bool,
          add_info varchar,
          is_postring_tree bool,
          postring_tree_root int4
      );
$$ LANGUAGE sql;

CREATE FUNCTION rum_internal_entry_page_items(
    rel_name text,
    blk_num int4
)
RETURNS TABLE(
    key varchar,
    attrnum int4,
    category varchar,
    down_link int4)
AS $$
  SELECT *
  FROM rum_page_items_info(rel_name, blk_num, 3)
      AS rum_page_items_info(
          key varchar,
          attrnum int4,
          category varchar,
          down_link int4
      );
$$ LANGUAGE sql;
