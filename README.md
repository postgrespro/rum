[![Build Status](https://api.travis-ci.com/postgrespro/rum.svg?branch=master)](https://travis-ci.com/postgrespro/rum)
[![PGXN version](https://badge.fury.io/pg/rum.svg)](https://badge.fury.io/pg/rum)
[![GitHub license](https://img.shields.io/badge/license-PostgreSQL-blue.svg)](https://raw.githubusercontent.com/postgrespro/rum/master/LICENSE)

[![Postgres Professional](img/PGpro-logo.svg)](https://postgrespro.com/)

# RUM - RUM access method

## Introduction

The **rum** module provides access method to work with the `RUM` indexes. It is based
on the `GIN` access method code.

`GIN` index allows you to perform fast full-text search using `tsvector` and
`tsquery` types. However, full-text search with `GIN` index has some performance
issues because positional and other additional information is not stored.

`RUM` solves these issues by storing additional information in a posting tree.
As compared to `GIN`, `RUM` index has the following benefits:

- Faster ranking. Ranking requires positional information. And after the
index scan we do not need an additional heap scan to retrieve lexeme positions
because `RUM` index stores them. 
- Faster phrase search. This improvement is related to the previous one as
phrase search also needs positional information.
- Faster ordering by timestamp. `RUM` index stores additional information together
with lexemes, so it is not necessary to perform a heap scan. 
- A possibility to perform depth-first search and therefore return first
results immediately. 

You can get an idea of `RUM` with the following diagram:

[![How RUM stores additional information](img/gin_rum.svg)](https://postgrespro.ru/docs/enterprise/current/rum?lang=en)

The drawback of `RUM` is that it has slower build and insert time as compared to `GIN`
This is because we need to store additional information besides keys and because
because `RUM` stores additional information together with keys and uses generic WAL records.

## License

This module is available under the [license](LICENSE) similar to
[PostgreSQL](http://www.postgresql.org/about/licence/).

## Installation

Before building and installing **rum**, you should ensure following are installed:

* PostgreSQL version is 9.6+.

Typical installation procedure may look like this:

### Using GitHub repository

    $ git clone https://github.com/postgrespro/rum
    $ cd rum
    $ make USE_PGXS=1
    $ make USE_PGXS=1 install
    $ make USE_PGXS=1 installcheck
    $ psql DB -c "CREATE EXTENSION rum;"

### Using PGXN

    $ USE_PGXS=1 pgxn install rum

> **Important:** Don't forget to set the `PG_CONFIG` variable in case you want to test `RUM` on a custom build of PostgreSQL. Read more [here](https://wiki.postgresql.org/wiki/Building_and_Installing_PostgreSQL_Extension_Modules).

## Tests

$ make check

This command runs:
- regression tests;
- isolation tests;
- tap tests.

    One of the tap tests downloads a 1GB archive and then unpacks it
    into a file weighing almost 3GB. It is disabled by default.

    To run this test, you need to set an environment variable:

        $ export PG_TEST_EXTRA=big_values

    The way to turn it off again:

        $ export -n PG_TEST_EXTRA

## Common operators and functions

The **rum** module provides next operators.

|       Operator       | Returns |                 Description
| -------------------- | ------- | ----------------------------------------------
| tsvector &lt;=&gt; tsquery | float4  | Returns distance between tsvector and tsquery.
| timestamp &lt;=&gt; timestamp | float8 | Returns distance between two timestamps.
| timestamp &lt;=&#124; timestamp | float8 | Returns distance only for left timestamps.
| timestamp &#124;=&gt; timestamp | float8 | Returns distance only for right timestamps.

The last three operations also work for types timestamptz, int2, int4, int8, float4, float8,
money and oid.

## Operator classes

**rum** provides the following operator classes.

### rum_tsvector_ops

For type: `tsvector`

This operator class stores `tsvector` lexemes with positional information. It supports
ordering by the `<=>` operator and prefix search. See the example below.

Let us assume we have the table:

```sql
CREATE TABLE test_rum(t text, a tsvector);

CREATE TRIGGER tsvectorupdate
BEFORE UPDATE OR INSERT ON test_rum
FOR EACH ROW EXECUTE PROCEDURE tsvector_update_trigger('a', 'pg_catalog.english', 't');

INSERT INTO test_rum(t) VALUES ('The situation is most beautiful');
INSERT INTO test_rum(t) VALUES ('It is a beautiful');
INSERT INTO test_rum(t) VALUES ('It looks like a beautiful place');
```

To create the **rum** index we need create an extension:

```sql
CREATE EXTENSION rum;
```

Then we can create new index:

```sql
CREATE INDEX rumidx ON test_rum USING rum (a rum_tsvector_ops);
```

And we can execute the following queries:

```sql
SELECT t, a <=> to_tsquery('english', 'beautiful | place') AS rank
    FROM test_rum
    WHERE a @@ to_tsquery('english', 'beautiful | place')
    ORDER BY a <=> to_tsquery('english', 'beautiful | place');
                t                |  rank
---------------------------------+---------
 It looks like a beautiful place | 8.22467
 The situation is most beautiful | 16.4493
 It is a beautiful               | 16.4493
(3 rows)

SELECT t, a <=> to_tsquery('english', 'place | situation') AS rank
    FROM test_rum
    WHERE a @@ to_tsquery('english', 'place | situation')
    ORDER BY a <=> to_tsquery('english', 'place | situation');
                t                |  rank
---------------------------------+---------
 The situation is most beautiful | 16.4493
 It looks like a beautiful place | 16.4493
(2 rows)
```

### rum_tsvector_hash_ops

For type: `tsvector`

This operator class stores a hash of `tsvector` lexemes with positional information.
It supports ordering by the `<=>` operator. It **doesn't** support prefix search.

### rum_TYPE_ops

For types: int2, int4, int8, float4, float8, money, oid, time, timetz, date,
interval, macaddr, inet, cidr, text, varchar, char, bytea, bit, varbit,
numeric, timestamp, timestamptz

Supported operations: `<`, `<=`, `=`, `>=`, `>` for all types and
`<=>`, `<=|` and `|=>` for int2, int4, int8, float4, float8, money, oid,
timestamp and timestamptz types.

This operator supports ordering by the `<=>`, `<=|` and `|=>` operators. It can be used with
`rum_tsvector_addon_ops`, `rum_tsvector_hash_addon_ops` and `rum_anyarray_addon_ops` operator classes.

### rum_tsvector_addon_ops

For type: `tsvector`

This operator class stores `tsvector` lexemes with any supported by module
field. See the example below.

Let us assume we have the table:

```sql
CREATE TABLE tsts (id int, t tsvector, d timestamp);

\copy tsts from 'rum/data/tsts.data'

CREATE INDEX tsts_idx ON tsts USING rum (t rum_tsvector_addon_ops, d)
    WITH (attach = 'd', to = 't');
```

Now we can execute the following queries:
```sql
EXPLAIN (costs off)
    SELECT id, d, d <=> '2016-05-16 14:21:25' FROM tsts WHERE t @@ 'wr&qh' ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;
                                    QUERY PLAN
-----------------------------------------------------------------------------------
 Limit
   ->  Index Scan using tsts_idx on tsts
         Index Cond: (t @@ '''wr'' & ''qh'''::tsquery)
         Order By: (d <=> 'Mon May 16 14:21:25 2016'::timestamp without time zone)
(4 rows)

SELECT id, d, d <=> '2016-05-16 14:21:25' FROM tsts WHERE t @@ 'wr&qh' ORDER BY d <=> '2016-05-16 14:21:25' LIMIT 5;
 id  |                d                |   ?column?
-----+---------------------------------+---------------
 355 | Mon May 16 14:21:22.326724 2016 |      2.673276
 354 | Mon May 16 13:21:22.326724 2016 |   3602.673276
 371 | Tue May 17 06:21:22.326724 2016 |  57597.326724
 406 | Wed May 18 17:21:22.326724 2016 | 183597.326724
 415 | Thu May 19 02:21:22.326724 2016 | 215997.326724
(5 rows)
```

> **Warning:** Currently RUM has bogus behaviour when one creates an index using ordering over pass-by-reference additional information. This is due to the fact that posting trees have fixed length right bound and fixed length non-leaf posting items. It isn't allowed to create such indexes.

### rum_tsvector_hash_addon_ops

For type: `tsvector`

This operator class stores a hash of `tsvector` lexemes with any supported by module
field.

It **doesn't** support prefix search.

### rum_tsquery_ops

For type: `tsquery`

It stores branches of query tree in additional information. For example, we have the table:
```sql
CREATE TABLE query (q tsquery, tag text);

INSERT INTO query VALUES ('supernova & star', 'sn'),
    ('black', 'color'),
    ('big & bang & black & hole', 'bang'),
    ('spiral & galaxy', 'shape'),
    ('black & hole', 'color');

CREATE INDEX query_idx ON query USING rum(q);
```

Now we can execute the following fast query:
```sql
SELECT * FROM query
    WHERE to_tsvector('black holes never exists before we think about them') @@ q;
        q         |  tag
------------------+-------
 'black'          | color
 'black' & 'hole' | color
(2 rows)
```

### rum_anyarray_ops

For type: `anyarray`

This operator class stores `anyarray` elements with length of the array.
It supports operators `&&`, `@>`, `<@`, `=`, `%` operators. It also supports ordering by `<=>` operator.
For example, we have the table:

```sql
CREATE TABLE test_array (i int2[]);

INSERT INTO test_array VALUES ('{}'), ('{0}'), ('{1,2,3,4}'), ('{1,2,3}'), ('{1,2}'), ('{1}');

CREATE INDEX idx_array ON test_array USING rum (i rum_anyarray_ops);
```

Now we can execute the query using index scan:

```sql
SET enable_seqscan TO off;

EXPLAIN (COSTS OFF) SELECT * FROM test_array WHERE i && '{1}' ORDER BY i <=> '{1}' ASC;
                QUERY PLAN
------------------------------------------
 Index Scan using idx_array on test_array
   Index Cond: (i && '{1}'::smallint[])
   Order By: (i <=> '{1}'::smallint[])
(3 rows

SELECT * FROM test_array WHERE i && '{1}' ORDER BY i <=> '{1}' ASC;
     i
-----------
 {1}
 {1,2}
 {1,2,3}
 {1,2,3,4}
(4 rows)
```

### rum_anyarray_addon_ops

For type: `anyarray`

This operator class stores `anyarray` elements with any supported by module
field.

## Functions for low-level inspect of the RUM index pages

The RUM index provides several functions for low-level inspect of all types of its pages:

### `rum_metapage_info(rel_name text, blk_num int4) returns record`

`rum_metapage_info` returns information about a RUM index metapage. For example:

```SQL
SELECT * FROM rum_metapage_info('rum_index', 0);
-[ RECORD 1 ]----+-----------
pending_head     | 4294967295
pending_tail     | 4294967295
tail_free_size   | 0
n_pending_pages  | 0
n_pending_tuples | 0
n_total_pages    | 87
n_entry_pages    | 80
n_data_pages     | 6
n_entries        | 1650
version          | 0xC0DE0002
```

### `rum_page_opaque_info(rel_name text, blk_num int4) returns record`

`rum_page_opaque_info` returns information about a RUM index opaque area: `left` and `right` links, `maxoff` -- the number of elements that are stored on the page (this parameter is used differently for different types of pages), `freespace` -- free space on the page.

For example:

```SQL
SELECT * FROM rum_page_opaque_info('rum_index', 10);
 leftlink | rightlink | maxoff | freespace | flags
----------+-----------+--------+-----------+--------
        6 |        11 |      0 |         0 | {leaf}
```

### `rum_internal_entry_page_items(rel_name text, blk_num int4) returns set of record`

`rum_internal_entry_page_items` returns information that is stored on the internal pages of the entry tree (it is extracted from `IndexTuples`). For example:

```SQL
SELECT * FROM rum_internal_entry_page_items('rum_index', 1);
               key               | attrnum |     category     | down_link
---------------------------------+---------+------------------+-----------
 3d                              |       1 | RUM_CAT_NORM_KEY |         3
 6k                              |       1 | RUM_CAT_NORM_KEY |         2
 a8                              |       1 | RUM_CAT_NORM_KEY |         4
...
 Tue May 10 21:21:22.326724 2016 |       2 | RUM_CAT_NORM_KEY |        83
 Sat May 14 19:21:22.326724 2016 |       2 | RUM_CAT_NORM_KEY |        84
 Wed May 18 17:21:22.326724 2016 |       2 | RUM_CAT_NORM_KEY |        85
 +inf                            |         |                  |        86
(79 rows)
```

RUM (like GIN) on the internal pages of the entry tree packs the downward link and the key in pairs of the following type: `(P_n, K_{n+1})`. It turns out that there is no key for `P_0` (it is assumed to be equal to `-inf`), and for the last key `K_{n+1}` there is no downward link (it is assumed that it is the largest key (or high key) in the subtree to which the `P_n` link leads). For this reason (the key is `+inf` because it is the rightmost page at the current level of the tree), in the example above, the last line contains the key `+inf` (this key does not have a downward link).

### `rum_leaf_entry_page_items(rel_name text, blk_num int4) returns set of record`

`rum_leaf_entry_page_items` returns information that is stored on the entry tree leaf pages (it is extracted from compressed posting lists). For example:

```SQL
SELECT * FROM rum_leaf_entry_page_items('rum_index', 10);
 key | attrnum |     category     | tuple_id | add_info_is_null | add_info | is_posting_tree | posting_tree_root
-----+---------+------------------+----------+------------------+----------+------------------+--------------------
 ay  |       1 | RUM_CAT_NORM_KEY | (0,16)   | t                |          | f                |
 ay  |       1 | RUM_CAT_NORM_KEY | (0,23)   | t                |          | f                |
 ay  |       1 | RUM_CAT_NORM_KEY | (2,1)    | t                |          | f                |
...
 az  |       1 | RUM_CAT_NORM_KEY | (0,15)   | t                |          | f                |
 az  |       1 | RUM_CAT_NORM_KEY | (0,22)   | t                |          | f                |
 az  |       1 | RUM_CAT_NORM_KEY | (1,4)    | t                |          | f                |
...
 b9  |       1 | RUM_CAT_NORM_KEY |          |                  |          | t                |                  7
...
(1602 rows)
```

Each posting list is an `IndexTuple` that stores the key value and a compressed list of `tids`. In the function `rum_leaf_entry_page_items()`, the key value is attached to each `tid` for convenience, but on the page it is stored in a single instance.

If the number of `tids` is too large, then instead of a posting list, a posting tree will be used for storage. In the example above, a posting tree was created (the key in the posting tree is the `tid`) for the key with the value `b9`. In this case, instead of the posting list, the magic number and the page number, which is the root of the posting tree, are stored inside the `IndexTuple`.

### `rum_internal_data_page_items(rel_name text, blk_num int4) returns set of record`

`rum_internal_data_page_items` returns information that is stored on the internal pages of the posting tree (it is extracted from arrays of `RumPostingItem` structures). For example:

```SQL
SELECT * FROM rum_internal_data_page_items('rum_index', 7);
 is_high_key | block_number | tuple_id | add_info_is_null | add_info
-------------+--------------+----------+------------------+----------
 t           |              | (0,0)    | t                |
 f           |            9 | (138,79) | t                |
 f           |            8 | (0,0)    | t                |
(3 rows)
```

Each element on the internal pages of the posting tree contains the high key (`tid`) value for the child page and a link to this child page (as well as additional information if it was added when creating the index).

At the beginning of the internal pages of the posting tree, the high key of this page is always stored (if it has the value `(0,0)`, this is equivalent to `+inf`; this is always performed if the page is the rightmost).

At the moment, RUM does not support storing (as additional information) the data type that is pass by reference on the internal pages of the posting tree. Therefore, this output is possible:

```SQL
 is_high_key | block_number | tuple_id | add_info_is_null |                    add_info
-------------+--------------+----------+------------------+------------------------------------------------
...
 f           |           23 | (39,43)  | f                | varlena types in posting tree is not supported
 f           |           22 | (74,9)   | f                | varlena types in posting tree is not supported
...
```

### `rum_leaf_data_page_items(rel_name text, blk_num int4) returns set of record`

`rum_leaf_data_page_items` the function returns information that is stored on the leaf pages of the postnig tree (it is extracted from compressed posting lists). For example:

```SQL
SELECT * FROM rum_leaf_data_page_items('rum_idx', 9);
 is_high_key | tuple_id  | add_info_is_null | add_info
-------------+-----------+------------------+----------
 t           | (138,79)  | t                |
 f           | (0,9)     | t                |
 f           | (1,23)    | t                |
 f           | (3,5)     | t                |
 f           | (3,22)    | t                |
```

Unlike entry tree leaf pages, on posting tree leaf pages, compressed posting lists are not stored in an `IndexTuple`. The high key is the largest key on the page.

## Todo

- Allow multiple additional information (lexemes positions + timestamp).
- Improve ranking function to support TF/IDF.
- Improve insert time.
- Improve GENERIC WAL to support shift (PostgreSQL core changes).

## Authors

Alexander Korotkov <a.korotkov@postgrespro.ru> Postgres Professional Ltd., Russia

Oleg Bartunov <o.bartunov@postgrespro.ru> Postgres Professional Ltd., Russia

Teodor Sigaev <teodor@postgrespro.ru> Postgres Professional Ltd., Russia

Arthur Zakirov <a.zakirov@postgrespro.ru> Postgres Professional Ltd., Russia

Pavel Borisov <p.borisov@postgrespro.com> Postgres Professional Ltd., Russia

Maxim Orlov <m.orlov@postgrespro.ru> Postgres Professional Ltd., Russia
