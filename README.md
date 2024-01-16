[![Build Status](https://api.travis-ci.com/postgrespro/rum.svg?branch=master)](https://travis-ci.com/postgrespro/rum)
[![PGXN version](https://badge.fury.io/pg/rum.svg)](https://badge.fury.io/pg/rum)
[![GitHub license](https://img.shields.io/badge/license-PostgreSQL-blue.svg)](https://raw.githubusercontent.com/postgrespro/rum/master/LICENSE)

[![Postgres Professional](img/PGpro-logo.png)](https://postgrespro.com/)

# RUM - RUM access method

## Introduction

The **rum** module provides an access method to work with a `RUM` index. It is based
on the `GIN` access method's code.

A `GIN` index allows performing fast full-text search using `tsvector` and
`tsquery` types. But full-text search with a GIN index has several problems:

- Slow ranking. It needs positional information about lexemes to do ranking. A `GIN`
index doesn't store positions of lexemes. So after index scanning, we need an
additional heap scan to retrieve lexeme positions.
- Slow phrase search with a `GIN` index. This problem relates to the previous
problem. It needs positional information to perform phrase search.
- Slow ordering by timestamp. A `GIN` index can't store some related information
in the index with lexemes. So it is necessary to perform an additional heap scan.

`RUM` solves these problems by storing additional information in a posting tree.
For example, positional information of lexemes or timestamps. You can get an
idea of `RUM` with the following diagram:

![How RUM stores additional information](img/gin_rum.png)

A drawback of `RUM` is that it has slower build and insert times than `GIN`.
This is because we need to store additional information besides keys and because
`RUM` uses generic Write-Ahead Log (WAL) records.

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
`rum_tsvector_addon_ops`, `rum_tsvector_hash_addon_ops' and `rum_anyarray_addon_ops` operator classes.

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

## Upgrading

Install the latest version and run in every database you want to upgrade:

```SQL
ALTER EXTENSION rum UPDATE;
```
You need to close this database server connection to apply changes.

You can check the version in the current database with psql command:

```
\dx
```

## Version Notes

### 1.4

The support of anyarray RUM indexes removed from RUM extension and move to 
[anyarray extension](https://github.com/postgrespro/anyarray).

## Extensibility

The recommendations for the creation of the new operator classes for RUM index are provided in [the manual](docs/README.md).

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
