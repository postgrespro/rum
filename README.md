[![Postgres Professional](img/PGpro-logo.png)](https://postgrespro.com/)

# RUM - RUM access method

## Introduction

The **rum** module provides access method to work with `RUM` index. It is based
on the `GIN` access methods code.

`GIN` index allows to perform fast full text search using `tsvector` and
`tsquery` types. But full text search with GIN index has several problems:

- Slow ranking. It is need position information about lexems to ranking. `GIN`
index doesn't store positions of lexems. So after index scan we need additional
heap scan to retreive lexems positions.
- Slow phrase search with `GIN` index. This problem relates with previous
problem. It is need position information to perform phrase search.
- Slow ordering by timestamp. `GIN` index can't store some related information
in index with lexemes. So it is necessary to perform additional heap scan.

`RUM` solves this problems by storing additional information in posting tree.
For example, positional information of lexemes or timestamps. You can get an
idea of `RUM` by the following picture:

![How RUM stores additional information](img/gin_rum.png)

Drawback of `RUM` is that it has slower build and insert time than `GIN`.
It is because we need to store additional information besides keys and because
`RUM` uses generic WAL.

## License

This module available under the same license as
[PostgreSQL](http://www.postgresql.org/about/licence/).

## Installation

Before build and install **rum** you should ensure following:

* PostgreSQL version is 9.6+.

Typical installation procedure may look like this:

    $ git clone https://github.com/postgrespro/rum
    $ cd rum
    $ make USE_PGXS=1
    $ sudo make USE_PGXS=1 install
    $ make USE_PGXS=1 installcheck
    $ psql DB -c "CREATE EXTENSION rum;"

## Common operators and functions

**rum** module provides next operators.

|       Operator       | Returns |                 Description
| -------------------- | ------- | ----------------------------------------------
| tsvector &lt;=&gt; tsquery | float4  | Returns distance between tsvector and tsquery.
| timestamp &lt;=&gt; timestamp | float8 | Returns distance between two timestamps.
| timestamp &lt;=&#124; timestamp | float8 | Returns distance only for left timestamps.
| timestamp &#124;=&gt; timestamp | float8 | Returns distance only for right timestamps.

Last three operations also works for types int2, int4, int8, float4, float8,
money and oid.

## Operator classes

**rum** provides next operator classes.

### rum_tsvector_ops

For type: `tsvector`

This operator class stores `tsvector` lexemes with positional information. Supports
ordering by `<=>` operator and prefix search. There is the example.

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
                t                |   rank
---------------------------------+-----------
 The situation is most beautiful | 0.0303964
 It is a beautiful               | 0.0303964
 It looks like a beautiful place | 0.0607927
(3 rows)

SELECT t, a <=> to_tsquery('english', 'place | situation') AS rank
    FROM test_rum
    WHERE a @@ to_tsquery('english', 'place | situation')
    ORDER BY a <=> to_tsquery('english', 'place | situation');
                t                |   rank
---------------------------------+-----------
 The situation is most beautiful | 0.0303964
 It looks like a beautiful place | 0.0303964
(2 rows)
```

### rum_tsvector_hash_ops

For type: `tsvector`

This operator class stores hash of `tsvector` lexemes with positional information.
Supports ordering by `<=>` operator. But **doesn't** support prefix search.

### rum_TYPE_ops

For types: int2, int4, int8, float4, float8, money, oid, time, timetz, date,
interval, macaddr, inet, cidr, text, varchar, char, bytea, bit, varbit,
numeric, timestamp, timestamptz

Supported operations: `<`, `<=`, `=`, `>=`, `>` for all types and
`<=>`, `<=|` and `|=>` for int2, int4, int8, float4, float8, money, oid,
timestamp and timestamptz types. 

Supports ordering by `<=>`, `<=|` and `|=>` operators. Can be used with
`rum_tsvector_addon_ops` operator class.

### rum_tsvector_addon_ops

For type: `tsvector`

This operator class stores `tsvector` lexems with any supported by module
field. There is the example.

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

### rum_tsvector_hash_addon_ops

For type: `tsvector`

This operator class stores hash of `tsvector` lexems with any supported by module
field.

**Doesn't** support prefix search.

### rum_tsquery_ops

For type: `tsquery`

Stores branches of query tree in additional information. For example we have the table:
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

## Todo

- Allow multiple additional information (lexemes positions + timestamp).
- Add support for arrays.
- Improve ranking function to support TF/IDF.
- Improve insert time.
- Improve GENERIC WAL to support shift (PostgreSQL core changes).

## Authors

Alexander Korotkov <a.korotkov@postgrespro.ru> Postgres Professional Ltd., Russia

Oleg Bartunov <oleg@sai.msu.su> Postgres Professional Ltd., Russia

Teodor Sigaev <teodor@sigaev.ru> Postgres Professional Ltd., Russia
