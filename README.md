# RUM - RUM access method

## Introduction

The **rum** module provides access method to work with RUM index. It is based
on the GIN access methods code.

## License

This module available under the same license as
[PostgreSQL](http://www.postgresql.org/about/licence/).

## Installation

Before build and install **rum** you should ensure following:

* PostgreSQL version is 9.6.

Typical installation procedure may look like this:

    $ git clone https://github.com/postgrespro/rum
    $ cd rum
    $ make USE_PGXS=1
    $ sudo make USE_PGXS=1 install
    $ make USE_PGXS=1 installcheck
    $ psql DB -c "CREATE EXTENSION rum;"

## New access method and operator class

The **rum** module provides the access method **rum** and the operator class
**rum_tsvector_ops**.

The module provides new operators.

|       Operator       | Returns |                 Description
| -------------------- | ------- | ----------------------------------------------
| tsvector &lt;=&gt; tsquery | float4  | Returns distance between tsvector and tsquery.

## Examples

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
=# SELECT t, a <=> to_tsquery('english', 'beautiful | place') AS rank
	FROM test_rum
	WHERE a @@ to_tsquery('english', 'beautiful | place')
	ORDER BY a <=> to_tsquery('english', 'beautiful | place');
                t                |   rank
---------------------------------+-----------
 The situation is most beautiful | 0.0303964
 It is a beautiful               | 0.0303964
 It looks like a beautiful place | 0.0607927
(3 rows)

=# SELECT t, a <=> to_tsquery('english', 'place | situation') AS rank
	FROM test_rum
	WHERE a @@ to_tsquery('english', 'place | situation')
	ORDER BY a <=> to_tsquery('english', 'place | situation');
                t                |   rank
---------------------------------+-----------
 The situation is most beautiful | 0.0303964
 It looks like a beautiful place | 0.0303964
(2 rows)
```

## Authors

Alexander Korotkov <a.korotkov@postgrespro.ru> Postgres Professional Ltd., Russia

Oleg Bartunov <oleg@sai.msu.su> Postgres Professional Ltd., Russia

Teodor Sigaev <teodor@sigaev.ru> Postgres Professional Ltd., Russia
