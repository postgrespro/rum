# Test for page level predicate locking in rum
#
# Test to verify serialization failures
#
# Queries are written in such a way that an index scan(from one transaction) and an index insert(from another transaction) will try to access the same part(sub-tree) of the index.

setup
{
 CREATE TABLE rum_tbl (id serial, tsv tsvector);

 CREATE TABLE text_table (id1 serial, t text[]);

 INSERT INTO text_table(t) SELECT array[chr(i) || chr(j)] FROM generate_series(65,90) i,
 generate_series(65,90) j ; 

 -- We need to use pseudorandom to generate values for test table
 -- In this case we use linear congruential generator because random() 
 -- function may generate different outputs with different systems
 DO $$
  DECLARE 
	c integer := 17;
	a integer := 261;
	m integer := 6760;
	Xi integer := 228;
 BEGIN
 FOR i in 1..338 LOOP
	INSERT INTO rum_tbl(tsv) VALUES ('');
	FOR j in 1..10 LOOP 
		UPDATE rum_tbl SET tsv = tsv || (SELECT to_tsvector('simple', t[1]) FROM text_table WHERE id1 = Xi % 676 + 1) WHERE id = i;
		Xi = (a * Xi + c) % m;
 	END LOOP;
 END LOOP;
 END;
 $$;

 CREATE INDEX rum_tbl_idx ON rum_tbl USING rum (tsv rum_tsvector_ops);
}

teardown
{
 DROP TABLE text_table;
 DROP TABLE rum_tbl;
}

session "s1"
setup		{ 
		  BEGIN ISOLATION LEVEL SERIALIZABLE;
		  set enable_seqscan=off;
		}
step "rxy1"	{ SELECT id, tsv FROM rum_tbl WHERE tsv @@ 'hx'; }
step "wx1"	{ INSERT INTO rum_tbl(tsv) values('qh'); }
step "c1"	{ COMMIT; }

session "s2"
setup		{ 
		  BEGIN ISOLATION LEVEL SERIALIZABLE;
		  set enable_seqscan=off;
		}

step "rxy2"	{ SELECT id, tsv FROM rum_tbl WHERE tsv @@ 'qh'; }
step "wy2"	{ INSERT INTO rum_tbl(tsv) values('hx'); }
step "c2"	{ COMMIT; }

permutation "rxy1" "wx1" "c1" "rxy2" "wy2" "c2"
permutation "rxy1" "wx1" "rxy2" "c1" "wy2" "c2"
permutation "rxy1" "wx1" "rxy2" "wy2" "c1" "c2"
permutation "rxy1" "wx1" "rxy2" "wy2" "c2" "c1"
permutation "rxy1" "rxy2" "wx1" "c1" "wy2" "c2"
permutation "rxy1" "rxy2" "wx1" "wy2" "c1" "c2"
permutation "rxy1" "rxy2" "wx1" "wy2" "c2" "c1"
permutation "rxy1" "rxy2" "wy2" "wx1" "c1" "c2"
permutation "rxy1" "rxy2" "wy2" "wx1" "c2" "c1"
permutation "rxy1" "rxy2" "wy2" "c2" "wx1" "c1"
permutation "rxy2" "rxy1" "wx1" "c1" "wy2" "c2"
permutation "rxy2" "rxy1" "wx1" "wy2" "c1" "c2"
permutation "rxy2" "rxy1" "wx1" "wy2" "c2" "c1"
permutation "rxy2" "rxy1" "wy2" "wx1" "c1" "c2"
permutation "rxy2" "rxy1" "wy2" "wx1" "c2" "c1"
permutation "rxy2" "rxy1" "wy2" "c2" "wx1" "c1"
permutation "rxy2" "wy2" "c2" "rxy1" "wx1" "c1"
