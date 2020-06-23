CREATE TABLE testweight_rum( t text, a tsvector, r text );

CREATE FUNCTION fill_weight_trigger() RETURNS trigger AS $$
begin
  new.a :=
     setweight(to_tsvector('pg_catalog.english', coalesce(new.r,'')), 'A') ||
     setweight(to_tsvector('pg_catalog.english', coalesce(new.t,'')), 'D');
  return new;
end
$$ LANGUAGE plpgsql;

CREATE TRIGGER tsvectorweightupdate
BEFORE INSERT OR UPDATE ON testweight_rum
FOR EACH ROW EXECUTE PROCEDURE fill_weight_trigger();

CREATE INDEX rumidx_weight ON testweight_rum USING rum (a rum_tsvector_ops);

\copy testweight_rum(t,r) from 'data/rum_weight.data' DELIMITER '|' ;

SET enable_seqscan=off;
SET enable_indexscan=off;

SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', 'ever:A|wrote');
SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', 'have:A&wish:DAC');
SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', 'have:A&wish:DAC');
SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', 'among:ABC');
SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', 'structure:D&ancient:BCD');
SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', '(complimentary:DC|sight)&(sending:ABC|heart)');
SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', '!gave:D & way');
SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', '(go<->go:a)&(think:d<->go)');
SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', '(go<->go:a)&(think:d<2>go)');
SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', 'go & (!reach:a | way<->reach)');
SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', 'go & (!reach:a & way<->reach)');
SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', 'reach:d & go & !way:a');
SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', 'show:d & seem & !town:a');
SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', '!way:a');
SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', 'go & !way:a');
SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', 'reach:d & !way:a');
SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', 'reach:d & go');
SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', 'think<->go:d | go<->see');
SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', 'reach:d<->think');
SELECT count(*) FROM testweight_rum WHERE a @@ to_tsquery('pg_catalog.english', 'reach<->think');


