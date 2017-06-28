use strict;
use warnings;

my $func_distance_template=<<EOT;
CREATE FUNCTION rum_TYPEIDENT_key_distance(internal,smallint,TYPENAME,smallint,tsvector,int,internal,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

EOT

my $opclass_distance_template=<<EOT;

ALTER OPERATOR FAMILY rum_TYPEIDENT_ops USING rum ADD
	FUNCTION	8	(TYPENAME,TYPENAME) rum_TYPEIDENT_key_distance(internal,smallint,TYPENAME,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

EOT

my @opinfo = map {
		$_->{TYPEIDENT}   = $_->{TYPENAME} if !exists $_->{TYPEIDENT};
		$_
	} (
	{
		TYPENAME	=>	'int2',
		func_tmpl	=>	\$func_distance_template,
		opclass_tmpl=>	\$opclass_distance_template,
	},
	{
		TYPENAME	=>	'int4',
		func_tmpl	=>	\$func_distance_template,
		opclass_tmpl=>	\$opclass_distance_template,
	},
	{
		TYPENAME	=>	'int8',
		func_tmpl	=>	\$func_distance_template,
		opclass_tmpl=>	\$opclass_distance_template,
	},
	{
		TYPENAME	=>	'float4',
		func_tmpl	=>	\$func_distance_template,
		opclass_tmpl=>	\$opclass_distance_template,
	},
	{
		TYPENAME	=>	'float8',
		func_tmpl	=>	\$func_distance_template,
		opclass_tmpl=>	\$opclass_distance_template,
	},
	{
		TYPENAME	=>	'money',
		func_tmpl	=>	\$func_distance_template,
		opclass_tmpl=>	\$opclass_distance_template,
	},
	{
		TYPENAME	=>	'oid',
		func_tmpl	=>	\$func_distance_template,
		opclass_tmpl=>	\$opclass_distance_template,
	},
);

##############Generate!!!

print <<EOT;
/*
 * RUM version 1.2
 */

/*--------------------anyarray-----------------------*/

CREATE FUNCTION rum_anyarray_config(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION rum_anyarray_similar(anyarray,anyarray)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT STABLE;

CREATE OPERATOR % (
	PROCEDURE = rum_anyarray_similar,
	LEFTARG = anyarray,
	RIGHTARG = anyarray,
	COMMUTATOR = '%',
	RESTRICT = contsel,
	JOIN = contjoinsel
);


CREATE OR REPLACE FUNCTION rum_anyarray_distance(anyarray,anyarray)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT STABLE;

CREATE OPERATOR <=> (
	PROCEDURE = rum_anyarray_distance,
	LEFTARG = anyarray,
	RIGHTARG = anyarray,
	COMMUTATOR = '<=>'
);


CREATE FUNCTION rum_extract_anyarray(anyarray,internal,internal,internal,internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION rum_extract_anyarray_query(anyarray,internal,smallint,internal,internal,internal,internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION rum_anyarray_consistent(internal, smallint, anyarray, integer, internal, internal, internal, internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION rum_anyarray_ordering(internal,smallint,tsvector,int,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


CREATE OPERATOR CLASS rum_anyarray_ops
DEFAULT FOR TYPE anyarray USING rum
AS
	OPERATOR	1	&&  (anyarray, anyarray),
	OPERATOR	2	@>  (anyarray, anyarray),
	OPERATOR	3	<@  (anyarray, anyarray),
	OPERATOR	4	=   (anyarray, anyarray),
	OPERATOR	5	%   (anyarray, anyarray),
	OPERATOR	20	<=> (anyarray, anyarray) FOR ORDER BY pg_catalog.float_ops,
	--dispatch function 1 for concrete type
	FUNCTION	2	rum_extract_anyarray(anyarray,internal,internal,internal,internal),
	FUNCTION	3	rum_extract_anyarray_query(anyarray,internal,smallint,internal,internal,internal,internal),
	FUNCTION	4	rum_anyarray_consistent(internal,smallint,anyarray,integer,internal,internal,internal,internal),
	FUNCTION	6	rum_anyarray_config(internal),
	FUNCTION	8	rum_anyarray_ordering(internal,smallint,tsvector,int,internal,internal,internal,internal,internal),
	STORAGE anyelement;

CREATE OPERATOR CLASS rum_anyarray_addon_ops
FOR TYPE anyarray USING rum
AS
	OPERATOR	1	&& (anyarray, anyarray),
	OPERATOR	2	@> (anyarray, anyarray),
	OPERATOR	3	<@ (anyarray, anyarray),
	OPERATOR	4	=  (anyarray, anyarray),
	--dispatch function 1 for concrete type
	FUNCTION	2	ginarrayextract(anyarray,internal,internal),
	FUNCTION	3	ginqueryarrayextract(anyarray,internal,smallint,internal,internal,internal,internal),
	FUNCTION	4	ginarrayconsistent(internal,smallint,anyarray,integer,internal,internal,internal,internal),
	STORAGE anyelement;

EOT

foreach my $t (@opinfo)
{
	print	"/*--------------------$t->{TYPENAME}-----------------------*/\n\n";

	for my $v (qw(func_tmpl opclass_tmpl))
	{
		next if !exists $t->{$v};

		my $x = ${$t->{$v}};

		for my $k (grep {uc($_) eq $_} keys %$t)
		{
			$x=~s/$k/$t->{$k}/g;
		}

		print $x;
	}
}
