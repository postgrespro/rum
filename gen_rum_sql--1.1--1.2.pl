use strict;
use warnings;

my $func_base_template=<<EOT;
CREATE FUNCTION rum_TYPEIDENT_extract_value(TYPENAME, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_TYPEIDENT_compare_prefix(TYPENAME, TYPENAME, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_TYPEIDENT_extract_query(TYPENAME, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

EOT

my $func_distance_template=<<EOT;
$func_base_template

CREATE FUNCTION rum_TYPEIDENT_distance(TYPENAME, TYPENAME)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=> (
	PROCEDURE = rum_TYPEIDENT_distance,
	LEFTARG = TYPENAME,
	RIGHTARG = TYPENAME,
	COMMUTATOR = <=>
);

CREATE FUNCTION rum_TYPEIDENT_left_distance(TYPENAME, TYPENAME)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <=| (
	PROCEDURE = rum_TYPEIDENT_left_distance,
	LEFTARG = TYPENAME,
	RIGHTARG = TYPENAME,
	COMMUTATOR = |=>
);

CREATE FUNCTION rum_TYPEIDENT_right_distance(TYPENAME, TYPENAME)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR |=> (
	PROCEDURE = rum_TYPEIDENT_right_distance,
	LEFTARG = TYPENAME,
	RIGHTARG = TYPENAME,
	COMMUTATOR = <=|
);

CREATE FUNCTION rum_TYPEIDENT_outer_distance(TYPENAME, TYPENAME, smallint)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_TYPEIDENT_config(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;


EOT

my $opclass_base_template=<<EOT;

CREATE OPERATOR CLASS rum_TYPEIDENT_ops
DEFAULT FOR TYPE TYPENAME USING rum
AS
	OPERATOR	1	  <		TYPESOPARG,
	OPERATOR	2	  <=	TYPESOPARG,
	OPERATOR	3	  =		TYPESOPARG,
	OPERATOR	4	  >=	TYPESOPARG,
	OPERATOR	5	  >		TYPESOPARG,
	FUNCTION	1	  TYPECMPFUNC(TYPECMPTYPE,TYPECMPTYPE),
	FUNCTION	2	  rum_TYPESUBIDENT_extract_value(TYPESUBNAME, internal),
	FUNCTION	3	  rum_TYPESUBIDENT_extract_query(TYPESUBNAME, internal, int2, internal, internal),
	FUNCTION	4	  rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	  rum_TYPESUBIDENT_compare_prefix(TYPESUBNAME,TYPESUBNAME,int2, internal),
STORAGE		 TYPENAME;

EOT

my $opclass_distance_template=<<EOT;

CREATE OPERATOR CLASS rum_TYPEIDENT_ops
DEFAULT FOR TYPE TYPENAME USING rum
AS
	OPERATOR	1	<	TYPESOPARG,
	OPERATOR	2	<=	TYPESOPARG,
	OPERATOR	3	=	TYPESOPARG,
	OPERATOR	4	>=	TYPESOPARG,
	OPERATOR	5	>	TYPESOPARG,
	OPERATOR	20	<=> (TYPENAME,TYPENAME) FOR ORDER BY pg_catalog.float_ops,
	OPERATOR	21	<=| (TYPENAME,TYPENAME) FOR ORDER BY pg_catalog.float_ops,
	OPERATOR	22	|=> (TYPENAME,TYPENAME) FOR ORDER BY pg_catalog.float_ops,
	FUNCTION	1	TYPECMPFUNC(TYPECMPTYPE,TYPECMPTYPE),
	FUNCTION	2	rum_TYPESUBIDENT_extract_value(TYPESUBNAME, internal),
	FUNCTION	3	rum_TYPESUBIDENT_extract_query(TYPESUBNAME, internal, int2, internal, internal),
	FUNCTION	4	rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	rum_TYPESUBIDENT_compare_prefix(TYPESUBNAME,TYPESUBNAME,int2, internal),
	-- support to TYPEIDENT distance in rum_tsvector_addon_ops
	FUNCTION	6	rum_TYPEIDENT_config(internal),
	FUNCTION	9	rum_TYPEIDENT_outer_distance(TYPENAME, TYPENAME, smallint),
STORAGE		 TYPENAME;

EOT

my @opinfo = map {
		$_->{TYPEIDENT}   = $_->{TYPENAME} if ! exists $_->{TYPEIDENT};
		$_->{TYPECMPTYPE} = $_->{TYPENAME} if !exists $_->{TYPECMPTYPE};
		$_->{TYPESUBNAME} = $_->{TYPENAME} if !exists $_->{TYPESUBNAME};
		$_->{TYPESUBIDENT}= $_->{TYPEIDENT} if ! exists $_->{TYPESUBIDENT};
		$_->{TYPESOPARG}= '' if ! exists $_->{TYPESOPARG};
		$_
	} (
	# timestamp/tz aren't here: they are in rum--1.0.sql
);

##############Generate!!!

print <<EOT;
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

CREATE FUNCTION rum_anyarray_ordering(internal,smallint,anyarray,int,internal,internal,internal,internal,internal)
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
	FUNCTION	8	rum_anyarray_ordering(internal,smallint,anyarray,int,internal,internal,internal,internal,internal),
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

# Drop doesn't work
#print <<EOT;
#ALTER OPERATOR FAMILY rum_timestamp_ops USING rum DROP FUNCTION 4
#	(timestamp, timestamp); -- strange definition
#ALTER OPERATOR FAMILY rum_timestamp_ops USING rum ADD  FUNCTION 4
#	rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal);
#EOT
