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

	{
		TYPENAME	=>	'int2',
		TYPECMPFUNC	=>	'btint2cmp',
		func_tmpl	=>	\$func_distance_template,
		opclass_tmpl=>	\$opclass_distance_template,
	},
	{
		TYPENAME	=>	'int4',
		TYPECMPFUNC	=>	'btint4cmp',
		func_tmpl	=>	\$func_distance_template,
		opclass_tmpl=>	\$opclass_distance_template,
	},
	{
		TYPENAME	=>	'int8',
		TYPECMPFUNC	=>	'btint8cmp',
		func_tmpl	=>	\$func_distance_template,
		opclass_tmpl=>	\$opclass_distance_template,
	},
	{
		TYPENAME	=>	'float4',
		TYPECMPFUNC	=>	'btfloat4cmp',
		func_tmpl	=>	\$func_distance_template,
		opclass_tmpl=>	\$opclass_distance_template,
	},
	{
		TYPENAME	=>	'float8',
		TYPECMPFUNC	=>	'btfloat8cmp',
		func_tmpl	=>	\$func_distance_template,
		opclass_tmpl=>	\$opclass_distance_template,
	},
	{
		TYPENAME	=>	'money',
		TYPECMPFUNC	=>	'cash_cmp',
		func_tmpl	=>	\$func_distance_template,
		opclass_tmpl=>	\$opclass_distance_template,
	},
	{
		TYPENAME	=>	'oid',
		TYPECMPFUNC	=>	'btoidcmp',
		func_tmpl	=>	\$func_distance_template,
		opclass_tmpl=>	\$opclass_distance_template,
	},
	{
		TYPENAME	=>	'time',
		TYPECMPFUNC	=>	'time_cmp',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
	},
	{
		TYPENAME	=>	'timetz',
		TYPECMPFUNC	=>	'timetz_cmp',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
	},
	{
		TYPENAME	=>	'date',
		TYPECMPFUNC	=>	'date_cmp',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
	},
	{
		TYPENAME	=>	'interval',
		TYPECMPFUNC	=>	'interval_cmp',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
	},
	{
		TYPENAME	=>	'macaddr',
		TYPECMPFUNC	=>	'macaddr_cmp',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
	},
	{
		TYPENAME	=>	'inet',
		TYPECMPFUNC	=>	'network_cmp',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
	},
	{
		TYPENAME	=>	'cidr',
		TYPECMPFUNC	=>	'network_cmp',
		TYPECMPTYPE	=>	'inet',
		TYPESOPARG	=>	'(inet, inet)',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
	},
	{
		TYPENAME	=>	'text',
		TYPECMPFUNC	=>	'bttextcmp',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
	},
	{
		TYPENAME	=>	'varchar',
		TYPECMPFUNC	=>	'bttextcmp',
		TYPECMPTYPE	=>	'text',
		TYPESUBIDENT=>	'text',
		TYPESUBNAME =>	'text',
		TYPESOPARG	=>	'(text, text)',
		opclass_tmpl=>	\$opclass_base_template,
	},
	{
		TYPENAME	=>	'"char"',
		TYPEIDENT	=>	'char',
		TYPECMPFUNC	=>	'btcharcmp',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
	},
	{
		TYPENAME	=>	'bytea',
		TYPECMPFUNC	=>	'byteacmp',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
	},
	{
		TYPENAME	=>	'bit',
		TYPECMPFUNC	=>	'bitcmp',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
	},
	{
		TYPENAME	=>	'varbit',
		TYPECMPFUNC	=>	'varbitcmp',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
	},
	{
		TYPENAME	=>	'numeric',
		TYPECMPFUNC	=>	'rum_numeric_cmp',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
	},
);

##############Generate!!!

print <<EOT;
CREATE FUNCTION rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

ALTER FUNCTION
	rum_tsquery_timestamp_consistent (internal,smallint,tsvector,int,internal,internal,internal,internal)
	RENAME TO rum_tsquery_addon_consistent;

CREATE FUNCTION rum_numeric_cmp(numeric, numeric)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR CLASS rum_tsvector_addon_ops
FOR TYPE tsvector USING rum
AS
	OPERATOR	1	@@ (tsvector, tsquery),
	--support function
	FUNCTION	1	gin_cmp_tslexeme(text, text),
	FUNCTION	2	rum_extract_tsvector(tsvector,internal,internal,internal,internal),
	FUNCTION	3	rum_extract_tsquery(tsquery,internal,smallint,internal,internal,internal,internal),
	FUNCTION	4	rum_tsquery_addon_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
	FUNCTION	5	gin_cmp_prefix(text,text,smallint,internal),
	FUNCTION	7	rum_tsquery_pre_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
	STORAGE	 text;

CREATE OPERATOR CLASS rum_tsvector_hash_addon_ops
FOR TYPE tsvector USING rum
AS
	OPERATOR	1	@@ (tsvector, tsquery),
	--support function
	FUNCTION	1	btint4cmp(integer, integer),
	FUNCTION	2	rum_extract_tsvector_hash(tsvector,internal,internal,internal,internal),
	FUNCTION	3	rum_extract_tsquery_hash(tsquery,internal,smallint,internal,internal,internal,internal),
	FUNCTION	4	rum_tsquery_addon_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
	FUNCTION	7	rum_tsquery_pre_consistent(internal,smallint,tsvector,int,internal,internal,internal,internal),
	STORAGE	 integer;

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
