use strict;
use warnings;

my $func_base_template=<<EOT;
CREATE FUNCTION rum_extract_value_TYPEIDENT(TYPENAME, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_compare_prefix_TYPEIDENT(TYPENAME, TYPENAME, int2, internal)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION rum_extract_query_TYPEIDENT(TYPENAME, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

EOT

my $opclass_base_template=<<EOT;

CREATE OPERATOR CLASS TYPEIDENT_ops
DEFAULT FOR TYPE TYPENAME USING rum
AS
	OPERATOR	1	  <	(TYPECMPTYPE, TYPECMPTYPE),
	OPERATOR	2	  <=(TYPECMPTYPE, TYPECMPTYPE),
	OPERATOR	3	  =	(TYPECMPTYPE, TYPECMPTYPE),
	OPERATOR	4	  >=(TYPECMPTYPE, TYPECMPTYPE),
	OPERATOR	5	  >	(TYPECMPTYPE, TYPECMPTYPE),
	FUNCTION	1	  TYPECMPFUNC(TYPECMPTYPE,TYPECMPTYPE),
	FUNCTION	2	  rum_extract_value_TYPESUBIDENT(TYPESUBNAME, internal),
	FUNCTION	3	  rum_extract_query_TYPESUBIDENT(TYPESUBNAME, internal, int2, internal, internal),
	FUNCTION	4	  rum_btree_consistent(internal,smallint,internal,int,internal,internal,internal,internal),
	FUNCTION	5	  rum_compare_prefix_TYPESUBIDENT(TYPESUBNAME,TYPESUBNAME,int2, internal),
STORAGE		 TYPENAME;

EOT

my @opinfo = map {
		$_->{TYPEIDENT}   = $_->{TYPENAME} if ! exists $_->{TYPEIDENT};
		$_->{TYPECMPTYPE} = $_->{TYPENAME} if !exists $_->{TYPECMPTYPE};
		$_->{TYPESUBNAME} = $_->{TYPENAME} if !exists $_->{TYPESUBNAME};
		$_->{TYPESUBIDENT}= $_->{TYPEIDENT} if ! exists $_->{TYPESUBIDENT};
		$_
	} (
	{
		TYPENAME	=>	'int2',
		TYPECMPFUNC	=>	'btint2cmp',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
	},
	{
		TYPENAME	=>	'int4',
		TYPECMPFUNC	=>	'btint4cmp',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
	},
	{
		TYPENAME	=>	'int8',
		TYPECMPFUNC	=>	'btint8cmp',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
	},
	{
		TYPENAME	=>	'float4',
		TYPECMPFUNC	=>	'btfloat4cmp',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
	},
	{
		TYPENAME	=>	'float8',
		TYPECMPFUNC	=>	'btfloat8cmp',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
	},
	{
		TYPENAME	=>	'money',
		TYPECMPFUNC	=>	'cash_cmp',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
	},
	{
		TYPENAME	=>	'oid',
		TYPECMPFUNC	=>	'btoidcmp',
		func_tmpl	=>	\$func_base_template,
		opclass_tmpl=>	\$opclass_base_template,
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
		TYPECMPTYPE	=> 'inet',
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
		TYPECMPTYPE	=> 'text',
		TYPESUBIDENT=> 'text',
		TYPESUBNAME => 'text',
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
		preamble	=>	<<EOT
CREATE FUNCTION rum_numeric_cmp(numeric, numeric)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

EOT
	},
);

foreach my $t (@opinfo)
{
	print	"/*--------------------$t->{TYPENAME}-----------------------*/\n\n";

	print $t->{preamble} if exists $t->{preamble};

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
