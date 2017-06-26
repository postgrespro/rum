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
	FUNCTION	8	rum_TYPEIDENT_key_distance(internal,smallint,TYPENAME,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

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

ALTER OPERATOR FAMILY rum_tsvector_ops USING rum DROP
	FUNCTION	8	(internal,smallint,tsvector,int,internal,internal,internal,internal,internal);

ALTER OPERATOR FAMILY rum_tsvector_hash_ops USING rum DROP
	FUNCTION	8	(internal,smallint,tsvector,int,internal,internal,internal,internal,internal);

CREATE OR REPLACE FUNCTION rum_tsquery_distance(internal,smallint,internal,smallint,tsvector,int,internal,internal,internal,internal,internal,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

ALTER OPERATOR FAMILY rum_tsvector_ops USING rum ADD
	FUNCTION	8	rum_tsquery_distance(internal,smallint,internal,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

ALTER OPERATOR FAMILY rum_tsvector_hash_ops USING rum ADD
	FUNCTION	8	rum_tsquery_distance(internal,smallint,internal,smallint,tsvector,int,internal,internal,internal,internal,internal,internal);

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
