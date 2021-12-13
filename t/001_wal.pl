# Test generic xlog record work for rum index replication.
use strict;
use warnings;
use Test::More tests => 31;

my $pg_15_modules;

BEGIN
{
	$pg_15_modules = eval
	{
		require PostgreSQL::Test::Cluster;
		require PostgreSQL::Test::Utils;
		return 1;
	};

	unless (defined $pg_15_modules)
	{
		$pg_15_modules = 0;

		require PostgresNode;
		require TestLib;
	}
}

note('PostgreSQL 15 modules are used: ' . ($pg_15_modules ? 'yes' : 'no'));


my $node_master;
my $node_standby;

# Run few queries on both master and standby and check their results match.
sub test_index_replay
{
	my ($test_name) = @_;

	# Check server version
	my $server_version = $node_master->safe_psql("postgres", "SELECT current_setting('server_version_num');") + 0;

	# Wait for standby to catch up
	my $applname = $node_standby->name;
	my $caughtup_query;

	if ($server_version < 100000)
	{
		$caughtup_query =
			"SELECT pg_current_xlog_location() <= replay_location FROM pg_stat_replication WHERE application_name = '$applname';";
	}
	else
	{
		$caughtup_query =
			"SELECT pg_current_wal_lsn() <= replay_lsn FROM pg_stat_replication WHERE application_name = '$applname';";
	}
	$node_master->poll_query_until('postgres', $caughtup_query)
	  or die "Timed out while waiting for standby 1 to catch up";

	my $queries = qq(SET enable_seqscan=off;
SET enable_bitmapscan=on;
SET enable_indexscan=on;
SELECT * FROM tst WHERE t \@@ to_tsquery('simple', 'qscfq');
SELECT * FROM tst WHERE t \@@ to_tsquery('simple', 'ztcow');
SELECT * FROM tst WHERE t \@@ to_tsquery('simple', 'jqljy');
SELECT * FROM tst WHERE t \@@ to_tsquery('simple', 'lvnex');
);

	# Run test queries and compare their result
	my $master_result = $node_master->psql("postgres", $queries);
	my $standby_result = $node_standby->psql("postgres", $queries);

	is($master_result, $standby_result, "$test_name: query result matches");
}

# Initialize master node

# Create node.
# Older versions of PostgreSQL modules use get_new_node function.
# Newer use standard perl object constructor syntax.
# Also applies for node_standby (below).
eval
{
	if ($pg_15_modules)
	{
		$node_master = PostgreSQL::Test::Cluster->new("master");
	}
	else
	{
		$node_master = PostgresNode::get_new_node("master");
	}
};

$node_master->init(allows_streaming => 1);
$node_master->start;
my $backup_name = 'my_backup';

# Take backup
$node_master->backup($backup_name);

# Create streaming standby linking to master
eval
{
	if ($pg_15_modules)
	{
		$node_standby = PostgreSQL::Test::Cluster->new("standby");
	}
	else
	{
		$node_standby = PostgresNode::get_new_node("standby");
	}
};

$node_standby->init_from_backup($node_master, $backup_name,
	has_streaming => 1);
$node_standby->start;

# Create some rum index on master
$node_master->psql("postgres", "CREATE EXTENSION rum;");
$node_master->psql("postgres", "CREATE TABLE tst (i int4, t tsvector);");
$node_master->psql("postgres", "INSERT INTO tst SELECT i%10,
					to_tsvector('simple', array_to_string(array(
								select substr('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', trunc(random() * 52)::integer + 1, 1)
								FROM   generate_series(i, i + 4)), ''))
					FROM generate_series(1,16000) i;");
$node_master->psql("postgres", "CREATE INDEX rumidx ON tst USING rum (t rum_tsvector_ops);");

# Test that queries give same result
test_index_replay('initial');

# Run 10 cycles of table modification. Run test queries after each modification.
for my $i (1..10)
{
	$node_master->psql("postgres", "DELETE FROM tst WHERE i = $i;");
	test_index_replay("delete $i");
	$node_master->psql("postgres", "VACUUM tst;");
	test_index_replay("vacuum $i");
	my ($start, $end) = (100001 + ($i - 1) * 10000, 100000 + $i * 10000);
	$node_master->psql("postgres", "INSERT INTO tst SELECT i%10,
						to_tsvector('simple', array_to_string(array(
									select substr('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', trunc(random() * 52)::integer + 1, 1)
									FROM   generate_series(i, i + 4)), ''))
						FROM generate_series($start,$end) i;");
	test_index_replay("insert $i");
}
