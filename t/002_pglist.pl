# Test RUM index with big base 'pglist'.
use strict;
use warnings;
use Config;
use Test::More;

plan skip_all => 'This test requires downloading a 1GB archive. ' .
				 'The unpacked file weighs almost 3GB. ' .
				 'Perform only if the big_values is enabled in PG_TEST_EXTRA'
	unless $ENV{PG_TEST_EXTRA} && $ENV{PG_TEST_EXTRA} =~ /\bbig_values\b/;

plan tests => 4;

my $node;

# Utility function

sub file_exists
{
	my ($file) = @_;
	return -e $file;
}

# Check the existence of the test base, install if necessary

sub install_pglist
{
	my $dir = Cwd->getcwd; #current directory

	my %config = (
		#directory with pglist dump must be inside the current directory
		pglist_tmp_dir	=> $dir . '/pglist_tmp/',
		dump_name		=> 'pglist-28-04-16.dump',
		dump_url		=> 'http://www.sai.msu.su/~megera/postgres/files/pglist-28-04-16.dump.gz',
		pglist_archive	=> $dir . '/pglist_tmp/' . 'pglist-28-04-16.dump.gz',
	);

	my $path_to_dump = $config{pglist_tmp_dir} . $config{dump_name};

	if (file_exists($path_to_dump))
	{
		note($config{dump_name} . ' already installed');
	}
	else
	{
		# Create folder /contrib/rum/pglist_tmp if not already exists
		mkdir($config{pglist_tmp_dir}, 0700)
			unless file_exists($config{pglist_tmp_dir});

		# Download archive pglist-28-04-16.dump.gz if not already exists
		unless (file_exists($config{pglist_archive}))
		{
			note('Downloading pglist dump in ' . $config{pglist_archive});

			# Flag "-nv" allows us to avoid frequent messages
			# about the download status in the log.
			# But it can be enabled for debugging purposes.
			system("wget -P $config{pglist_tmp_dir} -nv $config{dump_url}") == 0
				or die "Couldn't get archive by link: $?";
		}

		# Unzip the dump. Delete archive to save memory
		system("gzip -d $config{pglist_archive}") == 0
			or die "Couldn't extract archive: $?";

		file_exists($path_to_dump)
			or die "Failed to get " . $config{dump_name};

		note($config{dump_name} . ' is ready to use');
	}

	$node->psql("postgres", "CREATE DATABASE pglist");
	$node->psql("postgres", "CREATE ROLE oleg");
	my $command = "'" . $path_to_dump . "'";
	my $result = $node->psql("pglist", '\i ' . $command);
}

# Tests SELECT constructions to 'pglist' base

sub test_select
{
	note("Creating index 'rumidx_orderby_sent'");

	$node->safe_psql("pglist", "CREATE INDEX rumidx_orderby_sent ON pglist " .
							   "USING rum (fts rum_tsvector_timestamp_ops, sent) " .
							   "WITH (attach=sent, to=fts, order_by_attach=t)");

	note("Test ORDER BY timestamp");

	my $result1 = $node->safe_psql("pglist",
								   "SELECT sent, subject FROM pglist WHERE fts @@ " .
								   "to_tsquery('english', 'backend <-> crushed') " .
								   "ORDER BY sent <=| '2016-01-01 00:01' LIMIT 5");

	is($result1, '1999-06-02 11:52:46|Re: [HACKERS] PID of backend');

	note("Test tsvector filter");

	my $result2 = $node->safe_psql("pglist",
								   "SELECT count(*) FROM pglist " .
								   "WHERE fts @@ to_tsquery('english', 'tom & lane')");

	is($result2, '222813');

	$node->safe_psql("pglist", "DROP INDEX rumidx_orderby_sent");
}

sub test_order_by
{
	note("Creating index 'pglist_rum_idx'");

	$node->safe_psql("pglist",
					 "CREATE INDEX pglist_rum_idx ON pglist " .
					 "USING rum (fts rum_tsvector_ops)");

	note("Test ORDER BY tsvector");

	my $result3 = $node->safe_psql("pglist",
								   "SELECT id FROM pglist " .
								   "WHERE fts @@ to_tsquery('english', 'postgres:*') " .
								   "ORDER BY fts <=> " .
								   "to_tsquery('english', 'postgres:*') LIMIT 9");

	is((split(" ", $result3))[0], '816114');

	# Autovacuum after large update, with active RUM index crashes postgres
	note("Test Issue #19");

	my $stderr;
	$node->safe_psql("pglist", "DELETE FROM pglist WHERE id < 100000");
	$node->safe_psql("pglist", "vacuum", stderr => \$stderr);

	is($stderr, undef);

	$node->safe_psql("pglist", "DROP INDEX pglist_rum_idx");
}

# Start backend

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

if ($pg_15_modules)
{
	$node = PostgreSQL::Test::Cluster->new("master");
}
else
{
	$node = PostgresNode::get_new_node("master");
}

$node->init(allows_streaming => 1);
$node->append_conf("postgresql.conf", "shared_buffers='4GB'\n" .
				   "maintenance_work_mem='2GB'\n" .
				   "max_wal_size='2GB'\n" .
				   "work_mem='50MB'");
$node->start;

# Check the existence of the pglist base

note('Check the existence of the pglist base...');
my $check_pglist = $node->psql('postgres', "SELECT count(*) FROM pg_database " .
										   "WHERE datistemplate = false AND " .
										   "datname = 'pglist'");
if ($check_pglist == 1)
{
	note("pglist already exists");
}
else
{
	note("Create pglist database");
	install_pglist();
}

$node->psql("pglist", "CREATE EXTENSION rum");
note('Setup is completed successfully');

eval
{
	test_select();
	test_order_by();
	$node->stop();
	done_testing();
	1;
} or do {
	note('Something went wrong: $@\n');
};

