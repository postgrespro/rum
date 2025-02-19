use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# See storage/block.h
my $invalid_block_number = '4294967295';

# The function finds the leftmost leaf page of the Entry Tree.
# To do this, starting from the first page, it goes down the tree to the leaf.
sub find_min_leaf_entry_page
{
	my ($idx_name, $node) = @_;
	my $cur_page_num = 1;

	my $cur_flags = $node->safe_psql(
		"postgres", qq{
		SELECT flags FROM rum_page_opaque_info('$idx_name', $cur_page_num);
	});

	while ($cur_flags ne '{leaf}')
	{
		$cur_page_num = $node->safe_psql(
			"postgres", qq{
			SELECT down_link
			FROM rum_internal_entry_page_items('$idx_name', $cur_page_num)
			LIMIT 1;
		});

		$cur_flags = $node->safe_psql(
			"postgres", qq{
			SELECT flags FROM rum_page_opaque_info('$idx_name', $cur_page_num);
		});
	}

	return $cur_page_num;
}

# The function goes through the leaf pages of the Entry Tree from left to right
# and searches for the first entry with a link to the root of the Posting Tree.
#
# In $cur_page_num, it expects to get the leftmost leaf page of the Entry Tree.
# If no records with a link to the root of the Posting Tree are found, it
# returns -1.
sub find_root_posting_tree
{
	my ($idx_name, $cur_page_num, $node) = @_;

	while ($cur_page_num ne $invalid_block_number)
	{
		my $posting_tree_root = $node->safe_psql(
			"postgres", qq{
			SELECT posting_tree_root
			FROM rum_leaf_entry_page_items('$idx_name', $cur_page_num)
			WHERE is_posting_tree = 't' LIMIT 1;
		});

		if ($posting_tree_root ne '')
		{
			chomp $posting_tree_root;
			return $posting_tree_root;
		}

		$cur_page_num = $node->safe_psql(
			"postgres", qq{
			SELECT rightlink FROM rum_page_opaque_info('$idx_name', $cur_page_num);
		});
	}

	return -1;
}

# The function finds the leftmost leaf page of the Posting Tree. It works the
# same way as find_min_leaf_entry_page().
#
# In $cur_page_num, it expects to receive the root page of the Posting Tree.
sub find_min_leaf_posting_tree
{
	my ($idx_name, $cur_page_num, $node) = @_;

	my $cur_flags = $node->safe_psql(
		"postgres", qq{
		SELECT flags FROM rum_page_opaque_info('$idx_name', $cur_page_num);
	});

	while ($cur_flags ne '{data,leaf}')
	{
		$cur_page_num = $node->safe_psql(
			"postgres", qq{
			SELECT block_number
			FROM rum_internal_data_page_items('$idx_name', $cur_page_num)
			WHERE is_high_key = 'f' LIMIT 1;
		});

		$cur_flags = $node->safe_psql(
			"postgres", qq{
			SELECT flags FROM rum_page_opaque_info('$idx_name', $cur_page_num);
		});
	}

	return $cur_page_num;
}

# A function for comparing TIDs.
sub tid_less_or_equal
{
	my ($a, $b) = @_;

	$a =~ /\((\d+),(\d+)\)/ or die "Invalid TID: $a";
	my ($blk_a, $off_a) = ($1, $2);
	$b =~ /\((\d+),(\d+)\)/ or die "Invalid TID: $b";
	my ($blk_b, $off_b) = ($1, $2);

	return ($blk_a < $blk_b) || ($blk_a == $blk_b && $off_a <= $off_b);
}

# A function to check that an array of TIDs is sorted.
sub is_tid_list_sorted
{
	my (@tids) = @_;

	for my $i (0 .. $#tids - 1)
	{
		return 0 unless tid_less_or_equal($tids[$i], $tids[ $i + 1 ]);
	}

	return 1;
}

# tsts.data is the data file for the test table.
my $data_file_path = Cwd::getcwd() . "/data/tsts.data";
if (-e $data_file_path)
{
	plan tests => 11;
}
else
{
	plan skip_all => "tsts.data not found";
}

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->start;
$node->safe_psql("postgres", "CREATE EXTENSION rum;");

# Create a test table, fill it with data, and create an index.

$node->safe_psql(
	"postgres", qq{
	CREATE TABLE test_table (id int, t tsvector, d timestamp);
});

$node->safe_psql(
	"postgres", qq{
	DO \$\$
	BEGIN
		FOR i IN 1..5 LOOP
			COPY test_table FROM '$data_file_path';
		END LOOP;
	END;
	\$\$;
});

# It is necessary to create all types of pages in the Posting Tree.
$node->safe_psql(
	"postgres", qq{
	DO \$\$
	BEGIN
		FOR i IN 1..5000 LOOP
			INSERT INTO test_table(id, t, d)
			VALUES(i, 'b9', '2016-05-02 00:21:22.326724');
		END LOOP;
	END;
	\$\$;
});

$node->safe_psql(
	"postgres", qq{
	CREATE INDEX test_rum_idx_false ON test_table
	USING rum (t rum_tsvector_addon_ops, d)
	WITH (attach = 'd', to = 't', order_by_attach='f');
});

# Testing the rum_metapage_info() function.
my $meta = $node->safe_psql(
	"postgres", qq{
	SELECT n_total_pages, n_entry_pages, n_data_pages, n_entries
	FROM rum_metapage_info('test_rum_idx_false', 0);
});
my ($n_total_pages, $n_entry_pages, $n_data_pages, $n_entries) = split /\|/,
  $meta;
ok($n_total_pages > 0, "Total pages count is valid");
ok($n_entry_pages > 0, "Entry pages count > 0");
ok($n_data_pages > 0, "Data (posting tree) pages count > 0");
ok($n_total_pages == $n_entry_pages + $n_data_pages + 1,
	"Total pages = entry + data + metapage");
ok($n_entries == 1650,
	"The number of entries is equal to the number copied from data.tsts");

# Testing the rum_page_opaque_info() function.
my $opaque_meta = $node->safe_psql(
	"postgres", qq{
	SELECT flags FROM rum_page_opaque_info('test_rum_idx_false', 0);
});
ok($opaque_meta eq '{meta}',
	qq{rum_page_opaque_info('rum_idx', 0) returns {meta} flag});

$opaque_meta = $node->safe_psql(
	"postgres", qq{
	SELECT rightlink FROM rum_page_opaque_info('test_rum_idx_false', 0);
});
ok($opaque_meta eq $invalid_block_number,
	qq{InvalidBlockNumber should be equal to '4294967295'});

# Testing the rum_internal_entry_page_items() function.
my $entry_internal_flags = $node->safe_psql(
	"postgres", qq{
	SELECT flags FROM rum_page_opaque_info('test_rum_idx_false', 1);
});
SKIP:
{
	skip 'Page 1 is not an internal entry page', 1
	  if $entry_internal_flags ne '{}';

	my $entry_internal_key_attnum = $node->safe_psql(
		"postgres", qq{
		SELECT attrnum
		FROM rum_internal_entry_page_items('test_rum_idx_false', 1)
		WHERE attrnum IS NOT NULL
		GROUP BY attrnum
		LIMIT 1;
	});

	my @entry_internal_keys = split(
		/\n/,
		$node->safe_psql(
			"postgres", qq{
			SELECT key
			FROM rum_internal_entry_page_items('test_rum_idx_false', 1)
			WHERE attrnum = $entry_internal_key_attnum;
		}));
	my @entry_internal_keys_sorted = sort @entry_internal_keys;

	is_deeply(
		\@entry_internal_keys,
		\@entry_internal_keys_sorted,
		"rum_internal_entry_page_items() returns sorted keys");
}

# Testing the rum_leaf_entry_page_items() function.
my $entry_leaf_min_num =
  find_min_leaf_entry_page('test_rum_idx_false', $node);
my $entry_leaf_key_attnum = $node->safe_psql(
	"postgres", qq{
	SELECT attrnum
	FROM rum_leaf_entry_page_items('test_rum_idx_false', $entry_leaf_min_num)
	WHERE attrnum IS NOT NULL AND is_posting_tree = 'f'
	GROUP BY attrnum
	LIMIT 1;
});
SKIP:
{
	skip "The leftmost entry leaf page contains only the posting tree roots.",
	  if $entry_leaf_key_attnum eq '';

	my @entry_leaf_keys = split(
		/\n/,
		$node->safe_psql(
			"postgres", qq{
			SELECT key
			FROM rum_leaf_entry_page_items('test_rum_idx_false', $entry_leaf_min_num)
			WHERE is_posting_tree = 'f' AND attrnum = $entry_leaf_key_attnum;
		}));
	my @entry_leaf_keys_sorted = sort @entry_leaf_keys;
	is_deeply(\@entry_leaf_keys_sorted, \@entry_leaf_keys,
		"rum_leaf_entry_page_items() returns sorted keys");
}

# Testing the rum_internal_data_page_items() function.
my $posting_tree_root_num =
  find_root_posting_tree('test_rum_idx_false', $entry_leaf_min_num, $node);
my $posting_tree_root_flags;
if ($posting_tree_root_num != -1)
{
	$posting_tree_root_flags = $node->safe_psql(
		"postgres", qq{
		SELECT flags
		FROM rum_page_opaque_info('test_rum_idx_false', $posting_tree_root_num);
	});
}
SKIP:
{
	skip 'The root of the posting tree was not found', 1
	  if $posting_tree_root_num == -1 or $posting_tree_root_flags ne '{data}';

	my @posting_tree_root_tids = split(
		/\n/,
		$node->safe_psql(
			"postgres", qq{
		SELECT tuple_id
		FROM rum_internal_data_page_items('test_rum_idx_false', $posting_tree_root_num)
		WHERE is_high_key = 'f';
	}));

	# deleting the high key
	pop @posting_tree_root_tids;

	my @posting_tree_root_tids_sorted = sort @posting_tree_root_tids;
	is_deeply(\@posting_tree_root_tids_sorted,
		\@posting_tree_root_tids,
		"rum_internal_data_page_items() returns sorted tids");
}

# Testing the rum_leaf_data_page_items() function.
my $posting_tree_leaf_num = -1;
if ($posting_tree_root_num != -1)
{
	$posting_tree_leaf_num = find_min_leaf_posting_tree('test_rum_idx_false',
		$posting_tree_root_num, $node);
}
SKIP:
{
	skip 'The leaf page of the posting tree was not found', 1
	  if $posting_tree_leaf_num == -1;

	my @posting_tree_leaf_tids = split(
		/\n/,
		$node->safe_psql(
			"postgres", qq{
		SELECT tuple_id
		FROM rum_leaf_data_page_items('test_rum_idx_false', $posting_tree_leaf_num)
		WHERE is_high_key = 'f';
	}));

	ok( is_tid_list_sorted(@posting_tree_leaf_tids) == 1,
		"rum_leaf_data_page_items() returns sorted tids");
}

$node->stop('fast');
done_testing();
