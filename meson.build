# Copyright (c) 2025, Postgres Professional

# Does not support the PGXS infrastructure at this time. Please, compile as part
# of the contrib source tree.

extension = 'rum'
extversion = '1.3'

rum_sources = files(
  'src/btree_rum.c',
  'src/rum_arr_utils.c',
  'src/rum_ts_utils.c',
  'src/rumbtree.c',
  'src/rumbulk.c',
  'src/rumdatapage.c',
  'src/rumentrypage.c',
  'src/rumget.c',
  'src/ruminsert.c',
  'src/rumscan.c',
  'src/rumsort.c',
  'src/rumtsquery.c',
  'src/rumutil.c',
  'src/rumvacuum.c',
  'src/rumvalidate.c',
)

if host_system == 'windows'
  rum_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'rum',
    '--FILEDESC', 'RUM index access method',])
endif

rum = shared_module('rum',
  rum_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += rum

configure_file(
  input: 'rum_init.sql',
  output: extension + '--' + extversion + '.sql',
  copy: true,
  install: true,
  install_dir: contrib_data_args['install_dir'],
)

install_data(
  'rum.control',
  'rum--1.0--1.1.sql',
  'rum--1.1--1.2.sql',
  'rum--1.2--1.3.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'rum',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'security',
      'rum',
      'rum_validate',
      'rum_hash',
      'ruminv',
      'timestamp',
      'orderby',
      'orderby_hash',
      'altorder',
      'altorder_hash',
      'limits',
      'int2',
      'int4',
      'int8',
      'float4',
      'float8',
      'money',
      'oid',
      'time',
      'timetz',
      'date',
      'interval',
      'macaddr',
      'inet',
      'cidr',
      'text',
      'varchar',
      'char',
      'bytea',
      'bit',
      'varbit',
      'numeric',
      'rum_weight',
      'expr',
      'array',
    ],
    'regress_args': [
      '--temp-config', files('logical.conf')
    ],
  },
  'tap': {
    'tests': [
      't/001_wal.pl',
      't/002_pglist.pl',
    ],
    'test_kwargs': {'timeout': 3000},
  },
  'isolation': {
    'specs': [
      'predicate-rum',
      'predicate-rum-2',
    ],
    'regress_args': [
      '--temp-config', files('logical.conf'),
      '--load-extension=rum',
    ],
  },
}
