# Copyright (c) 2025, Postgres Professional

# Does not support the PGXS infrastructure at this time. Please, compile as part
# of the contrib source tree.

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
    '--FILEDESC', 'rum - provides access method to work with the RUM indexes.',])
endif

rum = shared_module('rum',
  rum_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += rum

install_data(
  'rum.control',
  'rum--1.0--1.1.sql',
  'rum--1.0.sql',
  'rum--1.1--1.2.sql',
  'rum--1.1.sql',
  'rum--1.2--1.3.sql',
  'rum--1.2.sql',
  'rum--1.3.sql',
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
  },
  'tap': {
    'tests': [
      't/001_wal.pl',
    ],
  },
}
