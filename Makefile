# contrib/rum/Makefile

MODULE_big = rum
EXTENSION = rum
EXTVERSION = 1.2
PGFILEDESC = "RUM index access method"

OBJS = src/rumsort.o src/rum_ts_utils.o src/rumtsquery.o \
	src/rumbtree.o src/rumbulk.o src/rumdatapage.o \
	src/rumentrypage.o src/rumget.o src/ruminsert.o \
	src/rumscan.o src/rumutil.o src/rumvacuum.o src/rumvalidate.o \
	src/btree_rum.o src/rum_arr_utils.o $(WIN32RES)

DATA = rum--1.0.sql
DATA_updates = rum--1.0--1.1.sql rum--1.1--1.2.sql
DATA_built = rum--$(EXTVERSION).sql $(DATA_updates)

INCLUDES = rum.h rumsort.h
RELATIVE_INCLUDES = $(addprefix src/, $(INCLUDES))

REGRESS = rum rum_hash ruminv timestamp orderby orderby_hash altorder \
	altorder_hash limits \
	int2 int4 int8 float4 float8 money oid \
    time timetz date interval \
    macaddr inet cidr text varchar char bytea bit varbit \
    numeric anyarray

LDFLAGS_SL += $(filter -lm, $(LIBS))

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/rum
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

wal-check: temp-install
	$(prove_check)

all: rum--$(EXTVERSION).sql

#9.6 requires 1.2 file but 10.0 could live with update files
rum--$(EXTVERSION).sql: $(DATA) $(DATA_updates)
	cat $(DATA) $(DATA_updates) > rum--$(EXTVERSION).sql

# rule for updates, e.g. rum--1.0--1.1.sql
rum--%.sql: gen_rum_sql--%.pl
	perl $< > $@

install: installincludes

installincludes:
	$(INSTALL_DATA) $(addprefix $(srcdir)/, $(RELATIVE_INCLUDES)) '$(DESTDIR)$(includedir_server)/'

uninstall: uninstallincludes

uninstallincludes:
	rm -f $(addprefix '$(DESTDIR)$(includedir_server)/', $(INCLUDES))
