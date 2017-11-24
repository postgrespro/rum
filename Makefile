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

DATA_first = rum--1.0.sql
DATA_updates = rum--1.0--1.1.sql rum--1.1--1.2.sql
DATA = $(DATA_first) rum--$(EXTVERSION).sql $(DATA_updates)

# Do not use DATA_built. It removes built files if clean target was used
SQL_built = rum--$(EXTVERSION).sql $(DATA_updates)

INCLUDES = rum.h rumsort.h
RELATIVE_INCLUDES = $(addprefix src/, $(INCLUDES))

LDFLAGS_SL += $(filter -lm, $(LIBS))

REGRESS = rum rum_hash ruminv timestamp orderby orderby_hash altorder \
	altorder_hash limits \
	int2 int4 int8 float4 float8 money oid \
    time timetz date interval \
    macaddr inet cidr text varchar char bytea bit varbit \
	numeric

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

ifeq ($(MAJORVERSION), 9.6)
# arrays are not supported on 9.6
else
REGRESS += array
endif

wal-check: temp-install
	$(prove_check)

all: $(SQL_built)

#9.6 requires 1.2 file but 10.0 could live with update files
rum--$(EXTVERSION).sql: $(DATA_first) $(DATA_updates)
	cat $(DATA_first) $(DATA_updates) > rum--$(EXTVERSION).sql

# rule for updates, e.g. rum--1.0--1.1.sql
rum--%.sql: gen_rum_sql--%.pl
	perl $< > $@

install: installincludes

installincludes:
	$(INSTALL) -d '$(DESTDIR)$(includedir_server)/'
	$(INSTALL_DATA) $(addprefix $(srcdir)/, $(RELATIVE_INCLUDES)) '$(DESTDIR)$(includedir_server)/'

uninstall: uninstallincludes

uninstallincludes:
	rm -f $(addprefix '$(DESTDIR)$(includedir_server)/', $(INCLUDES))

ISOLATIONCHECKS= predicate-rum predicate-rum-2

submake-isolation:
	$(MAKE) -C $(top_builddir)/src/test/isolation all

submake-rum:
	$(MAKE) -C $(top_builddir)/contrib/rum

isolationcheck: | submake-isolation submake-rum temp-install
	$(pg_isolation_regress_check) \
	    --temp-config $(top_srcdir)/contrib/rum/logical.conf \
		$(ISOLATIONCHECKS)
