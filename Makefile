# contrib/rum/Makefile

MODULE_big = rum
OBJS = src/rumsort.o src/rum_ts_utils.o src/rumtsquery.o \
	src/rumbtree.o src/rumbulk.o src/rumdatapage.o \
	src/rumentrypage.o src/rumget.o src/ruminsert.o \
	src/rumscan.o src/rumutil.o src/rumvacuum.o src/rumvalidate.o \
	src/btree_rum.o src/rum_arr_utils.o $(WIN32RES)

EXTENSION = rum
EXTVERSION = 1.2
DATA = rum--1.0.sql
DATA_updates = rum--1.0--1.1.sql rum--1.1--1.2.sql
DATA_built = rum--$(EXTVERSION).sql $(DATA_updates)
PGFILEDESC = "RUM index access method"
INCLUDES = src/rum.h src/rumsort.h

REGRESS = rum rum_hash ruminv timestamp orderby orderby_hash altorder \
	altorder_hash limits \
	int2 int4 int8 float4 float8 money oid \
    time timetz date interval \
    macaddr inet cidr text varchar char bytea bit varbit \
    numeric

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

rum--$(EXTVERSION).sql: $(DATA) $(DATA_updates)
	cat $(DATA) $(DATA_updates) > rum--$(EXTVERSION).sql

# rule for updates, e.g. rum--1.0--1.1.sql
rum--%.sql: gen_rum_sql--%.pl
	perl $< > $@

install: installincludes

installincludes:
	$(INSTALL_DATA) $(addprefix $(srcdir)/, $(INCLUDES)) '$(DESTDIR)$(includedir_server)/'

ISOLATIONCHECKS= predicate-rum predicate-rum-2

submake-isolation:
	$(MAKE) -C $(top_builddir)/src/test/isolation all

submake-rum:
	$(MAKE) -C $(top_builddir)/contrib/rum

isolationcheck: | submake-isolation submake-rum temp-install
	$(pg_isolation_regress_check) \
	    --temp-config $(top_srcdir)/contrib/rum/logical.conf \
	    $(ISOLATIONCHECKS)
