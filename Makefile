# contrib/rum/Makefile

MODULE_big = rum
OBJS = src/rumsort.o src/rum_ts_utils.o src/rumtsquery.o \
	src/rumbtree.o src/rumbulk.o src/rumdatapage.o \
	src/rumentrypage.o src/rumget.o src/ruminsert.o \
	src/rumscan.o src/rumutil.o src/rumvacuum.o src/rumvalidate.o \
	src/btree_rum.o $(WIN32RES)

EXTENSION = rum
DATA = rum--1.0.sql rum--1.0--1.1.sql rum--1.1.sql
PGFILEDESC = "RUM index access method"
INCLUDES = src/rum.h src/rumsort.h

REGRESS = rum rum_hash ruminv timestamp orderby orderby_hash altorder \
	altorder_hash limits \
	int2 int4 int8 float4 float8 money oid \
    time timetz date interval \
    macaddr inet cidr text varchar char bytea bit varbit \
    numeric

EXTRA_CLEAN += rum--1.1.sql rum--1.0--1.1.sql

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

all: rum--1.1.sql

#9.6 requires 1.1 file but 10.0 could live with 1.0 + 1.0-1.1 files
rum--1.1.sql:  rum--1.0.sql rum--1.0--1.1.sql
	cat rum--1.0.sql rum--1.0--1.1.sql > rum--1.1.sql

rum--1.0--1.1.sql: Makefile gen_rum_sql--1.0--1.1.pl
	perl gen_rum_sql--1.0--1.1.pl > rum--1.0--1.1.sql

install: installincludes

installincludes:
	$(INSTALL_DATA) $(addprefix $(srcdir)/, $(INCLUDES)) '$(DESTDIR)$(includedir_server)/'
