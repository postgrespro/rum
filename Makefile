# contrib/rum/Makefile

MODULE_big = rum
OBJS = src/rumsort.o src/rum_ts_utils.o src/rumtsquery.o \
	src/rumbtree.o src/rumbulk.o src/rumdatapage.o \
	src/rumentrypage.o src/rumget.o src/ruminsert.o \
	src/rumscan.o src/rumutil.o src/rumvacuum.o src/rumvalidate.o \
	src/rum_timestamp.o $(WIN32RES)

EXTENSION = rum
DATA = rum--1.0.sql
PGFILEDESC = "RUM index access method"

REGRESS = rum rum_hash ruminv timestamp orderby orderby_hash altorder \
	altorder_hash

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
