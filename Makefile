# contrib/rum/Makefile

MODULE_big = rum
OBJS = rumsort.o rum_ts_utils.o rumtsquery.o \
	rumbtree.o rumbulk.o rumdatapage.o \
	rumentrypage.o rumget.o ruminsert.o \
	rumscan.o rumutil.o rumvacuum.o rumvalidate.o \
	rum_timestamp.o $(WIN32RES)

EXTENSION = rum
DATA = rum--1.0.sql
PGFILEDESC = "RUM index access method"

REGRESS = rum ruminv timestamp orderby altorder

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
