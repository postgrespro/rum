# contrib/rum/Makefile

MODULE_big = rum
OBJS = ginsort.o rum_ts_utils.o \
	ginarrayproc.o ginbtree.o ginbulk.o gindatapage.o \
	ginentrypage.o ginfast.o ginget.o gininsert.o \
	ginscan.o ginutil.o ginvacuum.o $(WIN32RES)

EXTENSION = rum
DATA = rum--1.0.sql
PGFILEDESC = "RUM access method"

REGRESS = rum

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
