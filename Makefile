# contrib/rum/Makefile

MODULE_big = rum
OBJS = rumutil.o ruminsert.o $(WIN32RES)

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
