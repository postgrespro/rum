# contrib/rum/Makefile

MODULE_big = rum
EXTENSION = rum
EXTVERSION = 1.3
PGFILEDESC = "RUM index access method"

OBJS = src/rumsort.o src/rum_ts_utils.o src/rumtsquery.o \
	src/rumbtree.o src/rumbulk.o src/rumdatapage.o \
	src/rumentrypage.o src/rumget.o src/ruminsert.o \
	src/rumscan.o src/rumutil.o src/rumvacuum.o src/rumvalidate.o \
	src/btree_rum.o src/rum_arr_utils.o $(WIN32RES)

DATA_updates = rum--1.0--1.1.sql rum--1.1--1.2.sql \
			   rum--1.2--1.3.sql

DATA_built = $(EXTENSION)--$(EXTVERSION).sql

INCLUDES = rum.h rumsort.h
RELATIVE_INCLUDES = $(addprefix src/, $(INCLUDES))

LDFLAGS_SL += $(filter -lm, $(LIBS))

REGRESS = security rum rum_validate rum_hash ruminv timestamp \
	orderby orderby_hash altorder altorder_hash limits \
	int2 int4 int8 float4 float8 money oid \
	time timetz date interval \
	macaddr inet cidr text varchar char bytea bit varbit \
	numeric rum_weight expr array

TAP_TESTS = 1

ISOLATION = predicate-rum predicate-rum-2
ISOLATION_OPTS = --load-extension=rum
EXTRA_CLEAN = pglist_tmp

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

$(EXTENSION)--$(EXTVERSION).sql: rum_init.sql
	cat $^ > $@

#
# On versions 12 and 13 isolation tests cannot be run using pgxs.
# Override installcheck target to avoid the error. This is just a
# shortcut version of installcheck target from pgxs.mk that runs
# all other tests besides isolation tests.
#
ifdef USE_PGXS
ifeq ($(MAJORVERSION), $(filter 12% 13%, $(MAJORVERSION)))
installcheck: submake $(REGRESS_PREP)
ifdef REGRESS
	$(pg_regress_installcheck) $(REGRESS_OPTS) $(REGRESS)
endif
ifdef TAP_TESTS
	$(prove_installcheck)
endif
endif
endif

# --------------------------------------------------------
# Make conditional targets to save backward compatibility
# with PG11, PG10 and PG9.6.
# --------------------------------------------------------
ifeq ($(MAJORVERSION), $(filter 9.6% 10% 11%, $(MAJORVERSION)))

# arrays are not supported on 9.6
ifeq ($(MAJORVERSION), 9.6)
REGRESS := $(filter-out array, $(REGRESS))
endif

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

# For 9.6-11 we have to make specific target with tap tests
check: temp-install
	$(prove_check)

endif
