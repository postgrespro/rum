#!/usr/bin/env bash

#
# Copyright (c) 2019, Postgres Professional
#
# supported levels:
#		* standard
#		* hardcore
#

set -ux
status=0


# rebuild PostgreSQL with cassert support
if [ "$LEVEL" = "hardcore" ]; then

	set -e

	CUSTOM_PG_BIN=$PWD/pg_bin
	CUSTOM_PG_SRC=$PWD/postgresql

	# here PG_VERSION is provided by postgres:X-alpine docker image
	curl "https://ftp.postgresql.org/pub/source/v$PG_VERSION/postgresql-$PG_VERSION.tar.bz2" -o postgresql.tar.bz2
	echo "$PG_SHA256 *postgresql.tar.bz2" | sha256sum -c -

	mkdir $CUSTOM_PG_SRC

	tar \
		--extract \
		--file postgresql.tar.bz2 \
		--directory $CUSTOM_PG_SRC \
		--strip-components 1

	cd $CUSTOM_PG_SRC

	# enable additional options
	./configure \
		CFLAGS='-fno-omit-frame-pointer' \
		--enable-cassert \
		--prefix=$CUSTOM_PG_BIN \
		--quiet

	time make -s -j$(nproc) && make -s install

	# override default PostgreSQL instance
	export PATH=$CUSTOM_PG_BIN/bin:$PATH
	export LD_LIBRARY_PATH=$CUSTOM_PG_BIN/lib

	# show pg_config path (just in case)
	which pg_config

	cd -

	set +e
fi

# show pg_config just in case
pg_config

# perform code checks if asked to
if [ "$LEVEL" = "hardcore" ]; then

	# perform static analyzis
	scan-build --status-bugs \
	-disable-checker core.UndefinedBinaryOperatorResult \
	-disable-checker deadcode.DeadStores \
	make USE_PGXS=1 || status=$?

	# something's wrong, exit now!
	if [ $status -ne 0 ]; then exit 1; fi

	# don't forget to "make clean"
	make USE_PGXS=1 clean
fi


# build and install extension (using PG_CPPFLAGS and SHLIB_LINK for gcov)
make USE_PGXS=1 PG_CPPFLAGS="-coverage" SHLIB_LINK="-coverage" install

# initialize database
initdb -D $PGDATA

# set appropriate port
export PGPORT=55435
echo "port = $PGPORT" >> $PGDATA/postgresql.conf

# restart cluster 'test'
pg_ctl start -l /tmp/postgres.log -w || status=$?

# something's wrong, exit now!
if [ $status -ne 0 ]; then cat /tmp/postgres.log; exit 1; fi

# run regression tests
export PG_REGRESS_DIFF_OPTS="-w -U3" # for alpine's diff (BusyBox)
make USE_PGXS=1 installcheck || status=$?

# show diff if it exists
if test -f regression.diffs; then cat regression.diffs; fi

# something's wrong, exit now!
if [ $status -ne 0 ]; then exit 1; fi

# generate *.gcov files
gcov src/*.c src/*.h


set +ux


# send coverage stats to Codecov
bash <(curl -s https://codecov.io/bash)
