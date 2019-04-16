if [ -z ${PG_VERSION+x} ]; then
	echo PG_VERSION is not set!
	exit 1
fi

if [ -z ${LEVEL+x} ]; then
	LEVEL=standard
fi

echo PG_VERSION=${PG_VERSION}
echo LEVEL=${LEVEL}

sed \
	-e 's/${PG_VERSION}/'${PG_VERSION}/g \
	-e 's/${LEVEL}/'${LEVEL}/g \
Dockerfile.in > Dockerfile
