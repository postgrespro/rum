/*-------------------------------------------------------------------------
 *
 * rum_arr_utils.c
 *		various anyarray-search functions
 *
 * Portions Copyright (c) 2015-2016, Postgres Professional
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/hash.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/typcache.h"

#include "rum.h"

#include <math.h>


PG_FUNCTION_INFO_V1(rum_anyarray_config);


/*
 * Specifies additional information type for operator class.
 */
Datum
rum_anyarray_config(PG_FUNCTION_ARGS)
{
	RumConfig  *config = (RumConfig *) PG_GETARG_POINTER(0);

	config->addInfoTypeOid = INT4OID;
	config->strategyInfo[0].strategy = InvalidStrategy;

	PG_RETURN_VOID();
}
