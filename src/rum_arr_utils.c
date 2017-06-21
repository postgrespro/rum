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
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#include "rum.h"

#include <math.h>


#define RumOverlapStrategy		1
#define RumContainsStrategy		2
#define RumContainedStrategy	3
#define RumEqualStrategy		4
#define RumSimilarStrategy		5


PG_FUNCTION_INFO_V1(rum_anyarray_config);

PG_FUNCTION_INFO_V1(rum_extract_anyarray);
PG_FUNCTION_INFO_V1(rum_extract_anyarray_query);

PG_FUNCTION_INFO_V1(rum_anyarray_consistent);

PG_FUNCTION_INFO_V1(rum_anyarray_similar);
PG_FUNCTION_INFO_V1(rum_anyarray_distance);


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


/* ginarrayextract() */
Datum
rum_extract_anyarray(PG_FUNCTION_ARGS)
{
	/* Make copy of array input to ensure it doesn't disappear while in use */
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P_COPY(0);

	Datum	   *entries;
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	bool	  **entries_isnull = (bool **) PG_GETARG_POINTER(2);

	Datum	  **addInfo = (Datum **) PG_GETARG_POINTER(3);
	bool	  **addInfoIsNull = (bool **) PG_GETARG_POINTER(4);

	int16		elmlen;
	bool		elmbyval;
	char		elmalign;
	int			i;

	get_typlenbyvalalign(ARR_ELEMTYPE(array),
						 &elmlen, &elmbyval, &elmalign);

	deconstruct_array(array,
					  ARR_ELEMTYPE(array),
					  elmlen, elmbyval, elmalign,
					  &entries, entries_isnull, nentries);

	*addInfo = (Datum *) palloc(*nentries * sizeof(Datum));
	*addInfoIsNull = (bool *) palloc(*nentries * sizeof(bool));

	for (i = 0; i < *nentries; i++)
	{
		/* Use array's size as additional info */
		(*addInfo)[i] = Int32GetDatum(*nentries);
		(*addInfoIsNull)[i] = BoolGetDatum(false);
	}

	/* we should not free array, entries[i] points into it */
	PG_RETURN_POINTER(entries);
}

/* ginqueryarrayextract() */
Datum
rum_extract_anyarray_query(PG_FUNCTION_ARGS)
{
	/* Make copy of array input to ensure it doesn't disappear while in use */
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P_COPY(0);

	Datum	   *entries;
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	bool	  **entries_isnull = (bool **) PG_GETARG_POINTER(5);

	StrategyNumber strategy = PG_GETARG_UINT16(2);

	/* bool   **pmatch = (bool **) PG_GETARG_POINTER(3); */
	/* Pointer	   *extra_data = (Pointer *) PG_GETARG_POINTER(4); */

	int32	   *searchMode = (int32 *) PG_GETARG_POINTER(6);

	int16		elmlen;
	bool		elmbyval;
	char		elmalign;

	get_typlenbyvalalign(ARR_ELEMTYPE(array),
						 &elmlen, &elmbyval, &elmalign);

	deconstruct_array(array,
					  ARR_ELEMTYPE(array),
					  elmlen, elmbyval, elmalign,
					  &entries, entries_isnull, nentries);

	switch (strategy)
	{
		case RumOverlapStrategy:
			*searchMode = GIN_SEARCH_MODE_DEFAULT;
			break;
		case RumContainsStrategy:
			if (*nentries > 0)
				*searchMode = GIN_SEARCH_MODE_DEFAULT;
			else	/* everything contains the empty set */
				*searchMode = GIN_SEARCH_MODE_ALL;
			break;
		case RumContainedStrategy:
			/* empty set is contained in everything */
			*searchMode = GIN_SEARCH_MODE_INCLUDE_EMPTY;
			break;
		case RumEqualStrategy:
			if (*nentries > 0)
				*searchMode = GIN_SEARCH_MODE_DEFAULT;
			else
				*searchMode = GIN_SEARCH_MODE_INCLUDE_EMPTY;
			break;
		case RumSimilarStrategy:
			if (*nentries > 0)
				*searchMode = GIN_SEARCH_MODE_DEFAULT;
			else
				*searchMode = GIN_SEARCH_MODE_INCLUDE_EMPTY;
			break;
		default:
			elog(ERROR, "rum_extract_anyarray_query: unknown strategy number: %d",
				 strategy);
	}

	/* we should not free array, elems[i] points into it */
	PG_RETURN_POINTER(entries);
}



/* ginarrayconsistent() */
Datum
rum_anyarray_consistent(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);

	StrategyNumber strategy = PG_GETARG_UINT16(1);

	/* ArrayType  *query = PG_GETARG_ARRAYTYPE_P(2); */
	int32		nkeys = PG_GETARG_INT32(3);

	/* Pointer	   *extra_data = (Pointer *) PG_GETARG_POINTER(4); */
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);

	/* Datum	   *queryKeys = (Datum *) PG_GETARG_POINTER(6); */
	bool	   *nullFlags = (bool *) PG_GETARG_POINTER(7);

	Datum	   *addInfo = (Datum *) PG_GETARG_POINTER(8);
	bool	   *addInfoIsNull = (bool *) PG_GETARG_POINTER(9);

	bool		res;
	int32		i;

	switch (strategy)
	{
		case RumOverlapStrategy:
			/* result is not lossy */
			*recheck = false;
			/* must have a match for at least one non-null element */
			res = false;
			for (i = 0; i < nkeys; i++)
			{
				if (check[i] && !nullFlags[i])
				{
					res = true;
					break;
				}
			}
			break;
		case RumContainsStrategy:
			/* result is not lossy */
			*recheck = false;

			/* must have all elements in check[] true, and no nulls */
			res = true;
			for (i = 0; i < nkeys; i++)
			{
				if (!check[i] || nullFlags[i])
				{
					res = false;
					break;
				}
			}
			break;
		case RumContainedStrategy:
			/* we will need recheck */
			*recheck = true;

			/* query must have <= amount of elements than array */
			res = true;
			for (i = 0; i < nkeys; i++)
			{
				if (!addInfoIsNull[i] && DatumGetInt32(addInfo[i]) > nkeys)
				{
					res = false;
					break;
				}
			}
			break;
		case RumEqualStrategy:
			/* we will need recheck */
			*recheck = true;

			/*
			 * Must have all elements in check[] true; no discrimination
			 * against nulls here.  This is because array_contain_compare and
			 * array_eq handle nulls differently ...
			 *
			 * Also, query and array must have equal amount of elements.
			 */
			res = true;
			for (i = 0; i < nkeys; i++)
			{
				if (!check[i])
				{
					res = false;
					break;
				}

				if (!addInfoIsNull[i] && DatumGetInt32(addInfo[i]) != nkeys)
				{
					res = false;
					break;
				}
			}
			break;
		case RumSimilarStrategy:
			/* we will need recheck */
			*recheck = true;

			/* can't do anything else useful here */
			res = true;
			break;
		default:
			elog(ERROR, "rum_anyarray_consistent: unknown strategy number: %d",
				 strategy);
			res = false;
	}

	PG_RETURN_BOOL(res);
}



Datum
rum_anyarray_similar(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(true);
}

Datum
rum_anyarray_distance(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.0);
}
