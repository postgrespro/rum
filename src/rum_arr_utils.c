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
#include "access/nbtree.h"
#include "catalog/pg_am.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#include "rum.h"

#include <math.h>


#define RumOverlapStrategy		1
#define RumContainsStrategy		2
#define RumContainedStrategy	3
#define RumEqualStrategy		4
#define RumSimilarStrategy		5


#define LINEAR_LIMIT	5
#define NDIM			1


#define ARRNELEMS(x)	ArrayGetNItems(ARR_NDIM(x), ARR_DIMS(x))
#define ARRISVOID(x)	((x) == NULL || ARRNELEMS(x) == 0)

#define CHECKARRVALID(x) \
	do { \
		if (x) { \
			if (ARR_NDIM(x) != NDIM && ARR_NDIM(x) != 0) \
				ereport(ERROR, \
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), \
						 errmsg("array must have 1 dimension"))); \
			if (ARR_HASNULL(x)) \
				ereport(ERROR, \
						(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), \
						 errmsg("array must not contain nulls"))); \
		} \
	} while(0)


typedef struct AnyArrayTypeInfo
{
	Oid					typid;
	int16				typlen;
	bool				typbyval;
	char				typalign;
	MemoryContext		funcCtx;
	Oid					cmpFuncOid;
	bool				cmpFuncInited;
	FmgrInfo			cmpFunc;
	bool				hashFuncInited;
	Oid					hashFuncOid;
	FmgrInfo			hashFunc;
} AnyArrayTypeInfo;

typedef struct SimpleArray
{
	Datum			   *elems;
	int32			   *hashedElems;
	int32				nelems;
	int32				nHashedElems;
	AnyArrayTypeInfo   *info;
} SimpleArray;

typedef enum SimilarityType {
	AA_Cosine,
	AA_Jaccard,
	AA_Overlap
} SimilarityType;


PG_FUNCTION_INFO_V1(rum_anyarray_config);

PG_FUNCTION_INFO_V1(rum_extract_anyarray);
PG_FUNCTION_INFO_V1(rum_extract_anyarray_query);

PG_FUNCTION_INFO_V1(rum_anyarray_consistent);

PG_FUNCTION_INFO_V1(rum_anyarray_similar);
PG_FUNCTION_INFO_V1(rum_anyarray_distance);


static SimilarityType	SmlType;
static double			SmlLimit;


static Oid getAMProc(Oid amOid, Oid typid);

static AnyArrayTypeInfo *getAnyArrayTypeInfo(MemoryContext ctx, Oid typid);
static AnyArrayTypeInfo *getAnyArrayTypeInfoCached(FunctionCallInfo fcinfo, Oid typid);
static void freeAnyArrayTypeInfo(AnyArrayTypeInfo *info);
static void cmpFuncInit(AnyArrayTypeInfo *info);

static SimpleArray *Array2SimpleArray(AnyArrayTypeInfo *info, ArrayType *a);
static ArrayType *SimpleArray2Array(SimpleArray *s);
static void freeSimpleArray(SimpleArray *s);
static int cmpAscArrayElem(const void *a, const void *b, void *arg);
static int cmpDescArrayElem(const void *a, const void *b, void *arg);
static void sortSimpleArray(SimpleArray *s, int32 direction);
static void uniqSimpleArray(SimpleArray *s, bool onlyDuplicate);

static int getNumOfIntersect(SimpleArray *sa, SimpleArray *sb);
static double getSimilarity(SimpleArray *sa, SimpleArray *sb);



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


/*
 * Extract entries and queries
 */

/* Enhanced version of ginarrayextract() */
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

	CHECKARRVALID(array);

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

/* Enhanced version of ginqueryarrayextract() */
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

	CHECKARRVALID(array);

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
			*searchMode = GIN_SEARCH_MODE_DEFAULT;
			break;
		default:
			elog(ERROR, "rum_extract_anyarray_query: unknown strategy number: %d",
				 strategy);
	}

	/* we should not free array, elems[i] points into it */
	PG_RETURN_POINTER(entries);
}


/*
 * Consistency check
 */

/* Enhanced version of ginarrayconsistent() */
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


/*
 * Similarity and distance
 */

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


/*
 * Convenience routines
 */

static Oid
getAMProc(Oid amOid, Oid typid)
{
	Oid		opclassOid = GetDefaultOpClass(typid, amOid);
	Oid		procOid;

	Assert(amOid == BTREE_AM_OID || amOid == HASH_AM_OID);

	if (!OidIsValid(opclassOid))
	{
		typid = getBaseType(typid);
		opclassOid = GetDefaultOpClass(typid, amOid);


		if (!OidIsValid(opclassOid))
		{
			CatCList	*catlist;
			int			i;

			/*
			 * Search binary-coercible type
			 */
			catlist = SearchSysCacheList(CASTSOURCETARGET, 1,
										 ObjectIdGetDatum(typid),
										 0, 0, 0);
			for (i = 0; i < catlist->n_members; i++)
			{
				HeapTuple		tuple = &catlist->members[i]->tuple;
				Form_pg_cast	castForm = (Form_pg_cast)GETSTRUCT(tuple);

				if (castForm->castmethod == COERCION_METHOD_BINARY)
				{
					typid = castForm->casttarget;
					opclassOid = GetDefaultOpClass(typid, amOid);
					if(OidIsValid(opclassOid))
						break;
				}
			}

			ReleaseSysCacheList(catlist);
		}
	}

	if (!OidIsValid(opclassOid))
		return InvalidOid;

	procOid = get_opfamily_proc(get_opclass_family(opclassOid),
							 typid, typid,
							 (amOid == BTREE_AM_OID) ? BTORDER_PROC : HASHPROC);

	if (!OidIsValid(procOid))
	{
		typid = get_opclass_input_type(opclassOid);

		procOid = get_opfamily_proc(get_opclass_family(opclassOid),
								 typid, typid,
								 (amOid == BTREE_AM_OID) ? BTORDER_PROC : HASHPROC);
	}

	return procOid;
}


/*
 * AnyArrayTypeInfo functions
 */

static AnyArrayTypeInfo *
getAnyArrayTypeInfo(MemoryContext ctx, Oid typid)
{
	AnyArrayTypeInfo	*info;

	info = MemoryContextAlloc(ctx, sizeof(*info));

	info->typid = typid;
	info->cmpFuncOid = InvalidOid;
	info->hashFuncOid = InvalidOid;
	info->cmpFuncInited = false;
	info->hashFuncInited = false;
	info->funcCtx = ctx;

	get_typlenbyvalalign(typid, &info->typlen, &info->typbyval, &info->typalign);

	return info;
}

static AnyArrayTypeInfo *
getAnyArrayTypeInfoCached(FunctionCallInfo fcinfo, Oid typid)
{
	AnyArrayTypeInfo	*info = NULL;

	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

	if (info == NULL || info->typid != typid)
	{
		freeAnyArrayTypeInfo(info);
		info = getAnyArrayTypeInfo(fcinfo->flinfo->fn_mcxt, typid);
		fcinfo->flinfo->fn_extra = info;
	}

	return info;
}

static void
freeAnyArrayTypeInfo(AnyArrayTypeInfo *info)
{
	if (info)
	{
		/*
		 * there is no way to cleanup FmgrInfo...
		 */
		pfree(info);
	}
}

static void
cmpFuncInit(AnyArrayTypeInfo *info)
{
	if (info->cmpFuncInited == false)
	{
		if (!OidIsValid(info->cmpFuncOid))
		{
			info->cmpFuncOid = getAMProc(BTREE_AM_OID, info->typid);

			if (!OidIsValid(info->cmpFuncOid))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not find compare function")));
		}

		fmgr_info_cxt(info->cmpFuncOid, &info->cmpFunc, info->funcCtx);
		info->cmpFuncInited = true;
	}
}



/*
 * SimpleArray functions
 */

static SimpleArray *
Array2SimpleArray(AnyArrayTypeInfo *info, ArrayType *a)
{
	SimpleArray *s = palloc(sizeof(SimpleArray));

	CHECKARRVALID(a);

	s->info = info;
	s->nHashedElems = 0;
	s->hashedElems = NULL;

	if (ARRISVOID(a))
	{
		s->elems = NULL;
		s->nelems = 0;
	}
	else
	{
		deconstruct_array(a, info->typid,
						  info->typlen, info->typbyval, info->typalign,
						  &s->elems, NULL, &s->nelems);
	}

	return s;
}

static ArrayType *
SimpleArray2Array(SimpleArray *s)
{
	return construct_array(s->elems, s->nelems,
						   s->info->typid,
						   s->info->typlen,
						   s->info->typbyval,
						   s->info->typalign);
}

static void
freeSimpleArray(SimpleArray *s)
{
	if (s)
	{
		if (s->elems)
			pfree(s->elems);
		if (s->hashedElems)
			pfree(s->hashedElems);
		pfree(s);
	}
}

static int
cmpAscArrayElem(const void *a, const void *b, void *arg)
{
	FmgrInfo	*cmpFunc = (FmgrInfo*)arg;

	return DatumGetInt32(FunctionCall2Coll(cmpFunc, DEFAULT_COLLATION_OID, *(Datum*)a, *(Datum*)b));
}

static int
cmpDescArrayElem(const void *a, const void *b, void *arg)
{
	FmgrInfo	*cmpFunc = (FmgrInfo*)arg;

	return -DatumGetInt32(FunctionCall2Coll(cmpFunc, DEFAULT_COLLATION_OID, *(Datum*)a, *(Datum*)b));
}

static void
sortSimpleArray(SimpleArray *s, int32 direction)
{
	AnyArrayTypeInfo	*info = s->info;

	cmpFuncInit(info);

	if (s->nelems > 1)
	{
		qsort_arg(s->elems, s->nelems, sizeof(Datum),
				  (direction > 0) ? cmpAscArrayElem : cmpDescArrayElem,
				  &info->cmpFunc);
	}
}

static void
uniqSimpleArray(SimpleArray *s, bool onlyDuplicate)
{
	AnyArrayTypeInfo	*info = s->info;

	cmpFuncInit(info);

	if (s->nelems > 1)
	{
		Datum	*tmp, *dr;
		int32	num =  s->nelems;

		if (onlyDuplicate)
		{
			Datum	*head = s->elems;

			dr = s->elems;
			tmp = s->elems + 1;

			while (tmp - s->elems < num)
			{
				while (tmp - s->elems < num && cmpAscArrayElem(tmp, dr, &info->cmpFunc) == 0)
					tmp++;

				if (tmp - dr > 1)
				{
					*head = *dr;
					head++;
				}
				dr = tmp;
			}

			s->nelems = head - s->elems;
		}
		else
		{
			dr = s->elems;
			tmp = s->elems + 1;

			while (tmp - s->elems < num)
			{
				if (cmpAscArrayElem(tmp, dr, &info->cmpFunc) != 0 )
					*(++dr) = *tmp++;
				else
					tmp++;
			}

			s->nelems = dr + 1 - s->elems;
		}
	}
	else if (onlyDuplicate)
	{
		s->nelems = 0;
	}
}


/*
 * Similarity calculation
 */

static int
getNumOfIntersect(SimpleArray *sa, SimpleArray *sb)
{
	int					cnt = 0,
						cmp;
	Datum				*aptr = sa->elems,
						*bptr = sb->elems;
	AnyArrayTypeInfo	*info = sa->info;

	cmpFuncInit(info);

	sortSimpleArray(sa, 1);
	uniqSimpleArray(sa, false);
	sortSimpleArray(sb, 1);
	uniqSimpleArray(sb, false);

	while(aptr - sa->elems < sa->nelems && bptr - sb->elems < sb->nelems)
	{
		cmp = cmpAscArrayElem(aptr, bptr, &info->cmpFunc);

		if (cmp < 0)
			aptr++;
		else if (cmp > 0)
			bptr++;
		else
		{
			cnt++;
			aptr++;
			bptr++;
		}
	}

	return cnt;
}

static double
getSimilarity(SimpleArray *sa, SimpleArray *sb)
{
	int			inter;
	double		result = 0.0;

	inter = getNumOfIntersect(sa, sb);

	switch(SmlType)
	{
		case AA_Cosine:
			result = ((double)inter) / sqrt(((double)sa->nelems) * ((double)sb->nelems));
			break;
		case AA_Jaccard:
			result = ((double)inter) / (((double)sa->nelems) + ((double)sb->nelems) - ((double)inter));
			break;
		case AA_Overlap:
			result = inter;
			break;
		default:
			elog(ERROR, "unknown similarity type");
	}

	return result;
}
