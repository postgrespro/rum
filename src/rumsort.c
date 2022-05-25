/*-------------------------------------------------------------------------
 *
 * rumsort.c
 *	  Generalized tuple sorting routines.
 *
 * This module handles sorting of RumSortItem or RumScanItem structures.
 * It contains copy of static functions from
 * src/backend/utils/sort/tuplesort.c.
 *
 *
 * Portions Copyright (c) 2015-2022, Postgres Professional
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "rumsort.h"

#include "commands/tablespace.h"
#include "executor/executor.h"
#include "utils/logtape.h"
#include "utils/pg_rusage.h"
#include "utils/tuplesort.h"

#include "rum.h"				/* RumItem */

#if PG_VERSION_NUM >= 150000
#include "tuplesort15.c"
#elif PG_VERSION_NUM >= 140000
#include "tuplesort14.c"
#elif PG_VERSION_NUM >= 130000
#include "tuplesort13.c"
#elif PG_VERSION_NUM >= 120000
#include "tuplesort12.c"
#elif PG_VERSION_NUM >= 110000
#include "tuplesort11.c"
#elif PG_VERSION_NUM >= 100000
#include "tuplesort10.c"
#elif PG_VERSION_NUM >= 90600
#include "tuplesort96.c"
#endif

/*
 * We need extra field in a state structure but we should not modify struct RumTuplesortstate
 * which is inherited from Tuplesortstate core function.
 */
typedef struct RumTuplesortstateExt
{
	RumTuplesortstate ts;
	FmgrInfo   *cmp;
}			RumTuplesortstateExt;

static int	compare_rum_itempointer(ItemPointerData p1, ItemPointerData p2);
static int	comparetup_rum(const SortTuple *a, const SortTuple *b,
						   RumTuplesortstate * state, bool compareItemPointer);
static int	comparetup_rum_true(const SortTuple *a, const SortTuple *b,
								RumTuplesortstate * state);
static int	comparetup_rum_false(const SortTuple *a, const SortTuple *b,
								 RumTuplesortstate * state);
static int	comparetup_rumitem(const SortTuple *a, const SortTuple *b,
							   RumTuplesortstate * state);
static void copytup_rum(RumTuplesortstate * state, SortTuple *stup, void *tup);
static void copytup_rumitem(RumTuplesortstate * state, SortTuple *stup, void *tup);
static void *rum_tuplesort_getrum_internal(RumTuplesortstate * state, bool forward, bool *should_free);

static int
compare_rum_itempointer(ItemPointerData p1, ItemPointerData p2)
{
	if (p1.ip_blkid.bi_hi < p2.ip_blkid.bi_hi)
		return -1;
	else if (p1.ip_blkid.bi_hi > p2.ip_blkid.bi_hi)
		return 1;

	if (p1.ip_blkid.bi_lo < p2.ip_blkid.bi_lo)
		return -1;
	else if (p1.ip_blkid.bi_lo > p2.ip_blkid.bi_lo)
		return 1;

	if (p1.ip_posid < p2.ip_posid)
		return -1;
	else if (p1.ip_posid > p2.ip_posid)
		return 1;

	return 0;
}

static int
comparetup_rum(const SortTuple *a, const SortTuple *b, RumTuplesortstate * state, bool compareItemPointer)
{
	RumSortItem *i1,
			   *i2;
	float8		v1 = DatumGetFloat8(a->datum1);
	float8		v2 = DatumGetFloat8(b->datum1);
	int			i;

	if (v1 < v2)
		return -1;
	else if (v1 > v2)
		return 1;

	i1 = (RumSortItem *) a->tuple;
	i2 = (RumSortItem *) b->tuple;

	for (i = 1; i < state->nKeys; i++)
	{
		if (i1->data[i] < i2->data[i])
			return -1;
		else if (i1->data[i] > i2->data[i])
			return 1;
	}

	if (!compareItemPointer)
		return 0;

	/*
	 * If key values are equal, we sort on ItemPointer.
	 */
	return compare_rum_itempointer(i1->iptr, i2->iptr);
}

static int
comparetup_rum_true(const SortTuple *a, const SortTuple *b, RumTuplesortstate * state)
{
	return comparetup_rum(a, b, state, true);
}

static int
comparetup_rum_false(const SortTuple *a, const SortTuple *b, RumTuplesortstate * state)
{
	return comparetup_rum(a, b, state, false);
}

static int
comparetup_rumitem(const SortTuple *a, const SortTuple *b, RumTuplesortstate * state)
{
	RumItem    *i1,
			   *i2;

	/* Extract RumItem from RumScanItem */
	i1 = (RumItem *) a->tuple;
	i2 = (RumItem *) b->tuple;

	if (((RumTuplesortstateExt *) state)->cmp)
	{
		if (i1->addInfoIsNull || i2->addInfoIsNull)
		{
			if (!(i1->addInfoIsNull && i2->addInfoIsNull))
				return (i1->addInfoIsNull) ? 1 : -1;
			/* go to itempointer compare */
		}
		else
		{
			int			r;

			r = DatumGetInt32(FunctionCall2(((RumTuplesortstateExt *) state)->cmp,
											i1->addInfo,
											i2->addInfo));

			if (r != 0)
				return r;
		}
	}

	/*
	 * If key values are equal, we sort on ItemPointer.
	 */
	return compare_rum_itempointer(i1->iptr, i2->iptr);
}

static void
copytup_rum(RumTuplesortstate * state, SortTuple *stup, void *tup)
{
	RumSortItem *item = (RumSortItem *) tup;

	stup->datum1 = Float8GetDatum(state->nKeys > 0 ? item->data[0] : 0);
	stup->isnull1 = false;
	stup->tuple = tup;
	USEMEM(state, GetMemoryChunkSpace(tup));
}

static void
copytup_rumitem(RumTuplesortstate * state, SortTuple *stup, void *tup)
{
	stup->isnull1 = true;
	stup->tuple = palloc(sizeof(RumScanItem));
	memcpy(stup->tuple, tup, sizeof(RumScanItem));
	USEMEM(state, GetMemoryChunkSpace(stup->tuple));
}

#if PG_VERSION_NUM >= 150000
#define LT_TYPE LogicalTape *
#define LT_ARG tape
#define TAPE(state, LT_ARG) LT_ARG
#else
#define LT_TYPE int
#define LT_ARG tapenum
#define TAPE(state, LT_ARG) state->tapeset, LT_ARG
#endif

static Size
rum_item_size(RumTuplesortstate * state)
{
	if (state->copytup == copytup_rum)
		return RumSortItemSize(state->nKeys);
	else if (state->copytup == copytup_rumitem)
		return sizeof(RumScanItem);
	else
		elog (FATAL, "Unknown RUM state");
}

static void
writetup_rum_internal(RumTuplesortstate * state, LT_TYPE LT_ARG, SortTuple *stup)
{
	void *item = stup->tuple;
	size_t		size = rum_item_size(state);
	unsigned int writtenlen = size + sizeof(unsigned int);

	LogicalTapeWrite(TAPE(state, LT_ARG),
					 (void *) &writtenlen, sizeof(writtenlen));
	LogicalTapeWrite(TAPE(state, LT_ARG),
					 (void *) item, size);
#if PG_VERSION_NUM >= 150000
	if (state->sortopt & TUPLESORT_RANDOMACCESS)	/* need trailing length word? */
#else
	if (state->randomAccess)        /* need trailing length word? */
#endif
		LogicalTapeWrite(TAPE(state, LT_ARG),
						 (void *) &writtenlen, sizeof(writtenlen));
}

static void
writetup_rum(RumTuplesortstate * state, LT_TYPE LT_ARG, SortTuple *stup)
{
	writetup_rum_internal(state, LT_ARG, stup);
}

static void
writetup_rumitem(RumTuplesortstate * state, LT_TYPE LT_ARG, SortTuple *stup)
{
	writetup_rum_internal(state, LT_ARG, stup);
}

static void
readtup_rum_internal(RumTuplesortstate * state, SortTuple *stup,
					 LT_TYPE LT_ARG, unsigned int len, bool is_item)
{
	unsigned int tuplen = len - sizeof(unsigned int);
	size_t		size = rum_item_size(state);
	void	   *item = palloc(size);

	Assert(tuplen == size);

	USEMEM(state, GetMemoryChunkSpace(item));
#if PG_VERSION_NUM >= 150000
	LogicalTapeReadExact(LT_ARG, item, size);
#else
	LogicalTapeReadExact(state->tapeset, LT_ARG, item, size);
#endif
	stup->tuple = item;
	stup->isnull1 = is_item;

	if (!is_item)
		stup->datum1 = Float8GetDatum(state->nKeys > 0 ? ((RumSortItem *) item)->data[0] : 0);
#if PG_VERSION_NUM >= 150000
	if (state->sortopt & TUPLESORT_RANDOMACCESS)	/* need trailing length word? */
		LogicalTapeReadExact(LT_ARG, &tuplen, sizeof(tuplen));
#else
	if (state->randomAccess)
		LogicalTapeReadExact(state->tapeset, LT_ARG, &tuplen, sizeof(tuplen));
#endif
}

static void
readtup_rum(RumTuplesortstate * state, SortTuple *stup,
			LT_TYPE LT_ARG, unsigned int len)
{
	readtup_rum_internal(state, stup, LT_ARG, len, false);
}

static void
readtup_rumitem(RumTuplesortstate * state, SortTuple *stup,
				LT_TYPE LT_ARG, unsigned int len)
{
	readtup_rum_internal(state, stup, LT_ARG, len, true);
}

#if PG_VERSION_NUM >= 110000
#define tuplesort_begin_common(x,y) tuplesort_begin_common((x), NULL, (y))
#endif

RumTuplesortstate *
rum_tuplesort_begin_rum(int workMem, int nKeys, bool randomAccess,
						bool compareItemPointer)
{
#if PG_VERSION_NUM >= 150000
	RumTuplesortstate *state = tuplesort_begin_common(workMem,
													  randomAccess ?
													  TUPLESORT_RANDOMACCESS :
													  TUPLESORT_NONE);
#else
	RumTuplesortstate *state = tuplesort_begin_common(workMem, randomAccess);
#endif
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(state->sortcontext);

#ifdef TRACE_SORT
	if (trace_sort)
		elog(LOG,
			 "begin rum sort: nKeys = %d, workMem = %d, randomAccess = %c",
			 nKeys, workMem, randomAccess ? 't' : 'f');
#endif

	state->nKeys = nKeys;

	state->comparetup = compareItemPointer ? comparetup_rum_true : comparetup_rum_false;
	state->copytup = copytup_rum;
	state->writetup = writetup_rum;
	state->readtup = readtup_rum;

	MemoryContextSwitchTo(oldcontext);

	return state;
}

RumTuplesortstate *
rum_tuplesort_begin_rumitem(int workMem, FmgrInfo *cmp)
{
	RumTuplesortstate *state = tuplesort_begin_common(workMem, false);
	RumTuplesortstateExt *rs;
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(state->sortcontext);

	/* Allocate extended state in the same context as state */
	rs = palloc(sizeof(*rs));

#ifdef TRACE_SORT
	if (trace_sort)
		elog(LOG,
			 "begin rumitem sort: workMem = %d", workMem);
#endif

	rs->cmp = cmp;
	state->comparetup = comparetup_rumitem;
	state->copytup = copytup_rumitem;
	state->writetup = writetup_rumitem;
	state->readtup = readtup_rumitem;
	memcpy(&rs->ts, state, sizeof(RumTuplesortstate));
	pfree(state);				/* just to be sure *state isn't used anywhere
								 * else */

	MemoryContextSwitchTo(oldcontext);

	return (RumTuplesortstate *) rs;
}

/*
 * rum_tuplesort_end
 *
 *	Release resources and clean up.
 *
 * NOTE: after calling this, any pointers returned by rum_tuplesort_getXXX are
 * pointing to garbage.  Be careful not to attempt to use or free such
 * pointers afterwards!
 */
void
rum_tuplesort_end(RumTuplesortstate * state)
{
#if PG_VERSION_NUM >= 130000
	tuplesort_free(state);
#else
	tuplesort_end(state);
#endif
}

/*
 * Get sort state memory context.  Currently it is used only to allocate
 * RumSortItem.
 */
MemoryContext
rum_tuplesort_get_memorycontext(RumTuplesortstate * state)
{
	return state->sortcontext;
}

void
rum_tuplesort_putrum(RumTuplesortstate *state, RumSortItem *item)
{
	tuplesort_puttupleslot(state, (TupleTableSlot *) item);
}

void
rum_tuplesort_putrumitem(RumTuplesortstate *state, RumScanItem *item)
{
	tuplesort_puttupleslot(state, (TupleTableSlot *) item);
}

void
rum_tuplesort_performsort(RumTuplesortstate * state)
{
	tuplesort_performsort(state);
}

/*
 * Internal routine to fetch the next index tuple in either forward or back direction.
 * Returns NULL if no more tuples. Returned tuple belongs to tuplesort memory context. Caller may not rely on tuple remaining valid after any further manipulation of tuplesort.
 * If *should_free is set, the caller must pfree stup.tuple when done with it.
 *
 * NOTE: in PG 10 and newer tuple is always allocated tuple in tuplesort context and
 * should not be freed by caller.
 */
static void *
rum_tuplesort_getrum_internal(RumTuplesortstate * state, bool forward, bool *should_free)
{
#if PG_VERSION_NUM >= 100000
	*should_free = false;
	return (RumSortItem *)tuplesort_getindextuple(state, forward);
#else
	return (RumSortItem *)tuplesort_getindextuple(state, forward, should_free);
#endif
}

RumSortItem *
rum_tuplesort_getrum(RumTuplesortstate * state, bool forward, bool *should_free)
{
	return (RumSortItem *) rum_tuplesort_getrum_internal(state, forward, should_free);
}

RumScanItem *
rum_tuplesort_getrumitem(RumTuplesortstate * state, bool forward, bool *should_free)
{
	return (RumScanItem *) rum_tuplesort_getrum_internal(state, forward, should_free);
}
