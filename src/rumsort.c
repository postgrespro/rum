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
 * Portions Copyright (c) 2015-2019, Postgres Professional
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
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

/* For PGPRO since v.13 trace_sort is imported from backend by including its
 * declaration in guc.h (guc.h contains added Windows export/import magic to be done
 * during postgres.exe compilation).
 * For older or non-PGPRO versions on Windows platform trace_sort is not exported by
 * backend so it is declared local for this case.
 */
#ifdef TRACE_SORT
#if ( !defined (_MSC_VER) || (PG_VERSION_NUM >= 130000 && defined (PGPRO_VERSION)) )
#include "utils/guc.h"
#endif
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

static int	comparetup_rum_true(const SortTuple *a, const SortTuple *b,
								RumTuplesortstate * state);
static int	comparetup_rum_false(const SortTuple *a, const SortTuple *b,
								 RumTuplesortstate * state);
static int	comparetup_rum(const SortTuple *a, const SortTuple *b,
						   RumTuplesortstate * state, bool compareItemPointer);
static int	comparetup_rumitem(const SortTuple *a, const SortTuple *b,
							   RumTuplesortstate * state);
static void copytup_rum(RumTuplesortstate * state, SortTuple *stup, void *tup);
static void copytup_rumitem(RumTuplesortstate * state, SortTuple *stup, void *tup);

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
	if (i1->iptr.ip_blkid.bi_hi < i2->iptr.ip_blkid.bi_hi)
		return -1;
	else if (i1->iptr.ip_blkid.bi_hi > i2->iptr.ip_blkid.bi_hi)
		return 1;

	if (i1->iptr.ip_blkid.bi_lo < i2->iptr.ip_blkid.bi_lo)
		return -1;
	else if (i1->iptr.ip_blkid.bi_lo > i2->iptr.ip_blkid.bi_lo)
		return 1;

	if (i1->iptr.ip_posid < i2->iptr.ip_posid)
		return -1;
	else if (i1->iptr.ip_posid > i2->iptr.ip_posid)
		return 1;

	return 0;
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
	if (i1->iptr.ip_blkid.bi_hi < i2->iptr.ip_blkid.bi_hi)
		return -1;
	else if (i1->iptr.ip_blkid.bi_hi > i2->iptr.ip_blkid.bi_hi)
		return 1;

	if (i1->iptr.ip_blkid.bi_lo < i2->iptr.ip_blkid.bi_lo)
		return -1;
	else if (i1->iptr.ip_blkid.bi_lo > i2->iptr.ip_blkid.bi_lo)
		return 1;

	if (i1->iptr.ip_posid < i2->iptr.ip_posid)
		return -1;
	else if (i1->iptr.ip_posid > i2->iptr.ip_posid)
		return 1;

	return 0;
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
#define LT_DEF LogicalTape *unused
#define TAPE(state, tapenum) state->result_tape
#define LogicalTapeReadExact_compat(state, tapenum, args...) LogicalTapeReadExact(state->result_tape, ##args)
#else
#define LT_DEF int tapenum
#define TAPE(state, tapenum) state->tapeset, tapenum
#define LogicalTapeReadExact_compat(state, tapenum, args...) LogicalTapeReadExact(state->tapeset, tapenum, ##args)
#endif

static void
writetup_rum(RumTuplesortstate * state, LT_DEF, SortTuple *stup)
{
	RumSortItem *item = (RumSortItem *) stup->tuple;
	unsigned int writtenlen = RumSortItemSize(state->nKeys) + sizeof(unsigned int);

	LogicalTapeWrite(TAPE(state, tapenum),
					 (void *) &writtenlen, sizeof(writtenlen));
	LogicalTapeWrite(TAPE(state, tapenum),
					 (void *) item, RumSortItemSize(state->nKeys));
	if (state->randomAccess)	/* need trailing length word? */
		LogicalTapeWrite(TAPE(state, tapenum),
						 (void *) &writtenlen, sizeof(writtenlen));

	FREEMEM(state, GetMemoryChunkSpace(item));
	pfree(item);
}

static void
writetup_rumitem(RumTuplesortstate * state, LT_DEF, SortTuple *stup)
{
	RumScanItem *item = (RumScanItem *) stup->tuple;
	unsigned int writtenlen = sizeof(*item) + sizeof(unsigned int);

	LogicalTapeWrite(TAPE(state, tapenum),
					 (void *) &writtenlen, sizeof(writtenlen));
	LogicalTapeWrite(TAPE(state, tapenum),
					 (void *) item, sizeof(*item));
	if (state->randomAccess)	/* need trailing length word? */
		LogicalTapeWrite(TAPE(state, tapenum),
						 (void *) &writtenlen, sizeof(writtenlen));

	FREEMEM(state, GetMemoryChunkSpace(item));
	pfree(item);
}

static void
readtup_rum(RumTuplesortstate * state, SortTuple *stup,
			LT_DEF, unsigned int len)
{
	unsigned int tuplen = len - sizeof(unsigned int);
	RumSortItem *item = (RumSortItem *) palloc(RumSortItemSize(state->nKeys));

	Assert(tuplen == RumSortItemSize(state->nKeys));

	USEMEM(state, GetMemoryChunkSpace(item));
	LogicalTapeReadExact_compat(state, tapenum,
								(void *) item, RumSortItemSize(state->nKeys));
	stup->datum1 = Float8GetDatum(state->nKeys > 0 ? item->data[0] : 0);
	stup->isnull1 = false;
	stup->tuple = item;

	if (state->randomAccess)	/* need trailing length word? */
		LogicalTapeReadExact_compat(state, tapenum,
									&tuplen, sizeof(tuplen));
}

static void
readtup_rumitem(RumTuplesortstate * state, SortTuple *stup,
				LT_DEF, unsigned int len)
{
	unsigned int tuplen = len - sizeof(unsigned int);
	RumScanItem *item = (RumScanItem *) palloc(sizeof(RumScanItem));

	Assert(tuplen == sizeof(RumScanItem));

	USEMEM(state, GetMemoryChunkSpace(item));
	LogicalTapeReadExact_compat(state, tapenum,
								(void *) item, tuplen);
	stup->isnull1 = true;
	stup->tuple = item;

	if (state->randomAccess)	/* need trailing length word? */
		LogicalTapeReadExact_compat(state, tapenum,
									&tuplen, sizeof(tuplen));
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

#if PG_VERSION_NUM >= 110000
#define tuplesort_begin_common(x,y) tuplesort_begin_common((x), NULL, (y))
#endif

RumTuplesortstate *
rum_tuplesort_begin_rum(int workMem, int nKeys, bool randomAccess,
						bool compareItemPointer)
{
	RumTuplesortstate *state = tuplesort_begin_common(workMem, randomAccess);
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
	rs = palloc(sizeof(RumTuplesortstateExt));

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
	/* context swap probably not needed, but let's be safe */
	MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);

#ifdef TRACE_SORT
	long		spaceUsed;

	if (state->tapeset)
		spaceUsed = LogicalTapeSetBlocks(state->tapeset);
	else
		spaceUsed = (state->allowedMem - state->availMem + 1023) / 1024;
#endif

	/*
	 * Delete temporary "tape" files, if any.
	 *
	 * Note: want to include this in reported total cost of sort, hence need
	 * for two #ifdef TRACE_SORT sections.
	 */
	if (state->tapeset)
		LogicalTapeSetClose(state->tapeset);

#ifdef TRACE_SORT
	if (trace_sort)
	{
		if (state->tapeset)
			elog(LOG, "external sort ended, %ld disk blocks used: %s",
				 spaceUsed, pg_rusage_show(&state->ru_start));
		else
			elog(LOG, "internal sort ended, %ld KB used: %s",
				 spaceUsed, pg_rusage_show(&state->ru_start));
	}
#endif

	/* Free any execution state created for CLUSTER case */
	if (state->estate != NULL)
	{
		ExprContext *econtext = GetPerTupleExprContext(state->estate);

		ExecDropSingleTupleTableSlot(econtext->ecxt_scantuple);
		FreeExecutorState(state->estate);
	}

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Free the per-sort memory context, thereby releasing all working memory,
	 * including the Tuplesortstate struct itself.
	 */
	MemoryContextDelete(state->sortcontext);
}

void
rum_tuplesort_putrum(RumTuplesortstate * state, RumSortItem * item)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);
	SortTuple	stup;

	/*
	 * Copy the given tuple into memory we control, and decrease availMem.
	 * Then call the common code.
	 */
	COPYTUP(state, &stup, (void *) item);

	puttuple_common(state, &stup);

	MemoryContextSwitchTo(oldcontext);
}

void
rum_tuplesort_putrumitem(RumTuplesortstate * state, RumScanItem * item)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);
	SortTuple	stup;

	/*
	 * Copy the given tuple into memory we control, and decrease availMem.
	 * Then call the common code.
	 */
	COPYTUP(state, &stup, (void *) item);

	puttuple_common(state, &stup);

	MemoryContextSwitchTo(oldcontext);
}

void
rum_tuplesort_performsort(RumTuplesortstate * state)
{
	tuplesort_performsort(state);
}

/*
 * Internal routine to fetch the next tuple in either forward or back
 * direction into *stup.  Returns false if no more tuples.
 * If *should_free is set, the caller must pfree stup.tuple when done with it.
 */
static bool
rum_tuplesort_gettuple_common(RumTuplesortstate * state, bool forward,
							  SortTuple *stup, bool *should_free)
{
	bool		res = tuplesort_gettuple_common(state, forward, stup
#if PG_VERSION_NUM < 100000
												,should_free
#endif
	);

	switch (state->status)
	{
		case TSS_SORTEDINMEM:
			*should_free = false;
			break;

		case TSS_SORTEDONTAPE:
		case TSS_FINALMERGE:
			*should_free = true;
			break;

		default:
			elog(ERROR, "invalid tuplesort state");
			return false;		/* keep compiler quiet */
	}

	return res;
}

RumSortItem *
rum_tuplesort_getrum(RumTuplesortstate * state, bool forward, bool *should_free)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);
	SortTuple	stup;

	if (!rum_tuplesort_gettuple_common(state, forward, &stup, should_free))
		stup.tuple = NULL;

	MemoryContextSwitchTo(oldcontext);

	return (RumSortItem *) stup.tuple;
}

RumScanItem *
rum_tuplesort_getrumitem(RumTuplesortstate * state, bool forward, bool *should_free)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);
	SortTuple	stup;

	if (!rum_tuplesort_gettuple_common(state, forward, &stup, should_free))
		stup.tuple = NULL;

	MemoryContextSwitchTo(oldcontext);

	return (RumScanItem *) stup.tuple;
}

/*
 * rum_tuplesort_merge_order - report merge order we'll use for given memory
 * (note: "merge order" just means the number of input tapes in the merge).
 *
 * This is exported for use by the planner.  allowedMem is in bytes.
 */
int
rum_tuplesort_merge_order(long allowedMem)
{
	int			mOrder;

	/*
	 * We need one tape for each merge input, plus another one for the output,
	 * and each of these tapes needs buffer space.  In addition we want
	 * MERGE_BUFFER_SIZE workspace per input tape (but the output tape doesn't
	 * count).
	 *
	 * Note: you might be thinking we need to account for the memtuples[]
	 * array in this calculation, but we effectively treat that as part of the
	 * MERGE_BUFFER_SIZE workspace.
	 */
	mOrder = (allowedMem - TAPE_BUFFER_OVERHEAD) /
		(MERGE_BUFFER_SIZE + TAPE_BUFFER_OVERHEAD);

	/* Even in minimum memory, use at least a MINORDER merge */
	mOrder = Max(mOrder, MINORDER);

	return mOrder;
}
