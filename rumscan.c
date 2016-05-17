/*-------------------------------------------------------------------------
 *
 * rumscan.c
 *	  routines to manage scans of inverted index relations
 *
 *
 * Portions Copyright (c) 2015-2016, Postgres Professional
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/relscan.h"
#include "pgstat.h"
#include "utils/memutils.h"

#include "rum.h"

IndexScanDesc
rumbeginscan(Relation rel, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	RumScanOpaque so;

	scan = RelationGetIndexScan(rel, nkeys, norderbys);

	/* allocate private workspace */
	so = (RumScanOpaque) palloc(sizeof(RumScanOpaqueData));
	so->sortstate = NULL;
	so->keys = NULL;
	so->nkeys = 0;
	so->firstCall = true;
	so->tempCtx = AllocSetContextCreate(CurrentMemoryContext,
										"Rum scan temporary context",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);
	initRumState(&so->rumstate, scan->indexRelation);

	scan->opaque = so;

	return scan;
}

/*
 * Create a new RumScanEntry, unless an equivalent one already exists,
 * in which case just return it
 */
static RumScanEntry
rumFillScanEntry(RumScanOpaque so, OffsetNumber attnum,
				 StrategyNumber strategy, int32 searchMode,
				 Datum queryKey, RumNullCategory queryCategory,
				 bool isPartialMatch, Pointer extra_data)
{
	RumState   *rumstate = &so->rumstate;
	RumScanEntry scanEntry;
	uint32		i;

	/*
	 * Look for an existing equivalent entry.
	 *
	 * Entries with non-null extra_data are never considered identical, since
	 * we can't know exactly what the opclass might be doing with that.
	 */
	if (extra_data == NULL || !isPartialMatch)
	{
		for (i = 0; i < so->totalentries; i++)
		{
			RumScanEntry prevEntry = so->entries[i];

			if (prevEntry->extra_data == NULL &&
				prevEntry->isPartialMatch == isPartialMatch &&
				prevEntry->strategy == strategy &&
				prevEntry->searchMode == searchMode &&
				prevEntry->attnum == attnum &&
				rumCompareEntries(rumstate, attnum,
								  prevEntry->queryKey,
								  prevEntry->queryCategory,
								  queryKey,
								  queryCategory) == 0)
			{
				/* Successful match */
				return prevEntry;
			}
		}
	}

	/* Nope, create a new entry */
	scanEntry = (RumScanEntry) palloc(sizeof(RumScanEntryData));
	scanEntry->queryKey = queryKey;
	scanEntry->queryCategory = queryCategory;
	scanEntry->isPartialMatch = isPartialMatch;
	scanEntry->extra_data = extra_data;
	scanEntry->strategy = strategy;
	scanEntry->searchMode = searchMode;
	scanEntry->attnum = attnum;

	scanEntry->buffer = InvalidBuffer;
	ItemPointerSetMin(&scanEntry->curItem);
	scanEntry->matchBitmap = NULL;
	scanEntry->matchIterator = NULL;
	scanEntry->matchResult = NULL;
	scanEntry->list = NULL;
	scanEntry->nlist = 0;
	scanEntry->offset = InvalidOffsetNumber;
	scanEntry->isFinished = false;
	scanEntry->reduceResult = false;

	/* Add it to so's array */
	if (so->totalentries >= so->allocentries)
	{
		so->allocentries *= 2;
		so->entries = (RumScanEntry *)
			repalloc(so->entries, so->allocentries * sizeof(RumScanEntry));
	}
	so->entries[so->totalentries++] = scanEntry;

	return scanEntry;
}

/*
 * Initialize the next RumScanKey using the output from the extractQueryFn
 */
static void
rumFillScanKey(RumScanOpaque so, OffsetNumber attnum,
			   StrategyNumber strategy, int32 searchMode,
			   Datum query, uint32 nQueryValues,
			   Datum *queryValues, RumNullCategory *queryCategories,
			   bool *partial_matches, Pointer *extra_data,
			   bool orderBy)
{
	RumScanKey	key = &(so->keys[so->nkeys++]);
	RumState   *rumstate = &so->rumstate;
	uint32		nUserQueryValues = nQueryValues;
	uint32		i;

	/* Non-default search modes add one "hidden" entry to each key */
	if (searchMode != GIN_SEARCH_MODE_DEFAULT)
		nQueryValues++;
	key->orderBy = orderBy;

	key->query = query;
	key->queryValues = queryValues;
	key->queryCategories = queryCategories;
	key->extra_data = extra_data;
	key->strategy = strategy;
	key->searchMode = searchMode;
	key->attnum = attnum;
	key->useAddToColumn = false;

	ItemPointerSetMin(&key->curItem);
	key->curItemMatches = false;
	key->recheckCurItem = false;
	key->isFinished = false;

	if (key->orderBy && key->attnum == rumstate->attrnOrderByColumn)
	{
		if (nQueryValues != 1)
			elog(ERROR, "extractQuery should return only one value");
		if (rumstate->canOuterOrdering[attnum - 1] == false)
			elog(ERROR,"doesn't support ordering as additional info");

		key->useAddToColumn = true;
		key->attnum = rumstate->attrnAddToColumn;
		key->nentries = 0;
		key->nuserentries = 0;

		key->outerAddInfoIsNull = true;

		key->scanEntry = NULL;
		key->entryRes = NULL;
		key->addInfo = NULL;
		key->addInfoIsNull = NULL;

		return;
	}

	key->nentries = nQueryValues;
	key->nuserentries = nUserQueryValues;
	key->scanEntry = (RumScanEntry *) palloc(sizeof(RumScanEntry) * nQueryValues);
	key->entryRes = (bool *) palloc0(sizeof(bool) * nQueryValues);
	key->addInfo = (Datum *) palloc0(sizeof(Datum) * nQueryValues);
	key->addInfoIsNull = (bool *) palloc(sizeof(bool) * nQueryValues);
	for (i = 0; i < nQueryValues; i++)
		key->addInfoIsNull[i] = true;

	for (i = 0; i < nQueryValues; i++)
	{
		Datum		queryKey;
		RumNullCategory queryCategory;
		bool		isPartialMatch;
		Pointer		this_extra;

		if (i < nUserQueryValues)
		{
			/* set up normal entry using extractQueryFn's outputs */
			queryKey = queryValues[i];
			queryCategory = queryCategories[i];
			isPartialMatch =
				(rumstate->canPartialMatch[attnum - 1] && partial_matches)
				? partial_matches[i] : false;
			this_extra = (extra_data) ? extra_data[i] : NULL;
		}
		else
		{
			/* set up hidden entry */
			queryKey = (Datum) 0;
			switch (searchMode)
			{
				case GIN_SEARCH_MODE_INCLUDE_EMPTY:
					queryCategory = RUM_CAT_EMPTY_ITEM;
					break;
				case GIN_SEARCH_MODE_ALL:
					queryCategory = RUM_CAT_EMPTY_QUERY;
					break;
				case GIN_SEARCH_MODE_EVERYTHING:
					queryCategory = RUM_CAT_EMPTY_QUERY;
					break;
				default:
					elog(ERROR, "unexpected searchMode: %d", searchMode);
					queryCategory = 0;	/* keep compiler quiet */
					break;
			}
			isPartialMatch = false;
			this_extra = NULL;

			/*
			 * We set the strategy to a fixed value so that rumFillScanEntry
			 * can combine these entries for different scan keys.  This is
			 * safe because the strategy value in the entry struct is only
			 * used for partial-match cases.  It's OK to overwrite our local
			 * variable here because this is the last loop iteration.
			 */
			strategy = InvalidStrategy;
		}

		key->scanEntry[i] = rumFillScanEntry(so, attnum,
											 strategy, searchMode,
											 queryKey, queryCategory,
											 isPartialMatch, this_extra);
	}
}

static void
freeScanKeys(RumScanOpaque so)
{
	uint32		i;

	if (so->keys == NULL)
		return;

	for (i = 0; i < so->nkeys; i++)
	{
		RumScanKey	key = so->keys + i;

		if (key->nentries > 0)
		{
			if (key->scanEntry)
				pfree(key->scanEntry);
			if (key->entryRes)
				pfree(key->entryRes);
			if (key->addInfo)
				pfree(key->addInfo);
			if (key->addInfoIsNull)
				pfree(key->addInfoIsNull);
			if (key->queryCategories)
				pfree(key->queryCategories);
		}
	}

	pfree(so->keys);
	so->keys = NULL;
	so->nkeys = 0;

	for (i = 0; i < so->totalentries; i++)
	{
		RumScanEntry entry = so->entries[i];

		if (entry->gdi)
		{
			freeRumBtreeStack(entry->gdi->stack);
			pfree(entry->gdi);
		}
		else
		{
			if (entry->buffer != InvalidBuffer)
				ReleaseBuffer(entry->buffer);
		}
		if (entry->list)
			pfree(entry->list);
		if (entry->addInfo)
			pfree(entry->addInfo);
		if (entry->addInfoIsNull)
			pfree(entry->addInfoIsNull);
		if (entry->matchIterator)
			tbm_end_iterate(entry->matchIterator);
		if (entry->matchBitmap)
			tbm_free(entry->matchBitmap);
		pfree(entry);
	}

	pfree(so->entries);
	so->entries = NULL;
	so->totalentries = 0;
}

static void
initScanKey(RumScanOpaque so, ScanKey skey, bool *hasNullQuery)
{
	Datum	   *queryValues;
	int32		nQueryValues = 0;
	bool	   *partial_matches = NULL;
	Pointer	   *extra_data = NULL;
	bool	   *nullFlags = NULL;
	int32		searchMode = GIN_SEARCH_MODE_DEFAULT;

	/*
	 * We assume that RUM-indexable operators are strict, so a null query
	 * argument means an unsatisfiable query.
	 */
	if (skey->sk_flags & SK_ISNULL)
	{
		so->isVoidRes = true;
		return;
	}

	/* OK to call the extractQueryFn */
	queryValues = (Datum *)
		DatumGetPointer(FunctionCall7Coll(&so->rumstate.extractQueryFn[skey->sk_attno - 1],
						so->rumstate.supportCollation[skey->sk_attno - 1],
											skey->sk_argument,
											PointerGetDatum(&nQueryValues),
										UInt16GetDatum(skey->sk_strategy),
										PointerGetDatum(&partial_matches),
											PointerGetDatum(&extra_data),
											PointerGetDatum(&nullFlags),
											PointerGetDatum(&searchMode)));

	/*
		* If bogus searchMode is returned, treat as RUM_SEARCH_MODE_ALL; note
		* in particular we don't allow extractQueryFn to select
		* RUM_SEARCH_MODE_EVERYTHING.
		*/
	if (searchMode < GIN_SEARCH_MODE_DEFAULT ||
		searchMode > GIN_SEARCH_MODE_ALL)
		searchMode = GIN_SEARCH_MODE_ALL;

	/* Non-default modes require the index to have placeholders */
	if (searchMode != GIN_SEARCH_MODE_DEFAULT)
		*hasNullQuery = true;

	/*
		* In default mode, no keys means an unsatisfiable query.
		*/
	if (queryValues == NULL || nQueryValues <= 0)
	{
		if (searchMode == GIN_SEARCH_MODE_DEFAULT)
		{
			so->isVoidRes = true;
			return;
		}
		nQueryValues = 0;	/* ensure sane value */
	}

	/*
		* If the extractQueryFn didn't create a nullFlags array, create one,
		* assuming that everything's non-null.  Otherwise, run through the
		* array and make sure each value is exactly 0 or 1; this ensures
		* binary compatibility with the RumNullCategory representation. While
		* at it, detect whether any null keys are present.
		*/
	if (nullFlags == NULL)
		nullFlags = (bool *) palloc0(nQueryValues * sizeof(bool));
	else
	{
		int32		j;

		for (j = 0; j < nQueryValues; j++)
		{
			if (nullFlags[j])
			{
				nullFlags[j] = true;		/* not any other nonzero value */
				*hasNullQuery = true;
			}
		}
	}
	/* now we can use the nullFlags as category codes */

	rumFillScanKey(so, skey->sk_attno,
					skey->sk_strategy, searchMode,
					skey->sk_argument, nQueryValues,
					queryValues, (RumNullCategory *) nullFlags,
					partial_matches, extra_data,
					(skey->sk_flags & SK_ORDER_BY) ? true: false);
}

void
rumNewScanKey(IndexScanDesc scan)
{
	RumScanOpaque so = (RumScanOpaque) scan->opaque;
	int			i;
	bool		hasNullQuery = false;

	/* if no scan keys provided, allocate extra EVERYTHING RumScanKey */
	so->keys = (RumScanKey)
		palloc(Max(scan->numberOfKeys + scan->numberOfOrderBys, 1) *
														sizeof(RumScanKeyData));
	so->nkeys = 0;

	/* initialize expansible array of RumScanEntry pointers */
	so->totalentries = 0;
	so->allocentries = 32;
	so->entries = (RumScanEntry *)
		palloc0(so->allocentries * sizeof(RumScanEntry));

	so->isVoidRes = false;

	for (i = 0; i < scan->numberOfKeys; i++)
	{
		initScanKey(so, &scan->keyData[i], &hasNullQuery);
		if (so->isVoidRes)
			break;
	}

	for (i = 0; i < scan->numberOfOrderBys; i++)
	{
		initScanKey(so, &scan->orderByData[i], &hasNullQuery);
		if (so->isVoidRes)
			break;
	}

	if (scan->numberOfOrderBys > 0)
	{
		scan->xs_orderbyvals = palloc0(sizeof(Datum) * scan->numberOfOrderBys);
		scan->xs_orderbynulls = palloc(sizeof(bool) * scan->numberOfOrderBys);
		memset(scan->xs_orderbynulls, true, sizeof(bool) *
			   scan->numberOfOrderBys);
	}

	/*
	 * If there are no regular scan keys, generate an EVERYTHING scankey to
	 * drive a full-index scan.
	 */
	if (so->nkeys == 0 && !so->isVoidRes)
	{
		hasNullQuery = true;
		rumFillScanKey(so, FirstOffsetNumber,
					   InvalidStrategy, GIN_SEARCH_MODE_EVERYTHING,
					   (Datum) 0, 0,
					   NULL, NULL, NULL, NULL, false);
	}

	pgstat_count_index_scan(scan->indexRelation);
}

void
rumrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		  ScanKey orderbys, int norderbys)
{
	/* remaining arguments are ignored */
	RumScanOpaque so = (RumScanOpaque) scan->opaque;

	so->firstCall = true;

	freeScanKeys(so);

	if (scankey && scan->numberOfKeys > 0)
	{
		memmove(scan->keyData, scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));
		memmove(scan->orderByData, orderbys,
				scan->numberOfOrderBys * sizeof(ScanKeyData));
	}

	if (so->sortstate)
		rum_tuplesort_end(so->sortstate);
}

void
rumendscan(IndexScanDesc scan)
{
	RumScanOpaque so = (RumScanOpaque) scan->opaque;

	freeScanKeys(so);

	if (so->sortstate)
		rum_tuplesort_end(so->sortstate);

	MemoryContextDelete(so->tempCtx);

	pfree(so);
}

Datum
rummarkpos(PG_FUNCTION_ARGS)
{
	elog(ERROR, "RUM does not support mark/restore");
	PG_RETURN_VOID();
}

Datum
rumrestrpos(PG_FUNCTION_ARGS)
{
	elog(ERROR, "RUM does not support mark/restore");
	PG_RETURN_VOID();
}
