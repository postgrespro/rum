/*-------------------------------------------------------------------------
 *
 * rumget.c
 *	  fetch tuples from a RUM scan.
 *
 *
 * Portions Copyright (c) 2015-2024, Postgres Professional
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "rumsort.h"
#include "rumtidbitmap.h"

#include "access/relscan.h"
#include "storage/predicate.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#if PG_VERSION_NUM >= 120000
#include "utils/float.h"
#endif
#if PG_VERSION_NUM >= 150000
#include "common/pg_prng.h"
#endif
#include "rum.h"

/* GUC parameter */
int			RumFuzzySearchLimit = 0;

static bool scanPage(RumState * rumstate, RumScanEntry entry, RumItem *item,
					 bool equalOk);
static void insertScanItem(RumScanOpaque so, bool recheck);
static int	scan_entry_cmp(const void *p1, const void *p2, void *arg);
static void entryGetItem(RumState * rumstate, RumScanEntry entry, bool *nextEntryList, Snapshot snapshot);

/*
 * Extract key value for ordering.
 *
 * XXX FIXME only pass-by-value!!! Value should be copied to
 * long-lived memory context and, somehow, freeed. Seems, the
 * last is real problem.
 */
#define SCAN_ENTRY_GET_KEY(entry, rumstate, itup)							\
do {																		\
	if ((entry)->useCurKey)													\
		(entry)->curKey = rumtuple_get_key(rumstate, itup, &(entry)->curKeyCategory); \
} while(0)

/*
 * Assign key value for ordering.
 *
 * XXX FIXME only pass-by-value!!! Value should be copied to
 * long-lived memory context and, somehow, freeed. Seems, the
 * last is real problem.
 */
#define SCAN_ITEM_PUT_KEY(entry, item, key, category)						\
do {																		\
	if ((entry)->useCurKey)													\
	{																		\
		(item).keyValue = key;												\
		(item).keyCategory = category;										\
	}																		\
} while(0)

static bool
callAddInfoConsistentFn(RumState * rumstate, RumScanKey key)
{
	uint32		i;
	bool		res = true;

	/* it should be true for search key, but it could be false for order key */
	Assert(key->attnum == key->attnumOrig);

	if (key->attnum != rumstate->attrnAddToColumn)
		return true;

	/*
	 * remember some addinfo value for later ordering by addinfo from
	 * another column
	 */

	key->outerAddInfoIsNull = true;

	if (key->addInfoKeys == NULL && key->willSort == false)
		return true;

	for (i = 0; i < key->nentries; i++)
	{
		if (key->entryRes[i] && key->addInfoIsNull[i] == false)
		{
			key->outerAddInfoIsNull = false;

			/*
			 * XXX FIXME only pass-by-value!!! Value should be copied to
			 * long-lived memory context and, somehow, freeed. Seems, the
			 * last is real problem.
			 * But actually it's a problem only for ordering, as restricting
			 * clause it used only inside this function.
			 */
			key->outerAddInfo = key->addInfo[i];
			break;
		}
	}

	if (key->addInfoKeys)
	{
		if (key->outerAddInfoIsNull)
			res = false; /* assume strict operator */

		for(i = 0; res && i < key->addInfoNKeys; i++)
		{
			RumScanKey subkey = key->addInfoKeys[i];
			int j;

			for(j=0; res && j<subkey->nentries; j++)
			{
				RumScanEntry	scanSubEntry = subkey->scanEntry[j];
				int cmp =
				DatumGetInt32(FunctionCall4Coll(
					&rumstate->comparePartialFn[scanSubEntry->attnumOrig - 1],
					rumstate->supportCollation[scanSubEntry->attnumOrig - 1],
					scanSubEntry->queryKey,
					key->outerAddInfo,
					UInt16GetDatum(scanSubEntry->strategy),
					PointerGetDatum(scanSubEntry->extra_data)
				));

				if (cmp != 0)
					res = false;
			}
		}
	}

	return res;
}

/*
 * Convenience function for invoking a key's consistentFn
 */
static bool
callConsistentFn(RumState * rumstate, RumScanKey key)
{
	bool		res;

	/* it should be true for search key, but it could be false for order key */
	Assert(key->attnum == key->attnumOrig);

	/*
	 * If we're dealing with a dummy EVERYTHING key, we don't want to call the
	 * consistentFn; just claim it matches.
	 */
	if (key->searchMode == GIN_SEARCH_MODE_EVERYTHING)
	{
		key->recheckCurItem = false;
		res = true;
	}
	else
	{
		/*
		 * Initialize recheckCurItem in case the consistentFn doesn't know it
		 * should set it.  The safe assumption in that case is to force
		 * recheck.
		 */
		key->recheckCurItem = true;

		res = DatumGetBool(FunctionCall10Coll(&rumstate->consistentFn[key->attnum - 1],
								 rumstate->supportCollation[key->attnum - 1],
											  PointerGetDatum(key->entryRes),
											  UInt16GetDatum(key->strategy),
											  key->query,
										   UInt32GetDatum(key->nuserentries),
											PointerGetDatum(key->extra_data),
									   PointerGetDatum(&key->recheckCurItem),
										   PointerGetDatum(key->queryValues),
									   PointerGetDatum(key->queryCategories),
											  PointerGetDatum(key->addInfo),
										  PointerGetDatum(key->addInfoIsNull)
											  ));
	}

	return res && callAddInfoConsistentFn(rumstate, key);
}

/*
 * Goes to the next page if current offset is outside of bounds
 */
static bool
moveRightIfItNeeded(RumBtreeData * btree, RumBtreeStack * stack)
{
	Page		page = BufferGetPage(stack->buffer);

	if (stack->off > PageGetMaxOffsetNumber(page))
	{
		/*
		 * We scanned the whole page, so we should take right page
		 */
		if (RumPageRightMost(page))
			return false;		/* no more pages */

		stack->buffer = rumStep(stack->buffer, btree->index, RUM_SHARE,
								ForwardScanDirection);
		stack->blkno = BufferGetBlockNumber(stack->buffer);
		stack->off = FirstOffsetNumber;
	}

	return true;
}

/*
 * Scan all pages of a posting tree and save all its heap ItemPointers
 * in scanEntry->matchSortstate
 */
static void
scanPostingTree(Relation index, RumScanEntry scanEntry,
				BlockNumber rootPostingTree, OffsetNumber attnum,
				RumState * rumstate, Datum idatum, RumNullCategory icategory,
				Snapshot snapshot)
{
	RumPostingTreeScan *gdi;
	Buffer		buffer;
	Page		page;

	Assert(ScanDirectionIsForward(scanEntry->scanDirection));
	/* Descend to the leftmost leaf page */
	gdi = rumPrepareScanPostingTree(index, rootPostingTree, true,
									ForwardScanDirection, attnum, rumstate);

	buffer = rumScanBeginPostingTree(gdi, NULL);

	IncrBufferRefCount(buffer); /* prevent unpin in freeRumBtreeStack */

	PredicateLockPage(index, BufferGetBlockNumber(buffer), snapshot);

	freeRumBtreeStack(gdi->stack);
	pfree(gdi);

	/*
	 * Loop iterates through all leaf pages of posting tree
	 */
	for (;;)
	{
		OffsetNumber maxoff,
					i;

		page = BufferGetPage(buffer);
		maxoff = RumPageGetOpaque(page)->maxoff;

		if ((RumPageGetOpaque(page)->flags & RUM_DELETED) == 0 &&
			maxoff >= FirstOffsetNumber)
		{
			RumScanItem	item;
			Pointer		ptr;

			MemSet(&item, 0, sizeof(item));
			ItemPointerSetMin(&item.item.iptr);

			ptr = RumDataPageGetData(page);
			for (i = FirstOffsetNumber; i <= maxoff; i++)
			{
				ptr = rumDataPageLeafRead(ptr, attnum, &item.item, false,
										  rumstate);
				SCAN_ITEM_PUT_KEY(scanEntry, item, idatum, icategory);
				rum_tuplesort_putrumitem(scanEntry->matchSortstate, &item);
			}

			scanEntry->predictNumberResult += maxoff;
		}

		if (RumPageRightMost(page))
			break;				/* no more pages */

		buffer = rumStep(buffer, index, RUM_SHARE, ForwardScanDirection);

		PredicateLockPage(index, BufferGetBlockNumber(buffer), snapshot);

	}

	UnlockReleaseBuffer(buffer);
}

/*
 * Collects TIDs into scanEntry->matchSortstate for all heap tuples that
 * match the search entry.  This supports three different match modes:
 *
 * 1. Partial-match support: scan from current point until the
 *	  comparePartialFn says we're done.
 * 2. SEARCH_MODE_ALL: scan from current point (which should be first
 *	  key for the current attnum) until we hit null items or end of attnum
 * 3. SEARCH_MODE_EVERYTHING: scan from current point (which should be first
 *	  key for the current attnum) until we hit end of attnum
 *
 * Returns true if done, false if it's necessary to restart scan from scratch
 */
static bool
collectMatchBitmap(RumBtreeData * btree, RumBtreeStack * stack,
				   RumScanEntry scanEntry, Snapshot snapshot)
{
	OffsetNumber attnum;
	Form_pg_attribute attr;
	FmgrInfo	*cmp = NULL;
	RumState	*rumstate = btree->rumstate;

	if (rumstate->useAlternativeOrder &&
		scanEntry->attnumOrig == rumstate->attrnAddToColumn)
	{
		cmp = &rumstate->compareFn[rumstate->attrnAttachColumn - 1];
	}

	/* Initialize  */
	scanEntry->matchSortstate = rum_tuplesort_begin_rumitem(work_mem, cmp);

	/* Null query cannot partial-match anything */
	if (scanEntry->isPartialMatch &&
		scanEntry->queryCategory != RUM_CAT_NORM_KEY)
		return true;

	/* Locate tupdesc entry for key column (for attbyval/attlen data) */
	attnum = scanEntry->attnumOrig;
	attr = RumTupleDescAttr(rumstate->origTupdesc, attnum - 1);

	for (;;)
	{
		Page		page;
		IndexTuple	itup;
		Datum		idatum;
		RumNullCategory icategory;

		/*
		 * stack->off points to the interested entry, buffer is already locked
		 */
		if (moveRightIfItNeeded(btree, stack) == false)
			return true;

		page = BufferGetPage(stack->buffer);
		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, stack->off));

		/*
		 * If tuple stores another attribute then stop scan
		 */
		if (rumtuple_get_attrnum(rumstate, itup) != attnum)
			return true;

		/* Safe to fetch attribute value */
		idatum = rumtuple_get_key(rumstate, itup, &icategory);

		/*
		 * Check for appropriate scan stop conditions
		 */
		if (scanEntry->isPartialMatch)
		{
			int32		cmp;

			/*
			 * In partial match, stop scan at any null (including
			 * placeholders); partial matches never match nulls
			 */
			if (icategory != RUM_CAT_NORM_KEY)
				return true;

			/*----------
			 * Check of partial match.
			 * case cmp == 0 => match
			 * case cmp > 0 => not match and finish scan
			 * case cmp < 0 => not match and continue scan
			 *----------
			 */
			cmp = DatumGetInt32(FunctionCall4Coll(&rumstate->comparePartialFn[attnum - 1],
							   rumstate->supportCollation[attnum - 1],
												  scanEntry->queryKey,
												  idatum,
										 UInt16GetDatum(scanEntry->strategy),
									PointerGetDatum(scanEntry->extra_data)));

			if (cmp > 0)
				return true;
			else if (cmp < 0)
			{
				stack->off++;
				continue;
			}
		}
		else if (scanEntry->searchMode == GIN_SEARCH_MODE_ALL)
		{
			/*
			 * In ALL mode, we are not interested in null items, so we can
			 * stop if we get to a null-item placeholder (which will be the
			 * last entry for a given attnum).  We do want to include NULL_KEY
			 * and EMPTY_ITEM entries, though.
			 */
			if (icategory == RUM_CAT_NULL_ITEM)
				return true;
		}

		/*
		 * OK, we want to return the TIDs listed in this entry.
		 */
		if (RumIsPostingTree(itup))
		{
			BlockNumber rootPostingTree = RumGetPostingTree(itup);

			/*
			 * We should unlock current page (but not unpin) during tree scan
			 * to prevent deadlock with vacuum processes.
			 *
			 * We save current entry value (idatum) to be able to re-find our
			 * tuple after re-locking
			 */
			if (icategory == RUM_CAT_NORM_KEY)
				idatum = datumCopy(idatum, attr->attbyval, attr->attlen);

			LockBuffer(stack->buffer, RUM_UNLOCK);

			/* Collect all the TIDs in this entry's posting tree */
			scanPostingTree(btree->index, scanEntry, rootPostingTree, attnum,
							rumstate, idatum, icategory, snapshot);

			/*
			 * We lock again the entry page and while it was unlocked insert
			 * might have occurred, so we need to re-find our position.
			 */
			LockBuffer(stack->buffer, RUM_SHARE);
			page = BufferGetPage(stack->buffer);
			if (!RumPageIsLeaf(page))
			{
				/*
				 * Root page becomes non-leaf while we unlock it. We will
				 * start again, this situation doesn't occur often - root can
				 * became a non-leaf only once per life of index.
				 */
				return false;
			}

			/* Search forward to re-find idatum */
			for (;;)
			{
				Datum		newDatum;
				RumNullCategory newCategory;

				if (moveRightIfItNeeded(btree, stack) == false)
					elog(ERROR, "lost saved point in index");	/* must not happen !!! */

				page = BufferGetPage(stack->buffer);
				itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, stack->off));

				if (rumtuple_get_attrnum(rumstate, itup) != attnum)
					elog(ERROR, "lost saved point in index");	/* must not happen !!! */
				newDatum = rumtuple_get_key(rumstate, itup,
											&newCategory);

				if (rumCompareEntries(rumstate, attnum,
									  newDatum, newCategory,
									  idatum, icategory) == 0)
					break;		/* Found! */

				stack->off++;
			}

			if (icategory == RUM_CAT_NORM_KEY && !attr->attbyval)
				pfree(DatumGetPointer(idatum));
		}
		else
		{
			int	i;
			char	*ptr = RumGetPosting(itup);
			RumScanItem item;

			MemSet(&item, 0, sizeof(item));
			ItemPointerSetMin(&item.item.iptr);
			for (i = 0; i < RumGetNPosting(itup); i++)
			{
				ptr = rumDataPageLeafRead(ptr, scanEntry->attnum, &item.item,
										  true, rumstate);
				SCAN_ITEM_PUT_KEY(scanEntry, item, idatum, icategory);
				rum_tuplesort_putrumitem(scanEntry->matchSortstate, &item);
			}

			scanEntry->predictNumberResult += RumGetNPosting(itup);
		}

		/*
		 * Done with this entry, go to the next
		 */
		stack->off++;
	}
}

/*
 * set right position in entry->list accordingly to markAddInfo.
 * returns true if there is not such position.
 */
static bool
setListPositionScanEntry(RumState * rumstate, RumScanEntry entry)
{
	OffsetNumber	StopLow	= entry->offset,
					StopHigh = entry->nlist;

	if (entry->useMarkAddInfo == false)
	{
		entry->offset = (ScanDirectionIsForward(entry->scanDirection)) ?
							0 : entry->nlist - 1;
		return false;
	}

	while (StopLow < StopHigh)
	{
		int			 res;

		entry->offset = StopLow + ((StopHigh - StopLow) >> 1);
		res = compareRumItem(rumstate, entry->attnumOrig, &entry->markAddInfo,
							entry->list + entry->offset);

		if (res < 0)
			StopHigh = entry->offset;
		else if (res > 0)
			StopLow = entry->offset + 1;
		else
			return false;
	}

	if (ScanDirectionIsForward(entry->scanDirection))
	{
		entry->offset = StopHigh;

		return (StopHigh >= entry->nlist);
	}
	else
	{
		if (StopHigh == 0)
			return true;

		entry->offset = StopHigh - 1;

		return false;
	}
}

/*
 * Start* functions setup beginning state of searches: finds correct buffer and pins it.
 */
static void
startScanEntry(RumState * rumstate, RumScanEntry entry, Snapshot snapshot)
{
	RumBtreeData btreeEntry;
	RumBtreeStack *stackEntry;
	Page		page;
	bool		needUnlock;

restartScanEntry:
	entry->buffer = InvalidBuffer;
	RumItemSetMin(&entry->curItem);
	entry->offset = InvalidOffsetNumber;
	entry->list = NULL;
	entry->gdi = NULL;
	entry->stack = NULL;
	entry->nlist = 0;
	entry->matchSortstate = NULL;
	entry->reduceResult = false;
	entry->predictNumberResult = 0;

	/*
	 * we should find entry, and begin scan of posting tree or just store
	 * posting list in memory
	 */
	rumPrepareEntryScan(&btreeEntry, entry->attnum,
						entry->queryKey, entry->queryCategory,
						rumstate);
	btreeEntry.searchMode = true;
	stackEntry = rumFindLeafPage(&btreeEntry, NULL);
	page = BufferGetPage(stackEntry->buffer);
	needUnlock = true;

	entry->isFinished = true;

	PredicateLockPage(rumstate->index, BufferGetBlockNumber(stackEntry->buffer), snapshot);

	if (entry->isPartialMatch ||
		(entry->queryCategory == RUM_CAT_EMPTY_QUERY &&
		 !entry->scanWithAddInfo))
	{
		/*
		 * btreeEntry.findItem locates the first item >= given search key.
		 * (For RUM_CAT_EMPTY_QUERY, it will find the leftmost index item
		 * because of the way the RUM_CAT_EMPTY_QUERY category code is
		 * assigned.)  We scan forward from there and collect all TIDs needed
		 * for the entry type.
		 */
		btreeEntry.findItem(&btreeEntry, stackEntry);
		if (collectMatchBitmap(&btreeEntry, stackEntry, entry, snapshot) == false)
		{
			/*
			 * RUM tree was seriously restructured, so we will cleanup all
			 * found data and rescan. See comments near 'return false' in
			 * collectMatchBitmap()
			 */
			if (entry->matchSortstate)
			{
				rum_tuplesort_end(entry->matchSortstate);
				entry->matchSortstate = NULL;
			}
			LockBuffer(stackEntry->buffer, RUM_UNLOCK);
			freeRumBtreeStack(stackEntry);
			goto restartScanEntry;
		}

		if (entry->matchSortstate)
		{
			rum_tuplesort_performsort(entry->matchSortstate);
			ItemPointerSetMin(&entry->collectRumItem.item.iptr);
			entry->isFinished = false;
		}
	}
	else if (btreeEntry.findItem(&btreeEntry, stackEntry) ||
			 (entry->queryCategory == RUM_CAT_EMPTY_QUERY &&
			  entry->scanWithAddInfo))
	{
		IndexTuple	itup;
		ItemId		itemid = PageGetItemId(page, stackEntry->off);

		/*
		 * We don't want to crash if line pointer is not used.
		 */
		if (entry->queryCategory == RUM_CAT_EMPTY_QUERY &&
			!ItemIdHasStorage(itemid))
			goto endScanEntry;

		itup = (IndexTuple) PageGetItem(page, itemid);

		if (RumIsPostingTree(itup))
		{
			BlockNumber rootPostingTree = RumGetPostingTree(itup);
			RumPostingTreeScan *gdi;
			OffsetNumber maxoff,
						i;
			Pointer		ptr;
			RumItem		item;

			ItemPointerSetMin(&item.iptr);

			/*
			 * We should unlock entry page before touching posting tree to
			 * prevent deadlocks with vacuum processes. Because entry is never
			 * deleted from page and posting tree is never reduced to the
			 * posting list, we can unlock page after getting BlockNumber of
			 * root of posting tree.
			 */
			LockBuffer(stackEntry->buffer, RUM_UNLOCK);
			needUnlock = false;
			gdi = rumPrepareScanPostingTree(rumstate->index, rootPostingTree, true,
											entry->scanDirection, entry->attnum, rumstate);

			entry->buffer = rumScanBeginPostingTree(gdi, entry->useMarkAddInfo ?
													&entry->markAddInfo : NULL);

			entry->gdi = gdi;

			PredicateLockPage(rumstate->index, BufferGetBlockNumber(entry->buffer), snapshot);

			/*
			 * We keep buffer pinned because we need to prevent deletion of
			 * page during scan. See RUM's vacuum implementation. RefCount is
			 * increased to keep buffer pinned after freeRumBtreeStack() call.
			 */
			page = BufferGetPage(entry->buffer);
			entry->predictNumberResult = gdi->stack->predictNumber * RumPageGetOpaque(page)->maxoff;

			/*
			 * Keep page content in memory to prevent durable page locking
			 */
			entry->list = (RumItem *) palloc(BLCKSZ * sizeof(RumItem));
			maxoff = RumPageGetOpaque(page)->maxoff;
			entry->nlist = maxoff;

			ptr = RumDataPageGetData(page);

			for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
			{
				ptr = rumDataPageLeafRead(ptr, entry->attnum, &item, true,
										  rumstate);
				entry->list[i - FirstOffsetNumber] = item;
			}

			LockBuffer(entry->buffer, RUM_UNLOCK);
			entry->isFinished = setListPositionScanEntry(rumstate, entry);
			if (!entry->isFinished)
				entry->curItem = entry->list[entry->offset];
		}
		else if (RumGetNPosting(itup) > 0)
		{
			entry->nlist = RumGetNPosting(itup);
			entry->predictNumberResult = (uint32)entry->nlist;
			entry->list = (RumItem *) palloc(sizeof(RumItem) * entry->nlist);

			rumReadTuple(rumstate, entry->attnum, itup, entry->list, true);
			entry->isFinished = setListPositionScanEntry(rumstate, entry);
			if (!entry->isFinished)
				entry->curItem = entry->list[entry->offset];
		}

		if (entry->queryCategory == RUM_CAT_EMPTY_QUERY &&
			entry->scanWithAddInfo)
			entry->stack = stackEntry;

		SCAN_ENTRY_GET_KEY(entry, rumstate, itup);
	}

endScanEntry:
	if (needUnlock)
		LockBuffer(stackEntry->buffer, RUM_UNLOCK);
	if (entry->stack == NULL)
		freeRumBtreeStack(stackEntry);
}

static void
startScanKey(RumState * rumstate, RumScanKey key)
{
	RumItemSetMin(&key->curItem);
	key->curItemMatches = false;
	key->recheckCurItem = false;
	key->isFinished = false;
}

/*
 * Compare entries position. At first consider isFinished flag, then compare
 * item pointers.
 */
static int
cmpEntries(RumState *rumstate, RumScanEntry e1, RumScanEntry e2)
{
	int res;

	if (e1->isFinished == true)
	{
		if (e2->isFinished == true)
			return 0;
		else
			return 1;
	}
	if (e2->isFinished)
		return -1;

	if (e1->attnumOrig != e2->attnumOrig)
		return (e1->attnumOrig < e2->attnumOrig) ? 1 : -1;

	res = compareRumItem(rumstate, e1->attnumOrig, &e1->curItem,
						&e2->curItem);

	return (ScanDirectionIsForward(e1->scanDirection)) ? res : -res;
}

static int
scan_entry_cmp(const void *p1, const void *p2, void *arg)
{
	RumScanEntry e1 = *((RumScanEntry *) p1);
	RumScanEntry e2 = *((RumScanEntry *) p2);

	return -cmpEntries(arg, e1, e2);
}

/*
 * The entryGetItem() function from rumget.c write the results to curItem
 * in a specific order. The functions below allow you to understand the order
 * in which the results will be returned. This is important in a multi-column
 * RUM index, when the results will be returned in different order for
 * different subquery conditions.
 */
static bool
isEntryOrderedByAddInfo(RumState *rumstate, RumScanEntry entry)
{
	if (rumstate->useAlternativeOrder)
		return (rumstate->attrnAddToColumn == entry->attnumOrig);
	else
		return false;
}

static bool
isKeyOrderedByAddInfo(RumState *rumstate, RumScanKey key)
{
	if (rumstate->useAlternativeOrder)
		return (rumstate->attrnAddToColumn == key->attnumOrig);
	else
		return false;
}

static bool
isScanWithAltOrderKeys(RumScanOpaque so)
{
	RumState *rumstate = &so->rumstate;

	bool withAltKeys = false;
	bool withUsualKeys = false;

	if (so->nkeys == 1)
		return false;

	for (int i = 0; i < so->nkeys; i++)
	{
		if (isKeyOrderedByAddInfo(rumstate, so->keys[i]))
			withAltKeys = true;
		else
			withUsualKeys = true;
	}

	return (withUsualKeys && withAltKeys);
}

static void
startScan(IndexScanDesc scan)
{
	MemoryContext oldCtx = CurrentMemoryContext;
	RumScanOpaque so = (RumScanOpaque) scan->opaque;
	RumState   *rumstate = &so->rumstate;
	uint32		i;
	RumScanType	scanType = RumFastScan;

	MemoryContextSwitchTo(so->keyCtx);
	for (i = 0; i < so->totalentries; i++)
	{
		startScanEntry(rumstate, so->entries[i], scan->xs_snapshot);
	}
	MemoryContextSwitchTo(oldCtx);

	if (RumFuzzySearchLimit > 0)
	{
		/*
		 * If all of keys more than threshold we will try to reduce result, we
		 * hope (and only hope, for intersection operation of array our
		 * supposition isn't true), that total result will not more than
		 * minimal predictNumberResult.
		 */
		bool		reduce = true;

		for (i = 0; i < so->totalentries; i++)
		{
			if (so->entries[i]->predictNumberResult <= so->totalentries * RumFuzzySearchLimit)
			{
				reduce = false;
				break;
			}
		}
		if (reduce)
		{
			for (i = 0; i < so->totalentries; i++)
			{
				so->entries[i]->predictNumberResult /= so->totalentries;
				so->entries[i]->reduceResult = true;
			}
		}
	}

	for (i = 0; i < so->nkeys; i++)
		startScanKey(rumstate, so->keys[i]);

	/*
	 * If there are several keys in the scan and its are ordered differently,
	 * need to set the scanWithAltOrderKeys flag and create a bitmap for
	 * TIDs that are suitable for regular keys (which are ordered by tid).
	 */
	if (isScanWithAltOrderKeys(so))
	{
		so->scanWithAltOrderKeys= true;
		so->tbm = rum_tbm_create(work_mem * 1024L, NULL);
	}

	/*
	 * Check if we can use a fast scan.
	 * Use fast scan iff all keys have preConsistent method. But we can stop
	 * checking if at least one key have not preConsistent method and use
	 * regular scan.
	 */
	for (i = 0; i < so->nkeys; i++)
	{
		RumScanKey	key = so->keys[i];

		/* Check first key is it used to full-index scan */
		if (i == 0 && key->nentries > 0 && key->scanEntry[i]->scanWithAddInfo)
		{
			scanType = RumFullScan;
			break;
		}
		/* Else check keys for preConsistent method */
		else if (!so->rumstate.canPreConsistent[key->attnum - 1])
		{
			scanType = RumRegularScan;
			break;
		}
	}

	if (scanType == RumFastScan)
	{
		for (i = 0; i < so->totalentries; i++)
		{
			RumScanEntry entry = so->entries[i];

			if (entry->isPartialMatch)
			{
				scanType = RumRegularScan;
				break;
			}
		}
	}

	ItemPointerSetInvalid(&so->item.iptr);

	if (scanType == RumFastScan)
	{
		/*
		 * We are going to use fast scan. Do some preliminaries. Start scan of
		 * each entry and sort entries by descending item pointers.
		 */
		so->sortedEntries = (RumScanEntry *) palloc(sizeof(RumScanEntry) *
													so->totalentries);
		memcpy(so->sortedEntries, so->entries, sizeof(RumScanEntry) *
			   so->totalentries);
		for (i = 0; i < so->totalentries; i++)
		{
			if (!so->sortedEntries[i]->isFinished)
				entryGetItem(&so->rumstate, so->sortedEntries[i], NULL, scan->xs_snapshot);
		}
		qsort_arg(so->sortedEntries, so->totalentries, sizeof(RumScanEntry),
				  scan_entry_cmp, rumstate);
	}

	so->scanType = scanType;
}

/*
 * Gets next ItemPointer from PostingTree. Note, that we copy
 * page into RumScanEntry->list array and unlock page, but keep it pinned
 * to prevent interference with vacuum
 */
static void
entryGetNextItem(RumState * rumstate, RumScanEntry entry, Snapshot snapshot)
{
	Page		page;

	for (;;)
	{
		if (entry->offset >= 0 && entry->offset < entry->nlist)
		{
			entry->curItem = entry->list[entry->offset];
			entry->offset += entry->scanDirection;
			return;
		}

		LockBuffer(entry->buffer, RUM_SHARE);
		page = BufferGetPage(entry->buffer);
		if (!RumPageIsLeaf(page))
		{
			/*
			 * Root page becomes non-leaf while we unlock it. just return.
			 */
			LockBuffer(entry->buffer, RUM_UNLOCK);
			return;
		}
		PredicateLockPage(rumstate->index, BufferGetBlockNumber(entry->buffer), snapshot);

		if (scanPage(rumstate, entry, &entry->curItem, false))
		{
			LockBuffer(entry->buffer, RUM_UNLOCK);
			return;
		}

		for (;;)
		{
			OffsetNumber maxoff,
						i;
			Pointer		ptr;
			RumItem		item;
			bool		searchBorder =
				(ScanDirectionIsForward(entry->scanDirection) &&
				 ItemPointerIsValid(&entry->curItem.iptr));
			/*
			 * It's needed to go by right link. During that we should refind
			 * first ItemPointer greater that stored
			 */
			if ((ScanDirectionIsForward(entry->scanDirection) && RumPageRightMost(page)) ||
				(ScanDirectionIsBackward(entry->scanDirection) && RumPageLeftMost(page)))
			{
				UnlockReleaseBuffer(entry->buffer);
				ItemPointerSetInvalid(&entry->curItem.iptr);

				entry->buffer = InvalidBuffer;
				entry->isFinished = true;
				entry->gdi->stack->buffer = InvalidBuffer;
				return;
			}

			entry->buffer = rumStep(entry->buffer, rumstate->index,
									RUM_SHARE, entry->scanDirection);
			entry->gdi->stack->buffer = entry->buffer;
			entry->gdi->stack->blkno = BufferGetBlockNumber(entry->buffer);
			page = BufferGetPage(entry->buffer);

			PredicateLockPage(rumstate->index, BufferGetBlockNumber(entry->buffer), snapshot);

			entry->offset = -1;
			maxoff = RumPageGetOpaque(page)->maxoff;
			entry->nlist = maxoff;
			ItemPointerSetMin(&item.iptr);
			ptr = RumDataPageGetData(page);

			for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
			{
				ptr = rumDataPageLeafRead(ptr, entry->attnum, &item, true,
										  rumstate);
				entry->list[i - FirstOffsetNumber] = item;

				if (searchBorder)
				{
					/* don't search position for backward scan,
					   because of split algorithm */
					int cmp = compareRumItem(rumstate,
											 entry->attnumOrig,
											 &entry->curItem,
											 &item);

					if (cmp > 0)
					{
						entry->offset = i - FirstOffsetNumber;
						searchBorder = false;
					}
				}
			}

			LockBuffer(entry->buffer, RUM_UNLOCK);

			if (entry->offset < 0)
			{
				if (ScanDirectionIsForward(entry->scanDirection) &&
					ItemPointerIsValid(&entry->curItem.iptr))
					/* go on next page */
					break;
				entry->offset = (ScanDirectionIsForward(entry->scanDirection)) ?
							0 : entry->nlist - 1;
			}

			entry->curItem = entry->list[entry->offset];
			entry->offset += entry->scanDirection;
			return;
		}
	}
}

static bool
entryGetNextItemList(RumState * rumstate, RumScanEntry entry, Snapshot snapshot)
{
	Page		page;
	IndexTuple	itup;
	RumBtreeData btree;
	bool		needUnlock;

	Assert(!entry->isFinished);
	Assert(entry->stack);
	Assert(ScanDirectionIsForward(entry->scanDirection));

	entry->buffer = InvalidBuffer;
	RumItemSetMin(&entry->curItem);
	entry->offset = InvalidOffsetNumber;
	entry->list = NULL;
	if (entry->gdi)
	{
		freeRumBtreeStack(entry->gdi->stack);
		pfree(entry->gdi);
	}
	entry->gdi = NULL;
	if (entry->list)
	{
		pfree(entry->list);
		entry->list = NULL;
		entry->nlist = 0;
	}
	entry->matchSortstate = NULL;
	entry->reduceResult = false;
	entry->predictNumberResult = 0;

	rumPrepareEntryScan(&btree, entry->attnum,
						entry->queryKey, entry->queryCategory,
						rumstate);

	LockBuffer(entry->stack->buffer, RUM_SHARE);
	/*
	 * stack->off points to the interested entry, buffer is already locked
	 */
	if (!moveRightIfItNeeded(&btree, entry->stack))
	{
		ItemPointerSetInvalid(&entry->curItem.iptr);
		entry->isFinished = true;
		LockBuffer(entry->stack->buffer, RUM_UNLOCK);
		return false;
	}

	page = BufferGetPage(entry->stack->buffer);
	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page,
													entry->stack->off));
	needUnlock = true;

	/*
	 * If tuple stores another attribute then stop scan
	 */
	if (rumtuple_get_attrnum(btree.rumstate, itup) != entry->attnum)
	{
		ItemPointerSetInvalid(&entry->curItem.iptr);
		entry->isFinished = true;
		LockBuffer(entry->stack->buffer, RUM_UNLOCK);
		return false;
	}

	/*
	 * OK, we want to return the TIDs listed in this entry.
	 */
	if (RumIsPostingTree(itup))
	{
		BlockNumber rootPostingTree = RumGetPostingTree(itup);
		RumPostingTreeScan *gdi;
		OffsetNumber maxoff,
					i;
		Pointer		ptr;
		RumItem		item;

		ItemPointerSetMin(&item.iptr);

		/*
		 * We should unlock entry page before touching posting tree to
		 * prevent deadlocks with vacuum processes. Because entry is never
		 * deleted from page and posting tree is never reduced to the
		 * posting list, we can unlock page after getting BlockNumber of
		 * root of posting tree.
		 */
		LockBuffer(entry->stack->buffer, RUM_UNLOCK);
		needUnlock = false;
		gdi = rumPrepareScanPostingTree(rumstate->index,
						rootPostingTree, true, entry->scanDirection,
						entry->attnumOrig, rumstate);

		entry->buffer = rumScanBeginPostingTree(gdi, NULL);
		entry->gdi = gdi;

		PredicateLockPage(rumstate->index, BufferGetBlockNumber(entry->buffer), snapshot);

		/*
		 * We keep buffer pinned because we need to prevent deletion of
		 * page during scan. See RUM's vacuum implementation. RefCount is
		 * increased to keep buffer pinned after freeRumBtreeStack() call.
		 */
		page = BufferGetPage(entry->buffer);
		entry->predictNumberResult = gdi->stack->predictNumber *
										RumPageGetOpaque(page)->maxoff;

		/*
		 * Keep page content in memory to prevent durable page locking
		 */
		entry->list = (RumItem *) palloc(BLCKSZ * sizeof(RumItem));
		maxoff = RumPageGetOpaque(page)->maxoff;
		entry->nlist = maxoff;

		ptr = RumDataPageGetData(page);

		for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
		{
			ptr = rumDataPageLeafRead(ptr, entry->attnum, &item, true,
									  rumstate);
			entry->list[i - FirstOffsetNumber] = item;
		}

		LockBuffer(entry->buffer, RUM_UNLOCK);
		entry->isFinished = false;
	}
	else if (RumGetNPosting(itup) > 0)
	{
		entry->nlist = RumGetNPosting(itup);
		entry->predictNumberResult = (uint32)entry->nlist;
		entry->list = (RumItem *) palloc(sizeof(RumItem) * entry->nlist);

		rumReadTuple(rumstate, entry->attnum, itup, entry->list, true);
		entry->isFinished = setListPositionScanEntry(rumstate, entry);
	}

	Assert(entry->nlist > 0 && entry->list);

	entry->curItem = entry->list[entry->offset];
	entry->offset += entry->scanDirection;

	SCAN_ENTRY_GET_KEY(entry, rumstate, itup);

	/*
	 * Done with this entry, go to the next for the future.
	 */
	entry->stack->off++;

	if (needUnlock)
		LockBuffer(entry->stack->buffer, RUM_UNLOCK);

	return true;
}

#if PG_VERSION_NUM < 150000
#define rum_rand() (((double) random()) / ((double) MAX_RANDOM_VALUE))
#else
#define rum_rand() pg_prng_double(&pg_global_prng_state)
#endif

#define dropItem(e) ( rum_rand() > ((double)RumFuzzySearchLimit)/((double)((e)->predictNumberResult)) )

/*
 * Sets entry->curItem to next heap item pointer for one entry of one scan key,
 * or sets entry->isFinished to true if there are no more.
 *
 * Item pointers must be returned in ascending order.
 */
static void
entryGetItem(RumState * rumstate, RumScanEntry entry, bool *nextEntryList, Snapshot snapshot)
{
	Assert(!entry->isFinished);

	if (nextEntryList)
		*nextEntryList = false;

	if (entry->matchSortstate)
	{
		Assert(ScanDirectionIsForward(entry->scanDirection));

		do
		{
			RumScanItem	collected;
			RumScanItem *current_collected;

			/* We are finished, but should return last result */
			if (ItemPointerIsMax(&entry->collectRumItem.item.iptr))
			{
				entry->isFinished = true;
				rum_tuplesort_end(entry->matchSortstate);
				entry->matchSortstate = NULL;
				break;
			}

			/* collectRumItem could store the begining of current result */
			if (!ItemPointerIsMin(&entry->collectRumItem.item.iptr))
				collected = entry->collectRumItem;
			else
				MemSet(&collected, 0, sizeof(collected));

			ItemPointerSetMin(&entry->curItem.iptr);

			for(;;)
			{
				bool	should_free;

				current_collected = rum_tuplesort_getrumitem(
					entry->matchSortstate,
					ScanDirectionIsForward(entry->scanDirection) ? true : false,
					&should_free);

				if (current_collected == NULL)
				{
					entry->curItem = collected.item;
					if (entry->useCurKey)
					{
						entry->curKey = collected.keyValue;
						entry->curKeyCategory = collected.keyCategory;
					}
					break;
				}

				if (ItemPointerIsMin(&collected.item.iptr) ||
					rumCompareItemPointers(&collected.item.iptr,
										   &current_collected->item.iptr) == 0)
				{
					Datum	joinedAddInfo = (Datum)0;
					bool	joinedAddInfoIsNull;

					if (ItemPointerIsMin(&collected.item.iptr))
					{
						joinedAddInfoIsNull = true; /* will change later */
						collected.item.addInfoIsNull = true;
					}
					else
						joinedAddInfoIsNull = collected.item.addInfoIsNull ||
										current_collected->item.addInfoIsNull;

					if (joinedAddInfoIsNull)
					{
						joinedAddInfoIsNull =
							(collected.item.addInfoIsNull &&
							 current_collected->item.addInfoIsNull);

						if (collected.item.addInfoIsNull == false)
							joinedAddInfo = collected.item.addInfo;
						else if (current_collected->item.addInfoIsNull == false)
							joinedAddInfo = current_collected->item.addInfo;
					}
					else if (rumstate->canJoinAddInfo[entry->attnumOrig - 1])
					{
						joinedAddInfo =
							FunctionCall2(
								&rumstate->joinAddInfoFn[entry->attnumOrig - 1],
								collected.item.addInfo,
								current_collected->item.addInfo);
					}
					else
					{
						joinedAddInfo = current_collected->item.addInfo;
					}

					collected.item.iptr = current_collected->item.iptr;
					collected.item.addInfoIsNull = joinedAddInfoIsNull;
					collected.item.addInfo = joinedAddInfo;
					if (entry->useCurKey)
					{
						collected.keyValue = current_collected->keyValue;
						collected.keyCategory = current_collected->keyCategory;
					}

					if (should_free)
						pfree(current_collected);
				}
				else
				{
					entry->curItem = collected.item;
					entry->collectRumItem = *current_collected;
					if (entry->useCurKey)
					{
						entry->curKey = collected.keyValue;
						entry->curKeyCategory = collected.keyCategory;
					}
					if (should_free)
						pfree(current_collected);
					break;
				}
			}

			if (current_collected == NULL)
			{
				/* mark next call as last */
				ItemPointerSetMax(&entry->collectRumItem.item.iptr);

				/* even current call is last */
				if (ItemPointerIsMin(&entry->curItem.iptr))
				{
					entry->isFinished = true;
					rum_tuplesort_end(entry->matchSortstate);
					entry->matchSortstate = NULL;
					break;
				}
			}
		} while (entry->reduceResult == true && dropItem(entry));
	}
	else if (!BufferIsValid(entry->buffer))
	{
		if (entry->offset >= 0 && entry->offset < entry->nlist)
		{
			entry->curItem = entry->list[entry->offset];
			entry->offset += entry->scanDirection;
		}
		else if (entry->stack)
		{
			entry->offset++;
			if (entryGetNextItemList(rumstate, entry, snapshot) && nextEntryList)
				*nextEntryList = true;
		}
		else
		{
			ItemPointerSetInvalid(&entry->curItem.iptr);
			entry->isFinished = true;
		}
	}
	/* Get next item from posting tree */
	else
	{
		do
		{
			entryGetNextItem(rumstate, entry, snapshot);
		} while (entry->isFinished == false &&
				 entry->reduceResult == true &&
				 dropItem(entry));
		if (entry->stack && entry->isFinished)
		{
			entry->isFinished = false;
			if (entryGetNextItemList(rumstate, entry, snapshot) && nextEntryList)
				*nextEntryList = true;
		}
	}
}

/*
 * Identify the "current" item among the input entry streams for this scan key,
 * and test whether it passes the scan key qual condition.
 *
 * The current item is the smallest curItem among the inputs.  key->curItem
 * is set to that value.  key->curItemMatches is set to indicate whether that
 * TID passes the consistentFn test.  If so, key->recheckCurItem is set true
 * iff recheck is needed for this item pointer
 *
 * If all entry streams are exhausted, sets key->isFinished to true.
 *
 * Item pointers must be returned in ascending order.
 */

static int
compareRumItemScanDirection(RumState *rumstate, AttrNumber attno,
							ScanDirection scanDirection,
							RumItem *a, RumItem *b)
{
	int res = compareRumItem(rumstate, attno, a, b);

	return  (ScanDirectionIsForward(scanDirection)) ? res : -res;

}

static int
compareCurRumItemScanDirection(RumState *rumstate, RumScanEntry entry,
							   RumItem *minItem)
{
	return compareRumItemScanDirection(rumstate,
						entry->attnumOrig,
						entry->scanDirection,
						&entry->curItem, minItem);
}

static int
rumCompareItemPointersScanDirection(ScanDirection scanDirection,
									const ItemPointerData *a,
									const ItemPointerData *b)
{
	int res = rumCompareItemPointers(a, b);

	return  (ScanDirectionIsForward(scanDirection)) ? res : -res;
}

static void
keyGetItem(RumState * rumstate, MemoryContext tempCtx, RumScanKey key)
{
	RumItem		minItem;
	uint32		i;
	RumScanEntry entry;
	bool		res;
	MemoryContext oldCtx;
	bool		allFinished = true;
	bool		minItemInited = false;

	Assert(!key->isFinished);

	/*
	 * Find the minimum of the active entry curItems.
	 */

	for (i = 0; i < key->nentries; i++)
	{
		entry = key->scanEntry[i];
		if (entry->isFinished == false)
		{
			allFinished = false;

			if (minItemInited == false ||
				compareCurRumItemScanDirection(rumstate, entry, &minItem) < 0)
			{
				minItem = entry->curItem;
				minItemInited = true;
			}
		}
	}

	if (allFinished)
	{
		/* all entries are finished */
		key->isFinished = true;
		return;
	}

	/*
	 * We might have already tested this item; if so, no need to repeat work.
	 */
	if (rumCompareItemPointers(&key->curItem.iptr, &minItem.iptr) == 0)
		return;

	/*
	 * OK, advance key->curItem and perform consistentFn test.
	 */
	key->curItem = minItem;

	/* prepare for calling consistentFn in temp context */
	oldCtx = MemoryContextSwitchTo(tempCtx);

	/*
	 * Prepare entryRes array to be passed to consistentFn.
	 */
	for (i = 0; i < key->nentries; i++)
	{
		entry = key->scanEntry[i];
		if (entry->isFinished == false &&
			rumCompareItemPointers(&entry->curItem.iptr, &key->curItem.iptr) == 0)
		{
			key->entryRes[i] = true;
			key->addInfo[i] = entry->curItem.addInfo;
			key->addInfoIsNull[i] = entry->curItem.addInfoIsNull;
		}
		else
		{
			key->entryRes[i] = false;
			key->addInfo[i] = (Datum) 0;
			key->addInfoIsNull[i] = true;
		}
	}

	res = callConsistentFn(rumstate, key);

	key->curItemMatches = res;

	/* clean up after consistentFn calls */
	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(tempCtx);
}

/*
 * The function puts all the suitable tids for
 * the passed RumScanKey into the RumTIDBitmap.
 */
static void
collectAllCurItemsToBitmap(RumScanKey key, RumTIDBitmap *tbm,
						   IndexScanDesc scan)
{
	RumScanOpaque		so = (RumScanOpaque) scan->opaque;
	RumState			*rumstate = &so->rumstate;
	ItemPointerData		advancePast;

	ItemPointerSetInvalid(&advancePast);

	for (;;)
	{
		/*
		 * Set the curItem for all the RumScanEntry
		 * of our RumScanKey after advancePast.
		 */
		for (int i = 0; i < key->nentries; i++)
		{
			RumScanEntry entry = key->scanEntry[i];

			while (entry->isFinished == false &&
				(!ItemPointerIsValid(&advancePast) ||
				rumCompareItemPointersScanDirection(entry->scanDirection,
								&entry->curItem.iptr, &advancePast) <= 0))
			{
				entryGetItem(rumstate, entry, NULL, scan->xs_snapshot);

				/*
				 * On the first iteration of the loop,
				 * RumScanEntry->curItem is the first available.
				 */
				if (!ItemPointerIsValid(&advancePast))
					break;
			}
		}

		/*
		 * For RumScanKey, we set the minimum
		 * curItem from its RumScanEntry.
		 */
		keyGetItem(rumstate, so->tempCtx, key);

		if (key->isFinished)
			return;

		/* If curItem is suitable, put it in the RumTIDBitmap */
		if (key->curItemMatches)
			rum_tbm_add_tuples(tbm, &key->curItem.iptr, 1, false);

		advancePast = key->curItem.iptr;
	}
}

/*
 * Get next heap item pointer (after advancePast) from scan.
 * Returns true if anything found.
 * On success, *item and *recheck are set.
 *
 * Note: this is very nearly the same logic as in keyGetItem(), except
 * that we know the keys are to be combined with AND logic, whereas in
 * keyGetItem() the combination logic is known only to the consistentFn.
 *
 * Note: In the case of a scan that contains several keys ordered in different
 * ways, you need to prepare. To do this, for all keys ordered by tid,
 * the appropriate TIDs are placed in the RumTIDBitmap. After that, all
 * these bitmaps intersect (because the RumScanKeys are connected via AND).
 */
static bool
scanGetItemRegular(IndexScanDesc scan, RumItem *advancePast,
				   RumItem *item, bool *recheck)
{
	RumScanOpaque so = (RumScanOpaque) scan->opaque;
	RumState   *rumstate = &so->rumstate;
	RumItem		myAdvancePast = *advancePast;
	uint32		i;
	bool		allFinished;
	bool		match, itemSet;

	RumTIDBitmap *cur_key_tbm = NULL;
	bool		soBitmapSet = false;
	bool		bitmapRecheck = false;

	/*
	 * Preparation to scan with altOrderKeys. This preparation
	 * needs to be done only in the first call to scanGetItemRegular().
	 */
	if (so->scanWithAltOrderKeys &&
		ItemPointerIsValid(&advancePast->iptr) == false)
	{
		/* For each key, put all the appropriate tids into the bitmap */
		for (int i = 0; i < so->nkeys; i++)
		{
			if (so->keys[i]->orderBy ||
				isKeyOrderedByAddInfo(rumstate, so->keys[i]))
				continue;

			cur_key_tbm = rum_tbm_create(work_mem * 1024L, NULL);
			collectAllCurItemsToBitmap(so->keys[i], cur_key_tbm, scan);

			/*
			 * so->tbm contains matching tids for
			 * all keys that are ordered by tid.
			 */
			if (soBitmapSet == false)
			{
				soBitmapSet = true;
				rum_tbm_union(so->tbm, cur_key_tbm);
			}
			else
				rum_tbm_intersect(so->tbm, cur_key_tbm);

			rum_tbm_free(cur_key_tbm);
		}
	}

	for (;;)
	{
		/*
		 * Advance any entries that are <= myAdvancePast according to
		 * scan direction. On first call myAdvancePast is invalid,
		 * so anyway we are needed to call entryGetItem()
		 */
		allFinished = true;

		for (i = 0; i < so->totalentries; i++)
		{
			RumScanEntry entry = so->entries[i];

			/* In the case of scanning with altOrderKeys, skip the usual keys */
			if (so->scanWithAltOrderKeys &&
				isEntryOrderedByAddInfo(rumstate, entry) == false)
				continue;

			while (entry->isFinished == false &&
				   (!ItemPointerIsValid(&myAdvancePast.iptr) ||
				   compareCurRumItemScanDirection(rumstate, entry,
												 &myAdvancePast) <= 0))
			{
				entryGetItem(rumstate, entry, NULL, scan->xs_snapshot);

				if (!ItemPointerIsValid(&myAdvancePast.iptr))
					break;
			}

			if (entry->isFinished == false)
				allFinished = false;
		}

		if (allFinished)
		{
			/* all entries exhausted, so we're done */
			return false;
		}

		/*
		 * Perform the consistentFn test for each scan key.  If any key
		 * reports isFinished, meaning its subset of the entries is exhausted,
		 * we can stop.  Otherwise, set *item to the minimum of the key
		 * curItems.
		 */

		itemSet = false;
		for (i = 0; i < so->nkeys; i++)
		{
			RumScanKey	key = so->keys[i];
			int			cmp;

			/* In the case of scanning with altOrderKeys, skip the usual keys */
			if (key->orderBy || (so->scanWithAltOrderKeys &&
				isKeyOrderedByAddInfo(rumstate, key) == false))
				continue;

			keyGetItem(&so->rumstate, so->tempCtx, key);

			if (key->isFinished)
				return false;	/* finished one of keys */

			if (itemSet == false)
			{
				*item = key->curItem;
				itemSet = true;
			}
			cmp = compareRumItem(rumstate, key->attnumOrig,
								 &key->curItem, item);
			if ((ScanDirectionIsForward(key->scanDirection) && cmp < 0) ||
				(ScanDirectionIsBackward(key->scanDirection) && cmp > 0))
				*item = key->curItem;
		}

		/*----------
		 * Now *item contains first ItemPointer after previous result.
		 *----------
		 */
		match = true;
		for (i = 0; match && i < so->nkeys; i++)
		{
			RumScanKey	key = so->keys[i];

			/* In the case of scanning with altOrderKeys, skip the usual keys */
			if (key->orderBy || (so->scanWithAltOrderKeys &&
				isKeyOrderedByAddInfo(rumstate, key) == false))
				continue;

			if (key->curItemMatches)
			{
				if (rumCompareItemPointers(&item->iptr, &key->curItem.iptr) == 0)
					continue;
			}
			match = false;
			break;
		}

		/*
		 * Now (if scan with altOrderKeys), if an *item is suitable for all
		 * altOrderKeys, you need to find it in the bitmap. If it's there,
		 * then it fits.
		 *
		 * Due to the fact that there may be lossy pages in the bitmap,
		 * you may need to recheck the *item.
		 */
		if (so->scanWithAltOrderKeys && match)
			match = rum_tbm_contains_tid(so->tbm, &item->iptr, &bitmapRecheck);

		if (match)
			break;

		/*
		 * No hit.  Update myAdvancePast to this TID, so that on the next pass
		 * we'll move to the next possible entry.
		 */
		myAdvancePast = *item;
	}

	/*
	 * We must return recheck = true if any of the keys are marked recheck.
	 */
	*recheck = false;
	for (i = 0; i < so->nkeys; i++)
	{
		RumScanKey	key = so->keys[i];

		if (key->orderBy)
		{
			int			j;

			/* Catch up order key with *item */
			for (j = 0; j < key->nentries; j++)
			{
				RumScanEntry entry = key->scanEntry[j];

				while (entry->isFinished == false &&
					   compareRumItem(rumstate, key->attnumOrig,
									  &entry->curItem, item) < 0)
				{
					entryGetItem(rumstate, entry, NULL, scan->xs_snapshot);
				}
			}
		}
		else if (key->recheckCurItem)
		{
			*recheck = true;
			break;
		}
	}

	/* If there was a lossy page in the bitmap, need to recheck */
	if (so->scanWithAltOrderKeys && bitmapRecheck)
		*recheck = true;

	return true;
}

/*
 * Finds part of page containing requested item using small index at the end
 * of page.
 */
static bool
scanPage(RumState * rumstate, RumScanEntry entry, RumItem *item, bool equalOk)
{
	int			j;
	RumItem		iter_item;
	Pointer		ptr;
	OffsetNumber first = FirstOffsetNumber,
				i,
				maxoff;
	int16		bound = -1;
	bool		found_eq = false;
	int			cmp;
	Page		page = BufferGetPage(entry->buffer);

	ItemPointerSetMin(&iter_item.iptr);

	if (ScanDirectionIsForward(entry->scanDirection) && !RumPageRightMost(page))
	{
		cmp = compareRumItem(rumstate, entry->attnumOrig,
							 RumDataPageGetRightBound(page), item);
		if (cmp < 0 || (cmp <= 0 && !equalOk))
			return false;
	}

	ptr = RumDataPageGetData(page);
	maxoff = RumPageGetOpaque(page)->maxoff;

	for (j = 0; j < RumDataLeafIndexCount; j++)
	{
		RumDataLeafItemIndex *index = &RumPageGetIndexes(page)[j];

		if (index->offsetNumer == InvalidOffsetNumber)
			break;

		if (rumstate->useAlternativeOrder)
		{
			RumItem		k;

			convertIndexToKey(index, &k);
			cmp = compareRumItem(rumstate, entry->attnumOrig, &k, item);
		}
		else
			cmp = rumCompareItemPointers(&index->iptr, &item->iptr);

		if (cmp < 0 || (cmp <= 0 && !equalOk))
		{
			ptr = RumDataPageGetData(page) + index->pageOffset;
			first = index->offsetNumer;
			iter_item.iptr = index->iptr;
		}
		else
		{
			if (ScanDirectionIsBackward(entry->scanDirection))
			{
				if (j + 1 < RumDataLeafIndexCount)
					maxoff = RumPageGetIndexes(page)[j+1].offsetNumer;
			}
			else
				maxoff = index->offsetNumer - 1;
			break;
		}
	}

	if (ScanDirectionIsBackward(entry->scanDirection) && first >= maxoff)
	{
		first = FirstOffsetNumber;
		ItemPointerSetMin(&iter_item.iptr);
		ptr = RumDataPageGetData(page);
	}

	entry->nlist = maxoff - first + 1;
	bound = -1;
	for (i = first; i <= maxoff; i++)
	{
		ptr = rumDataPageLeafRead(ptr, entry->attnum, &iter_item, true,
								  rumstate);
		entry->list[i - first] = iter_item;

		if (bound != -1)
			continue;

		cmp = compareRumItem(rumstate, entry->attnumOrig,
							 item, &iter_item);

		if (cmp <= 0)
		{
			bound = i - first;
			if (cmp == 0)
				found_eq = true;
		}
	}

	if (bound == -1)
	{
		if (ScanDirectionIsBackward(entry->scanDirection))
		{
			entry->offset = maxoff - first;
			goto end;
		}
		return false;

	}

	if (found_eq)
	{
		entry->offset = bound;
		if (!equalOk)
			entry->offset += entry->scanDirection;
	}
	else if (ScanDirectionIsBackward(entry->scanDirection))
		entry->offset = bound - 1;
	else
		entry->offset = bound;

	if (entry->offset < 0 || entry->offset >= entry->nlist)
		return false;

end:
	entry->curItem = entry->list[entry->offset];
	entry->offset += entry->scanDirection;
	return true;
}

/*
 * Find item of scan entry wich is greater or equal to the given item.
 */

static void
entryFindItem(RumState * rumstate, RumScanEntry entry, RumItem * item, Snapshot snapshot)
{
	if (entry->nlist == 0)
	{
		entry->isFinished = true;
		return;
	}

	/* Try to find in loaded part of page */
	if ((ScanDirectionIsForward(entry->scanDirection) &&
		 compareRumItem(rumstate, entry->attnumOrig,
						&entry->list[entry->nlist - 1], item) >= 0) ||
		(ScanDirectionIsBackward(entry->scanDirection) &&
		 compareRumItem(rumstate, entry->attnumOrig,
						&entry->list[0], item) <= 0))
	{
		if (compareRumItemScanDirection(rumstate, entry->attnumOrig,
							entry->scanDirection,
							&entry->curItem, item) >= 0 &&
							entry->offset >= 0 &&
							entry->offset < entry->nlist &&
							rumCompareItemPointers(&entry->curItem.iptr,
												   &entry->list[entry->offset].iptr) == 0)
			return;
		while (entry->offset >= 0 && entry->offset < entry->nlist)
		{
			if (compareRumItemScanDirection(rumstate, entry->attnumOrig,
							entry->scanDirection,
							&entry->list[entry->offset],
							item) >= 0)
			{
				entry->curItem = entry->list[entry->offset];
				entry->offset += entry->scanDirection;
				return;
			}
			entry->offset += entry->scanDirection;
		}
	}

	if (!BufferIsValid(entry->buffer))
	{
		entry->isFinished = true;
		return;
	}

	/* Check rest of page */
	LockBuffer(entry->buffer, RUM_SHARE);

	PredicateLockPage(rumstate->index, BufferGetBlockNumber(entry->buffer), snapshot);

	if (scanPage(rumstate, entry, item, true))
	{
		LockBuffer(entry->buffer, RUM_UNLOCK);
		return;
	}

	/* Try to traverse to another leaf page */
	entry->gdi->btree.items = item;
	entry->gdi->btree.curitem = 0;
	entry->gdi->btree.fullScan = false;

	entry->gdi->stack->buffer = entry->buffer;
	entry->gdi->stack = rumReFindLeafPage(&entry->gdi->btree, entry->gdi->stack);
	entry->buffer = entry->gdi->stack->buffer;

	PredicateLockPage(rumstate->index, BufferGetBlockNumber(entry->buffer), snapshot);

	if (scanPage(rumstate, entry, item, true))
	{
		LockBuffer(entry->buffer, RUM_UNLOCK);
		return;
	}

	/* At last try to traverse by direction */
	for (;;)
	{
		entry->buffer = rumStep(entry->buffer, rumstate->index,
								RUM_SHARE, entry->scanDirection);
		entry->gdi->stack->buffer = entry->buffer;

		if (entry->buffer == InvalidBuffer)
		{
			ItemPointerSetInvalid(&entry->curItem.iptr);
			entry->isFinished = true;
			return;
		}

		PredicateLockPage(rumstate->index, BufferGetBlockNumber(entry->buffer), snapshot);

		entry->gdi->stack->blkno = BufferGetBlockNumber(entry->buffer);

		if (scanPage(rumstate, entry, item, true))
		{
			LockBuffer(entry->buffer, RUM_UNLOCK);
			return;
		}
	}
}

/*
 * Do preConsistent check for all the key where applicable.
 */
static bool
preConsistentCheck(RumScanOpaque so)
{
	RumState   *rumstate = &so->rumstate;
	uint32		i,
				j;
	bool		recheck;

	for (j = 0; j < so->nkeys; j++)
	{
		RumScanKey	key = so->keys[j];
		bool		hasFalse = false;

		if (key->orderBy)
			continue;

		if (key->searchMode == GIN_SEARCH_MODE_EVERYTHING)
			continue;

		if (!so->rumstate.canPreConsistent[key->attnum - 1])
			continue;

		for (i = 0; i < key->nentries; i++)
		{
			RumScanEntry entry = key->scanEntry[i];

			key->entryRes[i] = entry->preValue;
			if (!entry->preValue)
				hasFalse = true;
		}

		if (!hasFalse)
			continue;

		if (!DatumGetBool(FunctionCall8Coll(&rumstate->preConsistentFn[key->attnum - 1],
								 rumstate->supportCollation[key->attnum - 1],
											PointerGetDatum(key->entryRes),
											UInt16GetDatum(key->strategy),
											key->query,
											UInt32GetDatum(key->nuserentries),
											PointerGetDatum(key->extra_data),
											PointerGetDatum(&recheck),
											PointerGetDatum(key->queryValues),
										PointerGetDatum(key->queryCategories)

											)))
			return false;
	}
	return true;
}

/*
 * Shift value of some entry which index in so->sortedEntries is equal or greater
 * to i.
 */
static void
entryShift(int i, RumScanOpaque so, bool find, Snapshot snapshot)
{
	int			minIndex = -1,
				j;
	uint32		minPredictNumberResult = 0;
	RumState   *rumstate = &so->rumstate;

	/*
	 * It's more efficient to move entry with smallest posting list/tree. So
	 * find one.
	 */
	for (j = i; j < so->totalentries; j++)
	{
		if (minIndex < 0 ||
		  so->sortedEntries[j]->predictNumberResult < minPredictNumberResult)
		{
			minIndex = j;
			minPredictNumberResult = so->sortedEntries[j]->predictNumberResult;
		}
	}

	/* Do shift of required type */
	if (find)
		entryFindItem(rumstate, so->sortedEntries[minIndex],
					  &so->sortedEntries[i - 1]->curItem, snapshot);
	else if (!so->sortedEntries[minIndex]->isFinished)
		entryGetItem(rumstate, so->sortedEntries[minIndex], NULL, snapshot);

	/* Restore order of so->sortedEntries */
	while (minIndex > 0 &&
		   cmpEntries(rumstate, so->sortedEntries[minIndex],
					  so->sortedEntries[minIndex - 1]) > 0)
	{
		RumScanEntry tmp;

		tmp = so->sortedEntries[minIndex];
		so->sortedEntries[minIndex] = so->sortedEntries[minIndex - 1];
		so->sortedEntries[minIndex - 1] = tmp;
		minIndex--;
	}
}

/*
 * Get next item pointer using fast scan.
 */
static bool
scanGetItemFast(IndexScanDesc scan, RumItem *advancePast,
				RumItem *item, bool *recheck)
{
	RumScanOpaque so = (RumScanOpaque) scan->opaque;
	int			i,
				j,
				k;
	bool		preConsistentResult,
				consistentResult;

	if (so->entriesIncrIndex >= 0)
	{
		for (k = so->entriesIncrIndex; k < so->totalentries; k++)
			entryShift(k, so, false, scan->xs_snapshot);
	}

	for (;;)
	{
		/*
		 * Our entries is ordered by descending of item pointers. The first
		 * goal is to find border where preConsistent becomes false.
		 */
		preConsistentResult = true;
		j = 0;
		k = 0;
		for (i = 0; i < so->totalentries; i++)
			so->sortedEntries[i]->preValue = true;
		for (i = 1; i < so->totalentries; i++)
		{
			if (cmpEntries(&so->rumstate, so->sortedEntries[i], so->sortedEntries[i - 1]) < 0)
			{
				k = i;
				for (; j < i; j++)
					so->sortedEntries[j]->preValue = false;

				if ((preConsistentResult = preConsistentCheck(so)) == false)
					break;
			}
		}

		/*
		 * If we found false in preConsistent then we can safely move entries
		 * which was true in preConsistent argument.
		 */
		if (so->sortedEntries[i - 1]->isFinished == true)
			return false;

		if (preConsistentResult == false)
		{
			entryShift(i, so, true, scan->xs_snapshot);
			continue;
		}

		/* Call consistent method */
		consistentResult = true;
		for (i = 0; i < so->nkeys; i++)
		{
			RumScanKey	key = so->keys[i];

			if (key->orderBy)
				continue;

			for (j = 0; j < key->nentries; j++)
			{
				RumScanEntry entry = key->scanEntry[j];

				if (entry->isFinished == false &&
					rumCompareItemPointers(&entry->curItem.iptr,
				&so->sortedEntries[so->totalentries - 1]->curItem.iptr) == 0)
				{
					key->entryRes[j] = true;
					key->addInfo[j] = entry->curItem.addInfo;
					key->addInfoIsNull[j] = entry->curItem.addInfoIsNull;
				}
				else
				{
					key->entryRes[j] = false;
					key->addInfo[j] = (Datum) 0;
					key->addInfoIsNull[j] = true;
				}
			}

			if (!callConsistentFn(&so->rumstate, key))
			{
				consistentResult = false;
				for (j = k; j < so->totalentries; j++)
					entryShift(j, so, false, scan->xs_snapshot);
				continue;
			}
		}

		if (consistentResult == false)
			continue;

		/* Calculate recheck from each key */
		*recheck = false;
		for (i = 0; i < so->nkeys; i++)
		{
			RumScanKey	key = so->keys[i];

			if (key->orderBy)
				continue;

			if (key->recheckCurItem)
			{
				*recheck = true;
				break;
			}
		}

		*item = so->sortedEntries[so->totalentries - 1]->curItem;
		so->entriesIncrIndex = k;

		return true;
	}
	return false;
}

/*
 * Get next item pointer using full-index scan.
 *
 * First key is used to full scan, other keys are only used for ranking.
 */
static bool
scanGetItemFull(IndexScanDesc scan, RumItem *advancePast,
				RumItem *item, bool *recheck)
{
	RumScanOpaque so = (RumScanOpaque) scan->opaque;
	RumScanKey	key;
	RumScanEntry entry;
	bool		nextEntryList;
	uint32		i;

	Assert(so->nkeys > 0 && so->totalentries > 0);
	Assert(so->entries[0]->scanWithAddInfo);

	/* Full-index scan key */
	key = so->keys[0];
	Assert(key->searchMode == GIN_SEARCH_MODE_EVERYTHING);
	/*
	 * This is first entry of the first key, which is used for full-index
	 * scan.
	 */
	entry = so->entries[0];

	if (entry->isFinished)
		return false;

	entryGetItem(&so->rumstate, entry, &nextEntryList, scan->xs_snapshot);

	if (entry->isFinished)
		return false;

	/* Fill outerAddInfo */
	key->entryRes[0] = true;
	key->addInfo[0] = entry->curItem.addInfo;
	key->addInfoIsNull[0] = entry->curItem.addInfoIsNull;
	callAddInfoConsistentFn(&so->rumstate, key);

	/* Move related order by entries */
	if (nextEntryList)
		for (i = 1; i < so->totalentries; i++)
		{
			RumScanEntry orderEntry = so->entries[i];
			if (orderEntry->nlist > 0)
			{
				orderEntry->isFinished = false;
				orderEntry->offset = InvalidOffsetNumber;
				RumItemSetMin(&orderEntry->curItem);
			}
		}

	for (i = 1; i < so->totalentries; i++)
	{
		RumScanEntry orderEntry = so->entries[i];

		while (orderEntry->isFinished == false &&
			   (!ItemPointerIsValid(&orderEntry->curItem.iptr) ||
			   compareCurRumItemScanDirection(&so->rumstate, orderEntry,
											  &entry->curItem) < 0))
			entryGetItem(&so->rumstate, orderEntry, NULL, scan->xs_snapshot);
	}

	*item = entry->curItem;
	*recheck = false;
	return true;
}

/*
 * Get next item whether using regular or fast scan.
 */
static bool
scanGetItem(IndexScanDesc scan, RumItem *advancePast,
			RumItem *item, bool *recheck)
{
	RumScanOpaque so = (RumScanOpaque) scan->opaque;

	if (so->scanType == RumFastScan)
		return scanGetItemFast(scan, advancePast, item, recheck);
	else if (so->scanType == RumFullScan)
		return scanGetItemFull(scan, advancePast, item, recheck);
	else
		return scanGetItemRegular(scan, advancePast, item, recheck);
}

#define RumIsNewKey(s)		( ((RumScanOpaque) scan->opaque)->keys == NULL )
#define RumIsVoidRes(s)		( ((RumScanOpaque) scan->opaque)->isVoidRes )

int64
rumgetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	RumScanOpaque so = (RumScanOpaque) scan->opaque;
	int64		ntids = 0;
	bool		recheck;
	RumItem		item;

	/*
	 * Set up the scan keys, and check for unsatisfiable query.
	 */
	if (RumIsNewKey(scan))
		rumNewScanKey(scan);

	if (RumIsVoidRes(scan))
		return 0;

	ntids = 0;

	so->entriesIncrIndex = -1;

	/*
	 * Now scan the main index.
	 */
	startScan(scan);

	ItemPointerSetInvalid(&item.iptr);

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		if (!scanGetItem(scan, &item, &item, &recheck))
			break;

		tbm_add_tuples(tbm, &item.iptr, 1, recheck);
		ntids++;
	}

	return ntids;
}

static float8
keyGetOrdering(RumState * rumstate, MemoryContext tempCtx, RumScanKey key,
			   ItemPointer iptr)
{
	RumScanEntry entry;
	uint32		i;

	if (key->useAddToColumn)
	{
		Assert(key->nentries == 0);
		Assert(key->nuserentries == 0);

		if (key->outerAddInfoIsNull)
			return get_float8_infinity();

		return DatumGetFloat8(FunctionCall3(
				&rumstate->outerOrderingFn[rumstate->attrnAttachColumn - 1],
											key->outerAddInfo,
											key->queryValues[0],
											UInt16GetDatum(key->strategy)
											));
	}
	else if (key->useCurKey)
	{
		Assert(key->nentries == 0);
		Assert(key->nuserentries == 0);

		if (key->curKeyCategory != RUM_CAT_NORM_KEY)
			return get_float8_infinity();

		return DatumGetFloat8(FunctionCall3(
										&rumstate->orderingFn[key->attnum - 1],
											key->curKey,
											key->query,
											UInt16GetDatum(key->strategy)
											));
	}

	for (i = 0; i < key->nentries; i++)
	{
		entry = key->scanEntry[i];
		if (entry->isFinished == false &&
			rumCompareItemPointers(&entry->curItem.iptr, iptr) == 0)
		{
			key->addInfo[i] = entry->curItem.addInfo;
			key->addInfoIsNull[i] = entry->curItem.addInfoIsNull;
			key->entryRes[i] = true;
		}
		else
		{
			key->addInfo[i] = (Datum) 0;
			key->addInfoIsNull[i] = true;
			key->entryRes[i] = false;
		}
	}

	return DatumGetFloat8(FunctionCall10Coll(&rumstate->orderingFn[key->attnum - 1],
								 rumstate->supportCollation[key->attnum - 1],
											 PointerGetDatum(key->entryRes),
											 UInt16GetDatum(key->strategy),
											 key->query,
										   UInt32GetDatum(key->nuserentries),
											 PointerGetDatum(key->extra_data),
									   PointerGetDatum(&key->recheckCurItem),
										   PointerGetDatum(key->queryValues),
									   PointerGetDatum(key->queryCategories),
											 PointerGetDatum(key->addInfo),
										  PointerGetDatum(key->addInfoIsNull)
											 ));
}

static void
insertScanItem(RumScanOpaque so, bool recheck)
{
	RumSortItem *item;
	uint32		i,
				j;

	item = (RumSortItem *)
		MemoryContextAllocZero(rum_tuplesort_get_memorycontext(so->sortstate),
							   RumSortItemSize(so->norderbys));
	item->iptr = so->item.iptr;
	item->recheck = recheck;

	if (AttributeNumberIsValid(so->rumstate.attrnAddToColumn) || so->willSort)
	{
		int			nOrderByAnother = 0,
					nOrderByKey = 0,
					countByAnother = 0,
					countByKey = 0;

		for (i = 0; i < so->nkeys; i++)
		{
			if (so->keys[i]->useAddToColumn)
			{
				so->keys[i]->outerAddInfoIsNull = true;
				nOrderByAnother++;
			}
			else if (so->keys[i]->useCurKey)
				nOrderByKey++;
		}

		for (i = 0; (countByAnother < nOrderByAnother || countByKey < nOrderByKey) &&
			 i < so->nkeys; i++)
		{
			if (countByAnother < nOrderByAnother &&
				so->keys[i]->attnum == so->rumstate.attrnAddToColumn &&
				so->keys[i]->outerAddInfoIsNull == false)
			{
				Assert(!so->keys[i]->orderBy);
				Assert(!so->keys[i]->useAddToColumn);

				for (j = i; j < so->nkeys; j++)
				{
					if (so->keys[j]->useAddToColumn &&
						so->keys[j]->outerAddInfoIsNull == true)
					{
						so->keys[j]->outerAddInfoIsNull = false;
						so->keys[j]->outerAddInfo = so->keys[i]->outerAddInfo;
						countByAnother++;
					}
				}
			}
			else if (countByKey < nOrderByKey && so->keys[i]->nentries > 0 &&
					 so->keys[i]->scanEntry[0]->useCurKey)
			{
				Assert(!so->keys[i]->orderBy);

				for (j = i + 1; j < so->nkeys; j++)
				{
					if (so->keys[j]->useCurKey)
					{
						so->keys[j]->curKey = so->keys[i]->scanEntry[0]->curKey;
						so->keys[j]->curKeyCategory =
							so->keys[i]->scanEntry[0]->curKeyCategory;
						countByKey++;
					}
				}
			}
		}
	}

	j = 0;
	for (i = 0; i < so->nkeys; i++)
	{
		if (!so->keys[i]->orderBy)
			continue;

		item->data[j] = keyGetOrdering(&so->rumstate, so->tempCtx, so->keys[i],
									   &so->item.iptr);

		j++;
	}
	rum_tuplesort_putrum(so->sortstate, item);
}

static void
reverseScan(IndexScanDesc scan)
{
	RumScanOpaque	so = (RumScanOpaque) scan->opaque;
	int				i, j;

	freeScanKeys(so);
	rumNewScanKey(scan);

	for(i=0; i<so->nkeys; i++)
	{
		RumScanKey	key = so->keys[i];

		key->scanDirection = - key->scanDirection;

		for(j=0; j<key->nentries; j++)
		{
			RumScanEntry	entry = key->scanEntry[j];

			entry->scanDirection = - entry->scanDirection;
		}
	}

	startScan(scan);
}

bool
rumgettuple(IndexScanDesc scan, ScanDirection direction)
{
	bool		recheck;
	RumScanOpaque so = (RumScanOpaque) scan->opaque;
	RumSortItem *item;
	bool		should_free;

#if PG_VERSION_NUM >= 120000
#define GET_SCAN_TID(scan)		((scan)->xs_heaptid)
#define SET_SCAN_TID(scan, tid)	((scan)->xs_heaptid = (tid))
#else
#define GET_SCAN_TID(scan)		((scan)->xs_ctup.t_self)
#define SET_SCAN_TID(scan, tid)	((scan)->xs_ctup.t_self = (tid))
#endif

	if (so->firstCall)
	{
		/*
		 * Set up the scan keys, and check for unsatisfiable query.
		 */
		if (RumIsNewKey(scan))
			rumNewScanKey(scan);

		so->firstCall = false;
		ItemPointerSetInvalid(&GET_SCAN_TID(scan));

		if (RumIsVoidRes(scan))
			return false;

		startScan(scan);
		if (so->naturalOrder == NoMovementScanDirection)
		{
			so->sortstate = rum_tuplesort_begin_rum(work_mem, so->norderbys,
						false, so->scanType == RumFullScan);


			while (scanGetItem(scan, &so->item, &so->item, &recheck))
			{
				insertScanItem(so, recheck);
			}
			rum_tuplesort_performsort(so->sortstate);
		}
	}

	if (so->naturalOrder != NoMovementScanDirection)
	{
		if (scanGetItem(scan, &so->item, &so->item, &recheck))
		{
			SET_SCAN_TID(scan, so->item.iptr);
			scan->xs_recheck = recheck;
			scan->xs_recheckorderby = false;

			return true;
		}
		else if (so->secondPass == false)
		{
			reverseScan(scan);
			so->secondPass = true;
			return rumgettuple(scan, direction);
		}

		return false;
	}

	item = rum_tuplesort_getrum(so->sortstate, true, &should_free);
	while (item)
	{
		uint32		i,
					j = 0;

		if (rumCompareItemPointers(&GET_SCAN_TID(scan), &item->iptr) == 0)
		{
			if (should_free)
				pfree(item);
			item = rum_tuplesort_getrum(so->sortstate, true, &should_free);
			continue;
		}

		SET_SCAN_TID(scan, item->iptr);
		scan->xs_recheck = item->recheck;
		scan->xs_recheckorderby = false;

		for (i = 0; i < so->nkeys; i++)
		{
			if (!so->keys[i]->orderBy)
				continue;
			scan->xs_orderbyvals[j] = Float8GetDatum(item->data[j]);
			scan->xs_orderbynulls[j] = false;

			j++;
		}

		if (should_free)
			pfree(item);
		return true;
	}

	return false;
}
