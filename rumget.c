/*-------------------------------------------------------------------------
 *
 * rumget.c
 *	  fetch tuples from a RUM scan.
 *
 *
 * Portions Copyright (c) 2015-2016, Postgres Professional
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "rumsort.h"

#include "access/relscan.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/memutils.h"

#include "rum.h"

/* GUC parameter */
int			RumFuzzySearchLimit = 0;

typedef struct pendingPosition
{
	Buffer		pendingBuffer;
	OffsetNumber firstOffset;
	OffsetNumber lastOffset;
	ItemPointerData item;
	bool	   *hasMatchKey;
} pendingPosition;

static bool scanPage(RumState * rumstate, RumScanEntry entry, ItemPointer item,
		 Page page, bool equalOk);
static void insertScanItem(RumScanOpaque so, bool recheck);
static int	scan_entry_cmp(const void *p1, const void *p2);
static void entryGetItem(RumState * rumstate, RumScanEntry entry);


/*
 * Convenience function for invoking a key's consistentFn
 */
static bool
callConsistentFn(RumState * rumstate, RumScanKey key)
{
	bool		res;

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

	if (res && key->attnum == rumstate->attrnAddToColumn)
	{
		uint32		i;

		/*
		 * remember some addinfo value for later ordering by addinfo from
		 * another column
		 */

		key->outerAddInfoIsNull = true;

		for (i = 0; i < key->nentries; i++)
		{
			if (key->entryRes[i] && key->addInfoIsNull[0] == false)
			{
				key->outerAddInfoIsNull = false;

				/*
				 * XXX FIXME only pass-by-value!!! Value should be copied to
				 * long-lived memory context and, somehow, freeed. Seems, the
				 * last is real problem
				 */
				key->outerAddInfo = key->addInfo[0];
				break;
			}
		}
	}

	return res;
}

/*
 * Tries to refind previously taken ItemPointer on a posting page.
 */
static bool
findItemInPostingPage(Page page, ItemPointer item, OffsetNumber *off,
					  OffsetNumber attnum, RumState * rumstate)
{
	OffsetNumber maxoff = RumPageGetOpaque(page)->maxoff;
	int			res;
	Pointer		ptr;
	RumKey		iter_item;

	ItemPointerSetMin(&iter_item.iptr);

	if (RumPageGetOpaque(page)->flags & RUM_DELETED)
		/* page was deleted by concurrent vacuum */
		return false;

	ptr = RumDataPageGetData(page);

	/*
	 * scan page to find equal or first greater value
	 */
	for (*off = FirstOffsetNumber; *off <= maxoff; (*off)++)
	{
		ptr = rumDataPageLeafReadPointer(ptr, attnum, &iter_item, rumstate);

		res = rumCompareItemPointers(item, &iter_item.iptr);
		if (res <= 0)
			return true;
	}

	return false;
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

		stack->buffer = rumStepRight(stack->buffer, btree->index, RUM_SHARE);
		stack->blkno = BufferGetBlockNumber(stack->buffer);
		stack->off = FirstOffsetNumber;
	}

	return true;
}

/*
 * Scan all pages of a posting tree and save all its heap ItemPointers
 * in scanEntry->matchBitmap
 */
static void
scanPostingTree(Relation index, RumScanEntry scanEntry,
	   BlockNumber rootPostingTree, OffsetNumber attnum, RumState * rumstate)
{
	RumPostingTreeScan *gdi;
	Buffer		buffer;
	Page		page;

	/* Descend to the leftmost leaf page */
	gdi = rumPrepareScanPostingTree(index, rootPostingTree, TRUE, attnum, rumstate);

	buffer = rumScanBeginPostingTree(gdi);
	IncrBufferRefCount(buffer); /* prevent unpin in freeRumBtreeStack */

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
			RumKey		item;
			Pointer		ptr;

			ItemPointerSetMin(&item.iptr);

			ptr = RumDataPageGetData(page);
			for (i = FirstOffsetNumber; i <= maxoff; i++)
			{
				ptr = rumDataPageLeafReadPointer(ptr, attnum, &item, rumstate);
				tbm_add_tuples(scanEntry->matchBitmap, &item.iptr, 1, false);
			}

			scanEntry->predictNumberResult += maxoff;
		}

		if (RumPageRightMost(page))
			break;				/* no more pages */

		buffer = rumStepRight(buffer, index, RUM_SHARE);
	}

	UnlockReleaseBuffer(buffer);
}

/*
 * Collects TIDs into scanEntry->matchBitmap for all heap tuples that
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
				   RumScanEntry scanEntry)
{
	OffsetNumber attnum;
	Form_pg_attribute attr;

	/* Initialize empty bitmap result */
	scanEntry->matchBitmap = tbm_create(work_mem * 1024L);

	/* Null query cannot partial-match anything */
	if (scanEntry->isPartialMatch &&
		scanEntry->queryCategory != RUM_CAT_NORM_KEY)
		return true;

	/* Locate tupdesc entry for key column (for attbyval/attlen data) */
	attnum = scanEntry->attnum;
	attr = btree->rumstate->origTupdesc->attrs[attnum - 1];

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
		if (rumtuple_get_attrnum(btree->rumstate, itup) != attnum)
			return true;

		/* Safe to fetch attribute value */
		idatum = rumtuple_get_key(btree->rumstate, itup, &icategory);

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
			cmp = DatumGetInt32(FunctionCall4Coll(&btree->rumstate->comparePartialFn[attnum - 1],
							   btree->rumstate->supportCollation[attnum - 1],
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
			scanPostingTree(btree->index, scanEntry, rootPostingTree, attnum, btree->rumstate);

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

				if (rumtuple_get_attrnum(btree->rumstate, itup) != attnum)
					elog(ERROR, "lost saved point in index");	/* must not happen !!! */
				newDatum = rumtuple_get_key(btree->rumstate, itup,
											&newCategory);

				if (rumCompareEntries(btree->rumstate, attnum,
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
			ItemPointerData *ipd = (ItemPointerData *) palloc(
							 sizeof(ItemPointerData) * RumGetNPosting(itup));

			rumReadTuplePointers(btree->rumstate, scanEntry->attnum, itup, ipd);

			tbm_add_tuples(scanEntry->matchBitmap,
						   ipd, RumGetNPosting(itup), false);
			scanEntry->predictNumberResult += RumGetNPosting(itup);
			pfree(ipd);
		}

		/*
		 * Done with this entry, go to the next
		 */
		stack->off++;
	}
}

/*
 * Start* functions setup beginning state of searches: finds correct buffer and pins it.
 */
static void
startScanEntry(RumState * rumstate, RumScanEntry entry)
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
	entry->matchBitmap = NULL;
	entry->matchResult = NULL;
	entry->reduceResult = FALSE;
	entry->predictNumberResult = 0;

	/*
	 * we should find entry, and begin scan of posting tree or just store
	 * posting list in memory
	 */
	rumPrepareEntryScan(&btreeEntry, entry->attnum,
						entry->queryKey, entry->queryCategory,
						rumstate);
	btreeEntry.searchMode = TRUE;
	stackEntry = rumFindLeafPage(&btreeEntry, NULL);
	page = BufferGetPage(stackEntry->buffer);
	needUnlock = TRUE;

	entry->isFinished = TRUE;

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
		if (collectMatchBitmap(&btreeEntry, stackEntry, entry) == false)
		{
			/*
			 * RUM tree was seriously restructured, so we will cleanup all
			 * found data and rescan. See comments near 'return false' in
			 * collectMatchBitmap()
			 */
			if (entry->matchBitmap)
			{
				if (entry->matchIterator)
					tbm_end_iterate(entry->matchIterator);
				entry->matchIterator = NULL;
				tbm_free(entry->matchBitmap);
				entry->matchBitmap = NULL;
			}
			LockBuffer(stackEntry->buffer, RUM_UNLOCK);
			freeRumBtreeStack(stackEntry);
			goto restartScanEntry;
		}

		if (entry->matchBitmap && !tbm_is_empty(entry->matchBitmap))
		{
			entry->matchIterator = tbm_begin_iterate(entry->matchBitmap);
			entry->isFinished = FALSE;
		}
	}
	else if (btreeEntry.findItem(&btreeEntry, stackEntry) ||
			 (entry->queryCategory == RUM_CAT_EMPTY_QUERY &&
			  entry->scanWithAddInfo))
	{
		IndexTuple	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, stackEntry->off));

		if (RumIsPostingTree(itup))
		{
			BlockNumber rootPostingTree = RumGetPostingTree(itup);
			RumPostingTreeScan *gdi;
			Page		page;
			OffsetNumber maxoff,
						i;
			Pointer		ptr;
			RumKey		item;

			ItemPointerSetMin(&item.iptr);

			/*
			 * We should unlock entry page before touching posting tree to
			 * prevent deadlocks with vacuum processes. Because entry is never
			 * deleted from page and posting tree is never reduced to the
			 * posting list, we can unlock page after getting BlockNumber of
			 * root of posting tree.
			 */
			LockBuffer(stackEntry->buffer, RUM_UNLOCK);
			needUnlock = FALSE;
			gdi = rumPrepareScanPostingTree(rumstate->index, rootPostingTree, TRUE, entry->attnum, rumstate);

			entry->buffer = rumScanBeginPostingTree(gdi);
			entry->gdi = gdi;
			entry->context = AllocSetContextCreate(CurrentMemoryContext,
												   "GiST temporary context",
												   ALLOCSET_DEFAULT_MINSIZE,
												   ALLOCSET_DEFAULT_INITSIZE,
												   ALLOCSET_DEFAULT_MAXSIZE);

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
			entry->list = (RumKey *) palloc(BLCKSZ * sizeof(RumKey));
			maxoff = RumPageGetOpaque(page)->maxoff;
			entry->nlist = maxoff;

			ptr = RumDataPageGetData(page);

			for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
			{
				ptr = rumDataPageLeafRead(ptr, entry->attnum, &item, rumstate);
				entry->list[i - FirstOffsetNumber] = item;
			}

			LockBuffer(entry->buffer, RUM_UNLOCK);
			entry->isFinished = FALSE;
		}
		else if (RumGetNPosting(itup) > 0)
		{
			entry->nlist = RumGetNPosting(itup);
			entry->predictNumberResult = entry->nlist;
			entry->list = (RumKey *) palloc(sizeof(RumKey) * entry->nlist);

			rumReadTuple(rumstate, entry->attnum, itup, entry->list);
			entry->isFinished = FALSE;
		}

		if (entry->queryCategory == RUM_CAT_EMPTY_QUERY &&
			entry->scanWithAddInfo)
			entry->stack = stackEntry;
	}

	if (needUnlock)
		LockBuffer(stackEntry->buffer, RUM_UNLOCK);
	if (entry->stack == NULL)
		freeRumBtreeStack(stackEntry);
}

static void
startScanKey(RumState * rumstate, RumScanKey key)
{
	ItemPointerSetMin(&key->curItem);
	key->curItemMatches = false;
	key->recheckCurItem = false;
	key->isFinished = false;
}

/*
 * Compare entries position. At first consider isFinished flag, then compare
 * item pointers.
 */
static int
cmpEntries(RumScanEntry e1, RumScanEntry e2)
{
	if (e1->isFinished == TRUE)
	{
		if (e2->isFinished == TRUE)
			return 0;
		else
			return 1;
	}
	if (e2->isFinished)
		return -1;
	return rumCompareItemPointers(&e1->curItem.iptr, &e2->curItem.iptr);
}

static int
scan_entry_cmp(const void *p1, const void *p2)
{
	RumScanEntry e1 = *((RumScanEntry *) p1);
	RumScanEntry e2 = *((RumScanEntry *) p2);

	return -cmpEntries(e1, e2);
}

static void
startScan(IndexScanDesc scan)
{
	MemoryContext oldCtx = CurrentMemoryContext;
	RumScanOpaque so = (RumScanOpaque) scan->opaque;
	RumState   *rumstate = &so->rumstate;
	uint32		i;
	bool		useFastScan = false;

	MemoryContextSwitchTo(so->keyCtx);
	for (i = 0; i < so->totalentries; i++)
	{
		startScanEntry(rumstate, so->entries[i]);
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
				so->entries[i]->reduceResult = TRUE;
			}
		}
	}

	for (i = 0; i < so->nkeys; i++)
		startScanKey(rumstate, so->keys + i);

	/*
	 * Check if we can use a fast scan: should exists at least one
	 * preConsistent method.
	 */
	for (i = 0; i < so->nkeys; i++)
	{
		RumScanKey	key = &so->keys[i];

		if (so->rumstate.canPreConsistent[key->attnum - 1])
		{
			useFastScan = true;
			break;
		}
	}

	if (useFastScan)
	{
		for (i = 0; i < so->totalentries; i++)
		{
			RumScanEntry entry = so->entries[i];

			if (entry->isPartialMatch)
			{
				useFastScan = false;
				break;
			}
		}
	}

	ItemPointerSetMin(&so->iptr);

	if (useFastScan)
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
				entryGetItem(&so->rumstate, so->sortedEntries[i]);
		}
		qsort(so->sortedEntries, so->totalentries, sizeof(RumScanEntry),
			  scan_entry_cmp);
	}

	so->useFastScan = useFastScan;
}

/*
 * Gets next ItemPointer from PostingTree. Note, that we copy
 * page into RumScanEntry->list array and unlock page, but keep it pinned
 * to prevent interference with vacuum
 */
static void
entryGetNextItem(RumState * rumstate, RumScanEntry entry)
{
	Page		page;

	for (;;)
	{
		if (entry->offset < entry->nlist)
		{
			entry->curItem = entry->list[entry->offset];
			entry->offset++;
			return;
		}

		LockBuffer(entry->buffer, RUM_SHARE);
		page = BufferGetPage(entry->buffer);

		if (scanPage(rumstate, entry, &entry->curItem.iptr,
					 BufferGetPage(entry->buffer),
					 false))
		{
			LockBuffer(entry->buffer, RUM_UNLOCK);
			return;
		}

		for (;;)
		{
			/*
			 * It's needed to go by right link. During that we should refind
			 * first ItemPointer greater that stored
			 */
			if (RumPageRightMost(page))
			{
				UnlockReleaseBuffer(entry->buffer);
				ItemPointerSetInvalid(&entry->curItem.iptr);

				entry->buffer = InvalidBuffer;
				entry->isFinished = TRUE;
				entry->gdi->stack->buffer = InvalidBuffer;
				return;
			}

			entry->buffer = rumStepRight(entry->buffer,
										 rumstate->index,
										 RUM_SHARE);
			entry->gdi->stack->buffer = entry->buffer;
			entry->gdi->stack->blkno = BufferGetBlockNumber(entry->buffer);
			page = BufferGetPage(entry->buffer);

			entry->offset = InvalidOffsetNumber;
			if (!ItemPointerIsValid(&entry->curItem.iptr) ||
			findItemInPostingPage(page, &entry->curItem.iptr, &entry->offset,
								  entry->attnum, rumstate))
			{
				OffsetNumber maxoff,
							i;
				Pointer		ptr;
				RumKey		item;

				ItemPointerSetMin(&item.iptr);

				/*
				 * Found position equal to or greater than stored
				 */
				maxoff = RumPageGetOpaque(page)->maxoff;
				entry->nlist = maxoff;

				ptr = RumDataPageGetData(page);

				for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
				{
					ptr = rumDataPageLeafRead(ptr, entry->attnum, &item,
											  rumstate);
					entry->list[i - FirstOffsetNumber] = item;
				}

				LockBuffer(entry->buffer, RUM_UNLOCK);

				if (!ItemPointerIsValid(&entry->curItem.iptr) ||
					rumCompareItemPointers(&entry->curItem.iptr,
								  &entry->list[entry->offset - 1].iptr) == 0)
				{
					/*
					 * First pages are deleted or empty, or we found exact
					 * position, so break inner loop and continue outer one.
					 */
					break;
				}

				/*
				 * Find greater than entry->curItem position, store it.
				 */
				entry->curItem = entry->list[entry->offset - 1];

				return;
			}
		}
	}
}

static void
entryGetNextItemList(RumState * rumstate, RumScanEntry entry)
{
	Page		page;
	IndexTuple	itup;
	RumBtreeData btree;
	bool		needUnlock;

	Assert(!entry->isFinished);
	Assert(entry->stack);

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
	entry->matchBitmap = NULL;
	entry->matchResult = NULL;
	entry->reduceResult = FALSE;
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
		entry->isFinished = TRUE;
		LockBuffer(entry->stack->buffer, RUM_UNLOCK);
		return;
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
		entry->isFinished = TRUE;
		LockBuffer(entry->stack->buffer, RUM_UNLOCK);
		return;
	}

	/*
	 * OK, we want to return the TIDs listed in this entry.
	 */
	if (RumIsPostingTree(itup))
	{
		BlockNumber rootPostingTree = RumGetPostingTree(itup);
		RumPostingTreeScan *gdi;
		Page		page;
		OffsetNumber maxoff,
					i;
		Pointer		ptr;
		RumKey		item;

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
						rootPostingTree, TRUE, entry->attnum, rumstate);

		entry->buffer = rumScanBeginPostingTree(gdi);
		entry->gdi = gdi;
		entry->context = AllocSetContextCreate(CurrentMemoryContext,
											   "GiST temporary context",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

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
		entry->list = (RumKey *) palloc(BLCKSZ * sizeof(RumKey));
		maxoff = RumPageGetOpaque(page)->maxoff;
		entry->nlist = maxoff;

		ptr = RumDataPageGetData(page);

		for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
		{
			ptr = rumDataPageLeafRead(ptr, entry->attnum, &item, rumstate);
			entry->list[i - FirstOffsetNumber] = item;
		}

		LockBuffer(entry->buffer, RUM_UNLOCK);
		entry->isFinished = FALSE;
	}
	else if (RumGetNPosting(itup) > 0)
	{
		entry->nlist = RumGetNPosting(itup);
		entry->predictNumberResult = entry->nlist;
		entry->list = (RumKey *) palloc(sizeof(RumKey) * entry->nlist);

		rumReadTuple(rumstate, entry->attnum, itup, entry->list);
		entry->isFinished = FALSE;
	}

	Assert(entry->nlist > 0);

	entry->offset++;
	entry->curItem = entry->list[entry->offset - 1];

	/*
	 * Done with this entry, go to the next for the future.
	 */
	entry->stack->off++;

	if (needUnlock)
		LockBuffer(entry->stack->buffer, RUM_UNLOCK);
}

#define rum_rand() (((double) random()) / ((double) MAX_RANDOM_VALUE))
#define dropItem(e) ( rum_rand() > ((double)RumFuzzySearchLimit)/((double)((e)->predictNumberResult)) )

/*
 * Sets entry->curItem to next heap item pointer for one entry of one scan key,
 * or sets entry->isFinished to TRUE if there are no more.
 *
 * Item pointers must be returned in ascending order.
 *
 * Note: this can return a "lossy page" item pointer, indicating that the
 * entry potentially matches all items on that heap page.  However, it is
 * not allowed to return both a lossy page pointer and exact (regular)
 * item pointers for the same page.  (Doing so would break the key-combination
 * logic in keyGetItem and scanGetItem; see comment in scanGetItem.)  In the
 * current implementation this is guaranteed by the behavior of tidbitmaps.
 */
static void
entryGetItem(RumState * rumstate, RumScanEntry entry)
{
	Assert(!entry->isFinished);

	if (entry->matchBitmap)
	{
		do
		{
			if (entry->matchResult == NULL ||
				entry->offset >= entry->matchResult->ntuples)
			{
				entry->matchResult = tbm_iterate(entry->matchIterator);

				if (entry->matchResult == NULL)
				{
					ItemPointerSetInvalid(&entry->curItem.iptr);
					tbm_end_iterate(entry->matchIterator);
					entry->matchIterator = NULL;
					entry->isFinished = TRUE;
					break;
				}

				/*
				 * Reset counter to the beginning of entry->matchResult. Note:
				 * entry->offset is still greater than matchResult->ntuples if
				 * matchResult is lossy.  So, on next call we will get next
				 * result from TIDBitmap.
				 */
				entry->offset = 0;
			}

			if (entry->matchResult->ntuples < 0)
			{
				/*
				 * lossy result, so we need to check the whole page
				 */
				ItemPointerSetLossyPage(&entry->curItem.iptr,
										entry->matchResult->blockno);

				/*
				 * We might as well fall out of the loop; we could not
				 * estimate number of results on this page to support correct
				 * reducing of result even if it's enabled
				 */
				break;
			}

			ItemPointerSet(&entry->curItem.iptr,
						   entry->matchResult->blockno,
						   entry->matchResult->offsets[entry->offset]);
			entry->offset++;
		} while (entry->reduceResult == TRUE && dropItem(entry));
	}
	else if (!BufferIsValid(entry->buffer))
	{
		entry->offset++;
		if (entry->offset <= entry->nlist)
		{
			entry->curItem = entry->list[entry->offset - 1];
		}
		else if (entry->stack)
		{
			entryGetNextItemList(rumstate, entry);
		}
		else
		{
			ItemPointerSetInvalid(&entry->curItem.iptr);
			entry->isFinished = TRUE;
		}
	}
	else
	{
		do
		{
			entryGetNextItem(rumstate, entry);
		} while (entry->isFinished == FALSE &&
				 entry->reduceResult == TRUE &&
				 dropItem(entry));
		if (entry->stack)
		{
			entry->isFinished = FALSE;
			entryGetNextItemList(rumstate, entry);
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
 * iff recheck is needed for this item pointer (including the case where the
 * item pointer is a lossy page pointer).
 *
 * If all entry streams are exhausted, sets key->isFinished to TRUE.
 *
 * Item pointers must be returned in ascending order.
 *
 * Note: this can return a "lossy page" item pointer, indicating that the
 * key potentially matches all items on that heap page.  However, it is
 * not allowed to return both a lossy page pointer and exact (regular)
 * item pointers for the same page.  (Doing so would break the key-combination
 * logic in scanGetItem.)
 */
static void
keyGetItem(RumState * rumstate, MemoryContext tempCtx, RumScanKey key)
{
	ItemPointerData minItem;
	ItemPointerData curPageLossy;
	uint32		i;
	uint32		lossyEntry;
	bool		haveLossyEntry;
	RumScanEntry entry;
	bool		res;
	MemoryContext oldCtx;

	Assert(!key->isFinished);

	/*
	 * Find the minimum of the active entry curItems.
	 *
	 * Note: a lossy-page entry is encoded by a ItemPointer with max value for
	 * offset (0xffff), so that it will sort after any exact entries for the
	 * same page.  So we'll prefer to return exact pointers not lossy
	 * pointers, which is good.
	 */
	ItemPointerSetMax(&minItem);

	for (i = 0; i < key->nentries; i++)
	{
		entry = key->scanEntry[i];
		if (entry->isFinished == FALSE &&
			rumCompareItemPointers(&entry->curItem.iptr, &minItem) < 0)
			minItem = entry->curItem.iptr;
	}

	if (ItemPointerIsMax(&minItem))
	{
		/* all entries are finished */
		key->isFinished = TRUE;
		return;
	}

	/*
	 * We might have already tested this item; if so, no need to repeat work.
	 * (Note: the ">" case can happen, if minItem is exact but we previously
	 * had to set curItem to a lossy-page pointer.)
	 */
	if (rumCompareItemPointers(&key->curItem, &minItem) >= 0)
		return;

	/*
	 * OK, advance key->curItem and perform consistentFn test.
	 */
	key->curItem = minItem;

	/*
	 * Lossy-page entries pose a problem, since we don't know the correct
	 * entryRes state to pass to the consistentFn, and we also don't know what
	 * its combining logic will be (could be AND, OR, or even NOT). If the
	 * logic is OR then the consistentFn might succeed for all items in the
	 * lossy page even when none of the other entries match.
	 *
	 * If we have a single lossy-page entry then we check to see if the
	 * consistentFn will succeed with only that entry TRUE.  If so, we return
	 * a lossy-page pointer to indicate that the whole heap page must be
	 * checked.  (On subsequent calls, we'll do nothing until minItem is past
	 * the page altogether, thus ensuring that we never return both regular
	 * and lossy pointers for the same page.)
	 *
	 * This idea could be generalized to more than one lossy-page entry, but
	 * ideally lossy-page entries should be infrequent so it would seldom be
	 * the case that we have more than one at once.  So it doesn't seem worth
	 * the extra complexity to optimize that case. If we do find more than
	 * one, we just punt and return a lossy-page pointer always.
	 *
	 * Note that only lossy-page entries pointing to the current item's page
	 * should trigger this processing; we might have future lossy pages in the
	 * entry array, but they aren't relevant yet.
	 */
	ItemPointerSetLossyPage(&curPageLossy,
							RumItemPointerGetBlockNumber(&key->curItem));

	lossyEntry = 0;
	haveLossyEntry = false;
	for (i = 0; i < key->nentries; i++)
	{
		entry = key->scanEntry[i];
		if (entry->isFinished == FALSE &&
			rumCompareItemPointers(&entry->curItem.iptr, &curPageLossy) == 0)
		{
			if (haveLossyEntry)
			{
				/* Multiple lossy entries, punt */
				key->curItem = curPageLossy;
				key->curItemMatches = true;
				key->recheckCurItem = true;
				return;
			}
			lossyEntry = i;
			haveLossyEntry = true;
		}
	}

	/* prepare for calling consistentFn in temp context */
	oldCtx = MemoryContextSwitchTo(tempCtx);

	if (haveLossyEntry)
	{
		/* Single lossy-page entry, so see if whole page matches */
		for (i = 0; i < key->nentries; i++)
		{
			key->addInfo[i] = (Datum) 0;
			key->addInfoIsNull[i] = true;
		}
		memset(key->entryRes, FALSE, key->nentries);
		key->entryRes[lossyEntry] = TRUE;

		if (callConsistentFn(rumstate, key))
		{
			/* Yes, so clean up ... */
			MemoryContextSwitchTo(oldCtx);
			MemoryContextReset(tempCtx);

			/* and return lossy pointer for whole page */
			key->curItem = curPageLossy;
			key->curItemMatches = true;
			key->recheckCurItem = true;
			return;
		}
	}

	/*
	 * At this point we know that we don't need to return a lossy whole-page
	 * pointer, but we might have matches for individual exact item pointers,
	 * possibly in combination with a lossy pointer.  Our strategy if there's
	 * a lossy pointer is to try the consistentFn both ways and return a hit
	 * if it accepts either one (forcing the hit to be marked lossy so it will
	 * be rechecked).  An exception is that we don't need to try it both ways
	 * if the lossy pointer is in a "hidden" entry, because the consistentFn's
	 * result can't depend on that.
	 *
	 * Prepare entryRes array to be passed to consistentFn.
	 */
	for (i = 0; i < key->nentries; i++)
	{
		entry = key->scanEntry[i];
		if (entry->isFinished == FALSE &&
			rumCompareItemPointers(&entry->curItem.iptr, &key->curItem) == 0)
		{
			key->entryRes[i] = TRUE;
			key->addInfo[i] = entry->curItem.addInfo;
			key->addInfoIsNull[i] = entry->curItem.addInfoIsNull;
		}
		else
		{
			key->entryRes[i] = FALSE;
			key->addInfo[i] = (Datum) 0;
			key->addInfoIsNull[i] = true;
		}
	}
	if (haveLossyEntry)
	{
		key->entryRes[lossyEntry] = TRUE;
		key->addInfo[lossyEntry] = (Datum) 0;
		key->addInfoIsNull[lossyEntry] = true;
	}

	res = callConsistentFn(rumstate, key);

	if (!res && haveLossyEntry && lossyEntry < key->nuserentries)
	{
		/* try the other way for the lossy item */
		key->entryRes[lossyEntry] = FALSE;
		key->addInfo[lossyEntry] = (Datum) 0;
		key->addInfoIsNull[lossyEntry] = true;

		res = callConsistentFn(rumstate, key);
	}

	key->curItemMatches = res;
	/* If we matched a lossy entry, force recheckCurItem = true */
	if (haveLossyEntry)
		key->recheckCurItem = true;

	/* clean up after consistentFn calls */
	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(tempCtx);
}

/*
 * Get next heap item pointer (after advancePast) from scan.
 * Returns true if anything found.
 * On success, *item and *recheck are set.
 *
 * Note: this is very nearly the same logic as in keyGetItem(), except
 * that we know the keys are to be combined with AND logic, whereas in
 * keyGetItem() the combination logic is known only to the consistentFn.
 */
static bool
scanGetItemRegular(IndexScanDesc scan, ItemPointer advancePast,
				   ItemPointerData *item, bool *recheck)
{
	RumScanOpaque so = (RumScanOpaque) scan->opaque;
	RumState   *rumstate = &so->rumstate;
	ItemPointerData myAdvancePast = *advancePast;
	uint32		i;
	bool		allFinished;
	bool		match;

	for (;;)
	{
		/*
		 * Advance any entries that are <= myAdvancePast.  In particular,
		 * since entry->curItem was initialized with ItemPointerSetMin, this
		 * ensures we fetch the first item for each entry on the first call.
		 */
		allFinished = TRUE;

		for (i = 0; i < so->totalentries; i++)
		{
			RumScanEntry entry = so->entries[i];

			while (entry->isFinished == FALSE &&
				   rumCompareItemPointers(&entry->curItem.iptr,
										  &myAdvancePast) <= 0)
				entryGetItem(rumstate, entry);

			if (entry->isFinished == FALSE)
				allFinished = FALSE;
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
		ItemPointerSetMax(item);

		for (i = 0; i < so->nkeys; i++)
		{
			RumScanKey	key = so->keys + i;

			if (key->orderBy)
				continue;

			keyGetItem(&so->rumstate, so->tempCtx, key);

			if (key->isFinished)
				return false;	/* finished one of keys */

			if (rumCompareItemPointers(&key->curItem, item) < 0)
				*item = key->curItem;
		}

		Assert(!ItemPointerIsMax(item));

		/*----------
		 * Now *item contains first ItemPointer after previous result.
		 *
		 * The item is a valid hit only if all the keys succeeded for either
		 * that exact TID, or a lossy reference to the same page.
		 *
		 * This logic works only if a keyGetItem stream can never contain both
		 * exact and lossy pointers for the same page.  Else we could have a
		 * case like
		 *
		 *		stream 1		stream 2
		 *		...             ...
		 *		42/6			42/7
		 *		50/1			42/0xffff
		 *		...             ...
		 *
		 * We would conclude that 42/6 is not a match and advance stream 1,
		 * thus never detecting the match to the lossy pointer in stream 2.
		 * (keyGetItem has a similar problem versus entryGetItem.)
		 *----------
		 */
		match = true;
		for (i = 0; i < so->nkeys; i++)
		{
			RumScanKey	key = so->keys + i;

			if (key->orderBy)
				continue;

			if (key->curItemMatches)
			{
				if (rumCompareItemPointers(item, &key->curItem) == 0)
					continue;
				if (ItemPointerIsLossyPage(&key->curItem) &&
					RumItemPointerGetBlockNumber(&key->curItem) ==
					RumItemPointerGetBlockNumber(item))
					continue;
			}
			match = false;
			break;
		}

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
		RumScanKey	key = so->keys + i;

		if (key->orderBy)
			continue;

		if (key->recheckCurItem)
		{
			*recheck = true;
			break;
		}
	}

	return TRUE;
}

/*
 * Finds part of page containing requested item using small index at the end
 * of page.
 */
static bool
scanPage(RumState * rumstate, RumScanEntry entry, ItemPointer item, Page page,
		 bool equalOk)
{
	int			j;
	RumKey		iter_item;
	Pointer		ptr;
	OffsetNumber first = FirstOffsetNumber,
				i,
				maxoff;
	bool		found;
	int			cmp;

	ItemPointerSetMin(&iter_item.iptr);

	if (!RumPageRightMost(page))
	{
		cmp = rumCompareItemPointers(RumDataPageGetRightBound(page), item);
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

		cmp = rumCompareItemPointers(&index->iptr, item);
		if (cmp < 0 || (cmp <= 0 && !equalOk))
		{
			ptr = RumDataPageGetData(page) + index->pageOffset;
			first = index->offsetNumer;
			iter_item.iptr = index->iptr;
		}
		else
		{
			maxoff = index->offsetNumer - 1;
			break;
		}
	}

	entry->nlist = maxoff - first + 1;
	entry->offset = InvalidOffsetNumber;
	found = false;
	for (i = first; i <= maxoff; i++)
	{
		ptr = rumDataPageLeafRead(ptr, entry->attnum, &iter_item, rumstate);
		entry->list[i - first] = iter_item;

		cmp = rumCompareItemPointers(item, &iter_item.iptr);
		if ((cmp < 0 || (cmp <= 0 && equalOk)) && entry->offset == InvalidOffsetNumber)
		{
			found = true;
			entry->offset = i - first + 1;
		}
	}
	if (!found)
		return false;

	entry->curItem = entry->list[entry->offset - 1];
	return true;
}

/*
 * Find item of scan entry wich is greater or equal to the given item.
 */
static void
entryFindItem(RumState * rumstate, RumScanEntry entry, RumKey * item)
{
	Page		page = NULL;

	if (entry->nlist == 0)
	{
		entry->isFinished = TRUE;
		return;
	}

	/* Try to find in loaded part of page */
	if (rumCompareItemPointers(&entry->list[entry->nlist - 1].iptr,
							   &item->iptr) >= 0)
	{
		if (rumCompareItemPointers(&entry->curItem.iptr, &item->iptr) >= 0)
			return;
		while (entry->offset < entry->nlist)
		{
			if (rumCompareItemPointers(&entry->list[entry->offset].iptr,
									   &item->iptr) >= 0)
			{
				entry->curItem = entry->list[entry->offset];
				entry->offset++;
				return;
			}
			entry->offset++;
		}
	}

	if (!BufferIsValid(entry->buffer))
	{
		entry->isFinished = TRUE;
		return;
	}

	/* Check rest of page */
	LockBuffer(entry->buffer, RUM_SHARE);

	if (scanPage(rumstate, entry, &item->iptr,
				 BufferGetPage(entry->buffer),
				 true))
	{
		LockBuffer(entry->buffer, RUM_UNLOCK);
		return;
	}

	/* Try to traverse to another leaf page */
	entry->gdi->btree.items = item;
	entry->gdi->btree.curitem = 0;

	entry->gdi->stack->buffer = entry->buffer;
	entry->gdi->stack = rumReFindLeafPage(&entry->gdi->btree, entry->gdi->stack);
	entry->buffer = entry->gdi->stack->buffer;

	page = BufferGetPage(entry->buffer);

	if (scanPage(rumstate, entry, &item->iptr,
				 BufferGetPage(entry->buffer),
				 true))
	{
		LockBuffer(entry->buffer, RUM_UNLOCK);
		return;
	}

	/* At last try to traverse by right links */
	for (;;)
	{
		/*
		 * It's needed to go by right link. During that we should refind first
		 * ItemPointer greater that stored
		 */
		BlockNumber blkno;

		blkno = RumPageGetOpaque(page)->rightlink;

		LockBuffer(entry->buffer, RUM_UNLOCK);
		if (blkno == InvalidBlockNumber)
		{
			ReleaseBuffer(entry->buffer);
			ItemPointerSetInvalid(&entry->curItem.iptr);
			entry->buffer = InvalidBuffer;
			entry->gdi->stack->buffer = InvalidBuffer;
			entry->isFinished = TRUE;
			return;
		}

		entry->buffer = ReleaseAndReadBuffer(entry->buffer,
											 rumstate->index,
											 blkno);
		entry->gdi->stack->buffer = entry->buffer;
		entry->gdi->stack->blkno = blkno;
		LockBuffer(entry->buffer, RUM_SHARE);
		page = BufferGetPage(entry->buffer);

		if (scanPage(rumstate, entry, &item->iptr,
					 BufferGetPage(entry->buffer),
					 true))
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
		RumScanKey	key = &so->keys[j];
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
entryShift(int i, RumScanOpaque so, bool find)
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
					  &so->sortedEntries[i - 1]->curItem);
	else if (!so->sortedEntries[minIndex]->isFinished)
		entryGetItem(rumstate, so->sortedEntries[minIndex]);

	/* Restore order of so->sortedEntries */
	while (minIndex > 0 &&
		   cmpEntries(so->sortedEntries[minIndex],
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
scanGetItemFast(IndexScanDesc scan, ItemPointer advancePast,
				ItemPointerData *item, bool *recheck)
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
			entryShift(k, so, false);
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
			if (cmpEntries(so->sortedEntries[i], so->sortedEntries[i - 1]) < 0)
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
		if (so->sortedEntries[i - 1]->isFinished == TRUE)
			return false;

		if (preConsistentResult == false)
		{
			entryShift(i, so, true);
			continue;
		}

		/* Call consistent method */
		consistentResult = true;
		for (i = 0; i < so->nkeys; i++)
		{
			RumScanKey	key = so->keys + i;

			if (key->orderBy)
				continue;

			for (j = 0; j < key->nentries; j++)
			{
				RumScanEntry entry = key->scanEntry[j];

				if (entry->isFinished == FALSE &&
					rumCompareItemPointers(&entry->curItem.iptr,
				&so->sortedEntries[so->totalentries - 1]->curItem.iptr) == 0)
				{
					key->entryRes[j] = TRUE;
					key->addInfo[j] = entry->curItem.addInfo;
					key->addInfoIsNull[j] = entry->curItem.addInfoIsNull;
				}
				else
				{
					key->entryRes[j] = FALSE;
					key->addInfo[j] = (Datum) 0;
					key->addInfoIsNull[j] = true;
				}
			}
			if (!callConsistentFn(&so->rumstate, key))
			{
				consistentResult = false;
				entryShift(k, so, false);
				continue;
			}
		}

		if (consistentResult == false)
			continue;

		/* Calculate recheck from each key */
		*recheck = false;
		for (i = 0; i < so->nkeys; i++)
		{
			RumScanKey	key = so->keys + i;

			if (key->orderBy)
				continue;

			if (key->recheckCurItem)
			{
				*recheck = true;
				break;
			}
		}

		*item = so->sortedEntries[so->totalentries - 1]->curItem.iptr;
		so->entriesIncrIndex = k;

		return true;
	}
	return false;
}

/*
 * Get next item whether using regular or fast scan.
 */
static bool
scanGetItem(IndexScanDesc scan, ItemPointer advancePast,
			ItemPointerData *item, bool *recheck)
{
	RumScanOpaque so = (RumScanOpaque) scan->opaque;

	if (so->useFastScan)
		return scanGetItemFast(scan, advancePast, item, recheck);
	else
		return scanGetItemRegular(scan, advancePast, item, recheck);
}


/*
 * Functions for scanning the pending list
 */


/*
 * Get ItemPointer of next heap row to be checked from pending list.
 * Returns false if there are no more. On pages with several heap rows
 * it returns each row separately, on page with part of heap row returns
 * per page data.  pos->firstOffset and pos->lastOffset are set to identify
 * the range of pending-list tuples belonging to this heap row.
 *
 * The pendingBuffer is presumed pinned and share-locked on entry, and is
 * pinned and share-locked on success exit.  On failure exit it's released.
 */
static bool
scanGetCandidate(IndexScanDesc scan, pendingPosition *pos)
{
	OffsetNumber maxoff;
	Page		page;
	IndexTuple	itup;

	ItemPointerSetInvalid(&pos->item);
	for (;;)
	{
		page = BufferGetPage(pos->pendingBuffer);

		maxoff = PageGetMaxOffsetNumber(page);
		if (pos->firstOffset > maxoff)
		{
			BlockNumber blkno = RumPageGetOpaque(page)->rightlink;

			if (blkno == InvalidBlockNumber)
			{
				UnlockReleaseBuffer(pos->pendingBuffer);
				pos->pendingBuffer = InvalidBuffer;

				return false;
			}
			else
			{
				/*
				 * Here we must prevent deletion of next page by insertcleanup
				 * process, which may be trying to obtain exclusive lock on
				 * current page.  So, we lock next page before releasing the
				 * current one
				 */
				Buffer		tmpbuf = ReadBuffer(scan->indexRelation, blkno);

				LockBuffer(tmpbuf, RUM_SHARE);
				UnlockReleaseBuffer(pos->pendingBuffer);

				pos->pendingBuffer = tmpbuf;
				pos->firstOffset = FirstOffsetNumber;
			}
		}
		else
		{
			itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, pos->firstOffset));
			pos->item = itup->t_tid;
			if (RumPageHasFullRow(page))
			{
				/*
				 * find itempointer to the next row
				 */
				for (pos->lastOffset = pos->firstOffset + 1; pos->lastOffset <= maxoff; pos->lastOffset++)
				{
					itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, pos->lastOffset));
					if (!ItemPointerEquals(&pos->item, &itup->t_tid))
						break;
				}
			}
			else
			{
				/*
				 * All itempointers are the same on this page
				 */
				pos->lastOffset = maxoff + 1;
			}

			/*
			 * Now pos->firstOffset points to the first tuple of current heap
			 * row, pos->lastOffset points to the first tuple of next heap row
			 * (or to the end of page)
			 */
			break;
		}
	}

	return true;
}

/*
 * Scan pending-list page from current tuple (off) up till the first of:
 * - match is found (then returns true)
 * - no later match is possible
 * - tuple's attribute number is not equal to entry's attrnum
 * - reach end of page
 *
 * datum[]/category[]/datumExtracted[] arrays are used to cache the results
 * of rumtuple_get_key() on the current page.
 */
static bool
matchPartialInPendingList(RumState * rumstate, Page page,
						  OffsetNumber off, OffsetNumber maxoff,
						  RumScanEntry entry,
						  Datum *datum, RumNullCategory * category,
						  bool *datumExtracted)
{
	IndexTuple	itup;
	int32		cmp;

	/* Partial match to a null is not possible */
	if (entry->queryCategory != RUM_CAT_NORM_KEY)
		return false;

	while (off < maxoff)
	{
		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, off));

		if (rumtuple_get_attrnum(rumstate, itup) != entry->attnum)
			return false;

		if (datumExtracted[off - 1] == false)
		{
			datum[off - 1] = rumtuple_get_key(rumstate, itup,
											  &category[off - 1]);
			datumExtracted[off - 1] = true;
		}

		/* Once we hit nulls, no further match is possible */
		if (category[off - 1] != RUM_CAT_NORM_KEY)
			return false;

		/*----------
		 * Check partial match.
		 * case cmp == 0 => match
		 * case cmp > 0 => not match and end scan (no later match possible)
		 * case cmp < 0 => not match and continue scan
		 *----------
		 */
		cmp = DatumGetInt32(FunctionCall4Coll(&rumstate->comparePartialFn[entry->attnum - 1],
							   rumstate->supportCollation[entry->attnum - 1],
											  entry->queryKey,
											  datum[off - 1],
											  UInt16GetDatum(entry->strategy),
										PointerGetDatum(entry->extra_data)));
		if (cmp == 0)
			return true;
		else if (cmp > 0)
			return false;

		off++;
	}

	return false;
}

/*
 * Set up the entryRes array for each key by looking at
 * every entry for current heap row in pending list.
 *
 * Returns true if each scan key has at least one entryRes match.
 * This corresponds to the situations where the normal index search will
 * try to apply the key's consistentFn.  (A tuple not meeting that requirement
 * cannot be returned by the normal search since no entry stream will
 * source its TID.)
 *
 * The pendingBuffer is presumed pinned and share-locked on entry.
 */
static bool
collectMatchesForHeapRow(IndexScanDesc scan, pendingPosition *pos)
{
	RumScanOpaque so = (RumScanOpaque) scan->opaque;
	OffsetNumber attrnum;
	Page		page;
	IndexTuple	itup;
	uint32		i,
				j;

	/*
	 * Reset all entryRes and hasMatchKey flags
	 */
	for (i = 0; i < so->nkeys; i++)
	{
		RumScanKey	key = so->keys + i;

		memset(key->entryRes, FALSE, key->nentries);
		memset(key->addInfo, FALSE, sizeof(Datum) * key->nentries);
		memset(key->addInfoIsNull, TRUE, key->nentries);
	}
	memset(pos->hasMatchKey, FALSE, so->nkeys);

	/*
	 * Outer loop iterates over multiple pending-list pages when a single heap
	 * row has entries spanning those pages.
	 */
	for (;;)
	{
		Datum		datum[BLCKSZ / sizeof(IndexTupleData)];
		RumNullCategory category[BLCKSZ / sizeof(IndexTupleData)];
		bool		datumExtracted[BLCKSZ / sizeof(IndexTupleData)];

		Assert(pos->lastOffset > pos->firstOffset);
		memset(datumExtracted + pos->firstOffset - 1, 0,
			   sizeof(bool) * (pos->lastOffset - pos->firstOffset));

		page = BufferGetPage(pos->pendingBuffer);

		for (i = 0; i < so->nkeys; i++)
		{
			RumScanKey	key = so->keys + i;

			for (j = 0; j < key->nentries; j++)
			{
				RumScanEntry entry = key->scanEntry[j];
				OffsetNumber StopLow = pos->firstOffset,
							StopHigh = pos->lastOffset,
							StopMiddle;

				/* If already matched on earlier page, do no extra work */
				if (key->entryRes[j])
					continue;

				/*
				 * Interesting tuples are from pos->firstOffset to
				 * pos->lastOffset and they are ordered by (attnum, Datum) as
				 * it's done in entry tree.  So we can use binary search to
				 * avoid linear scanning.
				 */
				while (StopLow < StopHigh)
				{
					int			res;

					StopMiddle = StopLow + ((StopHigh - StopLow) >> 1);

					itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, StopMiddle));

					attrnum = rumtuple_get_attrnum(&so->rumstate, itup);

					if (key->attnum < attrnum)
					{
						StopHigh = StopMiddle;
						continue;
					}
					if (key->attnum > attrnum)
					{
						StopLow = StopMiddle + 1;
						continue;
					}

					if (datumExtracted[StopMiddle - 1] == false)
					{
						datum[StopMiddle - 1] =
							rumtuple_get_key(&so->rumstate, itup,
											 &category[StopMiddle - 1]);
						datumExtracted[StopMiddle - 1] = true;
					}

					if (entry->queryCategory == RUM_CAT_EMPTY_QUERY)
					{
						/* special behavior depending on searchMode */
						if (entry->searchMode == GIN_SEARCH_MODE_ALL)
						{
							/* match anything except NULL_ITEM */
							if (category[StopMiddle - 1] == RUM_CAT_NULL_ITEM)
								res = -1;
							else
								res = 0;
						}
						else
						{
							/* match everything */
							res = 0;
						}
					}
					else
					{
						res = rumCompareEntries(&so->rumstate,
												entry->attnum,
												entry->queryKey,
												entry->queryCategory,
												datum[StopMiddle - 1],
												category[StopMiddle - 1]);
					}

					if (res == 0)
					{
						/*
						 * Found exact match (there can be only one, except in
						 * EMPTY_QUERY mode).
						 *
						 * If doing partial match, scan forward from here to
						 * end of page to check for matches.
						 *
						 * See comment above about tuple's ordering.
						 */
						if (entry->isPartialMatch)
							key->entryRes[j] =
								matchPartialInPendingList(&so->rumstate,
														  page,
														  StopMiddle,
														  pos->lastOffset,
														  entry,
														  datum,
														  category,
														  datumExtracted);
						else
						{
							key->entryRes[j] = true;
							if (OidIsValid(so->rumstate.addInfoTypeOid[i]))
								key->addInfo[j] = index_getattr(itup,
												 so->rumstate.oneCol ? 2 : 3,
										   so->rumstate.tupdesc[attrnum - 1],
													 &key->addInfoIsNull[j]);
						}

						/* done with binary search */
						break;
					}
					else if (res < 0)
						StopHigh = StopMiddle;
					else
						StopLow = StopMiddle + 1;
				}

				if (StopLow >= StopHigh && entry->isPartialMatch)
				{
					/*
					 * No exact match on this page.  If doing partial match,
					 * scan from the first tuple greater than target value to
					 * end of page.  Note that since we don't remember whether
					 * the comparePartialFn told us to stop early on a
					 * previous page, we will uselessly apply comparePartialFn
					 * to the first tuple on each subsequent page.
					 */
					key->entryRes[j] =
						matchPartialInPendingList(&so->rumstate,
												  page,
												  StopHigh,
												  pos->lastOffset,
												  entry,
												  datum,
												  category,
												  datumExtracted);
				}

				pos->hasMatchKey[i] |= key->entryRes[j];
			}
		}

		/* Advance firstOffset over the scanned tuples */
		pos->firstOffset = pos->lastOffset;

		if (RumPageHasFullRow(page))
		{
			/*
			 * We have examined all pending entries for the current heap row.
			 * Break out of loop over pages.
			 */
			break;
		}
		else
		{
			/*
			 * Advance to next page of pending entries for the current heap
			 * row.  Complain if there isn't one.
			 */
			ItemPointerData item = pos->item;

			if (scanGetCandidate(scan, pos) == false ||
				!ItemPointerEquals(&pos->item, &item))
				elog(ERROR, "could not find additional pending pages for same heap tuple");
		}
	}

	/*
	 * Now return "true" if all scan keys have at least one matching datum
	 */
	for (i = 0; i < so->nkeys; i++)
	{
		if (pos->hasMatchKey[i] == false)
			return false;
	}

	return true;
}

/*
 * Collect all matched rows from pending list into bitmap
 */
static int64
scanPendingInsert(IndexScanDesc scan)
{
	RumScanOpaque so = (RumScanOpaque) scan->opaque;
	MemoryContext oldCtx;
	bool		recheck,
				match;
	uint32		i;
	pendingPosition pos;
	Buffer		metabuffer = ReadBuffer(scan->indexRelation, RUM_METAPAGE_BLKNO);
	BlockNumber blkno;
	int64		ntids = 0;
	TIDBitmap  *tbm = so->tbm;

	LockBuffer(metabuffer, RUM_SHARE);
	blkno = RumPageGetMeta(BufferGetPage(metabuffer))->head;

	/*
	 * fetch head of list before unlocking metapage. head page must be pinned
	 * to prevent deletion by vacuum process
	 */
	if (blkno == InvalidBlockNumber)
	{
		/* No pending list, so proceed with normal scan */
		UnlockReleaseBuffer(metabuffer);
		return ntids;
	}

	pos.pendingBuffer = ReadBuffer(scan->indexRelation, blkno);
	LockBuffer(pos.pendingBuffer, RUM_SHARE);
	pos.firstOffset = FirstOffsetNumber;
	UnlockReleaseBuffer(metabuffer);
	pos.hasMatchKey = palloc(sizeof(bool) * so->nkeys);

	/*
	 * loop for each heap row. scanGetCandidate returns full row or row's
	 * tuples from first page.
	 */
	while (scanGetCandidate(scan, &pos))
	{
		/*
		 * Check entries in tuple and set up entryRes array.
		 *
		 * If pending tuples belonging to the current heap row are spread
		 * across several pages, collectMatchesForHeapRow will read all of
		 * those pages.
		 */
		if (!collectMatchesForHeapRow(scan, &pos))
			continue;

		/*
		 * Matching of entries of one row is finished, so check row using
		 * consistent functions.
		 */
		oldCtx = MemoryContextSwitchTo(so->tempCtx);
		recheck = false;
		match = true;

		for (i = 0; i < so->nkeys; i++)
		{
			RumScanKey	key = so->keys + i;

			if (!callConsistentFn(&so->rumstate, key))
			{
				match = false;
				break;
			}
			recheck |= key->recheckCurItem;
		}

		MemoryContextSwitchTo(oldCtx);
		MemoryContextReset(so->tempCtx);

		if (match)
		{
			if (tbm)
			{
				tbm_add_tuples(tbm, &pos.item, 1, recheck);
			}
			else
			{
				so->iptr = pos.item;
				insertScanItem(so, recheck);
			}
			ntids++;
		}
	}

	pfree(pos.hasMatchKey);
	return ntids;
}


#define RumIsNewKey(s)		( ((RumScanOpaque) scan->opaque)->keys == NULL )
#define RumIsVoidRes(s)		( ((RumScanOpaque) scan->opaque)->isVoidRes )

int64
rumgetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	RumScanOpaque so = (RumScanOpaque) scan->opaque;
	int64		ntids;
	bool		recheck;

	/*
	 * Set up the scan keys, and check for unsatisfiable query.
	 */
	if (RumIsNewKey(scan))
		rumNewScanKey(scan);

	if (RumIsVoidRes(scan))
		return 0;

	ntids = 0;

	/*
	 * First, scan the pending list and collect any matching entries into the
	 * bitmap.  After we scan a pending item, some other backend could post it
	 * into the main index, and so we might visit it a second time during the
	 * main scan.  This is okay because we'll just re-set the same bit in the
	 * bitmap.  (The possibility of duplicate visits is a major reason why RUM
	 * can't support the amgettuple API, however.) Note that it would not do
	 * to scan the main index before the pending list, since concurrent
	 * cleanup could then make us miss entries entirely.
	 */
	so->tbm = tbm;
	so->entriesIncrIndex = -1;
	ntids = scanPendingInsert(scan);

	/*
	 * Now scan the main index.
	 */
	startScan(scan);

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		if (!scanGetItem(scan, &so->iptr, &so->iptr, &recheck))
			break;

		if (ItemPointerIsLossyPage(&so->iptr))
			tbm_add_page(tbm, ItemPointerGetBlockNumber(&so->iptr));
		else
			tbm_add_tuples(tbm, &so->iptr, 1, recheck);
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
				&rumstate->outerOrderingFn[rumstate->attrnOrderByColumn - 1],
											key->outerAddInfo,
											key->queryValues[0],
											UInt16GetDatum(key->strategy)
											));
	}

	for (i = 0; i < key->nentries; i++)
	{
		entry = key->scanEntry[i];
		if (entry->isFinished == FALSE &&
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
		MemoryContextAlloc(rum_tuplesort_get_memorycontext(so->sortstate),
						   RumSortItemSize(so->norderbys));
	item->iptr = so->iptr;
	item->recheck = recheck;

	if (AttributeNumberIsValid(so->rumstate.attrnAddToColumn))
	{
		int			nOrderByAnother = 0,
					count = 0;

		for (i = 0; i < so->nkeys; i++)
		{
			if (so->keys[i].useAddToColumn)
			{
				so->keys[i].outerAddInfoIsNull = true;
				nOrderByAnother++;
			}
		}

		for (i = 0; count < nOrderByAnother && i < so->nkeys; i++)
		{
			if (so->keys[i].attnum == so->rumstate.attrnAddToColumn &&
				so->keys[i].outerAddInfoIsNull == false)
			{
				Assert(!so->keys[i].orderBy);
				Assert(!so->keys[i].useAddToColumn);

				for (j = i; j < so->nkeys; j++)
				{
					if (so->keys[j].useAddToColumn &&
						so->keys[j].outerAddInfoIsNull == true)
					{
						so->keys[j].outerAddInfoIsNull = false;
						so->keys[j].outerAddInfo = so->keys[i].outerAddInfo;
						count++;
					}
				}
			}
		}
	}

	j = 0;
	for (i = 0; i < so->nkeys; i++)
	{
		if (!so->keys[i].orderBy)
			continue;

		item->data[j] = keyGetOrdering(&so->rumstate, so->tempCtx, &so->keys[i], &so->iptr);

		/*
		 * elog(NOTICE, "%f %u:%u", item->data[j],
		 * ItemPointerGetBlockNumber(&item->iptr),
		 * ItemPointerGetOffsetNumber(&item->iptr));
		 */
		j++;
	}
	rum_tuplesort_putrum(so->sortstate, item);
}

bool
rumgettuple(IndexScanDesc scan, ScanDirection direction)
{
	bool		recheck;
	RumScanOpaque so = (RumScanOpaque) scan->opaque;
	RumSortItem *item;
	bool		should_free;

	if (so->firstCall)
	{
		so->norderbys = scan->numberOfOrderBys;

		/*
		 * Set up the scan keys, and check for unsatisfiable query.
		 */
		if (RumIsNewKey(scan))
			rumNewScanKey(scan);

		if (RumIsVoidRes(scan))
			PG_RETURN_INT64(0);

		so->tbm = NULL;
		so->entriesIncrIndex = -1;
		so->firstCall = false;
		so->sortstate = rum_tuplesort_begin_rum(work_mem, so->norderbys, false);

		scanPendingInsert(scan);

		/*
		 * Now scan the main index.
		 */
		startScan(scan);

		while (scanGetItem(scan, &so->iptr, &so->iptr, &recheck))
		{
			insertScanItem(so, recheck);
		}
		rum_tuplesort_performsort(so->sortstate);
	}

	item = rum_tuplesort_getrum(so->sortstate, true, &should_free);
	while (item)
	{
		uint32		i,
					j = 0;

		if (rumCompareItemPointers(&scan->xs_ctup.t_self, &item->iptr) == 0)
		{
			if (should_free)
				pfree(item);
			item = rum_tuplesort_getrum(so->sortstate, true, &should_free);
			continue;
		}

		scan->xs_ctup.t_self = item->iptr;
		scan->xs_recheck = item->recheck;
		scan->xs_recheckorderby = false;

		for (i = 0; i < so->nkeys; i++)
		{
			if (!so->keys[i].orderBy)
				continue;
			scan->xs_orderbyvals[j] = Float8GetDatum(item->data[j]);
			scan->xs_orderbynulls[j] = false;

			j++;
		}

		if (should_free)
			pfree(item);
		PG_RETURN_BOOL(true);
	}

	PG_RETURN_BOOL(false);
}
