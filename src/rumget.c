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

static bool scanPage(RumState * rumstate, RumScanEntry entry, RumKey *item,
					 bool equalOk);
static void insertScanItem(RumScanOpaque so, bool recheck);
static int	scan_entry_cmp(const void *p1, const void *p2, void *arg);
static void entryGetItem(RumState * rumstate, RumScanEntry entry, bool *nextEntryList);


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

	if (key->addInfoKeys == false && key->willSort == false)
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
	   BlockNumber rootPostingTree, OffsetNumber attnum, RumState * rumstate)
{
	RumPostingTreeScan *gdi;
	Buffer		buffer;
	Page		page;

	Assert(ScanDirectionIsForward(scanEntry->scanDirection));
	/* Descend to the leftmost leaf page */
	gdi = rumPrepareScanPostingTree(index, rootPostingTree, TRUE,
									ForwardScanDirection, attnum, rumstate);

	buffer = rumScanBeginPostingTree(gdi, NULL);
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
				ptr = rumDataPageLeafRead(ptr, attnum, &item, rumstate);
				rum_tuplesort_putrumkey(scanEntry->matchSortstate, &item);
			}

			scanEntry->predictNumberResult += maxoff;
		}

		if (RumPageRightMost(page))
			break;				/* no more pages */

		buffer = rumStep(buffer, index, RUM_SHARE, ForwardScanDirection);
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
				   RumScanEntry scanEntry)
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
	scanEntry->matchSortstate = rum_tuplesort_begin_rumkey(work_mem, cmp);

	/* Null query cannot partial-match anything */
	if (scanEntry->isPartialMatch &&
		scanEntry->queryCategory != RUM_CAT_NORM_KEY)
		return true;

	/* Locate tupdesc entry for key column (for attbyval/attlen data) */
	attnum = scanEntry->attnumOrig;
	attr = rumstate->origTupdesc->attrs[attnum - 1];

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
			scanPostingTree(btree->index, scanEntry, rootPostingTree, attnum, rumstate);

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
			RumKey	item;

			ItemPointerSetMin(&item.iptr);
			for (i = 0; i < RumGetNPosting(itup); i++)
			{
				ptr = rumDataPageLeafRead(ptr, scanEntry->attnum, &item,
										  rumstate);
				rum_tuplesort_putrumkey(scanEntry->matchSortstate, &item);
			}

			scanEntry->predictNumberResult += RumGetNPosting(itup);
		}

		if (scanEntry->forceUseBitmap)
			return true;
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
		res = compareRumKey(rumstate, entry->attnumOrig, &entry->markAddInfo,
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
startScanEntry(RumState * rumstate, RumScanEntry entry)
{
	RumBtreeData btreeEntry;
	RumBtreeStack *stackEntry;
	Page		page;
	bool		needUnlock;

restartScanEntry:
	entry->buffer = InvalidBuffer;
	RumItemSetMin(&entry->curRumKey);
	entry->offset = InvalidOffsetNumber;
	entry->list = NULL;
	entry->gdi = NULL;
	entry->stack = NULL;
	entry->nlist = 0;
	entry->matchSortstate = NULL;
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

	if (entry->isPartialMatch || entry->forceUseBitmap ||
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
			ItemPointerSetMin(&entry->collectRumKey.iptr);
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
			gdi = rumPrepareScanPostingTree(rumstate->index, rootPostingTree, TRUE,
											entry->scanDirection, entry->attnum, rumstate);

			entry->buffer = rumScanBeginPostingTree(gdi, entry->useMarkAddInfo ?
													&entry->markAddInfo : NULL);

			entry->gdi = gdi;
			entry->context = AllocSetContextCreate(CurrentMemoryContext,
												   "RUM entry temporary context",
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
			entry->isFinished = setListPositionScanEntry(rumstate, entry);
			if (!entry->isFinished)
				entry->curRumKey = entry->list[entry->offset];
		}
		else if (RumGetNPosting(itup) > 0)
		{
			entry->nlist = RumGetNPosting(itup);
			entry->predictNumberResult = entry->nlist;
			entry->list = (RumKey *) palloc(sizeof(RumKey) * entry->nlist);

			rumReadTuple(rumstate, entry->attnum, itup, entry->list);
			entry->isFinished = setListPositionScanEntry(rumstate, entry);
			if (!entry->isFinished)
				entry->curRumKey = entry->list[entry->offset];
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

	if (e1->isFinished == TRUE)
	{
		if (e2->isFinished == TRUE)
			return 0;
		else
			return 1;
	}
	if (e2->isFinished)
		return -1;

	if (e1->attnumOrig != e2->attnumOrig)
		return (e1->attnumOrig < e2->attnumOrig) ? 1 : -1;

	res = compareRumKey(rumstate, e1->attnumOrig, &e1->curRumKey,
						&e2->curRumKey);

	return (ScanDirectionIsForward(e1->scanDirection)) ? res : -res;
}

static int
scan_entry_cmp(const void *p1, const void *p2, void *arg)
{
	RumScanEntry e1 = *((RumScanEntry *) p1);
	RumScanEntry e2 = *((RumScanEntry *) p2);

	return -cmpEntries(arg, e1, e2);
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
		startScanKey(rumstate, so->keys[i]);

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

			if (entry->isPartialMatch || entry->forceUseBitmap)
			{
				scanType = RumRegularScan;
				break;
			}
		}
	}

	ItemPointerSetInvalid(&so->key.iptr);

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
				entryGetItem(&so->rumstate, so->sortedEntries[i], NULL);
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
entryGetNextItem(RumState * rumstate, RumScanEntry entry)
{
	Page		page;

	for (;;)
	{
		if (entry->offset >= 0 && entry->offset < entry->nlist)
		{
			entry->curRumKey = entry->list[entry->offset];
			entry->offset += entry->scanDirection;
			return;
		}

		LockBuffer(entry->buffer, RUM_SHARE);
		page = BufferGetPage(entry->buffer);

		if (scanPage(rumstate, entry, &entry->curRumKey, false))
		{
			LockBuffer(entry->buffer, RUM_UNLOCK);
			return;
		}

		for (;;)
		{
			OffsetNumber maxoff,
						i;
			Pointer		ptr;
			RumKey		item;
			bool		searchBorder =
				(ScanDirectionIsForward(entry->scanDirection) &&
				 ItemPointerIsValid(&entry->curRumKey.iptr));
			/*
			 * It's needed to go by right link. During that we should refind
			 * first ItemPointer greater that stored
			 */
			if ((ScanDirectionIsForward(entry->scanDirection) && RumPageRightMost(page)) ||
				(ScanDirectionIsBackward(entry->scanDirection) && RumPageLeftMost(page)))
			{
				UnlockReleaseBuffer(entry->buffer);
				ItemPointerSetInvalid(&entry->curRumKey.iptr);

				entry->buffer = InvalidBuffer;
				entry->isFinished = TRUE;
				entry->gdi->stack->buffer = InvalidBuffer;
				return;
			}

			entry->buffer = rumStep(entry->buffer, rumstate->index,
									RUM_SHARE, entry->scanDirection);
			entry->gdi->stack->buffer = entry->buffer;
			entry->gdi->stack->blkno = BufferGetBlockNumber(entry->buffer);
			page = BufferGetPage(entry->buffer);

			entry->offset = -1;
			maxoff = RumPageGetOpaque(page)->maxoff;
			entry->nlist = maxoff;
			ItemPointerSetMin(&item.iptr);
			ptr = RumDataPageGetData(page);

			for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
			{
				ptr = rumDataPageLeafRead(ptr, entry->attnum, &item, rumstate);
				entry->list[i - FirstOffsetNumber] = item;

				if (searchBorder)
				{
					/* don't search position for backward scan,
					   because of split algorithm */
					int cmp = compareRumKey(rumstate,
											entry->attnumOrig,
											&entry->curRumKey,
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
					ItemPointerIsValid(&entry->curRumKey.iptr))
					/* go on next page */
					break;
				entry->offset = (ScanDirectionIsForward(entry->scanDirection)) ?
							0 : entry->nlist - 1;
			}

			entry->curRumKey = entry->list[entry->offset];
			entry->offset += entry->scanDirection;
			return;
		}
	}
}

static bool
entryGetNextItemList(RumState * rumstate, RumScanEntry entry)
{
	Page		page;
	IndexTuple	itup;
	RumBtreeData btree;
	bool		needUnlock;

	Assert(!entry->isFinished);
	Assert(entry->stack);
	Assert(ScanDirectionIsForward(entry->scanDirection));

	entry->buffer = InvalidBuffer;
	RumItemSetMin(&entry->curRumKey);
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
		ItemPointerSetInvalid(&entry->curRumKey.iptr);
		entry->isFinished = TRUE;
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
		ItemPointerSetInvalid(&entry->curRumKey.iptr);
		entry->isFinished = TRUE;
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
						rootPostingTree, TRUE, entry->scanDirection,
						entry->attnumOrig, rumstate);

		entry->buffer = rumScanBeginPostingTree(gdi, NULL);
		entry->gdi = gdi;
		entry->context = AllocSetContextCreate(CurrentMemoryContext,
											   "RUM entry temporary context",
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
		entry->isFinished = setListPositionScanEntry(rumstate, entry);
	}

	Assert(entry->nlist > 0);

	entry->curRumKey = entry->list[entry->offset];
	entry->offset += entry->scanDirection;

	/*
	 * Done with this entry, go to the next for the future.
	 */
	entry->stack->off++;

	if (needUnlock)
		LockBuffer(entry->stack->buffer, RUM_UNLOCK);

	return true;
}

#define rum_rand() (((double) random()) / ((double) MAX_RANDOM_VALUE))
#define dropItem(e) ( rum_rand() > ((double)RumFuzzySearchLimit)/((double)((e)->predictNumberResult)) )

/*
 * Sets entry->curItem to next heap item pointer for one entry of one scan key,
 * or sets entry->isFinished to TRUE if there are no more.
 *
 * Item pointers must be returned in ascending order.
 */
static void
entryGetItem(RumState * rumstate, RumScanEntry entry, bool *nextEntryList)
{
	Assert(!entry->isFinished);

	if (nextEntryList)
		*nextEntryList = false;

	if (entry->matchSortstate)
	{
		Assert(ScanDirectionIsForward(entry->scanDirection));

		do
		{
			RumKey	collected;
			RumKey	*current_collected;

			/* We are finished, but should return last result */
			if (ItemPointerIsMax(&entry->collectRumKey.iptr))
			{
				entry->isFinished = TRUE;
				rum_tuplesort_end(entry->matchSortstate);
				entry->matchSortstate = NULL;
				break;
			}

			/* collectRumKey could store the begining of current result */
			if (!ItemPointerIsMin(&entry->collectRumKey.iptr))
				collected = entry->collectRumKey;
			else
				ItemPointerSetMin(&collected.iptr);

			ItemPointerSetMin(&entry->curRumKey.iptr);

			for(;;)
			{
				bool	should_free;

				current_collected = rum_tuplesort_getrumkey(
					entry->matchSortstate,
					ScanDirectionIsForward(entry->scanDirection) ? true : false,
					&should_free);

				if (current_collected == NULL)
				{
					entry->curRumKey = collected;
					break;
				}

				if (ItemPointerIsMin(&collected.iptr) ||
					rumCompareItemPointers(&collected.iptr,
										   &current_collected->iptr) == 0)
				{
					Datum	joinedAddInfo = (Datum)0;
					bool	joinedAddInfoIsNull;

					if (ItemPointerIsMin(&collected.iptr))
					{
						joinedAddInfoIsNull = true; /* wiil change later */
						collected.addInfoIsNull = true;
					}
					else
						joinedAddInfoIsNull = collected.addInfoIsNull ||
											current_collected->addInfoIsNull;

					if (joinedAddInfoIsNull)
					{
						joinedAddInfoIsNull =
							(collected.addInfoIsNull && current_collected->addInfoIsNull);

						if (collected.addInfoIsNull == false)
							joinedAddInfo = collected.addInfo;
						else if (current_collected->addInfoIsNull == false)
							joinedAddInfo = current_collected->addInfo;
					}
					else if (rumstate->canJoinAddInfo[entry->attnumOrig - 1])
					{
						joinedAddInfo =
							FunctionCall2(
								&rumstate->joinAddInfoFn[entry->attnumOrig - 1],
								collected.addInfo,
								current_collected->addInfo);
					}
					else
					{
						joinedAddInfo = current_collected->addInfo;
					}

					collected.iptr = current_collected->iptr;
					collected.addInfoIsNull = joinedAddInfoIsNull;
					collected.addInfo = joinedAddInfo;

					if (should_free)
						pfree(current_collected);
				}
				else
				{
					entry->curRumKey = collected;
					entry->collectRumKey = *current_collected;
					if (should_free)
						pfree(current_collected);
					break;
				}
			}

			if (current_collected == NULL)
			{
				/* mark next call as last */
				ItemPointerSetMax(&entry->collectRumKey.iptr);

				/* even current call is last */
				if (ItemPointerIsMin(&entry->curRumKey.iptr))
				{
					entry->isFinished = TRUE;
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
			entry->curRumKey = entry->list[entry->offset];
			entry->offset += entry->scanDirection;
		}
		else if (entry->stack)
		{
			entry->offset++;
			if (entryGetNextItemList(rumstate, entry) && nextEntryList)
				*nextEntryList = true;
		}
		else
		{
			ItemPointerSetInvalid(&entry->curRumKey.iptr);
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
		if (entry->stack && entry->isFinished)
		{
			entry->isFinished = FALSE;
			if (entryGetNextItemList(rumstate, entry) && nextEntryList)
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
 * If all entry streams are exhausted, sets key->isFinished to TRUE.
 *
 * Item pointers must be returned in ascending order.
 */

static int
compareRumKeyScanDirection(RumState *rumstate, AttrNumber attno,
						   ScanDirection scanDirection,
						   RumKey *a, RumKey *b)
{
	int res = compareRumKey(rumstate, attno, a, b);

	return  (ScanDirectionIsForward(scanDirection)) ? res : -res;

}

static int
compareCurRumKeyScanDirection(RumState *rumstate, RumScanEntry entry,
						   RumKey *minItem)
{
	return compareRumKeyScanDirection(rumstate,
						(entry->forceUseBitmap) ?
							InvalidAttrNumber : entry->attnumOrig,
						entry->scanDirection,
						&entry->curRumKey, minItem);
}

static void
keyGetItem(RumState * rumstate, MemoryContext tempCtx, RumScanKey key)
{
	RumKey		 minItem;
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
				compareCurRumKeyScanDirection(rumstate, entry, &minItem) < 0)
			{
				minItem = entry->curRumKey;
				minItemInited = true;
			}
		}
	}

	if (allFinished)
	{
		/* all entries are finished */
		key->isFinished = TRUE;
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
		if (entry->isFinished == FALSE &&
			rumCompareItemPointers(&entry->curRumKey.iptr, &key->curItem.iptr) == 0)
		{
			key->entryRes[i] = TRUE;
			key->addInfo[i] = entry->curRumKey.addInfo;
			key->addInfoIsNull[i] = entry->curRumKey.addInfoIsNull;
		}
		else
		{
			key->entryRes[i] = FALSE;
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
 * Get next heap item pointer (after advancePast) from scan.
 * Returns true if anything found.
 * On success, *item and *recheck are set.
 *
 * Note: this is very nearly the same logic as in keyGetItem(), except
 * that we know the keys are to be combined with AND logic, whereas in
 * keyGetItem() the combination logic is known only to the consistentFn.
 */
static bool
scanGetItemRegular(IndexScanDesc scan, RumKey *advancePast,
				   RumKey *item, bool *recheck)
{
	RumScanOpaque so = (RumScanOpaque) scan->opaque;
	RumState   *rumstate = &so->rumstate;
	RumKey		myAdvancePast = *advancePast;
	uint32		i;
	bool		allFinished;
	bool		match, itemSet;

	for (;;)
	{
		/*
		 * Advance any entries that are <= myAdvancePast according to
		 * scan direction. On first call myAdvancePast is invalid,
		 * so anyway we are needed to call entryGetItem()
		 */
		allFinished = TRUE;

		for (i = 0; i < so->totalentries; i++)
		{
			RumScanEntry entry = so->entries[i];

			while (entry->isFinished == FALSE &&
				   (!ItemPointerIsValid(&myAdvancePast.iptr) ||
				   compareCurRumKeyScanDirection(rumstate, entry,
												 &myAdvancePast) <= 0))
			{
				entryGetItem(rumstate, entry, NULL);

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

			if (key->orderBy)
				continue;

			keyGetItem(&so->rumstate, so->tempCtx, key);

			if (key->isFinished)
				return false;	/* finished one of keys */

			if (itemSet == false)
			{
				*item = key->curItem;
				itemSet = true;
			}
			cmp = compareRumKey(rumstate, key->attnumOrig,
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

			if (key->orderBy)
				continue;

			if (key->curItemMatches)
			{
				if (rumCompareItemPointers(&item->iptr, &key->curItem.iptr) == 0)
					continue;
			}
			match = false;
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
		RumScanKey	key = so->keys[i];

		if (key->orderBy)
			continue;

		if (key->recheckCurItem)
		{
			*recheck = true;
			break;
		}
	}

	return true;
}

/*
 * Finds part of page containing requested item using small index at the end
 * of page.
 */
static bool
scanPage(RumState * rumstate, RumScanEntry entry, RumKey *item,
		 bool equalOk)
{
	int			j;
	RumKey		iter_item;
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
		cmp = compareRumKey(rumstate, entry->attnumOrig,
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
			RumKey	k;

			convertIndexToKey(index, &k);
			cmp = compareRumKey(rumstate, entry->attnumOrig,
								&k, item);
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
		ptr = rumDataPageLeafRead(ptr, entry->attnum, &iter_item, rumstate);
		entry->list[i - first] = iter_item;

		if (bound != -1)
			continue;

		cmp = compareRumKey(rumstate, entry->attnumOrig,
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
	entry->curRumKey = entry->list[entry->offset];
	entry->offset += entry->scanDirection;
	return true;
}

/*
 * Find item of scan entry wich is greater or equal to the given item.
 */

static void
entryFindItem(RumState * rumstate, RumScanEntry entry, RumKey * item)
{
	if (entry->nlist == 0)
	{
		entry->isFinished = TRUE;
		return;
	}

	Assert(!entry->forceUseBitmap);

	/* Try to find in loaded part of page */
	if ((ScanDirectionIsForward(entry->scanDirection) &&
		 compareRumKey(rumstate, entry->attnumOrig,
					   &entry->list[entry->nlist - 1], item) >= 0) ||
		(ScanDirectionIsBackward(entry->scanDirection) &&
		 compareRumKey(rumstate, entry->attnumOrig,
					   &entry->list[0], item) <= 0))
	{
		if (compareRumKeyScanDirection(rumstate, entry->attnumOrig,
							entry->scanDirection,
							&entry->curRumKey, item) >= 0)
			return;
		while (entry->offset >= 0 && entry->offset < entry->nlist)
		{
			if (compareRumKeyScanDirection(rumstate, entry->attnumOrig,
							entry->scanDirection,
							&entry->list[entry->offset],
							item) >= 0)
			{
				entry->curRumKey = entry->list[entry->offset];
				entry->offset += entry->scanDirection;
				return;
			}
			entry->offset += entry->scanDirection;
		}
	}

	if (!BufferIsValid(entry->buffer))
	{
		entry->isFinished = TRUE;
		return;
	}

	/* Check rest of page */
	LockBuffer(entry->buffer, RUM_SHARE);

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
			ItemPointerSetInvalid(&entry->curRumKey.iptr);
			entry->isFinished = TRUE;
			return;
		}

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
					  &so->sortedEntries[i - 1]->curRumKey);
	else if (!so->sortedEntries[minIndex]->isFinished)
		entryGetItem(rumstate, so->sortedEntries[minIndex], NULL);

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
scanGetItemFast(IndexScanDesc scan, RumKey *advancePast,
				RumKey *item, bool *recheck)
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
			RumScanKey	key = so->keys[i];

			if (key->orderBy)
				continue;

			for (j = 0; j < key->nentries; j++)
			{
				RumScanEntry entry = key->scanEntry[j];

				if (entry->isFinished == FALSE &&
					rumCompareItemPointers(&entry->curRumKey.iptr,
				&so->sortedEntries[so->totalentries - 1]->curRumKey.iptr) == 0)
				{
					key->entryRes[j] = TRUE;
					key->addInfo[j] = entry->curRumKey.addInfo;
					key->addInfoIsNull[j] = entry->curRumKey.addInfoIsNull;
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
				for (j = k; j < so->totalentries; j++)
					entryShift(j, so, false);
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

		*item = so->sortedEntries[so->totalentries - 1]->curRumKey;
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
scanGetItemFull(IndexScanDesc scan, RumKey *advancePast,
				RumKey *item, bool *recheck)
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

	entryGetItem(&so->rumstate, entry, &nextEntryList);
	if (entry->isFinished == TRUE)
		return false;

	/* Fill outerAddInfo */
	key->entryRes[0] = TRUE;
	key->addInfo[0] = entry->curRumKey.addInfo;
	key->addInfoIsNull[0] = entry->curRumKey.addInfoIsNull;
	callAddInfoConsistentFn(&so->rumstate, key);

	/* Move related order by entries */
	if (nextEntryList)
		for (i = 1; i < so->totalentries; i++)
		{
			RumScanEntry orderEntry = so->entries[i];
			if (orderEntry->nlist > 0)
			{
				orderEntry->isFinished = FALSE;
				orderEntry->offset = InvalidOffsetNumber;
				RumItemSetMin(&orderEntry->curRumKey);
			}
		}

	for (i = 1; i < so->totalentries; i++)
	{
		RumScanEntry orderEntry = so->entries[i];

		while (orderEntry->isFinished == FALSE &&
			   (!ItemPointerIsValid(&orderEntry->curRumKey.iptr) ||
			   compareCurRumKeyScanDirection(&so->rumstate, orderEntry,
											 &entry->curRumKey) < 0))
			entryGetItem(&so->rumstate, orderEntry, NULL);
	}

	*item = entry->curRumKey;
	*recheck = false;
	return true;
}

/*
 * Get next item whether using regular or fast scan.
 */
static bool
scanGetItem(IndexScanDesc scan, RumKey *advancePast,
			RumKey *item, bool *recheck)
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

	/*
	 * Now scan the main index.
	 */
	startScan(scan);

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		if (!scanGetItem(scan, &so->key, &so->key, &recheck))
			break;

		tbm_add_tuples(tbm, &so->key.iptr, 1, recheck);
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

	for (i = 0; i < key->nentries; i++)
	{
		entry = key->scanEntry[i];
		if (entry->isFinished == FALSE &&
			rumCompareItemPointers(&entry->curRumKey.iptr, iptr) == 0)
		{
			key->addInfo[i] = entry->curRumKey.addInfo;
			key->addInfoIsNull[i] = entry->curRumKey.addInfoIsNull;
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
	item->iptr = so->key.iptr;
	item->recheck = recheck;

	if (AttributeNumberIsValid(so->rumstate.attrnAddToColumn))
	{
		int			nOrderByAnother = 0,
					count = 0;

		for (i = 0; i < so->nkeys; i++)
		{
			if (so->keys[i]->useAddToColumn)
			{
				so->keys[i]->outerAddInfoIsNull = true;
				nOrderByAnother++;
			}
		}

		for (i = 0; count < nOrderByAnother && i < so->nkeys; i++)
		{
			if (so->keys[i]->attnum == so->rumstate.attrnAddToColumn &&
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
						count++;
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
									   &so->key.iptr);

#if 0
		  elog(NOTICE, "%f %u:%u", item->data[j],
		  RumItemPointerGetBlockNumber(&item->iptr),
		  RumItemPointerGetOffsetNumber(&item->iptr));
#endif


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

	if (so->firstCall)
	{
		/*
		 * Set up the scan keys, and check for unsatisfiable query.
		 */
		if (RumIsNewKey(scan))
			rumNewScanKey(scan);

		so->firstCall = false;

		if (RumIsVoidRes(scan))
			return false;

		startScan(scan);
		if (so->naturalOrder == NoMovementScanDirection)
		{
			so->sortstate = rum_tuplesort_begin_rum(work_mem, so->norderbys,
						false,
						so->totalentries > 0 &&
						so->entries[0]->queryCategory == RUM_CAT_EMPTY_QUERY &&
						so->entries[0]->scanWithAddInfo);


			while (scanGetItem(scan, &so->key, &so->key, &recheck))
			{
				insertScanItem(so, recheck);
			}
			rum_tuplesort_performsort(so->sortstate);
		}
	}

	if (so->naturalOrder != NoMovementScanDirection)
	{
		if (scanGetItem(scan, &so->key, &so->key, &recheck))
		{
			scan->xs_ctup.t_self = so->key.iptr;
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
