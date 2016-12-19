/*-------------------------------------------------------------------------
 *
 * rumfast.c
 *	  Fast insert routines for the Postgres inverted index access method.
 *	  Pending entries are stored in linear list of pages.  Later on
 *	  (typically during VACUUM), rumInsertCleanup() will be invoked to
 *	  transfer pending entries into the regular index structure.  This
 *	  wins because bulk insertion is much more efficient than retail.
 *
 * Portions Copyright (c) 2015-2016, Postgres Professional
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/htup_details.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/datum.h"

#include "rum.h"

#define RUM_NDELETE_AT_ONCE 16

#define RUM_PAGE_FREESIZE \
	( BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(RumPageOpaqueData)) )

typedef struct KeyArray
{
	Datum	   *keys;			/* expansible array of keys */
	Datum	   *addInfo;		/* expansible array of additional information */
	bool	   *addInfoIsNull;	/* expansible array of	NULL flag of
								 * additional information */
	RumNullCategory *categories;	/* another expansible array */
	int32		nvalues;		/* current number of valid entries */
	int32		maxvalues;		/* allocated size of arrays */
} KeyArray;


/*
 * Build a pending-list page from the given array of tuples, and write it out.
 *
 * Returns amount of free space left on the page.
 */
static uint32
writeListPage(RumState *rumstate, Buffer buffer,
			  IndexTuple *tuples, uint32 ntuples, BlockNumber rightlink)
{
	Page		page;
	uint32		i,
				freesize;
	OffsetNumber l,
				off;
	GenericXLogState *state;

	state = GenericXLogStart(rumstate->index);

	page = GenericXLogRegisterBuffer(state, buffer, 0);
	RumInitPage(page, RUM_LIST, BufferGetPageSize(buffer));

	off = FirstOffsetNumber;

	for (i = 0; i < ntuples; i++)
	{
		Size		this_size = IndexTupleSize(tuples[i]);

		l = PageAddItem(page, (Item) tuples[i], this_size, off, false, false);

		if (l == InvalidOffsetNumber)
			elog(ERROR, "failed to add item to index page in \"%s\"",
				 RelationGetRelationName(rumstate->index));

		off++;
	}

	RumPageGetOpaque(page)->rightlink = rightlink;

	/*
	 * tail page may contain only whole row(s) or final part of row placed on
	 * previous pages (a "row" here meaning all the index tuples generated for
	 * one heap tuple)
	 */
	if (rightlink == InvalidBlockNumber)
	{
		RumPageSetFullRow(page);
		RumPageGetOpaque(page)->maxoff = 1;
	}
	else
	{
		RumPageGetOpaque(page)->maxoff = 0;
	}

	/* get free space before releasing buffer */
	freesize = PageGetExactFreeSpace(page);
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buffer);

	return freesize;
}

static void
makeSublist(RumState *rumstate, IndexTuple *tuples, uint32 ntuples,
			RumMetaPageData * res)
{
	Buffer		curBuffer = InvalidBuffer;
	Buffer		prevBuffer = InvalidBuffer;
	uint32		i,
				startTuple = 0;
	uint64		size = 0,
				tupsize;

	Assert(ntuples > 0);

	/*
	 * Split tuples into pages
	 */
	for (i = 0; i < ntuples; i++)
	{
		if (curBuffer == InvalidBuffer)
		{
			curBuffer = RumNewBuffer(rumstate->index);

			if (prevBuffer != InvalidBuffer)
			{
				res->nPendingPages++;
				writeListPage(rumstate, prevBuffer,
							  tuples + startTuple,
							  i - startTuple,
							  BufferGetBlockNumber(curBuffer));
			}
			else
			{
				res->head = BufferGetBlockNumber(curBuffer);
			}

			prevBuffer = curBuffer;
			startTuple = i;
			size = 0;
		}

		tupsize = MAXALIGN(IndexTupleSize(tuples[i])) + sizeof(ItemIdData);

		if (size + tupsize > RumListPageSize)
		{
			/* won't fit, force a new page and reprocess */
			i--;
			curBuffer = InvalidBuffer;
		}
		else
		{
			size += tupsize;
		}
	}

	/*
	 * Write last page
	 */
	res->tail = BufferGetBlockNumber(curBuffer);
	res->tailFreeSize = writeListPage(rumstate, curBuffer,
									  tuples + startTuple,
									  ntuples - startTuple,
									  InvalidBlockNumber);
	res->nPendingPages++;
	/* that was only one heap tuple */
	res->nPendingHeapTuples = 1;
}

/*
 * Write the index tuples contained in *collector into the index's
 * pending list.
 *
 * Function guarantees that all these tuples will be inserted consecutively,
 * preserving order
 */
void
rumHeapTupleFastInsert(RumState * rumstate, RumTupleCollector * collector)
{
	Relation	index = rumstate->index;
	Buffer		metabuffer;
	Page		metapage;
	RumMetaPageData *metadata = NULL;
	Buffer		buffer = InvalidBuffer;
	Page		page = NULL;
	bool		separateList = false;
	bool		needCleanup = false;
	GenericXLogState *state;

	if (collector->ntuples == 0)
		return;

	state = GenericXLogStart(rumstate->index);
	metabuffer = ReadBuffer(index, RUM_METAPAGE_BLKNO);

	if (collector->sumsize + collector->ntuples * sizeof(ItemIdData) > RumListPageSize)
	{
		/*
		 * Total size is greater than one page => make sublist
		 */
		separateList = true;
	}
	else
	{
		LockBuffer(metabuffer, RUM_EXCLUSIVE);
		metadata = RumPageGetMeta(BufferGetPage(metabuffer));

		if (metadata->head == InvalidBlockNumber ||
			collector->sumsize + collector->ntuples * sizeof(ItemIdData) > metadata->tailFreeSize)
		{
			/*
			 * Pending list is empty or total size is greater than freespace
			 * on tail page => make sublist
			 *
			 * We unlock metabuffer to keep high concurrency
			 */
			separateList = true;
			LockBuffer(metabuffer, RUM_UNLOCK);
		}
	}

	if (separateList)
	{
		/*
		 * We should make sublist separately and append it to the tail
		 */
		RumMetaPageData sublist;

		memset(&sublist, 0, sizeof(RumMetaPageData));
		makeSublist(rumstate, collector->tuples, collector->ntuples, &sublist);

		/*
		 * metapage was unlocked, see above
		 */
		LockBuffer(metabuffer, RUM_EXCLUSIVE);
		metapage = GenericXLogRegisterBuffer(state, metabuffer, 0);
		metadata = RumPageGetMeta(metapage);

		if (metadata->head == InvalidBlockNumber)
		{
			/*
			 * Main list is empty, so just insert sublist as main list
			 */
			metadata->head = sublist.head;
			metadata->tail = sublist.tail;
			metadata->tailFreeSize = sublist.tailFreeSize;

			metadata->nPendingPages = sublist.nPendingPages;
			metadata->nPendingHeapTuples = sublist.nPendingHeapTuples;
		}
		else
		{
			/*
			 * Merge lists
			 */
			buffer = ReadBuffer(index, metadata->tail);
			LockBuffer(buffer, RUM_EXCLUSIVE);
			page = GenericXLogRegisterBuffer(state, buffer, 0);

			Assert(RumPageGetOpaque(page)->rightlink == InvalidBlockNumber);

			RumPageGetOpaque(page)->rightlink = sublist.head;

			metadata->tail = sublist.tail;
			metadata->tailFreeSize = sublist.tailFreeSize;

			metadata->nPendingPages += sublist.nPendingPages;
			metadata->nPendingHeapTuples += sublist.nPendingHeapTuples;
		}
	}
	else
	{
		/*
		 * Insert into tail page.  Metapage is already locked
		 */
		OffsetNumber l,
					off;
		uint32		i;
		Size		tupsize;

		metapage = GenericXLogRegisterBuffer(state, metabuffer, 0);
		metadata = RumPageGetMeta(metapage);

		buffer = ReadBuffer(index, metadata->tail);
		LockBuffer(buffer, RUM_EXCLUSIVE);
		page = GenericXLogRegisterBuffer(state, buffer, 0);

		off = (PageIsEmpty(page)) ? FirstOffsetNumber :
			OffsetNumberNext(PageGetMaxOffsetNumber(page));

		/*
		 * Increase counter of heap tuples
		 */
		Assert(RumPageGetOpaque(page)->maxoff <= metadata->nPendingHeapTuples);
		RumPageGetOpaque(page)->maxoff++;
		metadata->nPendingHeapTuples++;

		for (i = 0; i < collector->ntuples; i++)
		{
			tupsize = IndexTupleSize(collector->tuples[i]);
			l = PageAddItem(page, (Item) collector->tuples[i], tupsize, off, false, false);

			if (l == InvalidOffsetNumber)
			{
				GenericXLogAbort(state);
				elog(ERROR, "failed to add item to index page in \"%s\"",
					 RelationGetRelationName(index));
			}

			off++;
		}

		metadata->tailFreeSize = PageGetExactFreeSpace(page);
	}

	/*
	 * Force pending list cleanup when it becomes too long. And,
	 * rumInsertCleanup could take significant amount of time, so we prefer to
	 * call it when it can do all the work in a single collection cycle. In
	 * non-vacuum mode, it shouldn't require maintenance_work_mem, so fire it
	 * while pending list is still small enough to fit into work_mem.
	 *
	 * rumInsertCleanup() should not be called inside our CRIT_SECTION.
	 */
	if (metadata->nPendingPages * RUM_PAGE_FREESIZE > work_mem * 1024L)
		needCleanup = true;

	GenericXLogFinish(state);

	if (buffer != InvalidBuffer)
		UnlockReleaseBuffer(buffer);

	UnlockReleaseBuffer(metabuffer);

	if (needCleanup)
		rumInsertCleanup(rumstate, false, NULL);
}

static IndexTuple
RumFastFormTuple(RumState * rumstate,
				 OffsetNumber attnum, Datum key, RumNullCategory category,
				 Datum addInfo,
				 bool addInfoIsNull)
{
	Datum		datums[3];
	bool		isnull[3];
	IndexTuple	itup;
	Size		newsize;

	/* Build the basic tuple: optional column number, plus key datum */

	if (rumstate->oneCol)
	{
		datums[0] = key;
		isnull[0] = (category != RUM_CAT_NORM_KEY);
		datums[1] = addInfo;
		isnull[1] = addInfoIsNull;
	}
	else
	{
		datums[0] = UInt16GetDatum(attnum);
		isnull[0] = false;
		datums[1] = key;
		isnull[1] = (category != RUM_CAT_NORM_KEY);
		datums[2] = addInfo;
		isnull[2] = addInfoIsNull;
	}

	itup = index_form_tuple(rumstate->tupdesc[attnum - 1], datums, isnull);

	/*
	 * Place category to the last byte of index tuple extending it's size if
	 * needed
	 */
	newsize = IndexTupleSize(itup);

	if (category != RUM_CAT_NORM_KEY)
	{
		Size		minsize;

		Assert(IndexTupleHasNulls(itup));
		minsize = IndexInfoFindDataOffset(itup->t_info) +
			heap_compute_data_size(rumstate->tupdesc[attnum - 1], datums, isnull) +
			sizeof(RumNullCategory);
		newsize = Max(newsize, minsize);
	}

	newsize = MAXALIGN(newsize);

	if (newsize > RumMaxItemSize)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			errmsg("index row size %lu exceeds maximum %lu for index \"%s\"",
				   (unsigned long) newsize,
				   (unsigned long) RumMaxItemSize,
				   RelationGetRelationName(rumstate->index))));
		pfree(itup);
		return NULL;
	}

	/*
	 * Resize tuple if needed
	 */
	if (newsize != IndexTupleSize(itup))
	{
		itup = repalloc(itup, newsize);

		/* set new size in tuple header */
		itup->t_info &= ~INDEX_SIZE_MASK;
		itup->t_info |= newsize;
	}

	/*
	 * Insert category byte, if needed
	 */
	if (category != RUM_CAT_NORM_KEY)
	{
		Assert(IndexTupleHasNulls(itup));
		RumSetNullCategory(itup, category);
	}

	return itup;
}


/*
 * Create temporary index tuples for a single indexable item (one index column
 * for the heap tuple specified by ht_ctid), and append them to the array
 * in *collector.  They will subsequently be written out using
 * rumHeapTupleFastInsert.  Note that to guarantee consistent state, all
 * temp tuples for a given heap tuple must be written in one call to
 * rumHeapTupleFastInsert.
 */
void
rumHeapTupleFastCollect(RumState * rumstate,
						RumTupleCollector * collector,
						OffsetNumber attnum, Datum value, bool isNull,
						ItemPointer ht_ctid)
{
	Datum	   *entries;
	RumNullCategory *categories;
	int32		i,
				nentries;
	Datum	   *addInfo;
	bool	   *addInfoIsNull;

	/*
	 * Extract the key values that need to be inserted in the index
	 */
	entries = rumExtractEntries(rumstate, attnum, value, isNull,
						   &nentries, &categories, &addInfo, &addInfoIsNull);

	/*
	 * Allocate/reallocate memory for storing collected tuples
	 */
	if (collector->tuples == NULL)
	{
		collector->lentuples = nentries * rumstate->origTupdesc->natts;
		collector->tuples = (IndexTuple *) palloc(sizeof(IndexTuple) * collector->lentuples);
	}

	while (collector->ntuples + nentries > collector->lentuples)
	{
		collector->lentuples *= 2;
		collector->tuples = (IndexTuple *) repalloc(collector->tuples,
								  sizeof(IndexTuple) * collector->lentuples);
	}

	/*
	 * Build an index tuple for each key value, and add to array.  In pending
	 * tuples we just stick the heap TID into t_tid.
	 */
	for (i = 0; i < nentries; i++)
	{
		IndexTuple	itup;

		itup = RumFastFormTuple(rumstate, attnum, entries[i], categories[i], addInfo[i], addInfoIsNull[i]);
		itup->t_tid = *ht_ctid;
		collector->tuples[collector->ntuples++] = itup;
		collector->sumsize += IndexTupleSize(itup);
	}
}

/*
 * Deletes pending list pages up to (not including) newHead page.
 * If newHead == InvalidBlockNumber then function drops the whole list.
 *
 * metapage is pinned and exclusive-locked throughout this function.
 *
 * Returns true if another cleanup process is running concurrently
 * (if so, we can just abandon our own efforts)
 */
static bool
shiftList(RumState *rumstate, Buffer metabuffer, BlockNumber newHead,
		  IndexBulkDeleteResult *stats)
{
	Page		metapage;
	RumMetaPageData *metadata;
	BlockNumber blknoToDelete;
	GenericXLogState *metastate;

	metastate = GenericXLogStart(rumstate->index);
	metapage = GenericXLogRegisterBuffer(metastate, metabuffer,
										 GENERIC_XLOG_FULL_IMAGE);
	metadata = RumPageGetMeta(metapage);
	blknoToDelete = metadata->head;

	do
	{
		Page		page;
		int64		nDeletedHeapTuples = 0;
		uint32		i,
					nDeleted = 0;
		Buffer		buffers[RUM_NDELETE_AT_ONCE];
		GenericXLogState *state;

		while (nDeleted < RUM_NDELETE_AT_ONCE && blknoToDelete != newHead)
		{
			buffers[nDeleted] = ReadBuffer(rumstate->index, blknoToDelete);
			LockBuffer(buffers[nDeleted], RUM_EXCLUSIVE);

			page = BufferGetPage(buffers[nDeleted]);

			nDeleted++;

			if (RumPageIsDeleted(page))
			{
				GenericXLogAbort(metastate);
				/* concurrent cleanup process is detected */
				for (i = 0; i < nDeleted; i++)
					UnlockReleaseBuffer(buffers[i]);

				return true;
			}

			nDeletedHeapTuples += RumPageGetOpaque(page)->maxoff;
			blknoToDelete = RumPageGetOpaque(page)->rightlink;
		}

		if (stats)
			stats->pages_deleted += nDeleted;

		metadata->head = blknoToDelete;

		Assert(metadata->nPendingPages >= nDeleted);
		metadata->nPendingPages -= nDeleted;
		Assert(metadata->nPendingHeapTuples >= nDeletedHeapTuples);
		metadata->nPendingHeapTuples -= nDeletedHeapTuples;

		if (blknoToDelete == InvalidBlockNumber)
		{
			metadata->tail = InvalidBlockNumber;
			metadata->tailFreeSize = 0;
			metadata->nPendingPages = 0;
			metadata->nPendingHeapTuples = 0;
		}

//		MarkBufferDirty(metabuffer);

		for (i = 0; i < nDeleted; i++)
		{
			state = GenericXLogStart(rumstate->index);
			page = GenericXLogRegisterBuffer(state, buffers[i], 0);

			RumPageGetOpaque(page)->flags = RUM_DELETED;
			GenericXLogFinish(state);
		}

		for (i = 0; i < nDeleted; i++)
			UnlockReleaseBuffer(buffers[i]);
	} while (blknoToDelete != newHead);

	GenericXLogFinish(metastate);

	return false;
}

/* Initialize empty KeyArray */
static void
initKeyArray(KeyArray *keys, int32 maxvalues)
{
	keys->keys = (Datum *) palloc(sizeof(Datum) * maxvalues);
	keys->addInfo = (Datum *) palloc(sizeof(Datum) * maxvalues);
	keys->addInfoIsNull = (bool *) palloc(sizeof(bool) * maxvalues);
	keys->categories = (RumNullCategory *)
		palloc(sizeof(RumNullCategory) * maxvalues);
	keys->nvalues = 0;
	keys->maxvalues = maxvalues;
}

/* Add datum to KeyArray, resizing if needed */
static void
addDatum(KeyArray *keys, Datum datum, Datum addInfo, bool addInfoIsNull, RumNullCategory category)
{
	if (keys->nvalues >= keys->maxvalues)
	{
		keys->maxvalues *= 2;
		keys->keys = (Datum *)
			repalloc(keys->keys, sizeof(Datum) * keys->maxvalues);
		keys->addInfo = (Datum *)
			repalloc(keys->addInfo, sizeof(Datum) * keys->maxvalues);
		keys->addInfoIsNull = (bool *)
			repalloc(keys->addInfoIsNull, sizeof(bool) * keys->maxvalues);
		keys->categories = (RumNullCategory *)
			repalloc(keys->categories, sizeof(RumNullCategory) * keys->maxvalues);
	}

	keys->keys[keys->nvalues] = datum;
	keys->categories[keys->nvalues] = category;
	keys->addInfo[keys->nvalues] = addInfo;
	keys->addInfoIsNull[keys->nvalues] = addInfoIsNull;
	keys->nvalues++;
}

/*
 * Collect data from a pending-list page in preparation for insertion into
 * the main index.
 *
 * Go through all tuples >= startoff on page and collect values in accum
 *
 * Note that ka is just workspace --- it does not carry any state across
 * calls.
 */
static void
processPendingPage(BuildAccumulator *accum, KeyArray *ka,
				   Page page, OffsetNumber startoff)
{
	ItemPointerData heapptr;
	OffsetNumber i,
				maxoff;
	OffsetNumber attrnum;

	/* reset *ka to empty */
	ka->nvalues = 0;

	maxoff = PageGetMaxOffsetNumber(page);
	Assert(maxoff >= FirstOffsetNumber);
	ItemPointerSetInvalid(&heapptr);
	attrnum = 0;

	for (i = startoff; i <= maxoff; i = OffsetNumberNext(i))
	{
		IndexTuple	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, i));
		OffsetNumber curattnum;
		Datum		curkey,
					addInfo = 0;
		bool		addInfoIsNull = true;
		RumNullCategory curcategory;

		/* Check for change of heap TID or attnum */
		curattnum = rumtuple_get_attrnum(accum->rumstate, itup);

		if (OidIsValid(accum->rumstate->addInfoTypeOid[curattnum - 1]))
		{
			Form_pg_attribute attr = accum->rumstate->addAttrs[curattnum - 1];
			Assert(attr);

			if (accum->rumstate->oneCol)
				addInfo = index_getattr(itup, 2,
					accum->rumstate->tupdesc[curattnum - 1], &addInfoIsNull);
			else
				addInfo = index_getattr(itup, 3,
					accum->rumstate->tupdesc[curattnum - 1], &addInfoIsNull);
			addInfo = datumCopy(addInfo, attr->attbyval, attr->attlen);
		}

		if (!ItemPointerIsValid(&heapptr))
		{
			heapptr = itup->t_tid;
			attrnum = curattnum;
		}
		else if (!(ItemPointerEquals(&heapptr, &itup->t_tid) &&
				   curattnum == attrnum))
		{
			/*
			 * rumInsertBAEntries can insert several datums per call, but only
			 * for one heap tuple and one column.  So call it at a boundary,
			 * and reset ka.
			 */
			rumInsertBAEntries(accum, &heapptr, attrnum,
							   ka->keys, ka->addInfo, ka->addInfoIsNull, ka->categories, ka->nvalues);
			ka->nvalues = 0;
			heapptr = itup->t_tid;
			attrnum = curattnum;
		}

		/* Add key to KeyArray */
		curkey = rumtuple_get_key(accum->rumstate, itup, &curcategory);
		addDatum(ka, curkey, addInfo, addInfoIsNull, curcategory);
	}

	/* Dump out all remaining keys */
	rumInsertBAEntries(accum, &heapptr, attrnum,
	  ka->keys, ka->addInfo, ka->addInfoIsNull, ka->categories, ka->nvalues);
}

/*
 * Move tuples from pending pages into regular RUM structure.
 *
 * This can be called concurrently by multiple backends, so it must cope.
 * On first glance it looks completely not concurrent-safe and not crash-safe
 * either.  The reason it's okay is that multiple insertion of the same entry
 * is detected and treated as a no-op by ruminsert.c.  If we crash after
 * posting entries to the main index and before removing them from the
 * pending list, it's okay because when we redo the posting later on, nothing
 * bad will happen.  Likewise, if two backends simultaneously try to post
 * a pending entry into the main index, one will succeed and one will do
 * nothing.  We try to notice when someone else is a little bit ahead of
 * us in the process, but that's just to avoid wasting cycles.  Only the
 * action of removing a page from the pending list really needs exclusive
 * lock.
 *
 * vac_delay indicates that rumInsertCleanup is called from vacuum process,
 * so call vacuum_delay_point() periodically.
 * If stats isn't null, we count deleted pending pages into the counts.
 */
void
rumInsertCleanup(RumState * rumstate,
				 bool vac_delay, IndexBulkDeleteResult *stats)
{
	Relation	index = rumstate->index;
	Buffer		metabuffer,
				buffer;
	Page		metapage,
				page;
	RumMetaPageData *metadata;
	MemoryContext opCtx,
				oldCtx;
	BuildAccumulator accum;
	KeyArray	datums;
	BlockNumber blkno;

	metabuffer = ReadBuffer(index, RUM_METAPAGE_BLKNO);
	LockBuffer(metabuffer, RUM_SHARE);

	metapage = BufferGetPage(metabuffer);
	metadata = RumPageGetMeta(metapage);

	if (metadata->head == InvalidBlockNumber)
	{
		/* Nothing to do */
		UnlockReleaseBuffer(metabuffer);
		return;
	}

	/*
	 * Read and lock head of pending list
	 */
	blkno = metadata->head;
	buffer = ReadBuffer(index, blkno);
	LockBuffer(buffer, RUM_SHARE);
	page = BufferGetPage(buffer);

	LockBuffer(metabuffer, RUM_UNLOCK);

	/*
	 * Initialize.  All temporary space will be in opCtx
	 */
	opCtx = AllocSetContextCreate(CurrentMemoryContext,
								  "RUM insert cleanup temporary context",
								  ALLOCSET_DEFAULT_MINSIZE,
								  ALLOCSET_DEFAULT_INITSIZE,
								  ALLOCSET_DEFAULT_MAXSIZE);

	oldCtx = MemoryContextSwitchTo(opCtx);

	initKeyArray(&datums, 128);
	rumInitBA(&accum);
	accum.rumstate = rumstate;

	/*
	 * At the top of this loop, we have pin and lock on the current page of
	 * the pending list.  However, we'll release that before exiting the loop.
	 * Note we also have pin but not lock on the metapage.
	 */
	for (;;)
	{
		if (RumPageIsDeleted(page))
		{
			/* another cleanup process is running concurrently */
			UnlockReleaseBuffer(buffer);
			break;
		}

		/*
		 * read page's datums into accum
		 */
		processPendingPage(&accum, &datums, page, FirstOffsetNumber);

		vacuum_delay_point();

		/*
		 * Is it time to flush memory to disk?	Flush if we are at the end of
		 * the pending list, or if we have a full row and memory is getting
		 * full.
		 *
		 * XXX using up maintenance_work_mem here is probably unreasonably
		 * much, since vacuum might already be using that much.
		 */
		if (RumPageGetOpaque(page)->rightlink == InvalidBlockNumber ||
			(RumPageHasFullRow(page) &&
			 (accum.allocatedMemory >= maintenance_work_mem * 1024L)))
		{
			RumKey	   *items;
			uint32		nlist;
			Datum		key;
			RumNullCategory category;
			OffsetNumber maxoff,
						attnum;

			/*
			 * Unlock current page to increase performance. Changes of page
			 * will be checked later by comparing maxoff after completion of
			 * memory flush.
			 */
			maxoff = PageGetMaxOffsetNumber(page);
			LockBuffer(buffer, RUM_UNLOCK);

			/*
			 * Moving collected data into regular structure can take
			 * significant amount of time - so, run it without locking pending
			 * list.
			 */
			rumBeginBAScan(&accum);
			while ((items = rumGetBAEntry(&accum,
								  &attnum, &key, &category, &nlist)) != NULL)
			{
				rumEntryInsert(rumstate, attnum, key, category,
							   items, nlist, NULL);
				vacuum_delay_point();
			}

			/*
			 * Lock the whole list to remove pages
			 */
			LockBuffer(metabuffer, RUM_EXCLUSIVE);
			LockBuffer(buffer, RUM_SHARE);

			if (RumPageIsDeleted(page))
			{
				/* another cleanup process is running concurrently */
				UnlockReleaseBuffer(buffer);
				LockBuffer(metabuffer, RUM_UNLOCK);
				break;
			}

			/*
			 * While we left the page unlocked, more stuff might have gotten
			 * added to it.  If so, process those entries immediately.  There
			 * shouldn't be very many, so we don't worry about the fact that
			 * we're doing this with exclusive lock. Insertion algorithm
			 * guarantees that inserted row(s) will not continue on next page.
			 * NOTE: intentionally no vacuum_delay_point in this loop.
			 */
			if (PageGetMaxOffsetNumber(page) != maxoff)
			{
				rumInitBA(&accum);
				processPendingPage(&accum, &datums, page, maxoff + 1);

				rumBeginBAScan(&accum);
				while ((items = rumGetBAEntry(&accum,
								  &attnum, &key, &category, &nlist)) != NULL)
				{
					rumEntryInsert(rumstate, attnum, key, category,
								   items, nlist, NULL);
				}
			}

			/*
			 * Remember next page - it will become the new list head
			 */
			blkno = RumPageGetOpaque(page)->rightlink;
			UnlockReleaseBuffer(buffer);		/* shiftList will do exclusive
												 * locking */

			/*
			 * remove read pages from pending list, at this point all content
			 * of read pages is in regular structure
			 */
			if (shiftList(rumstate, metabuffer, blkno, stats))
			{
				/* another cleanup process is running concurrently */
				LockBuffer(metabuffer, RUM_UNLOCK);
				break;
			}

			Assert(blkno == metadata->head);
			LockBuffer(metabuffer, RUM_UNLOCK);

			/*
			 * if we removed the whole pending list just exit
			 */
			if (blkno == InvalidBlockNumber)
				break;

			/*
			 * release memory used so far and reinit state
			 */
			MemoryContextReset(opCtx);
			initKeyArray(&datums, datums.maxvalues);
			rumInitBA(&accum);
		}
		else
		{
			blkno = RumPageGetOpaque(page)->rightlink;
			UnlockReleaseBuffer(buffer);
		}

		/*
		 * Read next page in pending list
		 */
		vacuum_delay_point();
		buffer = ReadBuffer(index, blkno);
		LockBuffer(buffer, RUM_SHARE);
		page = BufferGetPage(buffer);
	}

	ReleaseBuffer(metabuffer);

	/* Clean up temporary space */
	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(opCtx);
}
