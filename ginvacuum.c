/*-------------------------------------------------------------------------
 *
 * ginvacuum.c
 *	  delete & vacuum routines for the postgres GIN
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/gin/ginvacuum.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "commands/vacuum.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"

#include "rum.h"

typedef struct
{
	Relation	index;
	IndexBulkDeleteResult *result;
	IndexBulkDeleteCallback callback;
	void	   *callback_state;
	GinState	ginstate;
	BufferAccessStrategy strategy;
} GinVacuumState;


/*
 * Cleans array of ItemPointer (removes dead pointers)
 * Results are always stored in *cleaned, which will be allocated
 * if it's needed. In case of *cleaned!=NULL caller is responsible to
 * have allocated enough space. *cleaned and items may point to the same
 * memory address.
 */

static uint32
ginVacuumPostingList(GinVacuumState *gvs, OffsetNumber attnum, Pointer src, uint32 nitem, Pointer *cleaned, Size size, Size *newSize)
{
	uint32		i,
				j = 0;
	ItemPointerData iptr = {{0,0},0}, prevIptr;
	Datum		addInfo = 0;
	bool		addInfoIsNull;
	Pointer		dst = NULL, prev, ptr = src;

	/*
	 * just scan over ItemPointer array
	 */

	prevIptr = iptr;
	for (i = 0; i < nitem; i++)
	{
		prev = ptr;
		ptr = ginDataPageLeafRead(ptr, attnum, &iptr, &addInfo, &addInfoIsNull, &gvs->ginstate);
		if (gvs->callback(&iptr, gvs->callback_state))
		{
			gvs->result->tuples_removed += 1;
			if (!dst)
			{
				dst = (Pointer) palloc(size);
				*cleaned = dst;
				if (i != 0)
				{
					memcpy(dst, src, prev - src);
					dst += prev - src;
				}
			}
		}
		else
		{
			gvs->result->num_index_tuples += 1;
			if (i != j)
				dst = ginPlaceToDataPageLeaf(dst, attnum, &iptr, addInfo, addInfoIsNull, &prevIptr, &gvs->ginstate);
			j++;
			prevIptr = iptr;
		}
	}

	if (i != j)
		*newSize = dst - *cleaned;
	return j;
}

/*
 * Form a tuple for entry tree based on already encoded array of item pointers
 * with additional information.
 */
static IndexTuple
GinFormTuple(GinState *ginstate,
			 OffsetNumber attnum, Datum key, GinNullCategory category,
			 Pointer data,
			 Size dataSize,
			 uint32 nipd,
			 bool errorTooBig)
{
	Datum		datums[3];
	bool		isnull[3];
	IndexTuple	itup;
	uint32		newsize;

	/* Build the basic tuple: optional column number, plus key datum */
	if (ginstate->oneCol)
	{
		datums[0] = key;
		isnull[0] = (category != GIN_CAT_NORM_KEY);
		isnull[1] = true;
	}
	else
	{
		datums[0] = UInt16GetDatum(attnum);
		isnull[0] = false;
		datums[1] = key;
		isnull[1] = (category != GIN_CAT_NORM_KEY);
		isnull[2] = true;
	}

	itup = index_form_tuple(ginstate->tupdesc[attnum - 1], datums, isnull);

	/*
	 * Determine and store offset to the posting list, making sure there is
	 * room for the category byte if needed.
	 *
	 * Note: because index_form_tuple MAXALIGNs the tuple size, there may well
	 * be some wasted pad space.  Is it worth recomputing the data length to
	 * prevent that?  That would also allow us to Assert that the real data
	 * doesn't overlap the GinNullCategory byte, which this code currently
	 * takes on faith.
	 */
	newsize = IndexTupleSize(itup);

	GinSetPostingOffset(itup, newsize);

	GinSetNPosting(itup, nipd);

	/*
	 * Add space needed for posting list, if any.  Then check that the tuple
	 * won't be too big to store.
	 */

	if (nipd > 0)
	{
		newsize += dataSize;
	}

	if (category != GIN_CAT_NORM_KEY)
	{
		Assert(IndexTupleHasNulls(itup));
		newsize = newsize + sizeof(GinNullCategory);
	}
	newsize = MAXALIGN(newsize);

	if (newsize > Min(INDEX_SIZE_MASK, GinMaxItemSize))
	{
		if (errorTooBig)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			errmsg("index row size %lu exceeds maximum %lu for index \"%s\"",
				   (unsigned long) newsize,
				   (unsigned long) Min(INDEX_SIZE_MASK,
									   GinMaxItemSize),
				   RelationGetRelationName(ginstate->index))));
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
	 * Copy in the posting list, if provided
	 */
	if (nipd > 0)
	{
		char *ptr = GinGetPosting(itup);
		memcpy(ptr, data, dataSize);
	}

	/*
	 * Insert category byte, if needed
	 */
	if (category != GIN_CAT_NORM_KEY)
	{
		Assert(IndexTupleHasNulls(itup));
		GinSetNullCategory(itup, ginstate, category);
	}
	return itup;
}

/*
 * fills WAL record for vacuum leaf page
 */
static void
xlogVacuumPage(Relation index, Buffer buffer, OffsetNumber attrnum, GinState *ginstate)
{
	Page		page = BufferGetPage(buffer);
	XLogRecPtr	recptr;
	XLogRecData rdata[3];
	ginxlogVacuumPage data;
	char	   *backup;
	char		itups[BLCKSZ];
	uint32		len = 0;

	Assert(GinPageIsLeaf(page));

	if (!RelationNeedsWAL(index))
		return;

	data.node = index->rd_node;
	data.blkno = BufferGetBlockNumber(buffer);

	if (GinPageIsData(page))
	{
		Form_pg_attribute attr = ginstate->addAttrs[attrnum - 1];

		memcpy(itups, page, BLCKSZ);
		backup = GinDataPageGetData(itups);
		data.nitem = GinPageGetOpaque(itups)->maxoff;
		if (attr)
		{
			data.typlen = attr->attlen;
			data.typalign = attr->attalign;
			data.typbyval = attr->attbyval;
			data.typstorage = attr->attstorage;
		}
		if (data.nitem)
			len = MAXALIGN(GinDataPageSize - GinPageGetOpaque(itups)->freespace);
	}
	else
	{
		char	   *ptr;
		OffsetNumber i;

		ptr = backup = itups;
		for (i = FirstOffsetNumber; i <= PageGetMaxOffsetNumber(page); i++)
		{
			IndexTuple	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, i));

			memcpy(ptr, itup, IndexTupleSize(itup));
			ptr += MAXALIGN(IndexTupleSize(itup));
		}

		data.nitem = PageGetMaxOffsetNumber(page);
		len = ptr - backup;
	}

	rdata[0].buffer = buffer;
	rdata[0].buffer_std = (GinPageIsData(page)) ? FALSE : TRUE;
	rdata[0].len = 0;
	rdata[0].data = NULL;
	rdata[0].next = rdata + 1;

	rdata[1].buffer = InvalidBuffer;
	rdata[1].len = sizeof(ginxlogVacuumPage);
	rdata[1].data = (char *) &data;

	if (len == 0)
	{
		rdata[1].next = NULL;
	}
	else
	{
		rdata[1].next = rdata + 2;

		rdata[2].buffer = InvalidBuffer;
		rdata[2].len = len;
		rdata[2].data = backup;
		rdata[2].next = NULL;
	}

	recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_VACUUM_PAGE, rdata);
	PageSetLSN(page, recptr);
}

static bool
ginVacuumPostingTreeLeaves(GinVacuumState *gvs, OffsetNumber attnum, BlockNumber blkno, bool isRoot, Buffer *rootBuffer)
{
	Buffer		buffer;
	Page		page;
	bool		hasVoidPage = FALSE;

	buffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, blkno,
								RBM_NORMAL, gvs->strategy);
	page = BufferGetPage(buffer);

	/*
	 * We should be sure that we don't concurrent with inserts, insert process
	 * never release root page until end (but it can unlock it and lock
	 * again). New scan can't start but previously started ones work
	 * concurrently.
	 */

	if (isRoot)
		LockBufferForCleanup(buffer);
	else
		LockBuffer(buffer, GIN_EXCLUSIVE);

	Assert(GinPageIsData(page));

	if (GinPageIsLeaf(page))
	{
		OffsetNumber newMaxOff,
					oldMaxOff = GinPageGetOpaque(page)->maxoff;
		Pointer cleaned = NULL;
		Size newSize;

		newMaxOff = ginVacuumPostingList(gvs, attnum,
				GinDataPageGetData(page), oldMaxOff, &cleaned,
				GinDataPageSize - GinPageGetOpaque(page)->freespace, &newSize);

		/* saves changes about deleted tuple ... */
		if (oldMaxOff != newMaxOff)
		{
			START_CRIT_SECTION();

			if (newMaxOff > 0)
				memcpy(GinDataPageGetData(page), cleaned, newSize);

			pfree(cleaned);
			GinPageGetOpaque(page)->maxoff = newMaxOff;
			updateItemIndexes(page, attnum, &gvs->ginstate);

			MarkBufferDirty(buffer);
			xlogVacuumPage(gvs->index, buffer, attnum, &gvs->ginstate);

			END_CRIT_SECTION();

			/* if root is a leaf page, we don't desire further processing */
			if (!isRoot && GinPageGetOpaque(page)->maxoff < FirstOffsetNumber)
				hasVoidPage = TRUE;
		}
	}
	else
	{
		OffsetNumber i;
		bool		isChildHasVoid = FALSE;

		for (i = FirstOffsetNumber; i <= GinPageGetOpaque(page)->maxoff; i++)
		{
			PostingItem *pitem = (PostingItem *) GinDataPageGetItem(page, i);

			if (ginVacuumPostingTreeLeaves(gvs, attnum, PostingItemGetBlockNumber(pitem), FALSE, NULL))
				isChildHasVoid = TRUE;
		}

		if (isChildHasVoid)
			hasVoidPage = TRUE;
	}

	/*
	 * if we have root and theres void pages in tree, then we don't release
	 * lock to go further processing and guarantee that tree is unused
	 */
	if (!(isRoot && hasVoidPage))
	{
		UnlockReleaseBuffer(buffer);
	}
	else
	{
		Assert(rootBuffer);
		*rootBuffer = buffer;
	}

	return hasVoidPage;
}

/*
 * Delete a posting tree page.
 */
static void
ginDeletePage(GinVacuumState *gvs, BlockNumber deleteBlkno, BlockNumber leftBlkno,
			  BlockNumber parentBlkno, OffsetNumber myoff, bool isParentRoot)
{
	Buffer		dBuffer;
	Buffer		lBuffer;
	Buffer		pBuffer;
	Page		page,
				parentPage;
	BlockNumber	rightlink;

	/*
	 * Lock the pages in the same order as an insertion would, to avoid
	 * deadlocks: left, then right, then parent.
	 */
	lBuffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, leftBlkno,
								 RBM_NORMAL, gvs->strategy);
	dBuffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, deleteBlkno,
								 RBM_NORMAL, gvs->strategy);
	pBuffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, parentBlkno,
								 RBM_NORMAL, gvs->strategy);

	LockBuffer(lBuffer, GIN_EXCLUSIVE);
	LockBuffer(dBuffer, GIN_EXCLUSIVE);
	if (!isParentRoot)			/* parent is already locked by
								 * LockBufferForCleanup() */
		LockBuffer(pBuffer, GIN_EXCLUSIVE);

	START_CRIT_SECTION();

	/* Unlink the page by changing left sibling's rightlink */
	page = BufferGetPage(dBuffer);
	rightlink = GinPageGetOpaque(page)->rightlink;

	page = BufferGetPage(lBuffer);
	GinPageGetOpaque(page)->rightlink = rightlink;

	/* Delete downlink from parent */
	parentPage = BufferGetPage(pBuffer);
#ifdef USE_ASSERT_CHECKING
	do
	{
		PostingItem *tod = (PostingItem *) GinDataPageGetItem(parentPage, myoff);

		Assert(PostingItemGetBlockNumber(tod) == deleteBlkno);
	} while (0);
#endif
	GinPageDeletePostingItem(parentPage, myoff);

	page = BufferGetPage(dBuffer);

	/*
	 * we shouldn't change rightlink field to save workability of running
	 * search scan
	 */
	GinPageGetOpaque(page)->flags = GIN_DELETED;

	MarkBufferDirty(pBuffer);
	if (leftBlkno != InvalidBlockNumber)
		MarkBufferDirty(lBuffer);
	MarkBufferDirty(dBuffer);

	if (RelationNeedsWAL(gvs->index))
	{
		XLogRecPtr	recptr;
		XLogRecData rdata[4];
		ginxlogDeletePage data;
		int			n;

		data.node = gvs->index->rd_node;
		data.blkno = deleteBlkno;
		data.parentBlkno = parentBlkno;
		data.parentOffset = myoff;
		data.leftBlkno = leftBlkno;
		data.rightLink = GinPageGetOpaque(page)->rightlink;

		rdata[0].buffer = dBuffer;
		rdata[0].buffer_std = FALSE;
		rdata[0].data = NULL;
		rdata[0].len = 0;
		rdata[0].next = rdata + 1;

		rdata[1].buffer = pBuffer;
		rdata[1].buffer_std = FALSE;
		rdata[1].data = NULL;
		rdata[1].len = 0;
		rdata[1].next = rdata + 2;

		if (leftBlkno != InvalidBlockNumber)
		{
			rdata[2].buffer = lBuffer;
			rdata[2].buffer_std = FALSE;
			rdata[2].data = NULL;
			rdata[2].len = 0;
			rdata[2].next = rdata + 3;
			n = 3;
		}
		else
			n = 2;

		rdata[n].buffer = InvalidBuffer;
		rdata[n].buffer_std = FALSE;
		rdata[n].len = sizeof(ginxlogDeletePage);
		rdata[n].data = (char *) &data;
		rdata[n].next = NULL;

		recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_DELETE_PAGE, rdata);
		PageSetLSN(page, recptr);
		PageSetLSN(parentPage, recptr);
		if (leftBlkno != InvalidBlockNumber)
		{
			page = BufferGetPage(lBuffer);
			PageSetLSN(page, recptr);
		}
	}

	if (!isParentRoot)
		LockBuffer(pBuffer, GIN_UNLOCK);
	ReleaseBuffer(pBuffer);
	UnlockReleaseBuffer(lBuffer);
	UnlockReleaseBuffer(dBuffer);

	END_CRIT_SECTION();

	gvs->result->pages_deleted++;
}

typedef struct DataPageDeleteStack
{
	struct DataPageDeleteStack *child;
	struct DataPageDeleteStack *parent;

	BlockNumber blkno;			/* current block number */
	BlockNumber leftBlkno;		/* rightest non-deleted page on left */
	bool		isRoot;
} DataPageDeleteStack;

/*
 * scans posting tree and deletes empty pages
 */
static bool
ginScanToDelete(GinVacuumState *gvs, BlockNumber blkno, bool isRoot, DataPageDeleteStack *parent, OffsetNumber myoff)
{
	DataPageDeleteStack *me;
	Buffer		buffer;
	Page		page;
	bool		meDelete = FALSE;

	if (isRoot)
	{
		me = parent;
	}
	else
	{
		if (!parent->child)
		{
			me = (DataPageDeleteStack *) palloc0(sizeof(DataPageDeleteStack));
			me->parent = parent;
			parent->child = me;
			me->leftBlkno = InvalidBlockNumber;
		}
		else
			me = parent->child;
	}

	buffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, blkno,
								RBM_NORMAL, gvs->strategy);
	page = BufferGetPage(buffer);

	Assert(GinPageIsData(page));

	if (!GinPageIsLeaf(page))
	{
		OffsetNumber i;

		me->blkno = blkno;
		for (i = FirstOffsetNumber; i <= GinPageGetOpaque(page)->maxoff; i++)
		{
			PostingItem *pitem = (PostingItem *) GinDataPageGetItem(page, i);

			if (ginScanToDelete(gvs, PostingItemGetBlockNumber(pitem), FALSE, me, i))
				i--;
		}
	}

	if (GinPageGetOpaque(page)->maxoff < FirstOffsetNumber)
	{
		/* we never delete the left- or rightmost branch */
		if (me->leftBlkno != InvalidBlockNumber && !GinPageRightMost(page))
		{
			Assert(!isRoot);
			ginDeletePage(gvs, blkno, me->leftBlkno, me->parent->blkno, myoff, me->parent->isRoot);
			meDelete = TRUE;
		}
	}

	ReleaseBuffer(buffer);

	if (!meDelete)
		me->leftBlkno = blkno;

	return meDelete;
}

static void
ginVacuumPostingTree(GinVacuumState *gvs, OffsetNumber attnum, BlockNumber rootBlkno)
{
	Buffer		rootBuffer = InvalidBuffer;
	DataPageDeleteStack root,
			   *ptr,
			   *tmp;

	if (ginVacuumPostingTreeLeaves(gvs, attnum, rootBlkno, TRUE, &rootBuffer) == FALSE)
	{
		Assert(rootBuffer == InvalidBuffer);
		return;
	}

	memset(&root, 0, sizeof(DataPageDeleteStack));
	root.leftBlkno = InvalidBlockNumber;
	root.isRoot = TRUE;

	vacuum_delay_point();

	ginScanToDelete(gvs, rootBlkno, TRUE, &root, InvalidOffsetNumber);

	ptr = root.child;
	while (ptr)
	{
		tmp = ptr->child;
		pfree(ptr);
		ptr = tmp;
	}

	UnlockReleaseBuffer(rootBuffer);
}

/*
 * returns modified page or NULL if page isn't modified.
 * Function works with original page until first change is occurred,
 * then page is copied into temporary one.
 */
static Page
ginVacuumEntryPage(GinVacuumState *gvs, Buffer buffer, BlockNumber *roots, OffsetNumber *attnums, uint32 *nroot)
{
	Page		origpage = BufferGetPage(buffer),
				tmppage;
	OffsetNumber i,
				maxoff = PageGetMaxOffsetNumber(origpage);

	tmppage = origpage;

	*nroot = 0;

	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		IndexTuple	itup = (IndexTuple) PageGetItem(tmppage, PageGetItemId(tmppage, i));

		if (GinIsPostingTree(itup))
		{
			/*
			 * store posting tree's roots for further processing, we can't
			 * vacuum it just now due to risk of deadlocks with scans/inserts
			 */
			roots[*nroot] = GinGetDownlink(itup);
			attnums[*nroot] = gintuple_get_attrnum(&gvs->ginstate, itup);
			(*nroot)++;
		}
		else if (GinGetNPosting(itup) > 0)
		{
			/*
			 * if we already create temporary page, we will make changes in
			 * place
			 */
			Size cleanedSize;
			Pointer cleaned = NULL;
			uint32		newN =
				ginVacuumPostingList(gvs, gintuple_get_attrnum(&gvs->ginstate, itup),
					GinGetPosting(itup), GinGetNPosting(itup), &cleaned,
					IndexTupleSize(itup) - GinGetPostingOffset(itup),
					&cleanedSize);

			if (GinGetNPosting(itup) != newN)
			{
				OffsetNumber attnum;
				Datum		key;
				GinNullCategory category;

				/*
				 * Some ItemPointers was deleted, so we should remake our
				 * tuple
				 */

				if (tmppage == origpage)
				{
					/*
					 * On first difference we create temporary page in memory
					 * and copies content in to it.
					 */
					tmppage = PageGetTempPageCopy(origpage);

					/* set itup pointer to new page */
					itup = (IndexTuple) PageGetItem(tmppage, PageGetItemId(tmppage, i));
				}

				attnum = gintuple_get_attrnum(&gvs->ginstate, itup);
				key = gintuple_get_key(&gvs->ginstate, itup, &category);
				/* FIXME */
				itup = GinFormTuple(&gvs->ginstate, attnum, key, category,
									cleaned, cleanedSize, newN, true);
				pfree(cleaned);
				PageIndexTupleDelete(tmppage, i);

				if (PageAddItem(tmppage, (Item) itup, IndexTupleSize(itup), i, false, false) != i)
					elog(ERROR, "failed to add item to index page in \"%s\"",
						 RelationGetRelationName(gvs->index));

				pfree(itup);
			}
		}
	}

	return (tmppage == origpage) ? NULL : tmppage;
}

Datum
ginbulkdelete(PG_FUNCTION_ARGS)
{
	IndexVacuumInfo *info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);
	IndexBulkDeleteCallback callback = (IndexBulkDeleteCallback) PG_GETARG_POINTER(2);
	void	   *callback_state = (void *) PG_GETARG_POINTER(3);
	Relation	index = info->index;
	BlockNumber blkno = GIN_ROOT_BLKNO;
	GinVacuumState gvs;
	Buffer		buffer;
	BlockNumber rootOfPostingTree[BLCKSZ / (sizeof(IndexTupleData) + sizeof(ItemId))];
	OffsetNumber attnumOfPostingTree[BLCKSZ / (sizeof(IndexTupleData) + sizeof(ItemId))];
	uint32		nRoot;

	gvs.index = index;
	gvs.callback = callback;
	gvs.callback_state = callback_state;
	gvs.strategy = info->strategy;
	initGinState(&gvs.ginstate, index);

	/* first time through? */
	if (stats == NULL)
	{
		/* Yes, so initialize stats to zeroes */
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
		/* and cleanup any pending inserts */
		ginInsertCleanup(&gvs.ginstate, true, stats);
	}

	/* we'll re-count the tuples each time */
	stats->num_index_tuples = 0;
	gvs.result = stats;

	buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
								RBM_NORMAL, info->strategy);

	/* find leaf page */
	for (;;)
	{
		Page		page = BufferGetPage(buffer);
		IndexTuple	itup;

		LockBuffer(buffer, GIN_SHARE);

		Assert(!GinPageIsData(page));

		if (GinPageIsLeaf(page))
		{
			LockBuffer(buffer, GIN_UNLOCK);
			LockBuffer(buffer, GIN_EXCLUSIVE);

			if (blkno == GIN_ROOT_BLKNO && !GinPageIsLeaf(page))
			{
				LockBuffer(buffer, GIN_UNLOCK);
				continue;		/* check it one more */
			}
			break;
		}

		Assert(PageGetMaxOffsetNumber(page) >= FirstOffsetNumber);

		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
		blkno = GinGetDownlink(itup);
		Assert(blkno != InvalidBlockNumber);

		UnlockReleaseBuffer(buffer);
		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);
	}

	/* right now we found leftmost page in entry's BTree */

	for (;;)
	{
		Page		page = BufferGetPage(buffer);
		Page		resPage;
		uint32		i;

		Assert(!GinPageIsData(page));

		resPage = ginVacuumEntryPage(&gvs, buffer, rootOfPostingTree, attnumOfPostingTree, &nRoot);

		blkno = GinPageGetOpaque(page)->rightlink;

		if (resPage)
		{
			START_CRIT_SECTION();
			PageRestoreTempPage(resPage, page);
			MarkBufferDirty(buffer);
			xlogVacuumPage(gvs.index, buffer, InvalidOffsetNumber, &gvs.ginstate);
			UnlockReleaseBuffer(buffer);
			END_CRIT_SECTION();
		}
		else
		{
			UnlockReleaseBuffer(buffer);
		}

		vacuum_delay_point();

		for (i = 0; i < nRoot; i++)
		{
			ginVacuumPostingTree(&gvs, attnumOfPostingTree[i], rootOfPostingTree[i]);
			vacuum_delay_point();
		}

		if (blkno == InvalidBlockNumber)		/* rightmost page */
			break;

		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);
		LockBuffer(buffer, GIN_EXCLUSIVE);
	}

	PG_RETURN_POINTER(gvs.result);
}

Datum
ginvacuumcleanup(PG_FUNCTION_ARGS)
{
	IndexVacuumInfo *info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);
	Relation	index = info->index;
	bool		needLock;
	BlockNumber npages,
				blkno;
	BlockNumber totFreePages;
	GinState	ginstate;
	GinStatsData idxStat;

	/*
	 * In an autovacuum analyze, we want to clean up pending insertions.
	 * Otherwise, an ANALYZE-only call is a no-op.
	 */
	if (info->analyze_only)
	{
		if (IsAutoVacuumWorkerProcess())
		{
			initGinState(&ginstate, index);
			ginInsertCleanup(&ginstate, true, stats);
		}
		PG_RETURN_POINTER(stats);
	}

	/*
	 * Set up all-zero stats and cleanup pending inserts if ginbulkdelete
	 * wasn't called
	 */
	if (stats == NULL)
	{
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
		initGinState(&ginstate, index);
		ginInsertCleanup(&ginstate, true, stats);
	}

	memset(&idxStat, 0, sizeof(idxStat));

	/*
	 * XXX we always report the heap tuple count as the number of index
	 * entries.  This is bogus if the index is partial, but it's real hard to
	 * tell how many distinct heap entries are referenced by a GIN index.
	 */
	stats->num_index_tuples = info->num_heap_tuples;
	stats->estimated_count = info->estimated_count;

	/*
	 * Need lock unless it's local to this backend.
	 */
	needLock = !RELATION_IS_LOCAL(index);

	if (needLock)
		LockRelationForExtension(index, ExclusiveLock);
	npages = RelationGetNumberOfBlocks(index);
	if (needLock)
		UnlockRelationForExtension(index, ExclusiveLock);

	totFreePages = 0;

	for (blkno = GIN_ROOT_BLKNO; blkno < npages; blkno++)
	{
		Buffer		buffer;
		Page		page;

		vacuum_delay_point();

		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);
		LockBuffer(buffer, GIN_SHARE);
		page = (Page) BufferGetPage(buffer);

		if (PageIsNew(page) || GinPageIsDeleted(page))
		{
			Assert(blkno != GIN_ROOT_BLKNO);
			RecordFreeIndexPage(index, blkno);
			totFreePages++;
		}
		else if (GinPageIsData(page))
		{
			idxStat.nDataPages++;
		}
		else if (!GinPageIsList(page))
		{
			idxStat.nEntryPages++;

			if (GinPageIsLeaf(page))
				idxStat.nEntries += PageGetMaxOffsetNumber(page);
		}

		UnlockReleaseBuffer(buffer);
	}

	/* Update the metapage with accurate page and entry counts */
	idxStat.nTotalPages = npages;
	ginUpdateStats(info->index, &idxStat);

	/* Finally, vacuum the FSM */
	IndexFreeSpaceMapVacuum(info->index);

	stats->pages_free = totFreePages;

	if (needLock)
		LockRelationForExtension(index, ExclusiveLock);
	stats->num_pages = RelationGetNumberOfBlocks(index);
	if (needLock)
		UnlockRelationForExtension(index, ExclusiveLock);

	PG_RETURN_POINTER(stats);
}
