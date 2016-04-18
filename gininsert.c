/*-------------------------------------------------------------------------
 *
 * gininsert.c
 *	  insert routines for the postgres inverted index access method.
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/gin/gininsert.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam_xlog.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "storage/indexfsm.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/datum.h"

#include "rum.h"

typedef struct
{
	GinState	ginstate;
	double		indtuples;
	GinStatsData buildStats;
	MemoryContext tmpCtx;
	MemoryContext funcCtx;
	BuildAccumulator accum;
} GinBuildState;

/*
 * Creates new posting tree with one page, containing the given TIDs.
 * Returns the page number (which will be the root of this posting tree).
 *
 * items[] must be in sorted order with no duplicates.
 */
static BlockNumber
createPostingTree(GinState *ginstate, OffsetNumber attnum, Relation index,
	ItemPointerData *items, Datum *addInfo, bool *addInfoIsNull, uint32 nitems)
{
	BlockNumber blkno;
	Buffer		buffer = GinNewBuffer(index);
	Page		page;
	int			i;
	Pointer		ptr;
	ItemPointerData prev_iptr = {{0,0},0};
	static char	pageCopy[BLCKSZ];

	/* Assert that the items[] array will fit on one page */

	START_CRIT_SECTION();

	GinInitBuffer(buffer, GIN_DATA | GIN_LEAF);
	page = BufferGetPage(buffer, NULL, NULL, BGP_NO_SNAPSHOT_TEST);
	blkno = BufferGetBlockNumber(buffer);

	GinPageGetOpaque(page)->maxoff = nitems;
	ptr = GinDataPageGetData(page);
	for (i = 0; i < nitems; i++)
	{
		if (i > 0)
			prev_iptr = items[i - 1];
		ptr = ginPlaceToDataPageLeaf(ptr, attnum, &items[i], addInfo[i],
			addInfoIsNull[i], &prev_iptr, ginstate);
	}
	Assert(GinDataPageFreeSpacePre(page, ptr) >= 0);
	updateItemIndexes(page, attnum, ginstate);

	MarkBufferDirty(buffer);

	if (RelationNeedsWAL(index))
	{
		XLogRecPtr	recptr;
		XLogRecData rdata[2];
		ginxlogCreatePostingTree data;

		data.node = index->rd_node;
		data.blkno = blkno;
		data.nitem = nitems;

		if (ginstate->addAttrs[attnum - 1])
		{
			data.typlen = ginstate->addAttrs[attnum - 1]->attlen;
			data.typalign = ginstate->addAttrs[attnum - 1]->attalign;
			data.typbyval = ginstate->addAttrs[attnum - 1]->attbyval;
			data.typstorage = ginstate->addAttrs[attnum - 1]->attstorage;
		}

		rdata[0].buffer = InvalidBuffer;
		rdata[0].data = (char *) &data;
		rdata[0].len = MAXALIGN(sizeof(ginxlogCreatePostingTree));
		rdata[0].next = &rdata[1];

		memcpy(pageCopy, page, BLCKSZ);
		rdata[1].buffer = InvalidBuffer;
		rdata[1].data = GinDataPageGetData(pageCopy);
		rdata[1].len = GinDataPageSize - GinPageGetOpaque(pageCopy)->freespace;
		rdata[1].next = NULL;

		recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_CREATE_PTREE, rdata);
		PageSetLSN(page, recptr);
	}

	UnlockReleaseBuffer(buffer);

	END_CRIT_SECTION();

	return blkno;
}

/*
 * Form a tuple for entry tree.
 *
 * If the tuple would be too big to be stored, function throws a suitable
 * error if errorTooBig is TRUE, or returns NULL if errorTooBig is FALSE.
 *
 * See src/backend/access/gin/README for a description of the index tuple
 * format that is being built here.  We build on the assumption that we
 * are making a leaf-level key entry containing a posting list of nipd items.
 * If the caller is actually trying to make a posting-tree entry, non-leaf
 * entry, or pending-list entry, it should pass nipd = 0 and then overwrite
 * the t_tid fields as necessary.  In any case, ipd can be NULL to skip
 * copying any itempointers into the posting list; the caller is responsible
 * for filling the posting list afterwards, if ipd = NULL and nipd > 0.
 */
static IndexTuple
GinFormTuple(GinState *ginstate,
			 OffsetNumber attnum, Datum key, GinNullCategory category,
			 ItemPointerData *ipd,
			 Datum *addInfo,
			 bool *addInfoIsNull,
			 uint32 nipd,
			 bool errorTooBig)
{
	Datum		datums[3];
	bool		isnull[3];
	IndexTuple	itup;
	uint32		newsize;
	int			i;
	ItemPointerData nullItemPointer = {{0,0},0};

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
		newsize = ginCheckPlaceToDataPageLeaf(attnum, &ipd[0], addInfo[0],
			addInfoIsNull[0], &nullItemPointer, ginstate, newsize);
		for (i = 1; i < nipd; i++)
		{
			newsize = ginCheckPlaceToDataPageLeaf(attnum, &ipd[i], addInfo[i],
							addInfoIsNull[i], &ipd[i - 1], ginstate, newsize);
		}
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
		ptr = ginPlaceToDataPageLeaf(ptr, attnum, &ipd[0], addInfo[0],
								addInfoIsNull[0], &nullItemPointer, ginstate);
		for (i = 1; i < nipd; i++)
		{
			ptr = ginPlaceToDataPageLeaf(ptr, attnum, &ipd[i], addInfo[i],
										addInfoIsNull[i], &ipd[i-1], ginstate);
		}
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
 * Adds array of item pointers to tuple's posting list, or
 * creates posting tree and tuple pointing to tree in case
 * of not enough space.  Max size of tuple is defined in
 * GinFormTuple().  Returns a new, modified index tuple.
 * items[] must be in sorted order with no duplicates.
 */
static IndexTuple
addItemPointersToLeafTuple(GinState *ginstate,
						   IndexTuple old,
						   ItemPointerData *items, Datum *addInfo,
						   bool *addInfoIsNull, uint32 nitem,
						   GinStatsData *buildStats)
{
	OffsetNumber attnum;
	Datum		key;
	GinNullCategory category;
	IndexTuple	res;
	Datum		*oldAddInfo, *newAddInfo;
	bool		*oldAddInfoIsNull, *newAddInfoIsNull;
	ItemPointerData *newItems, *oldItems;
	int			oldNPosting, newNPosting;

	Assert(!GinIsPostingTree(old));

	attnum = gintuple_get_attrnum(ginstate, old);
	key = gintuple_get_key(ginstate, old, &category);

	oldNPosting = GinGetNPosting(old);

	oldItems = (ItemPointerData *)palloc(sizeof(ItemPointerData) * oldNPosting);
	oldAddInfo = (Datum *)palloc(sizeof(Datum) * oldNPosting);
	oldAddInfoIsNull = (bool *)palloc(sizeof(bool) * oldNPosting);

	newNPosting = oldNPosting + nitem;

	newItems = (ItemPointerData *)palloc(sizeof(ItemPointerData) * newNPosting);
	newAddInfo = (Datum *)palloc(sizeof(Datum) * newNPosting);
	newAddInfoIsNull = (bool *)palloc(sizeof(bool) * newNPosting);

	ginReadTuple(ginstate, attnum, old, oldItems, oldAddInfo, oldAddInfoIsNull);

	newNPosting = ginMergeItemPointers(newItems, newAddInfo, newAddInfoIsNull,
		items, addInfo, addInfoIsNull, nitem,
		oldItems, oldAddInfo, oldAddInfoIsNull, oldNPosting);


	/* try to build tuple with room for all the items */
	res = GinFormTuple(ginstate, attnum, key, category,
					   newItems, newAddInfo, newAddInfoIsNull, newNPosting,
					   false);

	if (!res)
	{
		/* posting list would be too big, convert to posting tree */
		BlockNumber postingRoot;
		GinPostingTreeScan *gdi;

		/*
		 * Initialize posting tree with the old tuple's posting list.  It's
		 * surely small enough to fit on one posting-tree page, and should
		 * already be in order with no duplicates.
		 */
		postingRoot = createPostingTree(ginstate,
										attnum,
										ginstate->index,
										oldItems,
										oldAddInfo,
										oldAddInfoIsNull,
										oldNPosting);

		/* During index build, count the newly-added data page */
		if (buildStats)
			buildStats->nDataPages++;

		/* Now insert the TIDs-to-be-added into the posting tree */
		gdi = ginPrepareScanPostingTree(ginstate->index, postingRoot, FALSE, attnum, ginstate);
		gdi->btree.isBuild = (buildStats != NULL);

		ginInsertItemPointers(ginstate, attnum, gdi, items, addInfo, addInfoIsNull, nitem, buildStats);

		pfree(gdi);

		/* And build a new posting-tree-only result tuple */
		res = GinFormTuple(ginstate, attnum, key, category, NULL, NULL, NULL, 0, true);
		GinSetPostingTree(res, postingRoot);
	}

	return res;
}

/*
 * Build a fresh leaf tuple, either posting-list or posting-tree format
 * depending on whether the given items list will fit.
 * items[] must be in sorted order with no duplicates.
 *
 * This is basically the same logic as in addItemPointersToLeafTuple,
 * but working from slightly different input.
 */
static IndexTuple
buildFreshLeafTuple(GinState *ginstate,
					OffsetNumber attnum, Datum key, GinNullCategory category,
					ItemPointerData *items, Datum *addInfo,
					bool *addInfoIsNull, uint32 nitem,
					GinStatsData *buildStats)
{
	IndexTuple	res;

	/* try to build tuple with room for all the items */
	res = GinFormTuple(ginstate, attnum, key, category,
					   items, addInfo, addInfoIsNull, nitem, false);

	if (!res)
	{
		/* posting list would be too big, build posting tree */
		BlockNumber postingRoot;
		ItemPointerData prevIptr = {{0,0},0};
		Size size = 0;
		int itemsCount = 0;

		do
		{
			size = ginCheckPlaceToDataPageLeaf(attnum, &items[itemsCount],
				addInfo[itemsCount], addInfoIsNull[itemsCount], &prevIptr,
				ginstate, size);
			prevIptr = items[itemsCount];
			itemsCount++;
		}
		while (itemsCount < nitem && size < GinDataPageSize);
		itemsCount--;


		/*
		 * Build posting-tree-only result tuple.  We do this first so as to
		 * fail quickly if the key is too big.
		 */
		res = GinFormTuple(ginstate, attnum, key, category, NULL, NULL, NULL, 0, true);

		/*
		 * Initialize posting tree with as many TIDs as will fit on the first
		 * page.
		 */
		postingRoot = createPostingTree(ginstate,
										attnum,
										ginstate->index,
										items,
										addInfo,
										addInfoIsNull,
										itemsCount);

		/* During index build, count the newly-added data page */
		if (buildStats)
			buildStats->nDataPages++;

		/* Add any remaining TIDs to the posting tree */
		if (nitem > itemsCount)
		{
			GinPostingTreeScan *gdi;

			gdi = ginPrepareScanPostingTree(ginstate->index, postingRoot, FALSE, attnum, ginstate);
			gdi->btree.isBuild = (buildStats != NULL);

			ginInsertItemPointers(ginstate,
								  attnum,
								  gdi,
								  items + itemsCount,
								  addInfo + itemsCount,
								  addInfoIsNull + itemsCount,
								  nitem - itemsCount,
								  buildStats);

			pfree(gdi);
		}

		/* And save the root link in the result tuple */
		GinSetPostingTree(res, postingRoot);
	}

	return res;
}

/*
 * Insert one or more heap TIDs associated with the given key value.
 * This will either add a single key entry, or enlarge a pre-existing entry.
 *
 * During an index build, buildStats is non-null and the counters
 * it contains should be incremented as needed.
 */
void
ginEntryInsert(GinState *ginstate,
			   OffsetNumber attnum, Datum key, GinNullCategory category,
			   ItemPointerData *items,
			   Datum *addInfo,
			   bool *addInfoIsNull,
			   uint32 nitem,
			   GinStatsData *buildStats)
{
	GinBtreeData btree;
	GinBtreeStack *stack;
	IndexTuple	itup;
	Page		page;
	int i;

	if (!addInfoIsNull || !addInfo)
	{
		addInfoIsNull = (bool *)palloc(sizeof(bool) * nitem);
		addInfo = (Datum *)palloc(sizeof(Datum) * nitem);
		for (i = 0; i < nitem; i++)
		{
			addInfoIsNull[i] = true;
			addInfo[i] = (Datum) 0;
		}
	}

	/* During index build, count the to-be-inserted entry */
	if (buildStats)
		buildStats->nEntries++;

	ginPrepareEntryScan(&btree, attnum, key, category, ginstate);

	stack = ginFindLeafPage(&btree, NULL);
	page = BufferGetPage(stack->buffer, NULL, NULL, BGP_NO_SNAPSHOT_TEST);

	if (btree.findItem(&btree, stack))
	{
		/* found pre-existing entry */
		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, stack->off));

		if (GinIsPostingTree(itup))
		{
			/* add entries to existing posting tree */
			BlockNumber rootPostingTree = GinGetPostingTree(itup);
			GinPostingTreeScan *gdi;

			/* release all stack */
			LockBuffer(stack->buffer, GIN_UNLOCK);
			freeGinBtreeStack(stack);

			/* insert into posting tree */
			gdi = ginPrepareScanPostingTree(ginstate->index, rootPostingTree, FALSE, attnum, ginstate);
			gdi->btree.isBuild = (buildStats != NULL);
			ginInsertItemPointers(ginstate, attnum, gdi, items, addInfo, addInfoIsNull, nitem, buildStats);
			pfree(gdi);

			return;
		}

		/* modify an existing leaf entry */
		itup = addItemPointersToLeafTuple(ginstate, itup,
										  items, addInfo, addInfoIsNull, nitem, buildStats);

		btree.isDelete = TRUE;
	}
	else
	{
		/* no match, so construct a new leaf entry */
		itup = buildFreshLeafTuple(ginstate, attnum, key, category,
								   items, addInfo, addInfoIsNull, nitem, buildStats);
	}

	/* Insert the new or modified leaf tuple */
	btree.entry = itup;
	ginInsertValue(&btree, stack, buildStats);
	pfree(itup);
}

/*
 * Extract index entries for a single indexable item, and add them to the
 * BuildAccumulator's state.
 *
 * This function is used only during initial index creation.
 */
static void
ginHeapTupleBulkInsert(GinBuildState *buildstate, OffsetNumber attnum,
					   Datum value, bool isNull,
					   ItemPointer heapptr)
{
	Datum	   *entries;
	GinNullCategory *categories;
	int32		nentries;
	MemoryContext oldCtx;
	Datum	   *addInfo;
	bool	   *addInfoIsNull;
	int			i;
	Form_pg_attribute attr = buildstate->ginstate.addAttrs[attnum - 1];

	oldCtx = MemoryContextSwitchTo(buildstate->funcCtx);
	entries = ginExtractEntries(buildstate->accum.ginstate, attnum,
								value, isNull,
								&nentries, &categories,
								&addInfo, &addInfoIsNull);
	MemoryContextSwitchTo(oldCtx);
	for (i = 0; i < nentries; i++)
	{
		if (!addInfoIsNull[i])
		{
			addInfo[i] = datumCopy(addInfo[i], attr->attbyval, attr->attlen);
		}
	}

	ginInsertBAEntries(&buildstate->accum, heapptr, attnum,
					   entries, addInfo, addInfoIsNull, categories, nentries);

	buildstate->indtuples += nentries;

	MemoryContextReset(buildstate->funcCtx);
}

static void
ginBuildCallback(Relation index, HeapTuple htup, Datum *values,
				 bool *isnull, bool tupleIsAlive, void *state)
{
	GinBuildState *buildstate = (GinBuildState *) state;
	MemoryContext oldCtx;
	int			i;

	oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

	for (i = 0; i < buildstate->ginstate.origTupdesc->natts; i++)
		ginHeapTupleBulkInsert(buildstate, (OffsetNumber) (i + 1),
							   values[i], isnull[i],
							   &htup->t_self);

	/* If we've maxed out our available memory, dump everything to the index */
	if (buildstate->accum.allocatedMemory >= maintenance_work_mem * 1024L)
	{
		GinEntryAccumulatorItem *list;
		Datum		key;
		GinNullCategory category;
		uint32		nlist;
		OffsetNumber attnum;

		ginBeginBAScan(&buildstate->accum);
		while ((list = ginGetBAEntry(&buildstate->accum,
								  &attnum, &key, &category, &nlist)) != NULL)
		{
			ItemPointerData *iptrs = (ItemPointerData *)palloc(sizeof(ItemPointerData) *nlist);
			Datum *addInfo = (Datum *)palloc(sizeof(Datum) * nlist);
			bool *addInfoIsNull = (bool *)palloc(sizeof(bool) * nlist);
			int i;

			for (i = 0; i < nlist; i++)
			{
				iptrs[i] = list[i].iptr;
				addInfo[i] = list[i].addInfo;
				addInfoIsNull[i] = list[i].addInfoIsNull;
			}


			/* there could be many entries, so be willing to abort here */
			CHECK_FOR_INTERRUPTS();
			ginEntryInsert(&buildstate->ginstate, attnum, key, category,
						   iptrs, addInfo, addInfoIsNull, nlist, &buildstate->buildStats);
		}

		MemoryContextReset(buildstate->tmpCtx);
		ginInitBA(&buildstate->accum);
	}

	MemoryContextSwitchTo(oldCtx);
}

Datum
ginbuild(PG_FUNCTION_ARGS)
{
	Relation	heap = (Relation) PG_GETARG_POINTER(0);
	Relation	index = (Relation) PG_GETARG_POINTER(1);
	IndexInfo  *indexInfo = (IndexInfo *) PG_GETARG_POINTER(2);
	IndexBuildResult *result;
	double		reltuples;
	GinBuildState buildstate;
	Buffer		RootBuffer,
				MetaBuffer;
	GinEntryAccumulatorItem *list;
	Datum		key;
	GinNullCategory category;
	uint32		nlist;
	MemoryContext oldCtx;
	OffsetNumber attnum;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

	initGinState(&buildstate.ginstate, index);
	buildstate.indtuples = 0;
	memset(&buildstate.buildStats, 0, sizeof(GinStatsData));

	/* initialize the meta page */
	MetaBuffer = GinNewBuffer(index);

	/* initialize the root page */
	RootBuffer = GinNewBuffer(index);

	START_CRIT_SECTION();
	GinInitMetabuffer(MetaBuffer);
	MarkBufferDirty(MetaBuffer);
	GinInitBuffer(RootBuffer, GIN_LEAF);
	MarkBufferDirty(RootBuffer);

	if (RelationNeedsWAL(index))
	{
		XLogRecPtr	recptr;
		XLogRecData rdata;
		Page		page;

		rdata.buffer = InvalidBuffer;
		rdata.data = (char *) &(index->rd_node);
		rdata.len = sizeof(RelFileNode);
		rdata.next = NULL;

		recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_CREATE_INDEX, &rdata);

		page = BufferGetPage(RootBuffer, NULL, NULL, BGP_NO_SNAPSHOT_TEST);
		PageSetLSN(page, recptr);

		page = BufferGetPage(MetaBuffer, NULL, NULL, BGP_NO_SNAPSHOT_TEST);
		PageSetLSN(page, recptr);
	}

	UnlockReleaseBuffer(MetaBuffer);
	UnlockReleaseBuffer(RootBuffer);
	END_CRIT_SECTION();

	/* count the root as first entry page */
	buildstate.buildStats.nEntryPages++;

	/*
	 * create a temporary memory context that is reset once for each tuple
	 * inserted into the index
	 */
	buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											  "Gin build temporary context",
											  ALLOCSET_DEFAULT_MINSIZE,
											  ALLOCSET_DEFAULT_INITSIZE,
											  ALLOCSET_DEFAULT_MAXSIZE);

	buildstate.funcCtx = AllocSetContextCreate(buildstate.tmpCtx,
					 "Gin build temporary context for user-defined function",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	buildstate.accum.ginstate = &buildstate.ginstate;
	ginInitBA(&buildstate.accum);

	/*
	 * Do the heap scan.  We disallow sync scan here because dataPlaceToPage
	 * prefers to receive tuples in TID order.
	 */
	reltuples = IndexBuildHeapScan(heap, index, indexInfo, false,
								   ginBuildCallback, (void *) &buildstate);

	/* dump remaining entries to the index */
	oldCtx = MemoryContextSwitchTo(buildstate.tmpCtx);
	ginBeginBAScan(&buildstate.accum);
	while ((list = ginGetBAEntry(&buildstate.accum,
								 &attnum, &key, &category, &nlist)) != NULL)
	{
		ItemPointerData *iptrs = (ItemPointerData *)palloc(sizeof(ItemPointerData) *nlist);
		Datum *addInfo = (Datum *)palloc(sizeof(Datum) * nlist);
		bool *addInfoIsNull = (bool *)palloc(sizeof(bool) * nlist);
		int i;

		for (i = 0; i < nlist; i++)
		{
			iptrs[i] = list[i].iptr;
			addInfo[i] = list[i].addInfo;
			addInfoIsNull[i] = list[i].addInfoIsNull;
		}

		/* there could be many entries, so be willing to abort here */
		CHECK_FOR_INTERRUPTS();
		ginEntryInsert(&buildstate.ginstate, attnum, key, category,
					   iptrs, addInfo, addInfoIsNull, nlist, &buildstate.buildStats);
	}
	MemoryContextSwitchTo(oldCtx);

	MemoryContextDelete(buildstate.tmpCtx);

	/*
	 * Update metapage stats
	 */
	buildstate.buildStats.nTotalPages = RelationGetNumberOfBlocks(index);
	ginUpdateStats(index, &buildstate.buildStats);

	/*
	 * Return statistics
	 */
	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));

	result->heap_tuples = reltuples;
	result->index_tuples = buildstate.indtuples;

	PG_RETURN_POINTER(result);
}

/*
 *	ginbuildempty() -- build an empty gin index in the initialization fork
 */
Datum
ginbuildempty(PG_FUNCTION_ARGS)
{
	Relation	index = (Relation) PG_GETARG_POINTER(0);
	Buffer		RootBuffer,
				MetaBuffer;

	/* An empty GIN index has two pages. */
	MetaBuffer =
		ReadBufferExtended(index, INIT_FORKNUM, P_NEW, RBM_NORMAL, NULL);
	LockBuffer(MetaBuffer, BUFFER_LOCK_EXCLUSIVE);
	RootBuffer =
		ReadBufferExtended(index, INIT_FORKNUM, P_NEW, RBM_NORMAL, NULL);
	LockBuffer(RootBuffer, BUFFER_LOCK_EXCLUSIVE);

	/* Initialize and xlog metabuffer and root buffer. */
	START_CRIT_SECTION();
	GinInitMetabuffer(MetaBuffer);
	MarkBufferDirty(MetaBuffer);
	log_newpage_buffer(MetaBuffer);
	GinInitBuffer(RootBuffer, GIN_LEAF);
	MarkBufferDirty(RootBuffer);
	log_newpage_buffer(RootBuffer);
	END_CRIT_SECTION();

	/* Unlock and release the buffers. */
	UnlockReleaseBuffer(MetaBuffer);
	UnlockReleaseBuffer(RootBuffer);

	PG_RETURN_VOID();
}

/*
 * Insert index entries for a single indexable item during "normal"
 * (non-fast-update) insertion
 */
static void
ginHeapTupleInsert(GinState *ginstate, OffsetNumber attnum,
				   Datum value, bool isNull,
				   ItemPointer item)
{
	Datum	   *entries;
	GinNullCategory *categories;
	int32		i,
				nentries;
	Datum	   *addInfo;
	bool	   *addInfoIsNull;

	entries = ginExtractEntries(ginstate, attnum, value, isNull,
								&nentries, &categories, &addInfo, &addInfoIsNull);

	for (i = 0; i < nentries; i++)
		ginEntryInsert(ginstate, attnum, entries[i], categories[i],
					   item, &addInfo[i], &addInfoIsNull[i], 1, NULL);
}

Datum
gininsert(PG_FUNCTION_ARGS)
{
	Relation	index = (Relation) PG_GETARG_POINTER(0);
	Datum	   *values = (Datum *) PG_GETARG_POINTER(1);
	bool	   *isnull = (bool *) PG_GETARG_POINTER(2);
	ItemPointer ht_ctid = (ItemPointer) PG_GETARG_POINTER(3);

#ifdef NOT_USED
	Relation	heapRel = (Relation) PG_GETARG_POINTER(4);
	IndexUniqueCheck checkUnique = (IndexUniqueCheck) PG_GETARG_INT32(5);
#endif
	GinState	ginstate;
	MemoryContext oldCtx;
	MemoryContext insertCtx;
	int			i;

	insertCtx = AllocSetContextCreate(CurrentMemoryContext,
									  "Gin insert temporary context",
									  ALLOCSET_DEFAULT_MINSIZE,
									  ALLOCSET_DEFAULT_INITSIZE,
									  ALLOCSET_DEFAULT_MAXSIZE);

	oldCtx = MemoryContextSwitchTo(insertCtx);

	initGinState(&ginstate, index);

	if (GinGetUseFastUpdate(index))
	{
		GinTupleCollector collector;

		memset(&collector, 0, sizeof(GinTupleCollector));

		for (i = 0; i < ginstate.origTupdesc->natts; i++)
			ginHeapTupleFastCollect(&ginstate, &collector,
									(OffsetNumber) (i + 1),
									values[i], isnull[i],
									ht_ctid);

		ginHeapTupleFastInsert(&ginstate, &collector);
	}
	else
	{
		for (i = 0; i < ginstate.origTupdesc->natts; i++)
			ginHeapTupleInsert(&ginstate, (OffsetNumber) (i + 1),
							   values[i], isnull[i],
							   ht_ctid);
	}

	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(insertCtx);

	PG_RETURN_BOOL(false);
}
