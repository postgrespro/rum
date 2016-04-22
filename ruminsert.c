/*-------------------------------------------------------------------------
 *
 * ruminsert.c
 *	  insert routines for the postgres inverted index access method.
 *
 *
 * Portions Copyright (c) 2015-2016, Postgres Professional
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/generic_xlog.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/datum.h"

#include "rum.h"

typedef struct
{
	RumState	rumstate;
	double		indtuples;
	GinStatsData buildStats;
	MemoryContext tmpCtx;
	MemoryContext funcCtx;
	BuildAccumulator accum;
} RumBuildState;

/*
 * Creates new posting tree with one page, containing the given TIDs.
 * Returns the page number (which will be the root of this posting tree).
 *
 * items[] must be in sorted order with no duplicates.
 */
static BlockNumber
createPostingTree(RumState *rumstate, OffsetNumber attnum, Relation index,
	ItemPointerData *items, Datum *addInfo, bool *addInfoIsNull, uint32 nitems)
{
	BlockNumber blkno;
	Buffer		buffer = RumNewBuffer(index);
	Page		page;
	int			i;
	Pointer		ptr;
	ItemPointerData prev_iptr = {{0,0},0};
	GenericXLogState *state;

	state = GenericXLogStart(index);

	page = GenericXLogRegisterBuffer(state, buffer, 0);
	RumInitPage(page, RUM_DATA | RUM_LEAF, BufferGetPageSize(buffer));

	blkno = BufferGetBlockNumber(buffer);

	RumPageGetOpaque(page)->maxoff = nitems;
	ptr = RumDataPageGetData(page);
	for (i = 0; i < nitems; i++)
	{
		if (i > 0)
			prev_iptr = items[i - 1];
		ptr = rumPlaceToDataPageLeaf(ptr, attnum, &items[i], addInfo[i],
			addInfoIsNull[i], &prev_iptr, rumstate);
	}
	Assert(RumDataPageFreeSpacePre(page, ptr) >= 0);
	updateItemIndexes(page, attnum, rumstate);

	GenericXLogFinish(state);

	UnlockReleaseBuffer(buffer);

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
RumFormTuple(RumState *rumstate,
			 OffsetNumber attnum, Datum key, RumNullCategory category,
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
	if (rumstate->oneCol)
	{
		datums[0] = key;
		isnull[0] = (category != RUM_CAT_NORM_KEY);
		isnull[1] = true;
	}
	else
	{
		datums[0] = UInt16GetDatum(attnum);
		isnull[0] = false;
		datums[1] = key;
		isnull[1] = (category != RUM_CAT_NORM_KEY);
		isnull[2] = true;
	}

	itup = index_form_tuple(rumstate->tupdesc[attnum - 1], datums, isnull);

	/*
	 * Determine and store offset to the posting list, making sure there is
	 * room for the category byte if needed.
	 *
	 * Note: because index_form_tuple MAXALIGNs the tuple size, there may well
	 * be some wasted pad space.  Is it worth recomputing the data length to
	 * prevent that?  That would also allow us to Assert that the real data
	 * doesn't overlap the RumNullCategory byte, which this code currently
	 * takes on faith.
	 */
	newsize = IndexTupleSize(itup);

	RumSetPostingOffset(itup, newsize);

	RumSetNPosting(itup, nipd);

	/*
	 * Add space needed for posting list, if any.  Then check that the tuple
	 * won't be too big to store.
	 */

	if (nipd > 0)
	{
		newsize = rumCheckPlaceToDataPageLeaf(attnum, &ipd[0], addInfo[0],
			addInfoIsNull[0], &nullItemPointer, rumstate, newsize);
		for (i = 1; i < nipd; i++)
		{
			newsize = rumCheckPlaceToDataPageLeaf(attnum, &ipd[i], addInfo[i],
							addInfoIsNull[i], &ipd[i - 1], rumstate, newsize);
		}
	}

	if (category != RUM_CAT_NORM_KEY)
	{
		Assert(IndexTupleHasNulls(itup));
		newsize = newsize + sizeof(RumNullCategory);
	}
	newsize = MAXALIGN(newsize);

	if (newsize > Min(INDEX_SIZE_MASK, RumMaxItemSize))
	{
		if (errorTooBig)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			errmsg("index row size %lu exceeds maximum %lu for index \"%s\"",
				   (unsigned long) newsize,
				   (unsigned long) Min(INDEX_SIZE_MASK,
									   RumMaxItemSize),
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
	 * Copy in the posting list, if provided
	 */
	if (nipd > 0)
	{
		char *ptr = RumGetPosting(itup);
		ptr = rumPlaceToDataPageLeaf(ptr, attnum, &ipd[0], addInfo[0],
								addInfoIsNull[0], &nullItemPointer, rumstate);
		for (i = 1; i < nipd; i++)
		{
			ptr = rumPlaceToDataPageLeaf(ptr, attnum, &ipd[i], addInfo[i],
										addInfoIsNull[i], &ipd[i-1], rumstate);
		}
	}

	/*
	 * Insert category byte, if needed
	 */
	if (category != RUM_CAT_NORM_KEY)
	{
		Assert(IndexTupleHasNulls(itup));
		RumSetNullCategory(itup, rumstate, category);
	}
	return itup;
}

/*
 * Adds array of item pointers to tuple's posting list, or
 * creates posting tree and tuple pointing to tree in case
 * of not enough space.  Max size of tuple is defined in
 * RumFormTuple().  Returns a new, modified index tuple.
 * items[] must be in sorted order with no duplicates.
 */
static IndexTuple
addItemPointersToLeafTuple(RumState *rumstate,
						   IndexTuple old,
						   ItemPointerData *items, Datum *addInfo,
						   bool *addInfoIsNull, uint32 nitem,
						   GinStatsData *buildStats)
{
	OffsetNumber attnum;
	Datum		key;
	RumNullCategory category;
	IndexTuple	res;
	Datum		*oldAddInfo, *newAddInfo;
	bool		*oldAddInfoIsNull, *newAddInfoIsNull;
	ItemPointerData *newItems, *oldItems;
	int			oldNPosting, newNPosting;

	Assert(!RumIsPostingTree(old));

	attnum = rumtuple_get_attrnum(rumstate, old);
	key = rumtuple_get_key(rumstate, old, &category);

	oldNPosting = RumGetNPosting(old);

	oldItems = (ItemPointerData *)palloc(sizeof(ItemPointerData) * oldNPosting);
	oldAddInfo = (Datum *)palloc(sizeof(Datum) * oldNPosting);
	oldAddInfoIsNull = (bool *)palloc(sizeof(bool) * oldNPosting);

	newNPosting = oldNPosting + nitem;

	newItems = (ItemPointerData *)palloc(sizeof(ItemPointerData) * newNPosting);
	newAddInfo = (Datum *)palloc(sizeof(Datum) * newNPosting);
	newAddInfoIsNull = (bool *)palloc(sizeof(bool) * newNPosting);

	rumReadTuple(rumstate, attnum, old, oldItems, oldAddInfo, oldAddInfoIsNull);

	newNPosting = rumMergeItemPointers(newItems, newAddInfo, newAddInfoIsNull,
		items, addInfo, addInfoIsNull, nitem,
		oldItems, oldAddInfo, oldAddInfoIsNull, oldNPosting);


	/* try to build tuple with room for all the items */
	res = RumFormTuple(rumstate, attnum, key, category,
					   newItems, newAddInfo, newAddInfoIsNull, newNPosting,
					   false);

	if (!res)
	{
		/* posting list would be too big, convert to posting tree */
		BlockNumber postingRoot;
		RumPostingTreeScan *gdi;

		/*
		 * Initialize posting tree with the old tuple's posting list.  It's
		 * surely small enough to fit on one posting-tree page, and should
		 * already be in order with no duplicates.
		 */
		postingRoot = createPostingTree(rumstate,
										attnum,
										rumstate->index,
										oldItems,
										oldAddInfo,
										oldAddInfoIsNull,
										oldNPosting);

		/* During index build, count the newly-added data page */
		if (buildStats)
			buildStats->nDataPages++;

		/* Now insert the TIDs-to-be-added into the posting tree */
		gdi = rumPrepareScanPostingTree(rumstate->index, postingRoot, FALSE, attnum, rumstate);
		gdi->btree.isBuild = (buildStats != NULL);

		rumInsertItemPointers(rumstate, attnum, gdi, items, addInfo, addInfoIsNull, nitem, buildStats);

		pfree(gdi);

		/* And build a new posting-tree-only result tuple */
		res = RumFormTuple(rumstate, attnum, key, category, NULL, NULL, NULL, 0, true);
		RumSetPostingTree(res, postingRoot);
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
buildFreshLeafTuple(RumState *rumstate,
					OffsetNumber attnum, Datum key, RumNullCategory category,
					ItemPointerData *items, Datum *addInfo,
					bool *addInfoIsNull, uint32 nitem,
					GinStatsData *buildStats)
{
	IndexTuple	res;

	/* try to build tuple with room for all the items */
	res = RumFormTuple(rumstate, attnum, key, category,
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
			size = rumCheckPlaceToDataPageLeaf(attnum, &items[itemsCount],
				addInfo[itemsCount], addInfoIsNull[itemsCount], &prevIptr,
				rumstate, size);
			prevIptr = items[itemsCount];
			itemsCount++;
		}
		while (itemsCount < nitem && size < RumDataPageSize);
		itemsCount--;


		/*
		 * Build posting-tree-only result tuple.  We do this first so as to
		 * fail quickly if the key is too big.
		 */
		res = RumFormTuple(rumstate, attnum, key, category, NULL, NULL, NULL, 0, true);

		/*
		 * Initialize posting tree with as many TIDs as will fit on the first
		 * page.
		 */
		postingRoot = createPostingTree(rumstate,
										attnum,
										rumstate->index,
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
			RumPostingTreeScan *gdi;

			gdi = rumPrepareScanPostingTree(rumstate->index, postingRoot, FALSE, attnum, rumstate);
			gdi->btree.isBuild = (buildStats != NULL);

			rumInsertItemPointers(rumstate,
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
		RumSetPostingTree(res, postingRoot);
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
rumEntryInsert(RumState *rumstate,
			   OffsetNumber attnum, Datum key, RumNullCategory category,
			   ItemPointerData *items,
			   Datum *addInfo,
			   bool *addInfoIsNull,
			   uint32 nitem,
			   GinStatsData *buildStats)
{
	RumBtreeData btree;
	RumBtreeStack *stack;
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

	rumPrepareEntryScan(&btree, attnum, key, category, rumstate);

	stack = rumFindLeafPage(&btree, NULL);
	page = BufferGetPage(stack->buffer, NULL, NULL, BGP_NO_SNAPSHOT_TEST);

	if (btree.findItem(&btree, stack))
	{
		/* found pre-existing entry */
		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, stack->off));

		if (RumIsPostingTree(itup))
		{
			/* add entries to existing posting tree */
			BlockNumber rootPostingTree = RumGetPostingTree(itup);
			RumPostingTreeScan *gdi;

			/* release all stack */
			LockBuffer(stack->buffer, RUM_UNLOCK);
			freeRumBtreeStack(stack);

			/* insert into posting tree */
			gdi = rumPrepareScanPostingTree(rumstate->index, rootPostingTree, FALSE, attnum, rumstate);
			gdi->btree.isBuild = (buildStats != NULL);
			rumInsertItemPointers(rumstate, attnum, gdi, items, addInfo, addInfoIsNull, nitem, buildStats);
			pfree(gdi);

			return;
		}

		/* modify an existing leaf entry */
		itup = addItemPointersToLeafTuple(rumstate, itup,
										  items, addInfo, addInfoIsNull, nitem, buildStats);

		btree.isDelete = TRUE;
	}
	else
	{
		/* no match, so construct a new leaf entry */
		itup = buildFreshLeafTuple(rumstate, attnum, key, category,
								   items, addInfo, addInfoIsNull, nitem, buildStats);
	}

	/* Insert the new or modified leaf tuple */
	btree.entry = itup;
	rumInsertValue(rumstate->index, &btree, stack, buildStats);
	pfree(itup);
}

/*
 * Extract index entries for a single indexable item, and add them to the
 * BuildAccumulator's state.
 *
 * This function is used only during initial index creation.
 */
static void
rumHeapTupleBulkInsert(RumBuildState *buildstate, OffsetNumber attnum,
					   Datum value, bool isNull,
					   ItemPointer heapptr)
{
	Datum	   *entries;
	RumNullCategory *categories;
	int32		nentries;
	MemoryContext oldCtx;
	Datum	   *addInfo;
	bool	   *addInfoIsNull;
	int			i;
	Form_pg_attribute attr = buildstate->rumstate.addAttrs[attnum - 1];

	oldCtx = MemoryContextSwitchTo(buildstate->funcCtx);
	entries = rumExtractEntries(buildstate->accum.rumstate, attnum,
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

	rumInsertBAEntries(&buildstate->accum, heapptr, attnum,
					   entries, addInfo, addInfoIsNull, categories, nentries);

	buildstate->indtuples += nentries;

	MemoryContextReset(buildstate->funcCtx);
}

static void
rumBuildCallback(Relation index, HeapTuple htup, Datum *values,
				 bool *isnull, bool tupleIsAlive, void *state)
{
	RumBuildState *buildstate = (RumBuildState *) state;
	MemoryContext oldCtx;
	int			i;

	oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

	for (i = 0; i < buildstate->rumstate.origTupdesc->natts; i++)
		rumHeapTupleBulkInsert(buildstate, (OffsetNumber) (i + 1),
							   values[i], isnull[i],
							   &htup->t_self);

	/* If we've maxed out our available memory, dump everything to the index */
	if (buildstate->accum.allocatedMemory >= maintenance_work_mem * 1024L)
	{
		RumEntryAccumulatorItem *list;
		Datum		key;
		RumNullCategory category;
		uint32		nlist;
		OffsetNumber attnum;

		rumBeginBAScan(&buildstate->accum);
		while ((list = rumGetBAEntry(&buildstate->accum,
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
			rumEntryInsert(&buildstate->rumstate, attnum, key, category,
						   iptrs, addInfo, addInfoIsNull, nlist, &buildstate->buildStats);
		}

		MemoryContextReset(buildstate->tmpCtx);
		rumInitBA(&buildstate->accum);
	}

	MemoryContextSwitchTo(oldCtx);
}

IndexBuildResult *
rumbuild(Relation heap, Relation index, struct IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	double			reltuples;
	RumBuildState	buildstate;
	Buffer			RootBuffer,
					MetaBuffer;
	RumEntryAccumulatorItem *list;
	Datum			key;
	RumNullCategory	category;
	uint32			nlist;
	MemoryContext		oldCtx;
	OffsetNumber		attnum;
	GenericXLogState   *state;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

	initRumState(&buildstate.rumstate, index);
	buildstate.indtuples = 0;
	memset(&buildstate.buildStats, 0, sizeof(GinStatsData));

	state = GenericXLogStart(index);

	/* initialize the meta page */
	MetaBuffer = RumNewBuffer(index);
	/* initialize the root page */
	RootBuffer = RumNewBuffer(index);

	RumInitMetabuffer(state, MetaBuffer);
	RumInitBuffer(state, RootBuffer, RUM_LEAF);

	GenericXLogFinish(state);

	UnlockReleaseBuffer(MetaBuffer);
	UnlockReleaseBuffer(RootBuffer);

	/* count the root as first entry page */
	buildstate.buildStats.nEntryPages++;

	/*
	 * create a temporary memory context that is reset once for each tuple
	 * inserted into the index
	 */
	buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											  "Rum build temporary context",
											  ALLOCSET_DEFAULT_MINSIZE,
											  ALLOCSET_DEFAULT_INITSIZE,
											  ALLOCSET_DEFAULT_MAXSIZE);

	buildstate.funcCtx = AllocSetContextCreate(buildstate.tmpCtx,
					 "Rum build temporary context for user-defined function",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	buildstate.accum.rumstate = &buildstate.rumstate;
	rumInitBA(&buildstate.accum);

	/*
	 * Do the heap scan.  We disallow sync scan here because dataPlaceToPage
	 * prefers to receive tuples in TID order.
	 */
	reltuples = IndexBuildHeapScan(heap, index, indexInfo, false,
								   rumBuildCallback, (void *) &buildstate);

	/* dump remaining entries to the index */
	oldCtx = MemoryContextSwitchTo(buildstate.tmpCtx);
	rumBeginBAScan(&buildstate.accum);
	while ((list = rumGetBAEntry(&buildstate.accum,
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
		rumEntryInsert(&buildstate.rumstate, attnum, key, category,
					   iptrs, addInfo, addInfoIsNull, nlist, &buildstate.buildStats);
	}
	MemoryContextSwitchTo(oldCtx);

	MemoryContextDelete(buildstate.tmpCtx);

	/*
	 * Update metapage stats
	 */
	buildstate.buildStats.nTotalPages = RelationGetNumberOfBlocks(index);
	rumUpdateStats(index, &buildstate.buildStats);

	/*
	 * Return statistics
	 */
	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));

	result->heap_tuples = reltuples;
	result->index_tuples = buildstate.indtuples;

	return result;
}

/*
 *	rumbuildempty() -- build an empty rum index in the initialization fork
 */
void
rumbuildempty(Relation index)
{
	Buffer		RootBuffer,
				MetaBuffer;
	GenericXLogState   *state;

	state = GenericXLogStart(index);

	/* An empty RUM index has two pages. */
	MetaBuffer =
		ReadBufferExtended(index, INIT_FORKNUM, P_NEW, RBM_NORMAL, NULL);
	LockBuffer(MetaBuffer, BUFFER_LOCK_EXCLUSIVE);
	RootBuffer =
		ReadBufferExtended(index, INIT_FORKNUM, P_NEW, RBM_NORMAL, NULL);
	LockBuffer(RootBuffer, BUFFER_LOCK_EXCLUSIVE);

	/* Initialize and xlog metabuffer and root buffer. */
	RumInitMetabuffer(state, MetaBuffer);
	RumInitBuffer(state, RootBuffer, RUM_LEAF);

	GenericXLogFinish(state);

	/* Unlock and release the buffers. */
	UnlockReleaseBuffer(MetaBuffer);
	UnlockReleaseBuffer(RootBuffer);

	return ;
}

/*
 * Insert index entries for a single indexable item during "normal"
 * (non-fast-update) insertion
 */
static void
rumHeapTupleInsert(RumState *rumstate, OffsetNumber attnum,
				   Datum value, bool isNull,
				   ItemPointer item)
{
	Datum	   *entries;
	RumNullCategory *categories;
	int32		i,
				nentries;
	Datum	   *addInfo;
	bool	   *addInfoIsNull;

	entries = rumExtractEntries(rumstate, attnum, value, isNull,
								&nentries, &categories, &addInfo, &addInfoIsNull);

	for (i = 0; i < nentries; i++)
		rumEntryInsert(rumstate, attnum, entries[i], categories[i],
					   item, &addInfo[i], &addInfoIsNull[i], 1, NULL);
}

bool
ruminsert(Relation index, Datum *values, bool *isnull,
		  ItemPointer ht_ctid, Relation heapRel,
		  IndexUniqueCheck checkUnique)
{
	RumState	rumstate;
	MemoryContext oldCtx;
	MemoryContext insertCtx;
	int			i;

	insertCtx = AllocSetContextCreate(CurrentMemoryContext,
									  "Rum insert temporary context",
									  ALLOCSET_DEFAULT_MINSIZE,
									  ALLOCSET_DEFAULT_INITSIZE,
									  ALLOCSET_DEFAULT_MAXSIZE);

	oldCtx = MemoryContextSwitchTo(insertCtx);

	initRumState(&rumstate, index);

	if (RumGetUseFastUpdate(index))
	{
		RumTupleCollector collector;

		memset(&collector, 0, sizeof(RumTupleCollector));

		for (i = 0; i < rumstate.origTupdesc->natts; i++)
			rumHeapTupleFastCollect(&rumstate, &collector,
									(OffsetNumber) (i + 1),
									values[i], isnull[i],
									ht_ctid);

		rumHeapTupleFastInsert(&rumstate, &collector);
	}
	else
	{
		for (i = 0; i < rumstate.origTupdesc->natts; i++)
			rumHeapTupleInsert(&rumstate, (OffsetNumber) (i + 1),
							   values[i], isnull[i],
							   ht_ctid);
	}

	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(insertCtx);

	return false;
}
