/*-------------------------------------------------------------------------
 *
 * rum_debug_funcs.c
 *		Functions to investigate the content of RUM indexes
 *
 * Copyright (c) 2025, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/itup.h"
#include "access/relation.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/lockdefs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/varlena.h"
#include "tsearch/ts_type.h"

#include "rum.h"

PG_FUNCTION_INFO_V1(rum_metapage_info);
PG_FUNCTION_INFO_V1(rum_page_opaque_info);
PG_FUNCTION_INFO_V1(rum_page_items_info);

/*
 * Below are declarations of enums, structures, and macros
 * for the rum_page_items_info() function, which is used
 * to extract data from different types of pages in the RUM
 * index. Their use is implied only inside rum_debug_funcs.c.
 */

#define RumCurPitemAddInfoIsNormal(piState) \
	(!((piState)->curPitem.item.addInfoIsNull) && \
	(piState)->curKeyAddInfoOid != InvalidOid)

#define RumAddInfoIsPositions(piState) \
	((piState)->curKeyAddInfoOid == BYTEAOID)

#define RumIsEntryInternalHighKey(piState) \
	(RumPageRightMost((piState)->page) && \
	(piState)->curTupleNum == (piState)->maxoff)

#define RumIsDataPage(piState) \
	((piState)->pageType == LEAF_DATA_PAGE || \
	(piState)->pageType == INTERNAL_DATA_PAGE)

#define RumIsEntryPage(piState) \
	((piState)->pageType == LEAF_ENTRY_PAGE || \
	(piState)->pageType == INTERNAL_ENTRY_PAGE)

#define RumGetAddInfoAttr(piState) \
	((piState)->rumState->addAttrs[(piState)->curKeyAttnum - 1])

#define RumGetNewIndexTuple(piState) \
do { \
	(piState)->curItup = \
		(IndexTuple) PageGetItem((piState)->page, \
					PageGetItemId((piState)->page, \
					(piState)->curTupleNum)); \
} while(0)

#define RumGetNewItemPostingList(piState) \
do { \
	(piState)->itemPtr = \
		rumDataPageLeafRead((piState)->itemPtr, \
		(piState)->curKeyAttnum, \
		&((piState)->curPitem.item), \
		false, (piState)->rumState); \
} while(0);

#define RumWriteResAddInfoIsNullToValues(piState, counter) \
do { \
	(piState)->values[(counter)] = \
		BoolGetDatum((piState)->curPitem.item.addInfoIsNull); \
} while(0)

#if PG_VERSION_NUM >= 160000
#define RumWriteResIptrToValues(piState, counter) \
do { \
	(piState)->values[(counter)] = \
		ItemPointerGetDatum(&((piState)->curPitem.item.iptr)); \
} while(0)
#else
#define RumWriteResIptrToValues(piState, counter) \
do { \
	(piState)->values[(counter)] = \
		PointerGetDatum(&((piState)->curPitem.item.iptr)); \
} while(0)
#endif

#define RumWriteResBlckNumToValues(piState, counter) \
do { \
	(piState)->values[(counter)] = \
		UInt32GetDatum(RumPostingItemGetBlockNumber(&((piState)->curPitem))); \
} while(0)

#define RumWriteResAddInfoToValues(piState, counter) \
do { \
	(piState)->values[(counter)] = \
		get_datum_text_by_oid((piState)->curPitem.item.addInfo, \
		(piState)->curKeyAddInfoOid); \
} while(0)

#define RumWriteResAddInfoPosToValues(piState, counter) \
do { \
	(piState)->values[(counter)] = \
		get_positions_to_text_datum((piState)->curPitem.item.addInfo); \
} while(0)

#define RumReadHighKeyDataPage(piState) \
do { \
	memcpy(&((piState)->curPitem.item), \
		RumDataPageGetRightBound((piState)->page), \
		sizeof(RumItem)); \
} while(0)

#define RumReadKeyDataPage(piState) \
do { \
	memcpy(&((piState)->curPitem), \
		RumDataPageGetItem((piState)->page, \
		(piState)->srfFctx->call_cntr), sizeof(RumPostingItem)); \
} while(0)

#define RumPrepareResultTuple(piState) \
do { \
	(piState)->resultTuple = \
		heap_form_tuple((piState)->srfFctx->tuple_desc, \
		(piState)->values, (piState)->nulls); \
	(piState)->result = \
		HeapTupleGetDatum((piState)->resultTuple); \
} while(0)

#define RumPrepareCurPitemToPostingList(piState) \
	memset(&((piState)->curPitem), 0, sizeof(RumPostingItem))

/*
 * This is necessary in order for the prepare_scan()
 * function to determine the type of the scanned page.
 */
typedef enum pageTypeFlags
{
	LEAF_DATA_PAGE = 0,
	INTERNAL_DATA_PAGE = 1,
	LEAF_ENTRY_PAGE = 2,
	INTERNAL_ENTRY_PAGE = 3
} pageTypeFlags;

/*
 * The size of the result arrays (values
 * and nulls, see RumPageItemsStateData
 * structure) depends on the type of page.
 */
typedef enum pageTypeResSize
{
	LEAF_DATA_PAGE_RES_SIZE = 4,
	INTERNAL_DATA_PAGE_RES_SIZE = 5,
	LEAF_ENTRY_PAGE_RES_SIZE = 8,
	INTERNAL_ENTRY_PAGE_RES_SIZE = 5,
} pageTypeResSize;

/*
 * A structure that stores information between
 * calls to the rum_page_items_info() function.
 * This information is necessary to scan the page.
 */
typedef struct RumPageItemsStateData
{
	/*
	 * A pointer to the RumState structure
	 * that describes the scanned index.
	 */
	RumState		*rumState;

	/* Scanned page info  */
	Page			page;
	uint32			pageNum;

	/*
	 * The type of the scanned page, can be:
	 * {} -- INTERNAL_ENTRY_PAGE
	 * {leaf} -- LEAF_ENTRY_PAGE
	 * {data} -- INTERNAL_DATA_PAGE
	 * {data, leaf} -- LEAF_DATA_PAGE
	 */
	pageTypeFlags	pageType;

	/*
	 * The number of scanned items per page.
	 *
	 * On the {leaf, data} page, this is the number of
	 * RumItem structures that are in the compressed posting list.
	 *
	 * On the {data} page, this is the number of RumPostingItem structures.
	 *
	 * On the {leaf} page, this is the number of IndexTuple, each of
	 * which contains a compressed posting list. In this case, the size
	 * of the Posting list is determined using RumGetNPosting(itup).
	 *
	 * On the {} page, this is the number of IndexTuple.
	 */
	int				maxoff;

	/* Pointer to the current scanning item */
	Pointer			itemPtr;

	/*
	 * It is used where posting lists are scanned.
	 * Sometimes only the RumItem it contains is used.
	 */
	RumPostingItem	curPitem;

	/* Current IndexTuple on the page */
	IndexTuple		curItup;

	/* The number of the current IndexTuple on the page */
	OffsetNumber	curTupleNum;

	/* The number of the current element in the current IndexTuple */
	int				curTupleItemNum;

	/*
	 * The number of the child page that
	 * is stored in the current IndexTuple
	 */
	BlockNumber		curTupleDownLink;

	/*
	 * If the current IndexTuple is scanned, then
	 * you need to move on to the next one.
	 */
	bool			needNewTuple;

	/*
	 * Parameters of the current key in the IndexTuple
	 * or the key for which the posting tree was built.
	 */
	OffsetNumber	curKeyAttnum;
	Datum			curKey;
	RumNullCategory	curKeyCategory;
	Oid				curKeyOid;

	/* Information about the type of additional information */
	bool			curKeyAddInfoIsNull;
	Oid				curKeyAddInfoOid;
	bool			curKeyAddInfoByval;

	/*
	 * To generate the results of each
	 * function call rum_page_items_info()
	 */
	Datum			result;
	HeapTuple		resultTuple;
	Datum			*values;
	bool			*nulls;
	FuncCallContext	*srfFctx;
} RumPageItemsStateData;

typedef RumPageItemsStateData *RumPageItemsState;

/*
 * This function and get_rel_raw_page() are derived
 * from the separation of the get_raw_page_internal()
 * function, which was copied from the pageinspect code.
 * It is needed in order to call the initRumState()
 * function if necessary.
 */
static Relation
get_rel_from_name(text *relName)
{
	RangeVar   *relrv;
	Relation	rel;

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relName));
	rel = relation_openrv(relrv, AccessShareLock);

#if PG_VERSION_NUM >= 150000
	if (!RELKIND_HAS_STORAGE(rel->rd_rel->relkind))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot get raw page from relation \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail_relkind_not_supported(rel->rd_rel->relkind)));
#else
	/* Check that this relation has storage */
	if (rel->rd_rel->relkind == RELKIND_VIEW)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot get raw page from view \"%s\"",
						RelationGetRelationName(rel))));
	if (rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot get raw page from composite type \"%s\"",
						RelationGetRelationName(rel))));
	if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot get raw page from foreign table \"%s\"",
						RelationGetRelationName(rel))));
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot get raw page from partitioned table \"%s\"",
						RelationGetRelationName(rel))));
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot get raw page from partitioned index \"%s\"",
						RelationGetRelationName(rel))));
#endif

	/*
	 * Reject attempts to read non-local temporary relations; we would be
	 * likely to get wrong data since we have no visibility into the owning
	 * session's local buffers.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions")));

	return rel;
}

/*
 * Get a copy of the relation page.
 */
static Page
get_rel_page(Relation rel, BlockNumber blkNo)
{
	Buffer		buf;
	Page		page;

	if (blkNo >= RelationGetNumberOfBlocksInFork(rel, MAIN_FORKNUM))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("block number %u is out of range for relation \"%s\"",
						blkNo, RelationGetRelationName(rel))));

	buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkNo, RBM_NORMAL, NULL);
	if (!BufferIsValid(buf))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not read block %u of relation \"%s\"",
						blkNo, RelationGetRelationName(rel))));

	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = (Page) palloc(BLCKSZ);
	memcpy(page, BufferGetPage(buf), BLCKSZ);
	LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buf);

	return page;
}

/*
 * Functions for checks.
 */
static void
check_superuser(void)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use this function")));
}

static void
check_page_opaque_data_size(Page page)
{
	if (PageGetSpecialSize(page) != MAXALIGN(sizeof(RumPageOpaqueData)))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("input page is not a valid RUM metapage"),
				 errdetail("Expected special size %d, got %d.",
						   (int) MAXALIGN(sizeof(RumPageOpaqueData)),
						   (int) PageGetSpecialSize(page))));
}

static void
check_page_is_meta_page(RumPageOpaque opaq)
{
	if (opaq->flags != RUM_META)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("input page is not a RUM metapage"),
				 errdetail("Flags %04X, expected %04X",
						   opaq->flags, RUM_META)));
}

static void
check_page_is_leaf_data_page(RumPageOpaque opaq)
{
	if (opaq->flags != (RUM_DATA | RUM_LEAF))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("input page is not a RUM {data, leaf} page"),
				 errdetail("Flags %04X, expected %04X",
						   opaq->flags, (RUM_DATA | RUM_LEAF))));
}

static void
check_page_is_internal_data_page(RumPageOpaque opaq)
{
	if (opaq->flags != (RUM_DATA & ~RUM_LEAF))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("input page is not a RUM {data} page"),
				 errdetail("Flags %04X, expected %04X",
						   opaq->flags, (RUM_DATA & ~RUM_LEAF))));
}

static void
check_page_is_leaf_entry_page(RumPageOpaque opaq)
{
	if (opaq->flags != RUM_LEAF)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("input page is not a RUM {leaf} page"),
				 errdetail("Flags %04X, expected %04X",
						   opaq->flags, RUM_LEAF)));
}

static void
check_page_is_internal_entry_page(RumPageOpaque opaq)
{
	if (opaq->flags != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("input page is not a RUM {} page"),
				 errdetail("Flags %04X, expected %04X",
						   opaq->flags, 0)));
}

/*
 * The function is used to output
 * information stored in Datum as text.
 */
static Datum
get_datum_text_by_oid(Datum info, Oid infoId)
{
	Oid			funcId;
	bool		isVarlena = false;
	char	   *infoStr;

	Assert(OidIsValid(infoId));

	getTypeOutputInfo(infoId, &funcId, &isVarlena);

	infoStr = OidOutputFunctionCall(funcId, info);

	return CStringGetTextDatum(infoStr);
}

/*
 * This function returns the key category as text.
 */
static Datum
category_get_datum_text(RumNullCategory category)
{
	char		categoryArr[][20] = {"RUM_CAT_NORM_KEY",
									 "RUM_CAT_NULL_KEY",
									 "RUM_CAT_EMPTY_ITEM",
									 "RUM_CAT_NULL_ITEM",
									 "RUM_CAT_EMPTY_QUERY"};

	switch (category)
	{
		case RUM_CAT_NORM_KEY:
			return CStringGetTextDatum(categoryArr[0]);

		case RUM_CAT_NULL_KEY:
			return CStringGetTextDatum(categoryArr[1]);

		case RUM_CAT_EMPTY_ITEM:
			return CStringGetTextDatum(categoryArr[2]);

		case RUM_CAT_NULL_ITEM:
			return CStringGetTextDatum(categoryArr[3]);

		case RUM_CAT_EMPTY_QUERY:
			return CStringGetTextDatum(categoryArr[4]);
	}

	/* In case of an error, return 0 */
	return (Datum) 0;
}

/*
 * The function extracts the weight and
 * returns the corresponding letter.
 */
static char
pos_get_weight(WordEntryPos position)
{
	char		res = 'D';

	switch (WEP_GETWEIGHT(position))
	{
		case 3:
			return 'A';
		case 2:
			return 'B';
		case 1:
			return 'C';
	}

	return res;
}

/*
 * A function for extracting the positions of lexemes
 * from additional information. Returns a string in
 * which the positions of the lexemes are recorded.
 */

#define POS_STR_BUF_LENGTH 1024
#define POS_MAX_VAL_LENGTH 16

static Datum
get_positions_to_text_datum(Datum addInfo)
{
	bytea	   *positions;
	char	   *ptrt;
	WordEntryPos position = 0;
	int32		npos;

	Datum		res;
	char	   *positionsStr;
	char	   *positionsStrCurPtr;
	int			curMaxStrLenght;

	positions = DatumGetByteaP(addInfo);
	ptrt = (char *) VARDATA_ANY(positions);
	npos = count_pos(VARDATA_ANY(positions),
					 VARSIZE_ANY_EXHDR(positions));

	/* Initialize the string */
	positionsStr = (char *) palloc(POS_STR_BUF_LENGTH * sizeof(char));
	positionsStr[0] = '\0';
	curMaxStrLenght = POS_STR_BUF_LENGTH - 1;
	positionsStrCurPtr = positionsStr;

	/* Extract the positions of the lexemes and put them in the string */
	for (int i = 0; i < npos; i++)
	{
		/* At each iteration decode the position */
		ptrt = decompress_pos(ptrt, &position);

		/* Write this position and weight in the string */
		if (pos_get_weight(position) == 'D')
			sprintf(positionsStrCurPtr, "%d,", WEP_GETPOS(position));
		else
			sprintf(positionsStrCurPtr, "%d%c,",
					WEP_GETPOS(position), pos_get_weight(position));

		/* Moving the pointer forward */
		positionsStrCurPtr += strlen(positionsStrCurPtr);

		/*
		 * Check that there is not too little left to the end of the line and,
		 * if necessary, overspend the memory.
		 */
		if (curMaxStrLenght - (positionsStrCurPtr - positionsStr)
			<= POS_MAX_VAL_LENGTH)
		{
			curMaxStrLenght += POS_STR_BUF_LENGTH;
			positionsStr = (char *) repalloc(positionsStr,
											 curMaxStrLenght * sizeof(char));
			positionsStrCurPtr = positionsStr + strlen(positionsStr);
		}
	}

	/*
	 * Delete the last comma if there has been at least one iteration of the
	 * loop.
	 */
	if (npos > 0)
		positionsStr[strlen(positionsStr) - 1] = '\0';

	res = CStringGetTextDatum(positionsStr);
	pfree(positionsStr);
	return res;
}

/*
 * This function gets the attribute number of the
 * current tuple key from the RumState structure.
 */
static Oid
get_cur_tuple_key_oid(RumPageItemsState piState)
{
	TupleDesc	origTupleDesc;
	OffsetNumber attnum;

	attnum = piState->curKeyAttnum;
	origTupleDesc = piState->rumState->origTupdesc;

	return TupleDescAttr(origTupleDesc, attnum - 1)->atttypid;
}

/*
 * The function is used to extract values
 * from a previously read IndexTuple.
 */
static void
get_entry_index_tuple_values(RumPageItemsState piState)
{
	RumState   *rumState = piState->rumState;

	/* Scanning the IndexTuple */
	piState->curKeyAttnum = rumtuple_get_attrnum(rumState, piState->curItup);

	piState->curKey = rumtuple_get_key(rumState,
									   piState->curItup,
									   &(piState->curKeyCategory));

	piState->curKeyOid = get_cur_tuple_key_oid(piState);

	if (piState->pageType == INTERNAL_ENTRY_PAGE)
		piState->curTupleDownLink = RumGetDownlink(piState->curItup);

	if (piState->pageType == LEAF_ENTRY_PAGE &&
		RumGetAddInfoAttr(piState))
	{
		piState->curKeyAddInfoOid = RumGetAddInfoAttr(piState)->atttypid;
		piState->curKeyAddInfoByval = RumGetAddInfoAttr(piState)->attbyval;
	}
}

/*
 * The leaf and internal pages of the Entry Tree contain IndexTuples.
 * On leaf pages, IndexTuples contain the key and the posting list
 * (or a link to the posting tree) that relates to it. On internal
 * pages, IndexTuples contain a key and a link to a child page.
 *
 * This function is used to put the key values in the result arrays
 * values and nulls.
 */
static void
write_res_cur_tuple_key_to_values(RumPageItemsState piState)
{
	int			counter = 0;

	if (piState->curKeyCategory == RUM_CAT_NORM_KEY)
		piState->values[counter++] = get_datum_text_by_oid(piState->curKey,
														   piState->curKeyOid);
	else
		piState->nulls[counter++] = true;

	piState->values[counter++] = UInt16GetDatum(piState->curKeyAttnum);

	piState->values[counter++] =
		category_get_datum_text(piState->curKeyCategory);

	if (piState->pageType == INTERNAL_ENTRY_PAGE)
		piState->values[counter] = UInt32GetDatum(piState->curTupleDownLink);
}

/*
 * This function is designed to search for a
 * link to a target page on the pages of the
 * posting tree. It sequentially scans each
 * level of the posting tree and returns true
 * if a link to the target page was found on
 * any of the pages of the posting tree.
 */
static bool
find_page_in_posting_tree(BlockNumber targetPageNum,
						  BlockNumber curPageNum,
						  RumState * rumState)
{
	Page		curPage;
	RumPageOpaque curOpaq;
	RumPostingItem curPitem;
	BlockNumber nextPageNum;

	/* Page loop */
	for (;;)
	{
		curPage = get_rel_page(rumState->index, curPageNum);

		/* The page cannot be new */
		if (PageIsNew(curPage))
		{
			pfree(curPage);
			return false;
		}

		/* Getting a page description from an opaque area */
		curOpaq = RumPageGetOpaque(curPage);

		/* If this is a leaf page, we stop the loop */
		if (curOpaq->flags == (RUM_DATA | RUM_LEAF))
		{
			pfree(curPage);
			return false;
		}

		/*
		 * Reading the first RumPostingItem from the current page. This is
		 * necessary to remember the link down.
		 */
		memcpy(&curPitem,
			   RumDataPageGetItem(curPage, 1), sizeof(RumPostingItem));
		nextPageNum = RumPostingItemGetBlockNumber(&curPitem);

		/* The loop that scans the page */
		for (int i = 1; i <= curOpaq->maxoff; i++)
		{
			/* Reading the RumPostingItem from the current page */
			memcpy(&curPitem,
				   RumDataPageGetItem(curPage, i), sizeof(RumPostingItem));

			if (targetPageNum == RumPostingItemGetBlockNumber(&curPitem))
			{
				pfree(curPage);
				return true;
			}
		}

		/* Go to the next page */

		/* If a step to the right is impossible, step down */
		if (curOpaq->rightlink == InvalidBlockNumber)
			curPageNum = nextPageNum;

		/* Step to the right */
		else
			curPageNum = curOpaq->rightlink;

		pfree(curPage);
	}
}

/*
 * This function is used to sequentially find
 * the roots of the posting tree. In the first
 * call, *curPageNum should be the leftmost
 * leaf page of the entry tree, and *curTupleNum
 * should be equal to FirstOffsetNumber. Then, for
 * each call, the function will return the root
 * number of the posting tree until they exhaust.
 */
static bool
find_posting_tree_root(BlockNumber *curPageNum,
					   OffsetNumber *curTupleNum,
					   OffsetNumber *curKeyAttnum,
					   BlockNumber *postingRootNum,
					   RumState * rumState)
{
	Page		curPage;
	RumPageOpaque curOpaq;
	IndexTuple	curItup;

	for (;;)
	{
		/* Getting rel by name and page by number */
		curPage = get_rel_page(rumState->index, *curPageNum);

		/* The page cannot be new */
		if (PageIsNew(curPage))
			break;

		/* Getting a page description from an opaque area */
		curOpaq = RumPageGetOpaque(curPage);

		Assert(curOpaq->flags == RUM_LEAF);

		/* Scanning current page */
		while (*curTupleNum <= PageGetMaxOffsetNumber(curPage))
		{
			curItup = (IndexTuple)
				PageGetItem(curPage, PageGetItemId(curPage, *curTupleNum));

			(*curTupleNum)++;

			*curKeyAttnum = rumtuple_get_attrnum(rumState, curItup);

			if (RumIsPostingTree(curItup))
			{
				*postingRootNum = RumGetPostingTree(curItup);
				pfree(curPage);
				return true;
			}
		}

		/*
		 * If haven't found anything, need to move on or terminate the
		 * function if the pages are over.
		 */
		if (curOpaq->rightlink == InvalidBlockNumber)
			break;
		else
		{
			*curPageNum = curOpaq->rightlink;
			*curTupleNum = FirstOffsetNumber;
			pfree(curPage);
		}
	}

	/* Error case */
	*curPageNum = InvalidBlockNumber;
	*postingRootNum = InvalidBlockNumber;
	*curTupleNum = InvalidOffsetNumber;
	*curKeyAttnum = InvalidOffsetNumber;
	pfree(curPage);
	return false;
}

/*
 * The function is used to find the number
 * of the leftmost leaf page of the entry tree.
 */
static BlockNumber
find_min_entry_leaf_page(RumPageItemsState piState)
{
	RumState   *rumState = piState->rumState;

	/*
	 * The page search starts from the first internal page of the entry tree.
	 */
	BlockNumber curPageNum = 1;
	Page		curPage;
	RumPageOpaque curOpaq;
	IndexTuple	curItup;

	for (;;)
	{
		/* Getting page by number */
		curPage = get_rel_page(rumState->index, curPageNum);

		/* The page cannot be new */
		if (PageIsNew(curPage))
			return InvalidOffsetNumber;

		/* Getting a page description from an opaque area */
		curOpaq = RumPageGetOpaque(curPage);

		/* If the required page is found */
		if (curOpaq->flags == RUM_LEAF && RumPageLeftMost(curPage))
		{
			pfree(curPage);
			return curPageNum;
		}

		/* If curPage is still an internal page */
		else if (curOpaq->flags == 0)
		{
			/* Read the first IndexTuple */
			curItup = (IndexTuple) PageGetItem(curPage,
											   PageGetItemId(curPage, 1));

			/* Step onto the child page */
			curPageNum = RumGetDownlink(curItup);
			pfree(curPage);
		}

		else					/* Error case */
		{
			pfree(curPage);
			return InvalidBlockNumber;
		}
	}
}

/*
 * When scanning a posting tree page, the key used to build the posting tree and
 * the corresponding attribute number are not known. This function determines
 * the attribute number of the key for which the posting tree was built.
 *
 * First, the function descends to the leftmost leaf page of the entry tree,
 * then searches for links to the posting tree there. In each posting tree,
 * it searches for the page number being scanned. If the desired page is found,
 * it returns the key attribute number contained in the IndexTuple, along with
 * a reference to the posting tree. If nothing is found, the function returns
 * InvalidOffsetNumber.
 */
static OffsetNumber
find_attnum_posting_tree_key(RumPageItemsState piState)
{
	BlockNumber targetPageNum = piState->pageNum;
	RumState   *rumState = piState->rumState;

	/* Returned result */
	OffsetNumber keyAttnum = InvalidOffsetNumber;

	/*
	 * The page search starts from the first internal page of the entry tree.
	 */
	BlockNumber curPageNum = 1;
	OffsetNumber curTupleNum = FirstOffsetNumber;
	BlockNumber postingRootNum = InvalidBlockNumber;

	/* Search for the leftmost leaf page of the entry tree */
	curPageNum = find_min_entry_leaf_page(piState);

	/*
	 * At each iteration of the loop, we find the root of the posting tree,
	 * then we search for the desired page in this posting tree. The loop ends
	 * when a page is found, or when there is no longer a posting tree.
	 */
	while (find_posting_tree_root(&curPageNum, &curTupleNum,
								  &keyAttnum, &postingRootNum, rumState))
	{
		if (postingRootNum == targetPageNum ||
			find_page_in_posting_tree(targetPageNum,
									  postingRootNum, rumState))
			break;
	}

	return keyAttnum;
}

/*
 * An auxiliary function for preparing the scan.
 * Depending on the type of page, it fills in
 * piState and makes the necessary checks.
 */
static bool
prepare_scan(text *relName, uint32 blkNo,
			 RumPageItemsState * piState,
			 FuncCallContext *srfFctx,
			 pageTypeFlags pageType)
{
	Relation	rel;			/* needed to initialize the RumState structure */

	Page		page;			/* the page to be scanned */
	RumPageOpaque opaq;			/* data from the opaque area of the page */

	int			resSize;

	/* Getting rel by name and page by number */
	rel = get_rel_from_name(relName);
	page = get_rel_page(rel, blkNo);

	/* The page cannot be new */
	if (PageIsNew(page))
		return false;

	/* Checking the size of the opaque area of the page */
	check_page_opaque_data_size(page);

	/* Getting a page description from an opaque area */
	opaq = RumPageGetOpaque(page);

	/* Allocating memory for a long-lived structure */
	*piState = palloc(sizeof(RumPageItemsStateData));

	/* Initializing the RumState structure */
	(*piState)->rumState = palloc(sizeof(RumState));
	initRumState((*piState)->rumState, rel);

	relation_close(rel, AccessShareLock);

	/* Writing the page and page type into a long-lived structure */
	(*piState)->srfFctx = srfFctx;
	(*piState)->page = page;
	(*piState)->pageNum = blkNo;
	(*piState)->pageType = pageType;

	/* The number of results returned depends on the type of page */
	if ((*piState)->pageType == LEAF_DATA_PAGE)
		resSize = LEAF_DATA_PAGE_RES_SIZE;

	else if ((*piState)->pageType == INTERNAL_DATA_PAGE)
		resSize = INTERNAL_DATA_PAGE_RES_SIZE;

	else if ((*piState)->pageType == LEAF_ENTRY_PAGE)
		resSize = LEAF_ENTRY_PAGE_RES_SIZE;

	else
		resSize = INTERNAL_ENTRY_PAGE_RES_SIZE;

	/* Allocating memory for arrays of results */
	(*piState)->values = (Datum *) palloc(resSize * sizeof(Datum));
	(*piState)->nulls = (bool *) palloc(resSize * sizeof(bool));

	/*
	 * Depending on the type of page, it performs the necessary checks and
	 * writes the necessary data into a long-lived structure.
	 */
	if (RumIsDataPage(*piState))
	{
		if ((*piState)->pageType == LEAF_DATA_PAGE)
			check_page_is_leaf_data_page(opaq);

		else
			check_page_is_internal_data_page(opaq);

		(*piState)->maxoff = opaq->maxoff;
		(*piState)->itemPtr = RumDataPageGetData(page);

		/*
		 * If the scanned page belongs to a posting tree, we do not know which
		 * key this posting tree was built for. However, we need to know the
		 * attribute number of the key in order to correctly determine the
		 * type of additional information that can be associated with it.
		 *
		 * The find_attnum_posting_tree_key() function is used to find the key
		 * attribute number. The function scans the index and searches for the
		 * page we are scanning in the posting tree, while remembering which
		 * key this posting tree was built for.
		 */
		(*piState)->curKeyAttnum = find_attnum_posting_tree_key(*piState);

		/* Error handling find_attnum_posting_tree_key() */
		if ((*piState)->curKeyAttnum == InvalidOffsetNumber)
			return false;

		if (RumGetAddInfoAttr(*piState))
		{
			(*piState)->curKeyAddInfoOid =
				RumGetAddInfoAttr(*piState)->atttypid;
			(*piState)->curKeyAddInfoByval =
				RumGetAddInfoAttr(*piState)->attbyval;
		}
	}

	else						/* The entry tree page case */
	{
		if ((*piState)->pageType == LEAF_ENTRY_PAGE)
		{
			check_page_is_leaf_entry_page(opaq);

			(*piState)->needNewTuple = true;
		}

		else
			check_page_is_internal_entry_page(opaq);

		(*piState)->maxoff = PageGetMaxOffsetNumber(page);
		(*piState)->curTupleNum = FirstOffsetNumber;
	}

	return true;
}

/*
 * An auxiliary function for reading information from leaf
 * and internal pages of the Posting Tree. For each call,
 * it returns the next result to be returned from the
 * rum_page_items_info() function.
 */

#define VARLENA_MSG "varlena types in posting tree is " \
					"not supported"

static void
data_page_get_next_result(RumPageItemsState piState)
{
	int			counter = 0;

	/* Before returning the result, need to reset the nulls array */
	if (piState->pageType == LEAF_DATA_PAGE)
		memset(piState->nulls, 0,
			   LEAF_DATA_PAGE_RES_SIZE * sizeof(bool));
	else
		memset(piState->nulls, 0,
			   INTERNAL_DATA_PAGE_RES_SIZE * sizeof(bool));

	Assert(RumIsDataPage(piState));

	/* Reading high key */
	if (piState->srfFctx->call_cntr == 0)
		RumReadHighKeyDataPage(piState);

	/* Reading information from Posting List */
	else if (piState->pageType == LEAF_DATA_PAGE)
	{
		/*
		 * it is necessary for the correct reading of the tid (see the
		 * function rumdatapageleafread())
		 */
		if (piState->srfFctx->call_cntr == 1)
			RumPrepareCurPitemToPostingList(piState);

		/* Read new item */
		RumGetNewItemPostingList(piState);
	}

	/* Reading information from the internal data page */
	else
		RumReadKeyDataPage(piState);

	/* Write the read information into arrays of results */

	/*
	 * This means whether the result tuple is the high key or not.
	 */
	if (piState->srfFctx->call_cntr == 0)
	{
		piState->values[counter++] = BoolGetDatum(true);

		if (piState->pageType == INTERNAL_DATA_PAGE)
			piState->nulls[counter++] = true;
	}

	else						/* If the result is not the high key */
	{
		piState->values[counter++] = BoolGetDatum(false);

		if (piState->pageType == INTERNAL_DATA_PAGE)
			RumWriteResBlckNumToValues(piState, counter++);
	}

	RumWriteResIptrToValues(piState, counter++);
	RumWriteResAddInfoIsNullToValues(piState, counter++);

	/*
	 * Return of additional information depends on the type of page and the
	 * type of additional information.
	 */
	if (RumCurPitemAddInfoIsNormal(piState))
	{
		if (piState->pageType == LEAF_DATA_PAGE &&
			piState->srfFctx->call_cntr != 0)
		{
			if (RumAddInfoIsPositions(piState))
				RumWriteResAddInfoPosToValues(piState, counter);

			else
				RumWriteResAddInfoToValues(piState, counter);
		}

		else					/* If the page is internal or result is high
								 * key */
		{
			if (piState->curKeyAddInfoByval == false)
				piState->values[counter] = CStringGetTextDatum(VARLENA_MSG);
			else
				RumWriteResAddInfoToValues(piState, counter);
		}
	}

	/* If no additional information is available */
	else
		piState->nulls[counter] = true;

	/* Forming the returned tuple */
	RumPrepareResultTuple(piState);
}

/*
 * IndexTuples are located on the internal pages of the Etnry Tree.
 * Each IndexTuple contains a key and a link to a child page. This
 * function reads these values and generates the result tuple.
 */
static void
entry_internal_page_get_next_result(RumPageItemsState piState)
{
	Assert(piState->pageType == INTERNAL_ENTRY_PAGE);

	/* Before returning the result, need to reset the nulls array */
	memset(piState->nulls, 0, INTERNAL_ENTRY_PAGE_RES_SIZE * sizeof(bool));

	/* Read the new IndexTuple */
	RumGetNewIndexTuple(piState);

	/* Scanning the IndexTuple that we received earlier */
	get_entry_index_tuple_values(piState);

	/*
	 * On the rightmost page, in the last IndexTuple, there is a high key,
	 * which is assumed to be equal to +inf.
	 */
	if (RumIsEntryInternalHighKey(piState))
	{
		piState->values[0] = CStringGetTextDatum("+inf");
		piState->nulls[1] = true;
		piState->nulls[2] = true;
		piState->values[3] = UInt32GetDatum(piState->curTupleDownLink);
	}

	/* Is not high key */
	else
		write_res_cur_tuple_key_to_values(piState);

	/* Forming the returned tuple */
	RumPrepareResultTuple(piState);

	/* Increase the counter before the next SRF call */
	piState->curTupleNum++;
}

/*
 * The Entry Tree leaf pages contain IndexTuples containing
 * the key and either a compressed posting list or a link to
 * the root page of the Posting Tree. This function reads all
 * values from posting list and generates the result tuple.
 */
static void
get_entry_leaf_posting_list_result(RumPageItemsState piState)
{
	/*
	 * Start writing from 3, because the previous ones are occupied by a
	 * cur_tuple_key
	 */
	int			counter = 3;

	Assert(piState->pageType == LEAF_ENTRY_PAGE);

	/* Reading the RumItem structures from the IndexTuple */
	RumGetNewItemPostingList(piState);

	/* Write the read information into arrays of results */
	write_res_cur_tuple_key_to_values(piState);
	RumWriteResIptrToValues(piState, counter++);
	RumWriteResAddInfoIsNullToValues(piState, counter++);

	if (RumCurPitemAddInfoIsNormal(piState))
	{
		if (RumAddInfoIsPositions(piState))
			RumWriteResAddInfoPosToValues(piState, counter++);

		else
			RumWriteResAddInfoToValues(piState, counter++);
	}

	else
		piState->nulls[counter++] = true;

	/* The current IndexTuple does not contain a posting tree */
	piState->values[counter++] = BoolGetDatum(false);
	piState->nulls[counter] = true;

	/*
	 * If the current IndexTuple has ended, i.e. we have scanned all its
	 * RumItems, then we need to enable the need_new_tuple flag so that the
	 * next time the function is called, we can read a new IndexTuple from the
	 * page.
	 */
	piState->curTupleItemNum++;
	if (piState->curTupleItemNum >
		RumGetNPosting(piState->curItup))
		piState->needNewTuple = true;

	/* Forming the returned tuple */
	RumPrepareResultTuple(piState);
}

/*
 * This function is used to prepare for scanning
 * the posting list on Entry Tree leaf pages.
 */
static void
prepare_new_entry_leaf_posting_list(RumPageItemsState piState)
{
	Assert(piState->pageType == LEAF_ENTRY_PAGE);

	/* Getting the posting list */
	piState->itemPtr = RumGetPosting(piState->curItup);
	piState->curTupleItemNum = 1;
	piState->needNewTuple = false;
	piState->curTupleNum++;

	/*
	 * Every time you read a new IndexTuple, you need to reset the tid for the
	 * rumDataPageLeafRead() function to work correctly.
	 */
	RumPrepareCurPitemToPostingList(piState);
}

/*
 * The Entry Tree leaf pages contain IndexTuples containing
 * the key and either a compressed posting list or a link to
 * the root page of the Posting Tree. This function reads all
 * values from Posting Tree and generates the result tuple.
 */
static void
get_entry_leaf_posting_tree_result(RumPageItemsState piState)
{
	/*
	 * Start writing from 3, because the previous ones are occupied by a
	 * cur_tuple_key
	 */
	int			counter = 3;

	Assert(piState->pageType == LEAF_ENTRY_PAGE);

	/* Returning the key value */
	write_res_cur_tuple_key_to_values(piState);

	/* Everything stored in the RumItem structure has a NULL value */
	piState->nulls[counter++] = true;
	piState->nulls[counter++] = true;
	piState->nulls[counter++] = true;

	/* Returning the root of the posting tree */
	piState->values[counter++] = true;
	piState->values[counter++] =
		UInt32GetDatum(RumGetPostingTree(piState->curItup));

	/* Forming the returned tuple */
	RumPrepareResultTuple(piState);

	/* The next call will require a new IndexTuple */
	piState->needNewTuple = true;
}

/*
 * The function reads information from compressed Posting lists on
 * Entry Tree leaf pages, each of which is located in the
 * corresponding IndexTuple. Therefore, first, if the previous
 * IndexTuple has ended, the new one is read. After that, the
 * current IndexTuple is scanned until it runs out. The IndexTuple
 * themselves are read until they end on the page.
 */
static void
entry_leaf_page_get_next_result(RumPageItemsState piState)
{
	Assert(piState->pageType == LEAF_ENTRY_PAGE);

	/* Before returning the result, need to reset the nulls array */
	memset(piState->nulls, 0, LEAF_ENTRY_PAGE_RES_SIZE * sizeof(bool));

	if (piState->needNewTuple)
	{
		/* Read the new IndexTuple */
		RumGetNewIndexTuple(piState);

		/* Getting key and key attribute number */
		get_entry_index_tuple_values(piState);

		/* Getting the posting list */
		prepare_new_entry_leaf_posting_list(piState);

		/*
		 * The case when there is a posting tree instead of a compressed
		 * posting list
		 */
		if (RumIsPostingTree(piState->curItup))
		{
			get_entry_leaf_posting_tree_result(piState);
			return;
		}
	}

	get_entry_leaf_posting_list_result(piState);
}

/*
 * The rum_metapage_info() function is used to retrieve
 * information stored on the meta page of the rum index.
 * To scan, need the index name and the page number.
 * (for the meta page blkNo = 0).
 */
Datum
rum_metapage_info(PG_FUNCTION_ARGS)
{
	/* Reading input arguments */
	text	   *relName = PG_GETARG_TEXT_PP(0);
	uint32		blkNo = PG_GETARG_UINT32(1);

	Relation	rel;			/* needed to initialize the RumState structure */

	RumPageOpaque opaq;			/* data from the opaque area of the page */
	RumMetaPageData *metaData;	/* data stored on the meta page */
	Page		page;			/* the page to be scanned */

	TupleDesc	tupDesc;		/* description of the result tuple */
	HeapTuple	resultTuple;	/* for the results */
	Datum		values[10];		/* return values */
	bool		nulls[10];		/* true if the corresponding value is NULL */

	/*
	 * To output the index version. If you change the index version, you may
	 * need to increase the buffer size.
	 */
	char		versionBuf[20];

	/* Only the superuser can use this */
	check_superuser();

	/* Getting rel by name and page by number */
	rel = get_rel_from_name(relName);
	page = get_rel_page(rel, blkNo);
	relation_close(rel, AccessShareLock);

	/* If the page is new, the function should return NULL */
	if (PageIsNew(page))
		PG_RETURN_NULL();

	/* Checking the size of the opaque area of the page */
	check_page_opaque_data_size(page);

	/* Getting a page description from an opaque area */
	opaq = RumPageGetOpaque(page);

	/* Checking the flags */
	check_page_is_meta_page(opaq);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Getting information from the meta page */
	metaData = RumPageGetMeta(page);

	memset(nulls, 0, sizeof(nulls));

	/*
	 * Writing data from metaData to values.
	 *
	 * The first five values are obsolete because the pending list was removed
	 * from the rum index.
	 */
	values[0] = Int64GetDatum(metaData->head);
	values[1] = Int64GetDatum(metaData->tail);
	values[2] = Int32GetDatum(metaData->tailFreeSize);
	values[3] = Int64GetDatum(metaData->nPendingPages);
	values[4] = Int64GetDatum(metaData->nPendingHeapTuples);
	values[5] = Int64GetDatum(metaData->nTotalPages);
	values[6] = Int64GetDatum(metaData->nEntryPages);
	values[7] = Int64GetDatum(metaData->nDataPages);
	values[8] = Int64GetDatum(metaData->nEntries);
	snprintf(versionBuf, sizeof(versionBuf), "0x%X", metaData->rumVersion);
	values[9] = CStringGetTextDatum(versionBuf);

	pfree(page);

	/* Build and return the result tuple */
	resultTuple = heap_form_tuple(tupDesc, values, nulls);

	/* Returning the result */
	return HeapTupleGetDatum(resultTuple);
}

/*
 * The rum_page_opaque_info() function is used to retrieve
 * information stored in the opaque area of the index rum
 * page. To scan, need the index name and the page number.
 */
Datum
rum_page_opaque_info(PG_FUNCTION_ARGS)
{
	/* Reading input arguments */
	text	   *relName = PG_GETARG_TEXT_PP(0);
	uint32		blkNo = PG_GETARG_UINT32(1);

	Relation	rel;			/* needed to initialize the RumState structure */

	RumPageOpaque opaq;			/* data from the opaque area of the page */
	Page		page;			/* the page to be scanned */

	HeapTuple	resultTuple;	/* for the results */
	TupleDesc	tupDesc;		/* description of the result tuple */

	Datum		values[5];		/* return values */
	bool		nulls[5];		/* true if the corresponding value is NULL */
	Datum		flags[16];		/* array with flags in text format */
	int			nFlags = 0;		/* index in the array of flags */
	uint16		flagBits;		/* flags in the opaque area of the page */

	/* Only the superuser can use this */
	check_superuser();

	/* Getting rel by name and raw page by number */
	rel = get_rel_from_name(relName);
	page = get_rel_page(rel, blkNo);
	relation_close(rel, AccessShareLock);

	/* If the page is new, the function should return NULL */
	if (PageIsNew(page))
		PG_RETURN_NULL();

	/* Checking the size of the opaque area of the page */
	check_page_opaque_data_size(page);

	/* Getting a page description from an opaque area */
	opaq = RumPageGetOpaque(page);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Convert the flags bitmask to an array of human-readable names */
	flagBits = opaq->flags;
	if (flagBits & RUM_DATA)
		flags[nFlags++] = CStringGetTextDatum("data");
	if (flagBits & RUM_LEAF)
		flags[nFlags++] = CStringGetTextDatum("leaf");
	if (flagBits & RUM_DELETED)
		flags[nFlags++] = CStringGetTextDatum("deleted");
	if (flagBits & RUM_META)
		flags[nFlags++] = CStringGetTextDatum("meta");
	if (flagBits & RUM_LIST)
		flags[nFlags++] = CStringGetTextDatum("list");
	if (flagBits & RUM_LIST_FULLROW)
		flags[nFlags++] = CStringGetTextDatum("list_fullrow");
	flagBits &= ~(RUM_DATA | RUM_LEAF | RUM_DELETED | RUM_META | RUM_LIST |
				  RUM_LIST_FULLROW);
	if (flagBits)
	{
		/* any flags we don't recognize are printed in hex */
		flags[nFlags++] = DirectFunctionCall1(to_hex32, Int32GetDatum(flagBits));
	}

	memset(nulls, 0, sizeof(nulls));

	/*
	 * Writing data from metaData to values.
	 */
	values[0] = Int64GetDatum(opaq->leftlink);
	values[1] = Int64GetDatum(opaq->rightlink);
	values[2] = Int32GetDatum(opaq->maxoff);
	values[3] = Int32GetDatum(opaq->freespace);

#if PG_VERSION_NUM >= 160000
	values[4] = PointerGetDatum(construct_array_builtin(flags, nFlags, TEXTOID));
#elif PG_VERSION_NUM >= 130000
	values[4] = PointerGetDatum(construct_array(flags, nFlags,
												TEXTOID, -1, false, TYPALIGN_INT));
#else
	values[4] = PointerGetDatum(construct_array(flags, nFlags,
												TEXTOID, -1, false, 'i'));
#endif

	pfree(page);

	/* Build and return the result tuple. */
	resultTuple = heap_form_tuple(tupDesc, values, nulls);

	/* Returning the result */
	return HeapTupleGetDatum(resultTuple);
}

/*
 * The main universal function used to scan all
 * page types (except for the meta page). There
 * are four SQL wrappers around this function,
 * each of which scans a specific page type. The
 * page_type argument is used to select the type
 * of page to scan.
 */
Datum
rum_page_items_info(PG_FUNCTION_ARGS)
{
	/* Reading input arguments */
	text	   *relName = PG_GETARG_TEXT_PP(0);
	uint32		blkNo = PG_GETARG_UINT32(1);
	pageTypeFlags pageType = PG_GETARG_UINT32(2);

	int			counter;

	/*
	 * The context of the function calls and the pointer to the long-lived
	 * piState structure.
	 */
	FuncCallContext *fctx;
	RumPageItemsStateData *piState;

	/* Only the superuser can use this */
	check_superuser();

	/*
	 * In the case of the first function call, it is necessary to get the page
	 * by its number and create a RumState structure for scanning the page.
	 */
	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupDesc;	/* description of the result tuple */
		MemoryContext oldMctx;	/* the old function memory context */

		/*
		 * Initializing the FuncCallContext structure and switching the memory
		 * context to the one needed for structures that must be saved during
		 * multiple calls.
		 */
		fctx = SRF_FIRSTCALL_INIT();
		oldMctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		/* Before scanning the page, you need to prepare piState */
		if (!prepare_scan(relName, blkNo, &piState, fctx, pageType))
		{
			MemoryContextSwitchTo(oldMctx);
			PG_RETURN_NULL();
		}

		Assert(RumIsDataPage(piState) || RumIsEntryPage(piState));

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		/* Needed to for subsequent recording tupledesc in fctx */
		BlessTupleDesc(tupDesc);

		/*
		 * Save a pointer to a long-lived structure and tuple descriptor for
		 * our result type in fctx.
		 */
		fctx->user_fctx = piState;
		fctx->tuple_desc = tupDesc;

		/* Switching to the old memory context */
		MemoryContextSwitchTo(oldMctx);
	}

	/* Preparing to use the FuncCallContext */
	fctx = SRF_PERCALL_SETUP();

	/* In the current call, we are reading data from the previous one */
	piState = fctx->user_fctx;

	/* The counter is defined differently on different pages */
	if (RumIsDataPage(piState))
		counter = fctx->call_cntr;
	else
		counter = piState->curTupleNum;

	/*
	 * Go through the page.
	 *
	 * When scanning a Posting Tree page, the counter is fctx->call_cntr,
	 * which is 0 on the first call. The first call is special because it
	 * returns the high key from the pages of the Posting Tree (the high key
	 * is not counted in maxoff).
	 *
	 * On Entry tree pages, the high key is stored in the IndexTuple.
	 */
	if (counter <= piState->maxoff)
	{
		if (RumIsDataPage(piState))
			data_page_get_next_result(piState);

		else if (piState->pageType == LEAF_ENTRY_PAGE)
			entry_leaf_page_get_next_result(piState);

		else
			entry_internal_page_get_next_result(piState);

		/* Returning the result of the current call */
		SRF_RETURN_NEXT(fctx, piState->result);
	}

	/* Completing the function */
	SRF_RETURN_DONE(fctx);
}
