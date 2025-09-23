/*-------------------------------------------------------------------------
 *
 * rum.h
 *	  Exported definitions for RUM index.
 *
 * Portions Copyright (c) 2015-2024, Postgres Professional
 * Portions Copyright (c) 2006-2022, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */

#ifndef __RUM_H__
#define __RUM_H__

#include "access/amapi.h"
#include "access/generic_xlog.h"
#include "access/gin.h"
#include "access/itup.h"
#include "access/sdir.h"
#include "lib/rbtree.h"
#include "storage/bufmgr.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "tsearch/ts_type.h"

#include "rumsort.h"

/* RUM distance strategies */
#define RUM_DISTANCE			20
#define RUM_LEFT_DISTANCE		21
#define RUM_RIGHT_DISTANCE		22

/*
 * Page opaque data in a inverted index page.
 *
 * Note: RUM does not include a page ID word as do the other index types.
 * This is OK because the opaque data is only 8 bytes and so can be reliably
 * distinguished by size.  Revisit this if the size ever increases.
 * Further note: as of 9.2, SP-GiST also uses 8-byte special space.  This is
 * still OK, as long as RUM isn't using all of the high-order bits in its
 * flags word, because that way the flags word cannot match the page ID used
 * by SP-GiST.
 */
typedef struct RumPageOpaqueData
{
	BlockNumber leftlink;		/* prev page if any */
	BlockNumber rightlink;		/* next page if any */
	OffsetNumber maxoff;		/* number entries on RUM_DATA page: number of
								 * heap ItemPointers on RUM_DATA|RUM_LEAF page
								 * or number of PostingItems on RUM_DATA &
								 * ~RUM_LEAF page. On RUM_LIST page, number of
								 * heap tuples. */
	OffsetNumber freespace;
	uint16		flags;			/* see bit definitions below */
}	RumPageOpaqueData;

typedef RumPageOpaqueData *RumPageOpaque;

#define RUM_DATA		  (1 << 0)
#define RUM_LEAF		  (1 << 1)
#define RUM_DELETED		  (1 << 2)
#define RUM_META		  (1 << 3)
#define RUM_LIST		  (1 << 4)
#define RUM_LIST_FULLROW  (1 << 5)		/* makes sense only on RUM_LIST page */

/* Page numbers of fixed-location pages */
#define RUM_METAPAGE_BLKNO	(0)
#define RUM_ROOT_BLKNO		(1)

typedef struct RumMetaPageData
{
	/*
	 * RUM version number
	 */
	uint32		rumVersion;

	/*
	 * Pointers to head and tail of pending list, which consists of RUM_LIST
	 * pages.  These store fast-inserted entries that haven't yet been moved
	 * into the regular RUM structure.
	 * XXX unused - pending list is removed.
	 */
	BlockNumber head;
	BlockNumber tail;

	/*
	 * Free space in bytes in the pending list's tail page.
	 */
	uint32		tailFreeSize;

	/*
	 * We store both number of pages and number of heap tuples that are in the
	 * pending list.
	 */
	BlockNumber nPendingPages;
	int64		nPendingHeapTuples;

	/*
	 * Statistics for planner use (accurate as of last VACUUM)
	 */
	BlockNumber nTotalPages;
	BlockNumber nEntryPages;
	BlockNumber nDataPages;
	int64		nEntries;
}	RumMetaPageData;

#define RUM_CURRENT_VERSION		(0xC0DE0002)

#define RumPageGetMeta(p) \
	((RumMetaPageData *) PageGetContents(p))

/*
 * Macros for accessing a RUM index page's opaque data
 */
#define RumPageGetOpaque(page) ( (RumPageOpaque) PageGetSpecialPointer(page) )

#define RumPageIsLeaf(page)    ( (RumPageGetOpaque(page)->flags & RUM_LEAF) != 0 )
#define RumPageSetLeaf(page)   ( RumPageGetOpaque(page)->flags |= RUM_LEAF )
#define RumPageSetNonLeaf(page)    ( RumPageGetOpaque(page)->flags &= ~RUM_LEAF )
#define RumPageIsData(page)    ( (RumPageGetOpaque(page)->flags & RUM_DATA) != 0 )
#define RumPageSetData(page)   ( RumPageGetOpaque(page)->flags |= RUM_DATA )
#define RumPageIsList(page)    ( (RumPageGetOpaque(page)->flags & RUM_LIST) != 0 )
#define RumPageSetList(page)   ( RumPageGetOpaque(page)->flags |= RUM_LIST )
#define RumPageHasFullRow(page)    ( (RumPageGetOpaque(page)->flags & RUM_LIST_FULLROW) != 0 )
#define RumPageSetFullRow(page)   ( RumPageGetOpaque(page)->flags |= RUM_LIST_FULLROW )

#define RumPageIsDeleted(page) ( (RumPageGetOpaque(page)->flags & RUM_DELETED) != 0 )
#define RumPageSetDeleted(page)    ( RumPageGetOpaque(page)->flags |= RUM_DELETED)
#define RumPageSetNonDeleted(page) ( RumPageGetOpaque(page)->flags &= ~RUM_DELETED)

#define RumPageRightMost(page) ( RumPageGetOpaque(page)->rightlink == InvalidBlockNumber)
#define RumPageLeftMost(page) ( RumPageGetOpaque(page)->leftlink == InvalidBlockNumber)

/*
 * We use our own ItemPointerGet(BlockNumber|GetOffsetNumber)
 * to avoid Asserts, since sometimes the ip_posid isn't "valid"
 */
#define RumItemPointerGetBlockNumber(pointer) \
	BlockIdGetBlockNumber(&(pointer)->ip_blkid)

#define RumItemPointerGetOffsetNumber(pointer) \
	((pointer)->ip_posid)

/*
 * Special-case item pointer values needed by the RUM search logic.
 *	MIN: sorts less than any valid item pointer
 *	MAX: sorts greater than any valid item pointer
 *	LOSSY PAGE: indicates a whole heap page, sorts after normal item
 *				pointers for that page
 * Note that these are all distinguishable from an "invalid" item pointer
 * (which is InvalidBlockNumber/0) as well as from all normal item
 * pointers (which have item numbers in the range 1..MaxHeapTuplesPerPage).
 */
#define ItemPointerSetMin(p)  \
	ItemPointerSet((p), (BlockNumber)0, (OffsetNumber)0)
#define ItemPointerIsMin(p)  \
	(RumItemPointerGetOffsetNumber(p) == (OffsetNumber)0 && \
	 RumItemPointerGetBlockNumber(p) == (BlockNumber)0)
#define ItemPointerSetMax(p)  \
	ItemPointerSet((p), InvalidBlockNumber, (OffsetNumber)0xfffe)
#define ItemPointerIsMax(p)  \
	(RumItemPointerGetOffsetNumber(p) == (OffsetNumber)0xfffe && \
	 RumItemPointerGetBlockNumber(p) == InvalidBlockNumber)
#define ItemPointerSetLossyPage(p, b)  \
	ItemPointerSet((p), (b), (OffsetNumber)0xffff)
#define ItemPointerIsLossyPage(p)  \
	(RumItemPointerGetOffsetNumber(p) == (OffsetNumber)0xffff && \
	 RumItemPointerGetBlockNumber(p) != InvalidBlockNumber)

typedef struct RumItem
{
	ItemPointerData iptr;
	bool		addInfoIsNull;
	Datum		addInfo;
}	RumItem;

#define RumItemSetMin(item)  \
do { \
	ItemPointerSetMin(&((item)->iptr)); \
	(item)->addInfoIsNull = true; \
	(item)->addInfo = (Datum) 0; \
} while (0)

/*
 * Posting item in a non-leaf posting-tree page
 */
typedef struct
{
	/* We use BlockIdData not BlockNumber to avoid padding space wastage */
	BlockIdData child_blkno;
	RumItem		item;
} PostingItem;

#define PostingItemGetBlockNumber(pointer) \
	BlockIdGetBlockNumber(&(pointer)->child_blkno)

#define PostingItemSetBlockNumber(pointer, blockNumber) \
	BlockIdSet(&((pointer)->child_blkno), (blockNumber))

/*
 * Category codes to distinguish placeholder nulls from ordinary NULL keys.
 * Note that the datatype size and the first two code values are chosen to be
 * compatible with the usual usage of bool isNull flags.
 *
 * RUM_CAT_EMPTY_QUERY is never stored in the index; and notice that it is
 * chosen to sort before not after regular key values.
 */
typedef signed char RumNullCategory;

#define RUM_CAT_NORM_KEY		0		/* normal, non-null key value */
#define RUM_CAT_NULL_KEY		1		/* null key value */
#define RUM_CAT_EMPTY_ITEM		2		/* placeholder for zero-key item */
#define RUM_CAT_NULL_ITEM		3		/* placeholder for null item */
#define RUM_CAT_EMPTY_QUERY		(-1)	/* placeholder for full-scan query */

/*
 * Access macros for null category byte in entry tuples
 */
#define RumCategoryOffset(itup,rumstate) \
	(IndexInfoFindDataOffset((itup)->t_info) + \
	 ((rumstate)->oneCol ? 0 : sizeof(int16)))

#define RumGetNullCategory(itup) \
	  (*((RumNullCategory *) ((char*)(itup) + IndexTupleSize(itup) - sizeof(RumNullCategory))))
#define RumSetNullCategory(itup,c) \
	  (*((RumNullCategory *) ((char*)(itup) + IndexTupleSize(itup) - sizeof(RumNullCategory))) = (c))

/*
 * Access macros for leaf-page entry tuples (see discussion in README)
 */
#define RumGetNPosting(itup)	RumItemPointerGetOffsetNumber(&(itup)->t_tid)
#define RumSetNPosting(itup,n)	ItemPointerSetOffsetNumber(&(itup)->t_tid,n)
#define RUM_TREE_POSTING		((OffsetNumber)0xffff)
#define RumIsPostingTree(itup)	(RumGetNPosting(itup) == RUM_TREE_POSTING)
#define RumSetPostingTree(itup, blkno)	\
	(RumSetNPosting((itup),RUM_TREE_POSTING), \
	 ItemPointerSetBlockNumber(&(itup)->t_tid, blkno))
#define RumGetPostingTree(itup) RumItemPointerGetBlockNumber(&(itup)->t_tid)

#define RumGetPostingOffset(itup)	RumItemPointerGetBlockNumber(&(itup)->t_tid)
#define RumSetPostingOffset(itup,n) ItemPointerSetBlockNumber(&(itup)->t_tid,n)
#define RumGetPosting(itup)			((Pointer) ((char*)(itup) + RumGetPostingOffset(itup)))

/*
 * Maximum size of an item on entry tree page. Make sure that we fit at least
 * three items on each page. (On regular B-tree indexes, we must fit at least
 * three items: two data items and the "high key". In RUM entry tree, we don't
 * currently store the high key explicitly, we just use the rightmost item on
 * the page, so it would actually be enough to fit two items.)
 */
#define RumMaxItemSize \
	Min(INDEX_SIZE_MASK, \
		MAXALIGN_DOWN(((BLCKSZ - \
						MAXALIGN(SizeOfPageHeaderData + 3 * sizeof(ItemIdData)) - \
						MAXALIGN(sizeof(RumPageOpaqueData))) / 3)))


/*
 * Access macros for non-leaf entry tuples
 */
#define RumGetDownlink(itup)	RumItemPointerGetBlockNumber(&(itup)->t_tid)
#define RumSetDownlink(itup,blkno)	ItemPointerSet(&(itup)->t_tid, blkno, InvalidOffsetNumber)


/*
 * Data (posting tree) pages
 */
/*
 * FIXME -- Currently RumItem is placed as a pages right bound and PostingItem
 * is placed as a non-leaf pages item. Both RumItem and PostingItem stores
 * AddInfo as a raw Datum, which is bogus. It is fine for pass-by-value
 * attributes, but it isn't for pass-by-reference, which may have variable
 * length of data. This AddInfo is used only by order_by_attach indexes, so it
 * isn't allowed to create index using ordering over pass-by-reference AddInfo,
 * see initRumState(). This can be solved by having non-fixed length right bound
 * and non-fixed non-leaf posting tree item.
 */
#define RumDataPageGetRightBound(page)	((RumItem*) PageGetContents(page))
#define RumDataPageGetData(page)	\
	(PageGetContents(page) + MAXALIGN(sizeof(RumItem)))
#define RumDataPageGetItem(page,i)	\
	(RumDataPageGetData(page) + ((i)-1) * sizeof(PostingItem))

#define RumDataPageGetFreeSpace(page)	\
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) \
	 - MAXALIGN(sizeof(RumItem)) /* right bound */ \
	 - RumPageGetOpaque(page)->maxoff * sizeof(PostingItem) \
	 - MAXALIGN(sizeof(RumPageOpaqueData)))

#define RumMaxLeafDataItems \
	((BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - \
	  MAXALIGN(sizeof(RumItem)) /* right bound */ - \
	  MAXALIGN(sizeof(RumPageOpaqueData))) \
	 / sizeof(ItemPointerData))

/*
 * List pages
 */
#define RumListPageSize  \
	( BLCKSZ - SizeOfPageHeaderData - MAXALIGN(sizeof(RumPageOpaqueData)) )

typedef struct
{
	ItemPointerData iptr;
	OffsetNumber offsetNumer;
	uint16		pageOffset;
	Datum		addInfo; /* optional */
}	RumDataLeafItemIndex;

#define RumDataLeafIndexCount 32

#define RumDataPageSize \
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) \
	 - MAXALIGN(sizeof(RumItem)) /* right bound */ \
	 - MAXALIGN(sizeof(RumPageOpaqueData)) \
	 - MAXALIGN(sizeof(RumDataLeafItemIndex) * RumDataLeafIndexCount))

#define RumDataPageFreeSpacePre(page,ptr) \
	(RumDataPageSize \
	 - ((ptr) - RumDataPageGetData(page)))

#define RumPageGetIndexes(page) \
	((RumDataLeafItemIndex *)(RumDataPageGetData(page) + RumDataPageSize))

/*
 * Storage type for RUM's reloptions
 */
typedef struct RumOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	bool		useAlternativeOrder;
	int			attachColumn;
	int			addToColumn;
}	RumOptions;

#define ALT_ADD_INFO_NULL_FLAG		(0x8000)

/* Macros for buffer lock/unlock operations */
#define RUM_UNLOCK	BUFFER_LOCK_UNLOCK
#define RUM_SHARE	BUFFER_LOCK_SHARE
#define RUM_EXCLUSIVE  BUFFER_LOCK_EXCLUSIVE

#define MAX_STRATEGIES	(8)
typedef struct RumConfig
{
	Oid			addInfoTypeOid;

	struct {
		StrategyNumber	strategy;
		ScanDirection	direction;
	}		strategyInfo[MAX_STRATEGIES];
}	RumConfig;

/*
 * RumState: working data structure describing the index being worked on
 */
typedef struct RumState
{
	Relation	index;
	bool		isBuild;
	bool		oneCol;			/* true if single-column index */
	bool		useAlternativeOrder;
	AttrNumber	attrnAttachColumn;
	AttrNumber	attrnAddToColumn;

	/*
	 * origTupDesc is the nominal tuple descriptor of the index, ie, the i'th
	 * attribute shows the key type (not the input data type!) of the i'th
	 * index column.  In a single-column index this describes the actual leaf
	 * index tuples.  In a multi-column index, the actual leaf tuples contain
	 * a smallint column number followed by a key datum of the appropriate
	 * type for that column.  We set up tupdesc[i] to describe the actual
	 * rowtype of the index tuples for the i'th column, ie, (int2, keytype).
	 * Note that in any case, leaf tuples contain more data than is known to
	 * the TupleDesc; see access/gin/README for details.
	 */
	TupleDesc	origTupdesc;
	TupleDesc	tupdesc[INDEX_MAX_KEYS];
	RumConfig	rumConfig[INDEX_MAX_KEYS];
	Form_pg_attribute addAttrs[INDEX_MAX_KEYS];

	/*
	 * Per-index-column opclass support functions
	 */
	FmgrInfo	compareFn[INDEX_MAX_KEYS];
	FmgrInfo	extractValueFn[INDEX_MAX_KEYS];
	FmgrInfo	extractQueryFn[INDEX_MAX_KEYS];
	FmgrInfo	consistentFn[INDEX_MAX_KEYS];
	FmgrInfo	comparePartialFn[INDEX_MAX_KEYS];		/* optional method */
	FmgrInfo	configFn[INDEX_MAX_KEYS];		/* optional method */
	FmgrInfo	preConsistentFn[INDEX_MAX_KEYS];		/* optional method */
	FmgrInfo	orderingFn[INDEX_MAX_KEYS];		/* optional method */
	FmgrInfo	outerOrderingFn[INDEX_MAX_KEYS];		/* optional method */
	FmgrInfo	joinAddInfoFn[INDEX_MAX_KEYS];		/* optional method */
	/* canPartialMatch[i] is true if comparePartialFn[i] is valid */
	bool		canPartialMatch[INDEX_MAX_KEYS];
	/* canPreConsistent[i] is true if preConsistentFn[i] is valid */
	bool		canPreConsistent[INDEX_MAX_KEYS];
	/* canOrdering[i] is true if orderingFn[i] is valid */
	bool		canOrdering[INDEX_MAX_KEYS];
	bool		canOuterOrdering[INDEX_MAX_KEYS];
	bool		canJoinAddInfo[INDEX_MAX_KEYS];
	/* Collations to pass to the support functions */
	Oid			supportCollation[INDEX_MAX_KEYS];
}	RumState;

/* Accessor for the i'th attribute of tupdesc. */
#if PG_VERSION_NUM > 100000
#define RumTupleDescAttr(tupdesc, i) (TupleDescAttr(tupdesc, i))
#else
#define RumTupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])
#endif

/* rumutil.c */
extern bytea *rumoptions(Datum reloptions, bool validate);
extern bool rumproperty(Oid index_oid, int attno,
			 IndexAMProperty prop, const char *propname,
			 bool *res, bool *isnull);
extern PGDLLEXPORT Datum rumhandler(PG_FUNCTION_ARGS);
extern void initRumState(RumState * state, Relation index);
extern Buffer RumNewBuffer(Relation index);
extern void RumInitBuffer(GenericXLogState *state, Buffer buffer, uint32 flags,
						  bool isBuild);
extern void RumInitPage(Page page, uint32 f, Size pageSize);
extern void RumInitMetabuffer(GenericXLogState *state, Buffer metaBuffer,
							  bool isBuild);
extern int rumCompareEntries(RumState * rumstate, OffsetNumber attnum,
				  Datum a, RumNullCategory categorya,
				  Datum b, RumNullCategory categoryb);
extern int rumCompareAttEntries(RumState * rumstate,
					 OffsetNumber attnuma, Datum a, RumNullCategory categorya,
				   OffsetNumber attnumb, Datum b, RumNullCategory categoryb);
extern Datum *rumExtractEntries(RumState * rumstate, OffsetNumber attnum,
				  Datum value, bool isNull,
				  int32 *nentries, RumNullCategory ** categories,
				  Datum **addInfo, bool **addInfoIsNull);

extern OffsetNumber rumtuple_get_attrnum(RumState * rumstate, IndexTuple tuple);
extern Datum rumtuple_get_key(RumState * rumstate, IndexTuple tuple,
				 RumNullCategory * category);

extern void rumGetStats(Relation index, GinStatsData *stats);
extern void rumUpdateStats(Relation index, const GinStatsData *stats,
						   bool isBuild);

/* ruminsert.c */
extern IndexBuildResult *rumbuild(Relation heap, Relation index,
		 struct IndexInfo *indexInfo);
extern void rumbuildempty(Relation index);
extern bool ruminsert(Relation index, Datum *values, bool *isnull,
		  ItemPointer ht_ctid, Relation heapRel,
		  IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
		  , bool indexUnchanged
#endif
#if PG_VERSION_NUM >= 100000
		  , struct IndexInfo *indexInfo
#endif
		  );
extern void rumEntryInsert(RumState * rumstate,
			   OffsetNumber attnum, Datum key, RumNullCategory category,
			   RumItem * items, uint32 nitem, GinStatsData *buildStats);

/* rumbtree.c */

typedef struct RumBtreeStack
{
	BlockNumber blkno;
	Buffer		buffer;
	OffsetNumber off;
	/* predictNumber contains predicted number of pages on current level */
	uint32		predictNumber;
	struct RumBtreeStack *parent;
}	RumBtreeStack;

typedef struct RumBtreeData *RumBtree;

typedef struct RumBtreeData
{
	/* search methods */
	BlockNumber (*findChildPage) (RumBtree, RumBtreeStack *);
	bool		(*isMoveRight) (RumBtree, Page);
	bool		(*findItem) (RumBtree, RumBtreeStack *);

	/* insert methods */
	OffsetNumber (*findChildPtr) (RumBtree, Page, BlockNumber, OffsetNumber);
	BlockNumber (*getLeftMostPage) (RumBtree, Page);
	bool		(*isEnoughSpace) (RumBtree, Buffer, OffsetNumber);
	void		(*placeToPage) (RumBtree, Page, OffsetNumber);
	Page		(*splitPage) (RumBtree, Buffer, Buffer, Page, Page, OffsetNumber);
	void		(*fillRoot) (RumBtree, Buffer, Buffer, Buffer, Page, Page, Page);

	bool		isData;
	bool		searchMode;

	Relation	index;
	RumState   *rumstate;
	bool		fullScan;
	ScanDirection scanDirection;

	BlockNumber rightblkno;

	AttrNumber	entryAttnum;

	/* Entry options */
	Datum		entryKey;
	RumNullCategory entryCategory;
	IndexTuple	entry;
	bool		isDelete;

	/* Data (posting tree) options */
	RumItem	   *items;

	uint32		nitem;
	uint32		curitem;

	PostingItem pitem;
}	RumBtreeData;

extern RumBtreeStack *rumPrepareFindLeafPage(RumBtree btree, BlockNumber blkno);
extern RumBtreeStack *rumFindLeafPage(RumBtree btree, RumBtreeStack * stack);
extern RumBtreeStack *rumReFindLeafPage(RumBtree btree, RumBtreeStack * stack);
extern Buffer rumStep(Buffer buffer, Relation index, int lockmode,
					  ScanDirection scanDirection);
extern void freeRumBtreeStack(RumBtreeStack * stack);
extern void rumInsertValue(Relation index, RumBtree btree, RumBtreeStack * stack,
			   GinStatsData *buildStats);
extern void rumFindParents(RumBtree btree, RumBtreeStack * stack, BlockNumber rootBlkno);

/* rumentrypage.c */
extern void rumPrepareEntryScan(RumBtree btree, OffsetNumber attnum,
					Datum key, RumNullCategory category,
					RumState * rumstate);
extern void rumEntryFillRoot(RumBtree btree, Buffer root, Buffer lbuf, Buffer rbuf,
				 Page page, Page lpage, Page rpage);
extern IndexTuple rumPageGetLinkItup(RumBtree btree, Buffer buf, Page page);
extern void rumReadTuple(RumState * rumstate, OffsetNumber attnum,
			 IndexTuple itup, RumItem * items, bool copyAddInfo);
extern void rumReadTuplePointers(RumState * rumstate, OffsetNumber attnum,
					 IndexTuple itup, ItemPointerData *ipd);
extern void updateItemIndexes(Page page, OffsetNumber attnum, RumState * rumstate);
extern void checkLeafDataPage(RumState * rumstate, AttrNumber attrnum, Page page);

/* rumdatapage.c */
extern int	rumCompareItemPointers(const ItemPointerData *a, const ItemPointerData *b);
extern int	compareRumItem(RumState * state, const AttrNumber attno,
						  const RumItem * a, const RumItem * b);
extern void convertIndexToKey(RumDataLeafItemIndex *src, RumItem *dst);
extern Pointer rumPlaceToDataPageLeaf(Pointer ptr, OffsetNumber attnum,
					   RumItem * item, ItemPointer prev, RumState * rumstate);
extern Size rumCheckPlaceToDataPageLeaf(OffsetNumber attnum,
			RumItem * item, ItemPointer prev, RumState * rumstate, Size size);
extern uint32 rumMergeRumItems(RumState * rumstate, AttrNumber attno,
							   RumItem * dst,
							   RumItem * a, uint32 na,
							   RumItem * b, uint32 nb);
extern void RumDataPageAddItem(Page page, void *data, OffsetNumber offset);
extern void RumPageDeletePostingItem(Page page, OffsetNumber offset);

typedef struct
{
	RumBtreeData btree;
	RumBtreeStack *stack;
}	RumPostingTreeScan;

extern RumPostingTreeScan *rumPrepareScanPostingTree(Relation index,
						  BlockNumber rootBlkno, bool searchMode,
						  ScanDirection scanDirection,
						  OffsetNumber attnum, RumState * rumstate);
extern void rumInsertItemPointers(RumState * rumstate,
					  OffsetNumber attnum,
					  RumPostingTreeScan * gdi,
					  RumItem * items, uint32 nitem,
					  GinStatsData *buildStats);
extern Buffer rumScanBeginPostingTree(RumPostingTreeScan * gdi, RumItem *item);
extern void rumDataFillRoot(RumBtree btree, Buffer root, Buffer lbuf, Buffer rbuf,
				Page page, Page lpage, Page rpage);
extern void rumPrepareDataScan(RumBtree btree, Relation index, OffsetNumber attnum, RumState * rumstate);

/* rumscan.c */

typedef struct RumScanItem
{
	RumItem		item;
	Datum		keyValue;
	RumNullCategory keyCategory;
}	RumScanItem;

/*
 * RumScanKeyData describes a single RUM index qualifier expression.
 *
 * From each qual expression, we extract one or more specific index search
 * conditions, which are represented by RumScanEntryData.  It's quite
 * possible for identical search conditions to be requested by more than
 * one qual expression, in which case we merge such conditions to have just
 * one unique RumScanEntry --- this is particularly important for efficiency
 * when dealing with full-index-scan entries.  So there can be multiple
 * RumScanKeyData.scanEntry pointers to the same RumScanEntryData.
 *
 * In each RumScanKeyData, nentries is the true number of entries, while
 * nuserentries is the number that extractQueryFn returned (which is what
 * we report to consistentFn).  The "user" entries must come first.
 */
typedef struct RumScanKeyData *RumScanKey;

typedef struct RumScanEntryData *RumScanEntry;

typedef struct RumScanKeyData
{
	/* Real number of entries in scanEntry[] (always > 0) */
	uint32		nentries;
	/* Number of entries that extractQueryFn and consistentFn know about */
	uint32		nuserentries;

	/* array of RumScanEntry pointers, one per extracted search condition */
	RumScanEntry *scanEntry;

	/* array of check flags, reported to consistentFn */
	bool	   *entryRes;
	/* array of additional information, used in consistentFn and orderingFn */
	Datum	   *addInfo;
	bool	   *addInfoIsNull;
	/* additional information, used in outerOrderingFn */
	bool		useAddToColumn;
	Datum		outerAddInfo;
	bool		outerAddInfoIsNull;

	/* Key information, used in orderingFn */
	Datum		curKey;
	RumNullCategory curKeyCategory;
	bool		useCurKey;

	/* other data needed for calling consistentFn */
	Datum		query;
	/* NB: these three arrays have only nuserentries elements! */
	Datum	   *queryValues;
	RumNullCategory *queryCategories;
	Pointer	   *extra_data;
	StrategyNumber strategy;
	int32		searchMode;
	OffsetNumber attnum;
	OffsetNumber attnumOrig;

	/*
	 * Match status data.  curItem is the TID most recently tested (could be a
	 * lossy-page pointer).  curItemMatches is TRUE if it passes the
	 * consistentFn test; if so, recheckCurItem is the recheck flag.
	 * isFinished means that all the input entry streams are finished, so this
	 * key cannot succeed for any later TIDs.
	 */
	RumItem		curItem;
	bool		curItemMatches;
	bool		recheckCurItem;
	bool		isFinished;
	bool		orderBy;
	bool		willSort; /* just a copy of RumScanOpaqueData.willSort */
	ScanDirection	scanDirection;

	/* array of keys, used to scan using additional information as keys */
	RumScanKey *addInfoKeys;
	uint32		addInfoNKeys;
}	RumScanKeyData;

typedef struct RumScanEntryData
{
	/* query key and other information from extractQueryFn */
	Datum		queryKey;
	RumNullCategory queryCategory;
	bool		isPartialMatch;
	Pointer		extra_data;
	StrategyNumber strategy;
	int32		searchMode;
	OffsetNumber attnum;
	OffsetNumber attnumOrig;

	/* Current page in posting tree */
	Buffer		buffer;

	/* current ItemPointer to heap */
	RumItem		curItem;

	/* Used for ordering using distance */
	Datum		curKey;
	RumNullCategory curKeyCategory;
	bool		useCurKey;

	/*
	 * For a partial-match or full-scan query, we accumulate all TIDs and
	 * and additional information here
	 */
	RumTuplesortstate *matchSortstate;
	RumScanItem	collectRumItem;

	/* for full-scan query with order-by */
	RumBtreeStack *stack;
	bool		scanWithAddInfo;

	/* used for Posting list and one page in Posting tree */
	RumItem	   *list;
	int16		nlist;
	int16		offset;

	ScanDirection scanDirection;
	bool		isFinished;
	bool		reduceResult;
	uint32		predictNumberResult;

	/* used to scan posting tree */
	RumPostingTreeScan *gdi;

	/* used in fast scan in addition to preConsistentFn */
	bool		preValue;

	/* Find by AddInfo */
	bool		useMarkAddInfo;
	RumItem		markAddInfo;
}	RumScanEntryData;

typedef struct
{
	ItemPointerData iptr;
	float8		distance;
	bool		recheck;
}	RumOrderingItem;

typedef enum
{
	RumFastScan,
	RumRegularScan,
	RumFullScan
}	RumScanType;

typedef struct RumScanOpaqueData
{
	/* tempCtx is used to hold consistent and ordering functions data */
	MemoryContext tempCtx;
	/* keyCtx is used to hold key and entry data */
	MemoryContext keyCtx;
	RumState	rumstate;

	RumScanKey *keys;				/* one per scan qualifier expr */
	uint32		nkeys;

	RumScanEntry *entries;			/* one per index search condition */
	RumScanEntry *sortedEntries;	/* Sorted entries. Used in fast scan */
	int			entriesIncrIndex;	/* used in fast scan */
	uint32		totalentries;
	uint32		allocentries;		/* allocated length of entries[] and
									   sortedEntries[] */

	RumTuplesortstate *sortstate;
	int			norderbys;		/* Number of columns in ordering.
								   Will be assigned to sortstate->nKeys */

	RumItem		item;			/* current item used in index scan */
	bool		firstCall;

	bool		isVoidRes;		/* true if query is unsatisfiable */
	bool		willSort;		/* is there any columns in ordering */
	RumScanType	scanType;

	ScanDirection	naturalOrder;
	bool			secondPass;
}	RumScanOpaqueData;

typedef RumScanOpaqueData *RumScanOpaque;

extern IndexScanDesc rumbeginscan(Relation rel, int nkeys, int norderbys);
extern void rumendscan(IndexScanDesc scan);
extern void rumrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		  ScanKey orderbys, int norderbys);
extern Datum rummarkpos(PG_FUNCTION_ARGS);
extern Datum rumrestrpos(PG_FUNCTION_ARGS);
extern void rumNewScanKey(IndexScanDesc scan);
extern void freeScanKeys(RumScanOpaque so);

/* rumget.c */
extern int64 rumgetbitmap(IndexScanDesc scan, TIDBitmap *tbm);
extern bool rumgettuple(IndexScanDesc scan, ScanDirection direction);

/* rumvacuum.c */
extern IndexBulkDeleteResult *rumbulkdelete(IndexVacuumInfo *info,
			  IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback,
			  void *callback_state);
extern IndexBulkDeleteResult *rumvacuumcleanup(IndexVacuumInfo *info,
				 IndexBulkDeleteResult *stats);

/* rumvalidate.c */
extern bool rumvalidate(Oid opclassoid);

/* rumbulk.c */
#if PG_VERSION_NUM <= 100006 || PG_VERSION_NUM == 110000
typedef RBNode RBTNode;
#endif

typedef struct RumEntryAccumulator
{
	RBTNode		rbnode;
	Datum		key;
	RumNullCategory category;
	OffsetNumber attnum;
	bool		shouldSort;
	RumItem	   *list;
	uint32		maxcount;		/* allocated size of list[] */
	uint32		count;			/* current number of list[] entries */
}	RumEntryAccumulator;

typedef struct
{
	RumState   *rumstate;
	long		allocatedMemory;
	RumEntryAccumulator *entryallocator;
	uint32		eas_used;
	RBTree	   *tree;
#if PG_VERSION_NUM >= 100000
	RBTreeIterator tree_walk;
#endif
	RumItem	   *sortSpace;
	uint32		sortSpaceN;
} BuildAccumulator;

extern void rumInitBA(BuildAccumulator *accum);
extern void rumInsertBAEntries(BuildAccumulator *accum,
				   ItemPointer heapptr, OffsetNumber attnum,
				   Datum *entries, Datum *addInfo, bool *addInfoIsNull,
				   RumNullCategory * categories, int32 nentries);
extern void rumBeginBAScan(BuildAccumulator *accum);
extern RumItem *rumGetBAEntry(BuildAccumulator *accum,
			  OffsetNumber *attnum, Datum *key, RumNullCategory * category,
			  uint32 *n);

/* rum_ts_utils.c */
#define RUM_CONFIG_PROC				6
#define RUM_PRE_CONSISTENT_PROC		7
#define RUM_ORDERING_PROC			8
#define RUM_OUTER_ORDERING_PROC		9
#define RUM_ADDINFO_JOIN			10
#define RUMNProcs					10

#define LOWERMASK 0x1F

extern PGDLLEXPORT Datum rum_extract_tsvector(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum rum_extract_tsquery(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum rum_tsvector_config(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum rum_tsquery_pre_consistent(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum rum_tsquery_distance(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum rum_ts_distance_tt(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum rum_ts_distance_ttf(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum rum_ts_distance_td(PG_FUNCTION_ARGS);

extern PGDLLEXPORT Datum tsquery_to_distance_query(PG_FUNCTION_ARGS);

extern char* decompress_pos(char *ptr, WordEntryPos *pos);
extern unsigned int count_pos(char *ptr, int len);

/* rum_arr_utils.c */
typedef enum SimilarityType
{
	SMT_COSINE  = 1,
	SMT_JACCARD = 2,
	SMT_OVERLAP = 3
} SimilarityType;

#define RUM_SIMILARITY_FUNCTION_DEFAULT		SMT_COSINE
#define RUM_SIMILARITY_THRESHOLD_DEFAULT	0.5

extern PGDLLEXPORT Datum rum_anyarray_config(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum rum_extract_anyarray(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum rum_extract_anyarray_query(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum rum_anyarray_consistent(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum rum_anyarray_ordering(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum rum_anyarray_similar(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum rum_anyarray_distance(PG_FUNCTION_ARGS);


/* GUC parameters */
extern int		RumFuzzySearchLimit;
extern float8	RumArraySimilarityThreshold;
extern int		RumArraySimilarityFunction;


/*
 * Functions for reading ItemPointers with additional information. Used in
 * various .c files and have to be inline for being fast.
 */

#define SEVENTHBIT	(0x40)
#define SIXMASK		(0x3F)

/*
 * Read next item pointer from leaf data page. Replaces current item pointer
 * with the next one. Zero item pointer should be passed in order to read the
 * first item pointer. Also reads value of addInfoIsNull flag which is stored
 * with item pointer.
 */
static inline char *
rumDataPageLeafReadItemPointer(char *ptr, ItemPointer iptr, bool *addInfoIsNull)
{
	uint32		blockNumberIncr = 0;
	uint16		offset = 0;
	int			i;
	uint8		v;

	i = 0;
	do
	{
		v = *ptr;
		ptr++;
		blockNumberIncr |= (v & (~HIGHBIT)) << i;
		Assert(i < 28 || ((i == 28) && ((v & (~HIGHBIT)) < (1 << 4))));
		i += 7;
	}
	while (v & HIGHBIT);

	Assert((uint64) iptr->ip_blkid.bi_lo + ((uint64) iptr->ip_blkid.bi_hi << 16) +
		   (uint64) blockNumberIncr < ((uint64) 1 << 32));

	blockNumberIncr += iptr->ip_blkid.bi_lo + (iptr->ip_blkid.bi_hi << 16);

	iptr->ip_blkid.bi_lo = blockNumberIncr & 0xFFFF;
	iptr->ip_blkid.bi_hi = (blockNumberIncr >> 16) & 0xFFFF;

	i = 0;

	while (true)
	{
		v = *ptr;
		ptr++;
		Assert(i < 14 || ((i == 14) && ((v & SIXMASK) < (1 << 2))));

		if (v & HIGHBIT)
		{
			offset |= (v & (~HIGHBIT)) << i;
		}
		else
		{
			offset |= (v & SIXMASK) << i;
			if (addInfoIsNull)
				*addInfoIsNull = (v & SEVENTHBIT) ? true : false;
			break;
		}
		i += 7;
	}

	Assert(OffsetNumberIsValid(offset));
	iptr->ip_posid = offset;

	return ptr;
}

/*
 * Reads next item pointer and additional information from leaf data page.
 * Replaces current item pointer with the next one. Zero item pointer should be
 * passed in order to read the first item pointer.
 *
 * It is necessary to pass copyAddInfo=true if additional information is used
 * when the data page is unlocked. If the additional information is used without
 * locking one can get unexpected behaviour.
 */
static inline Pointer
rumDataPageLeafRead(Pointer ptr, OffsetNumber attnum, RumItem * item,
					bool copyAddInfo, RumState * rumstate)
{
	Form_pg_attribute attr;

	if (rumstate->useAlternativeOrder)
	{
		memcpy(&item->iptr, ptr, sizeof(ItemPointerData));
		ptr += sizeof(ItemPointerData);

		if (item->iptr.ip_posid & ALT_ADD_INFO_NULL_FLAG)
		{
			item->iptr.ip_posid &= ~ALT_ADD_INFO_NULL_FLAG;
			item->addInfoIsNull = true;
		}
		else
		{
			item->addInfoIsNull = false;
		}
	}
	else
		ptr = rumDataPageLeafReadItemPointer(ptr, &item->iptr,
											 &item->addInfoIsNull);

	Assert(item->iptr.ip_posid != InvalidOffsetNumber);

	if (!item->addInfoIsNull)
	{
		attr = rumstate->addAttrs[attnum - 1];

		Assert(attr);

		if (attr->attbyval)
		{
			/* do not use aligment for pass-by-value types */
			union
			{
				int16		i16;
				int32		i32;
			}			u;

			switch (attr->attlen)
			{
				case sizeof(char):
					item->addInfo = Int8GetDatum(*ptr);
					break;
				case sizeof(int16):
					memcpy(&u.i16, ptr, sizeof(int16));
					item->addInfo = Int16GetDatum(u.i16);
					break;
				case sizeof(int32):
					memcpy(&u.i32, ptr, sizeof(int32));
					item->addInfo = Int32GetDatum(u.i32);
					break;
#if SIZEOF_DATUM == 8
				case sizeof(Datum):
					memcpy(&item->addInfo, ptr, sizeof(Datum));
					break;
#endif
				default:
					elog(ERROR, "unsupported byval length: %d",
						 (int) (attr->attlen));
			}
		}
		else
		{
			Datum		addInfo;

			ptr = (Pointer) att_align_pointer(ptr, attr->attalign, attr->attlen,
											  ptr);
			addInfo = fetch_att(ptr, attr->attbyval, attr->attlen);
			item->addInfo = copyAddInfo ?
				datumCopy(addInfo, attr->attbyval, attr->attlen) : addInfo;
		}

		ptr = (Pointer) att_addlength_pointer(ptr, attr->attlen, ptr);
	}
	return ptr;
}

/*
 * Reads next item pointer from leaf data page.
 * Replaces current item pointer with the next one. Zero item pointer should be
 * passed in order to read the first item pointer.
 */
static inline Pointer
rumDataPageLeafReadPointer(Pointer ptr, OffsetNumber attnum, RumItem * item,
						   RumState * rumstate)
{
	Form_pg_attribute attr;

	if (rumstate->useAlternativeOrder)
	{
		memcpy(&item->iptr, ptr, sizeof(ItemPointerData));
		ptr += sizeof(ItemPointerData);

		if (item->iptr.ip_posid & ALT_ADD_INFO_NULL_FLAG)
		{
			item->iptr.ip_posid &= ~ALT_ADD_INFO_NULL_FLAG;
			item->addInfoIsNull = true;
		}
		else
		{
			item->addInfoIsNull = false;
		}
	}
	else
		ptr = rumDataPageLeafReadItemPointer(ptr, &item->iptr,
											 &item->addInfoIsNull);

	Assert(item->iptr.ip_posid != InvalidOffsetNumber);

	if (!item->addInfoIsNull)
	{
		attr = rumstate->addAttrs[attnum - 1];

		Assert(attr);

		if (!attr->attbyval)
			ptr = (Pointer) att_align_pointer(ptr, attr->attalign, attr->attlen,
											  ptr);

		ptr = (Pointer) att_addlength_pointer(ptr, attr->attlen, ptr);
	}
	return ptr;
}

extern Datum FunctionCall10Coll(FmgrInfo *flinfo, Oid collation,
				   Datum arg1, Datum arg2,
				   Datum arg3, Datum arg4, Datum arg5,
				   Datum arg6, Datum arg7, Datum arg8,
				   Datum arg9, Datum arg10);

/* PostgreSQL version-agnostic creation of memory context */
#if PG_VERSION_NUM >= 120000
#define RumContextCreate(parent, name) \
	AllocSetContextCreate(parent, name, ALLOCSET_DEFAULT_SIZES)
#elif PG_VERSION_NUM >= 110000
	#define RumContextCreate(parent, name) \
		AllocSetContextCreateExtended(parent, name, \
									  ALLOCSET_DEFAULT_MINSIZE, \
									  ALLOCSET_DEFAULT_INITSIZE, \
									  ALLOCSET_DEFAULT_MAXSIZE)
#else
	#define RumContextCreate(parent, name) \
		AllocSetContextCreate(parent, name, \
							  ALLOCSET_DEFAULT_MINSIZE, \
							  ALLOCSET_DEFAULT_INITSIZE, \
							  ALLOCSET_DEFAULT_MAXSIZE)
#endif

#endif   /* __RUM_H__ */
