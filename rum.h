/*-------------------------------------------------------------------------
 *
 * bloom.h
 *	  Exported definitions for RUM index.
 *
 * Portions Copyright (c) 2015-2016, Postgres Professional
 * Portions Copyright (c) 2006-2016, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */

#ifndef __RUM_H__
#define __RUM_H__

#include "access/amapi.h"
#include "access/genam.h"
#include "access/gin.h"
#include "access/itup.h"
#include "fmgr.h"
#include "lib/rbtree.h"
#include "storage/bufmgr.h"
#include "utils/tuplesort.h"

typedef struct XLogRecData
{
	char	   *data;			/* start of rmgr data to include */
	uint32		len;			/* length of rmgr data to include */
	Buffer		buffer;			/* buffer associated with data, if any */
	bool		buffer_std;		/* buffer has standard pd_lower/pd_upper */
	struct XLogRecData *next;	/* next struct in chain, or NULL */
} XLogRecData;

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
	BlockNumber rightlink;		/* next page if any */
	OffsetNumber maxoff;		/* number entries on RUM_DATA page: number of
								 * heap ItemPointers on RUM_DATA|RUM_LEAF page
								 * or number of PostingItems on RUM_DATA &
								 * ~RUM_LEAF page. On RUM_LIST page, number of
								 * heap tuples. */
	OffsetNumber freespace;
	uint16		flags;			/* see bit definitions below */
} RumPageOpaqueData;

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
	 * Pointers to head and tail of pending list, which consists of RUM_LIST
	 * pages.  These store fast-inserted entries that haven't yet been moved
	 * into the regular RUM structure.
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

	/*
	 * RUM version number (ideally this should have been at the front, but too
	 * late now.  Don't move it!)
	 *
	 * Currently 1 (for indexes initialized in 9.1 or later)
	 *
	 * Version 0 (indexes initialized in 9.0 or before) is compatible but may
	 * be missing null entries, including both null keys and placeholders.
	 * Reject full-index-scan attempts on such indexes.
	 */
	int32		rumVersion;
} RumMetaPageData;

#define RUM_CURRENT_VERSION		1

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
	ItemPointerSet((p), InvalidBlockNumber, (OffsetNumber)0xffff)
#define ItemPointerIsMax(p)  \
	(RumItemPointerGetOffsetNumber(p) == (OffsetNumber)0xffff && \
	 RumItemPointerGetBlockNumber(p) == InvalidBlockNumber)
#define ItemPointerSetLossyPage(p, b)  \
	ItemPointerSet((p), (b), (OffsetNumber)0xffff)
#define ItemPointerIsLossyPage(p)  \
	(RumItemPointerGetOffsetNumber(p) == (OffsetNumber)0xffff && \
	 RumItemPointerGetBlockNumber(p) != InvalidBlockNumber)

/*
 * Posting item in a non-leaf posting-tree page
 */
typedef struct
{
	/* We use BlockIdData not BlockNumber to avoid padding space wastage */
	BlockIdData child_blkno;
	ItemPointerData key;
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
/*#define RumGetNullCategory(itup,rumstate) \
  	(*((RumNullCategory *) ((char*)(itup) + RumCategoryOffset(itup,rumstate))))
  #define RumSetNullCategory(itup,rumstate,c) \
	(*((RumNullCategory *) ((char*)(itup) + RumCategoryOffset(itup,rumstate))) = (c))*/

#define RumGetNullCategory(itup,rumstate) \
      (*((RumNullCategory *) ((char*)(itup) + IndexTupleSize(itup) - sizeof(RumNullCategory))))
#define RumSetNullCategory(itup,rumstate,c) \
      (*((RumNullCategory *) ((char*)(itup) + IndexTupleSize(itup) - sizeof(RumNullCategory))) = (c))

/*
 * Access macros for leaf-page entry tuples (see discussion in README)
 */
#define RumGetNPosting(itup)	RumItemPointerGetOffsetNumber(&(itup)->t_tid)
#define RumSetNPosting(itup,n)	ItemPointerSetOffsetNumber(&(itup)->t_tid,n)
#define RUM_TREE_POSTING		((OffsetNumber)0xffff)
#define RumIsPostingTree(itup)	(RumGetNPosting(itup) == RUM_TREE_POSTING)
#define RumSetPostingTree(itup, blkno)	( RumSetNPosting((itup),RUM_TREE_POSTING), ItemPointerSetBlockNumber(&(itup)->t_tid, blkno) )
#define RumGetPostingTree(itup) RumItemPointerGetBlockNumber(&(itup)->t_tid)

#define RumGetPostingOffset(itup)	RumItemPointerGetBlockNumber(&(itup)->t_tid)
#define RumSetPostingOffset(itup,n) ItemPointerSetBlockNumber(&(itup)->t_tid,n)
#define RumGetPosting(itup)			((Pointer) ((char*)(itup) + RumGetPostingOffset(itup)))

#define RumMaxItemSize \
	MAXALIGN_DOWN(((BLCKSZ - SizeOfPageHeaderData - \
		MAXALIGN(sizeof(RumPageOpaqueData))) / 6 - sizeof(ItemIdData)))

/*
 * Access macros for non-leaf entry tuples
 */
#define RumGetDownlink(itup)	RumItemPointerGetBlockNumber(&(itup)->t_tid)
#define RumSetDownlink(itup,blkno)	ItemPointerSet(&(itup)->t_tid, blkno, InvalidOffsetNumber)


/*
 * Data (posting tree) pages
 */
#define RumDataPageGetRightBound(page)	((ItemPointer) PageGetContents(page))
#define RumDataPageGetData(page)	\
	(PageGetContents(page) + MAXALIGN(sizeof(ItemPointerData)))
#define RumSizeOfDataPageItem(page) \
	(RumPageIsLeaf(page) ? sizeof(ItemPointerData) : sizeof(PostingItem))
#define RumDataPageGetItem(page,i)	\
	(RumDataPageGetData(page) + ((i)-1) * RumSizeOfDataPageItem(page))

#define RumDataPageGetFreeSpace(page)	\
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) \
	 - MAXALIGN(sizeof(ItemPointerData)) \
	 - RumPageGetOpaque(page)->maxoff * RumSizeOfDataPageItem(page) \
	 - MAXALIGN(sizeof(RumPageOpaqueData)))

#define RumMaxLeafDataItems \
	((BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - \
	  MAXALIGN(sizeof(ItemPointerData)) - \
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
	uint16 pageOffset;
} RumDataLeafItemIndex;

#define RumDataLeafIndexCount 32

#define RumDataPageSize	\
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) \
	 - MAXALIGN(sizeof(ItemPointerData)) \
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
	bool		useFastUpdate;	/* use fast updates? */
} RumOptions;

#define RUM_DEFAULT_USE_FASTUPDATE	true
#define RumGetUseFastUpdate(relation) \
	((relation)->rd_options ? \
	 ((RumOptions *) (relation)->rd_options)->useFastUpdate : RUM_DEFAULT_USE_FASTUPDATE)


/* Macros for buffer lock/unlock operations */
#define RUM_UNLOCK	BUFFER_LOCK_UNLOCK
#define RUM_SHARE	BUFFER_LOCK_SHARE
#define RUM_EXCLUSIVE  BUFFER_LOCK_EXCLUSIVE


/*
 * RumState: working data structure describing the index being worked on
 */
typedef struct RumState
{
	Relation	index;
	bool		oneCol;			/* true if single-column index */

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
	Oid			addInfoTypeOid[INDEX_MAX_KEYS];
	Form_pg_attribute addAttrs[INDEX_MAX_KEYS];

	/*
	 * Per-index-column opclass support functions
	 */
	FmgrInfo	compareFn[INDEX_MAX_KEYS];
	FmgrInfo	extractValueFn[INDEX_MAX_KEYS];
	FmgrInfo	extractQueryFn[INDEX_MAX_KEYS];
	FmgrInfo	consistentFn[INDEX_MAX_KEYS];
	FmgrInfo	comparePartialFn[INDEX_MAX_KEYS];	/* optional method */
	FmgrInfo	configFn[INDEX_MAX_KEYS];			/* optional method */
	FmgrInfo	preConsistentFn[INDEX_MAX_KEYS];			/* optional method */
	FmgrInfo	orderingFn[INDEX_MAX_KEYS];			/* optional method */
	/* canPartialMatch[i] is true if comparePartialFn[i] is valid */
	bool		canPartialMatch[INDEX_MAX_KEYS];
	/* canPreConsistent[i] is true if preConsistentFn[i] is valid */
	bool		canPreConsistent[INDEX_MAX_KEYS];
	/* canOrdering[i] is true if orderingFn[i] is valid */
	bool		canOrdering[INDEX_MAX_KEYS];
	/* Collations to pass to the support functions */
	Oid			supportCollation[INDEX_MAX_KEYS];
} RumState;

typedef struct RumConfig
{
	Oid			addInfoTypeOid;
} RumConfig;

/* XLog stuff */

#define XLOG_RUM_CREATE_INDEX  0x00

#define XLOG_RUM_CREATE_PTREE  0x10

typedef struct rumxlogCreatePostingTree
{
	RelFileNode node;
	BlockNumber blkno;
	uint32		nitem;
	int16		typlen;

	bool		typbyval;
	char		typalign;
	char		typstorage;
	/* follows list of heap's ItemPointer */
} rumxlogCreatePostingTree;

#define XLOG_RUM_INSERT  0x20

typedef struct rumxlogInsert
{
	RelFileNode node;
	BlockNumber blkno;
	BlockNumber updateBlkno;
	OffsetNumber offset;
	OffsetNumber nitem;
	int16		typlen;

	bool		typbyval;
	char		typalign;
	char		typstorage;
	bool		isDelete;
	bool		isData;
	bool		isLeaf;

	/*
	 * follows: tuples or ItemPointerData or PostingItem or list of
	 * ItemPointerData
	 */
} rumxlogInsert;

#define XLOG_RUM_SPLIT	0x30

typedef struct rumxlogSplit
{
	RelFileNode node;
	BlockNumber lblkno;
	BlockNumber rootBlkno;
	BlockNumber rblkno;
	BlockNumber rrlink;
	OffsetNumber separator;
	OffsetNumber nitem;
	int16		typlen;

	bool		typbyval;
	char		typalign;
	char		typstorage;
	bool		isData;
	bool		isLeaf;
	bool		isRootSplit;

	BlockNumber leftChildBlkno;
	BlockNumber updateBlkno;

	ItemPointerData rightbound; /* used only in posting tree */
	/* follows: list of tuple or ItemPointerData or PostingItem */
} rumxlogSplit;

#define XLOG_RUM_VACUUM_PAGE	0x40

typedef struct rumxlogVacuumPage
{
	RelFileNode node;
	BlockNumber blkno;
	OffsetNumber nitem;
	int16		typlen;

	bool		typbyval;
	char		typalign;
	char		typstorage;
	/* follows content of page */
} rumxlogVacuumPage;

#define XLOG_RUM_DELETE_PAGE	0x50

typedef struct rumxlogDeletePage
{
	RelFileNode node;
	BlockNumber blkno;
	BlockNumber parentBlkno;
	OffsetNumber parentOffset;
	BlockNumber leftBlkno;
	BlockNumber rightLink;
} rumxlogDeletePage;

#define XLOG_RUM_UPDATE_META_PAGE 0x60

typedef struct rumxlogUpdateMeta
{
	RelFileNode node;
	RumMetaPageData metadata;
	BlockNumber prevTail;
	BlockNumber newRightlink;
	int32		ntuples;		/* if ntuples > 0 then metadata.tail was
								 * updated with that many tuples; else new sub
								 * list was inserted */
	/* array of inserted tuples follows */
} rumxlogUpdateMeta;

#define XLOG_RUM_INSERT_LISTPAGE  0x70

typedef struct rumxlogInsertListPage
{
	RelFileNode node;
	BlockNumber blkno;
	BlockNumber rightlink;
	int32		ntuples;
	/* array of inserted tuples follows */
} rumxlogInsertListPage;

#define XLOG_RUM_DELETE_LISTPAGE  0x80

#define RUM_NDELETE_AT_ONCE 16
typedef struct rumxlogDeleteListPages
{
	RelFileNode node;
	RumMetaPageData metadata;
	int32		ndeleted;
	BlockNumber toDelete[RUM_NDELETE_AT_ONCE];
} rumxlogDeleteListPages;


/* rumutil.c */
extern bytea *rumoptions(Datum reloptions, bool validate);
extern Datum rumhandler(PG_FUNCTION_ARGS);
extern void initRumState(RumState *state, Relation index);
extern Buffer RumNewBuffer(Relation index);
extern void RumInitBuffer(Buffer b, uint32 f);
extern void RumInitPage(Page page, uint32 f, Size pageSize);
extern void RumInitMetabuffer(Buffer b);
extern int rumCompareEntries(RumState *rumstate, OffsetNumber attnum,
				  Datum a, RumNullCategory categorya,
				  Datum b, RumNullCategory categoryb);
extern int rumCompareAttEntries(RumState *rumstate,
					 OffsetNumber attnuma, Datum a, RumNullCategory categorya,
				   OffsetNumber attnumb, Datum b, RumNullCategory categoryb);
extern Datum *rumExtractEntries(RumState *rumstate, OffsetNumber attnum,
				  Datum value, bool isNull,
				  int32 *nentries, RumNullCategory **categories,
				  Datum **addInfo, bool **addInfoIsNull);

extern OffsetNumber rumtuple_get_attrnum(RumState *rumstate, IndexTuple tuple);
extern Datum rumtuple_get_key(RumState *rumstate, IndexTuple tuple,
				 RumNullCategory *category);

extern void rumGetStats(Relation index, GinStatsData *stats);
extern void rumUpdateStats(Relation index, const GinStatsData *stats);

/* ruminsert.c */
extern IndexBuildResult *rumbuild(Relation heap, Relation index,
								  struct IndexInfo *indexInfo);
extern void rumbuildempty(Relation index);
extern bool ruminsert(Relation index, Datum *values, bool *isnull,
					  ItemPointer ht_ctid, Relation heapRel,
					  IndexUniqueCheck checkUnique);
extern void rumEntryInsert(RumState *rumstate,
			   OffsetNumber attnum, Datum key, RumNullCategory category,
			   ItemPointerData *items, Datum *addInfo,
			   bool *addInfoIsNull, uint32 nitem,
			   GinStatsData *buildStats);

/* rumbtree.c */

typedef struct RumBtreeStack
{
	BlockNumber blkno;
	Buffer		buffer;
	OffsetNumber off;
	/* predictNumber contains predicted number of pages on current level */
	uint32		predictNumber;
	struct RumBtreeStack *parent;
} RumBtreeStack;

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
	void		(*placeToPage) (RumBtree, Buffer, OffsetNumber, XLogRecData **);
	Page		(*splitPage) (RumBtree, Buffer, Buffer, OffsetNumber, XLogRecData **);
	void		(*fillRoot) (RumBtree, Buffer, Buffer, Buffer);

	bool		isData;
	bool		searchMode;

	Relation	index;
	RumState   *rumstate;		/* not valid in a data scan */
	bool		fullScan;
	bool		isBuild;

	BlockNumber rightblkno;

	/* Entry options */
	OffsetNumber entryAttnum;
	Datum		entryKey;
	RumNullCategory entryCategory;
	IndexTuple	entry;
	bool		isDelete;

	/* Data (posting tree) options */
	ItemPointerData *items;
	Datum		*addInfo;
	bool		*addInfoIsNull;

	uint32		nitem;
	uint32		curitem;

	PostingItem pitem;
} RumBtreeData;

extern RumBtreeStack *rumPrepareFindLeafPage(RumBtree btree, BlockNumber blkno);
extern RumBtreeStack *rumFindLeafPage(RumBtree btree, RumBtreeStack *stack);
extern RumBtreeStack *rumReFindLeafPage(RumBtree btree, RumBtreeStack *stack);
extern Buffer rumStepRight(Buffer buffer, Relation index, int lockmode);
extern void freeRumBtreeStack(RumBtreeStack *stack);
extern void rumInsertValue(RumBtree btree, RumBtreeStack *stack,
			   GinStatsData *buildStats);
extern void rumFindParents(RumBtree btree, RumBtreeStack *stack, BlockNumber rootBlkno);

/* rumentrypage.c */
extern void rumPrepareEntryScan(RumBtree btree, OffsetNumber attnum,
					Datum key, RumNullCategory category,
					RumState *rumstate);
extern void rumEntryFillRoot(RumBtree btree, Buffer root, Buffer lbuf, Buffer rbuf);
extern IndexTuple rumPageGetLinkItup(Buffer buf);
extern void rumReadTuple(RumState *rumstate, OffsetNumber attnum,
	IndexTuple itup, ItemPointerData *ipd, Datum *addInfo, bool *addInfoIsNull);
extern ItemPointerData updateItemIndexes(Page page, OffsetNumber attnum, RumState *rumstate);

/* rumdatapage.c */
extern int rumCompareItemPointers(ItemPointer a, ItemPointer b);
extern char *rumDataPageLeafWriteItemPointer(char *ptr, ItemPointer iptr, ItemPointer prev, bool addInfoIsNull);
extern Pointer rumPlaceToDataPageLeaf(Pointer ptr, OffsetNumber attnum,
	ItemPointer iptr, Datum addInfo, bool addInfoIsNull, ItemPointer prev,
	RumState *rumstate);
extern Size rumCheckPlaceToDataPageLeaf(OffsetNumber attnum,
	ItemPointer iptr, Datum addInfo, bool addInfoIsNull, ItemPointer prev,
	RumState *rumstate, Size size);
extern uint32 rumMergeItemPointers(ItemPointerData *dst, Datum *dst2, bool *dst3,
					 ItemPointerData *a, Datum *a2, bool *a3, uint32 na,
					 ItemPointerData *b, Datum * b2, bool *b3, uint32 nb);
extern void RumDataPageAddItem(Page page, void *data, OffsetNumber offset);
extern void RumPageDeletePostingItem(Page page, OffsetNumber offset);

typedef struct
{
	RumBtreeData btree;
	RumBtreeStack *stack;
} RumPostingTreeScan;

extern RumPostingTreeScan *rumPrepareScanPostingTree(Relation index,
						  BlockNumber rootBlkno, bool searchMode, OffsetNumber attnum, RumState *rumstate);
extern void rumInsertItemPointers(RumState *rumstate,
					  OffsetNumber attnum,
					  RumPostingTreeScan *gdi,
					  ItemPointerData *items,
					  Datum *addInfo,
					  bool *addInfoIsNull,
					  uint32 nitem,
					  GinStatsData *buildStats);
extern Buffer rumScanBeginPostingTree(RumPostingTreeScan *gdi);
extern void rumDataFillRoot(RumBtree btree, Buffer root, Buffer lbuf, Buffer rbuf);
extern void rumPrepareDataScan(RumBtree btree, Relation index, OffsetNumber attnum, RumState *rumstate);

/* rumscan.c */

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
	Datum	   *addInfo;
	bool	   *addInfoIsNull;

	/* other data needed for calling consistentFn */
	Datum		query;
	/* NB: these three arrays have only nuserentries elements! */
	Datum	   *queryValues;
	RumNullCategory *queryCategories;
	Pointer    *extra_data;
	StrategyNumber strategy;
	int32		searchMode;
	OffsetNumber attnum;

	/*
	 * Match status data.  curItem is the TID most recently tested (could be a
	 * lossy-page pointer).  curItemMatches is TRUE if it passes the
	 * consistentFn test; if so, recheckCurItem is the recheck flag.
	 * isFinished means that all the input entry streams are finished, so this
	 * key cannot succeed for any later TIDs.
	 */
	ItemPointerData curItem;
	bool		curItemMatches;
	bool		recheckCurItem;
	bool		isFinished;
	bool		orderBy;
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

	/* Current page in posting tree */
	Buffer		buffer;

	/* current ItemPointer to heap */
	ItemPointerData curItem;
	Datum		curAddInfo;
	bool		curAddInfoIsNull;

	/* for a partial-match or full-scan query, we accumulate all TIDs here */
	TIDBitmap  *matchBitmap;
	TBMIterator *matchIterator;
	TBMIterateResult *matchResult;

	/* used for Posting list and one page in Posting tree */
	ItemPointerData *list;
	Datum		*addInfo;
	bool		*addInfoIsNull;
	MemoryContext context;
	uint32		nlist;
	OffsetNumber offset;

	bool		isFinished;
	bool		reduceResult;
	bool		preValue;
	uint32		predictNumberResult;
	RumPostingTreeScan *gdi;
}	RumScanEntryData;

typedef struct
{
	ItemPointerData iptr;
	float8			distance;
	bool			recheck;
} RumOrderingItem;

typedef struct RumScanOpaqueData
{
	MemoryContext tempCtx;
	RumState	rumstate;

	RumScanKey	keys;			/* one per scan qualifier expr */
	uint32		nkeys;
	int			norderbys;

	RumScanEntry *entries;		/* one per index search condition */
	RumScanEntry *sortedEntries;		/* one per index search condition */
	int			entriesIncrIndex;
	uint32		totalentries;
	uint32		allocentries;	/* allocated length of entries[] */

	Tuplesortstate *sortstate;

	ItemPointerData iptr;
	bool		firstCall;
	bool		isVoidRes;		/* true if query is unsatisfiable */
	bool		useFastScan;
	TIDBitmap  *tbm;
} RumScanOpaqueData;

typedef RumScanOpaqueData *RumScanOpaque;

extern IndexScanDesc rumbeginscan(Relation rel, int nkeys, int norderbys);
extern void rumendscan(IndexScanDesc scan);
extern void rumrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
					  ScanKey orderbys, int norderbys);
extern Datum rummarkpos(PG_FUNCTION_ARGS);
extern Datum rumrestrpos(PG_FUNCTION_ARGS);
extern void rumNewScanKey(IndexScanDesc scan);

/* rumget.c */
extern int64 rumgetbitmap(IndexScanDesc scan, TIDBitmap *tbm);
extern Datum rumgettuple(PG_FUNCTION_ARGS);

/* rumvacuum.c */
extern IndexBulkDeleteResult *rumbulkdelete(IndexVacuumInfo *info,
			 IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback,
			 void *callback_state);
extern IndexBulkDeleteResult *rumvacuumcleanup(IndexVacuumInfo *info,
											   IndexBulkDeleteResult *stats);

typedef struct
{
	ItemPointerData	iptr;
	Datum			addInfo;
	bool			addInfoIsNull;
} RumEntryAccumulatorItem;

/* rumbulk.c */
typedef struct RumEntryAccumulator
{
	RBNode		rbnode;
	Datum		key;
	RumNullCategory category;
	OffsetNumber attnum;
	bool		shouldSort;
	RumEntryAccumulatorItem *list;
	uint32		maxcount;		/* allocated size of list[] */
	uint32		count;			/* current number of list[] entries */
} RumEntryAccumulator;

typedef struct
{
	RumState   *rumstate;
	long		allocatedMemory;
	RumEntryAccumulator *entryallocator;
	uint32		eas_used;
	RBTree	   *tree;
} BuildAccumulator;

extern void rumInitBA(BuildAccumulator *accum);
extern void rumInsertBAEntries(BuildAccumulator *accum,
				   ItemPointer heapptr, OffsetNumber attnum,
				   Datum *entries, Datum *addInfo, bool *addInfoIsNull,
				   RumNullCategory *categories, int32 nentries);
extern void rumBeginBAScan(BuildAccumulator *accum);
extern RumEntryAccumulatorItem *rumGetBAEntry(BuildAccumulator *accum,
			  OffsetNumber *attnum, Datum *key, RumNullCategory *category,
			  uint32 *n);

/* rumfast.c */

typedef struct RumTupleCollector
{
	IndexTuple *tuples;
	uint32		ntuples;
	uint32		lentuples;
	uint32		sumsize;
} RumTupleCollector;

extern void rumHeapTupleFastInsert(RumState *rumstate,
					   RumTupleCollector *collector);
extern void rumHeapTupleFastCollect(RumState *rumstate,
						RumTupleCollector *collector,
						OffsetNumber attnum, Datum value, bool isNull,
						ItemPointer ht_ctid);
extern void rumInsertCleanup(RumState *rumstate,
				 bool vac_delay, IndexBulkDeleteResult *stats);

/* rum_ts_utils.c */
#define RUM_CONFIG_PROC				   7
#define RUM_PRE_CONSISTENT_PROC		   8
#define RUM_ORDERING_PROC			   9

extern Datum rum_extract_tsvector(PG_FUNCTION_ARGS);
extern Datum rum_extract_tsquery(PG_FUNCTION_ARGS);
extern Datum rum_tsvector_config(PG_FUNCTION_ARGS);
extern Datum rum_tsquery_pre_consistent(PG_FUNCTION_ARGS);
extern Datum rum_tsquery_distance(PG_FUNCTION_ARGS);

/* GUC parameters */
extern PGDLLIMPORT int RumFuzzySearchLimit;

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
	uint32 blockNumberIncr = 0;
	uint16 offset = 0;
	int i;
	uint8 v;

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

	Assert((uint64)iptr->ip_blkid.bi_lo + ((uint64)iptr->ip_blkid.bi_hi << 16) +
				(uint64)blockNumberIncr < ((uint64)1 << 32));

	blockNumberIncr += iptr->ip_blkid.bi_lo + (iptr->ip_blkid.bi_hi << 16);

	iptr->ip_blkid.bi_lo = blockNumberIncr & 0xFFFF;
	iptr->ip_blkid.bi_hi = (blockNumberIncr >> 16) & 0xFFFF;

	i = 0;

	while(true)
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
 */
static inline Pointer
rumDataPageLeafRead(Pointer ptr, OffsetNumber attnum, ItemPointer iptr,
	Datum *addInfo, bool *addInfoIsNull, RumState *rumstate)
{
	Form_pg_attribute attr;
	bool isNull;

	ptr = rumDataPageLeafReadItemPointer(ptr, iptr, &isNull);

	Assert(iptr->ip_posid != InvalidOffsetNumber);

	if (addInfoIsNull)
		*addInfoIsNull = isNull;

	if (!isNull)
	{
		attr = rumstate->addAttrs[attnum - 1];
		ptr = (Pointer) att_align_pointer(ptr, attr->attalign, attr->attlen, ptr);
		if (addInfo)
			*addInfo = fetch_att(ptr,  attr->attbyval,  attr->attlen);
		ptr = (Pointer) att_addlength_pointer(ptr, attr->attlen, ptr);
	}
	return ptr;
}

extern Datum FunctionCall10Coll(FmgrInfo *flinfo, Oid collation,
								Datum arg1, Datum arg2,
								Datum arg3, Datum arg4, Datum arg5,
								Datum arg6, Datum arg7, Datum arg8,
								Datum arg9, Datum arg10);

#endif   /* __RUM_H__ */
