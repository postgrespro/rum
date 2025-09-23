/*-------------------------------------------------------------------------
 *
 * rum_debug_funcs.c
 *		Functions to investigate the content of RUM indexes
 *
 * Copyright (c) 2025, Postgres Professional
 *
 * IDENTIFICATION
 *		contrib/rum/rum_debug_funcs.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "fmgr.h"
#include "funcapi.h"
#include "catalog/namespace.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "access/relation.h"
#include "utils/varlena.h"
#include "rum.h"
#include "tsearch/ts_type.h"
#include "utils/lsyscache.h"
#include "catalog/pg_type_d.h"

PG_FUNCTION_INFO_V1(rum_metapage_info);
PG_FUNCTION_INFO_V1(rum_page_opaque_info);
PG_FUNCTION_INFO_V1(rum_page_items_info);

#define POS_STR_BUF_LENGHT 1024
#define POS_MAX_VAL_LENGHT 6

#define VARLENA_MSG "varlena types in posting tree is " \
					"not supported"

#define cur_pitem_addinfo_is_normal(icdata) \
	(!((icdata)->cur_pitem.item.addInfoIsNull) && \
	(icdata)->cur_key_add_info_oid != InvalidOid)

#define cur_high_key_addinfo_is_normal(icdata) \
	(!(((icdata)->cur_high_key)->addInfoIsNull) && \
	(icdata)->cur_key_add_info_oid != InvalidOid)

#define addinfo_is_positions(icdata) \
	((icdata)->cur_key_add_info_oid == BYTEAOID)

#define is_entry_internal_high_key(icdata) \
	(RumPageRightMost((icdata)->page) && \
	(icdata)->cur_tuple_num == (icdata)->maxoff)

#define cur_key_is_norm(icdata) \
	((icdata)->cur_key_category == RUM_CAT_NORM_KEY)

#define is_data_page(icdata) \
	((icdata)->page_type == LEAF_DATA_PAGE || \
	(icdata)->page_type == INTERNAL_DATA_PAGE)

#define is_entry_page(icdata) \
	((icdata)->page_type == LEAF_ENTRY_PAGE || \
	(icdata)->page_type == INTERNAL_ENTRY_PAGE)

#define get_add_info_attr(icdata) \
	((icdata)->rum_state_ptr->addAttrs[(icdata)->cur_key_attnum - 1])

#define get_new_index_tuple(icdata) \
do { \
	(icdata)->cur_itup = \
		(IndexTuple) PageGetItem((icdata)->page, \
					PageGetItemId((icdata)->page, \
					(icdata)->cur_tuple_num)); \
} while(0)

#define get_new_item_posting_list(icdata) \
do { \
	(icdata)->item_ptr = \
		rumDataPageLeafRead((icdata)->item_ptr, \
		(icdata)->cur_key_attnum, \
		&((icdata)->cur_pitem.item), \
		false, (icdata)->rum_state_ptr); \
} while(0);

#define write_res_addinfo_is_null_to_values(icdata, counter) \
do { \
	(icdata)->values[(counter)] = \
		BoolGetDatum((icdata)->cur_pitem.item.addInfoIsNull); \
} while(0)

#if PG_VERSION_NUM >= 160000
#define write_res_iptr_to_values(icdata, counter) \
do { \
	(icdata)->values[(counter)] = \
		ItemPointerGetDatum(&((icdata)->cur_pitem.item.iptr)); \
} while(0)
#else
#define write_res_iptr_to_values(icdata, counter) \
do { \
	(icdata)->values[(counter)] = \
		PointerGetDatum(&((icdata)->cur_pitem.item.iptr)); \
} while(0)
#endif

#define write_res_blck_num_to_values(icdata, counter) \
do { \
	(icdata)->values[(counter)] = \
		UInt32GetDatum(PostingItemGetBlockNumber(&((icdata)->cur_pitem))); \
} while(0)

#define write_res_addinfo_to_values(icdata, counter) \
do { \
	(icdata)->values[(counter)] = \
		get_datum_text_by_oid((icdata)->cur_pitem.item.addInfo, \
		(icdata)->cur_key_add_info_oid); \
} while(0)

#define write_res_addinfo_pos_to_values(icdata, counter) \
do { \
	(icdata)->values[(counter)] = \
		get_positions_to_text_datum((icdata)->cur_pitem.item.addInfo); \
} while(0)

#define read_high_key_data_page(icdata) \
do { \
	memcpy(&((icdata)->cur_pitem.item), \
		RumDataPageGetRightBound((icdata)->page), \
		sizeof(RumItem)); \
} while(0)

#define read_key_data_page(icdata) \
do { \
	memcpy(&((icdata)->cur_pitem), \
		RumDataPageGetItem((icdata)->page, \
		(icdata)->srf_fctx->call_cntr), sizeof(PostingItem)); \
} while(0)

#define prepare_result_tuple(icdata) \
do { \
	(icdata)->resultTuple = \
		heap_form_tuple((icdata)->srf_fctx->tuple_desc, \
		(icdata)->values, (icdata)->nulls); \
	(icdata)->result = \
		HeapTupleGetDatum((icdata)->resultTuple); \
} while(0)

#define prepare_cur_pitem_to_posting_list(icdata) \
	memset(&((icdata)->cur_pitem), 0, sizeof(PostingItem))

/*
 * This is necessary in order for the prepare_scan()
 * function to determine the type of the scanned page.
 */
typedef enum
{
	LEAF_DATA_PAGE = 0,
	INTERNAL_DATA_PAGE = 1,
	LEAF_ENTRY_PAGE = 2,
	INTERNAL_ENTRY_PAGE = 3
} page_type_flags;

/*
 * The size of the result arrays (values
 * and nulls, see rum_page_items_state
 * structure) depends on the type of page.
 */
typedef enum
{
	LEAF_DATA_PAGE_RES_SIZE = 4,
	INTERNAL_DATA_PAGE_RES_SIZE = 5,
	LEAF_ENTRY_PAGE_RES_SIZE = 8,
	INTERNAL_ENTRY_PAGE_RES_SIZE = 5,
} page_type_res_size;

/*
 * A structure that stores information between
 * calls to the rum_page_items_info() function.
 * This information is necessary to scan the page.
 */
typedef struct rum_page_items_state
{
	/* Scanned page info  */
	Page			page;
	uint32			page_num;

	/*
	 * The type of the scanned page, can be:
	 * {} -- INTERNAL_ENTRY_PAGE
	 * {leaf} -- LEAF_ENTRY_PAGE
	 * {data} -- INTERNAL_DATA_PAGE
	 * {data, leaf} -- LEAF_DATA_PAGE
	 */
	page_type_flags	page_type;

	/*
	 * The number of scanned items per page.
	 *
	 * On the {leaf, data} page, this is the number of
	 * RumItem structures that are in the compressed posting list.
	 *
	 * On the {data} page, this is the number of PostingItem structures.
	 *
	 * On the {leaf} page, this is the number of IndexTuple, each of
	 * which contains a compressed posting list. In this case, the size
	 * of the Posting list is determined using RumGetNPosting(itup).
	 *
	 * On the {} page, this is the number of IndexTuple.
	 */
	int				maxoff;

	/* Pointer to the current scanning item */
	Pointer			item_ptr;

	/*
	 * It is used where posting lists are scanned.
	 * Sometimes only the RumItem it contains is used.
	 */
	PostingItem		cur_pitem;

	/* Current IndexTuple on the page */
	IndexTuple		cur_itup;

	/* The number of the current IndexTuple on the page */
	OffsetNumber	cur_tuple_num;

	/* The number of the current element in the current IndexTuple */
	int				cur_tuple_item_num;

	/*
	 * The number of the child page that
	 * is stored in the current IndexTuple
	 */
	BlockNumber		cur_tuple_down_link;

	/*
	 * If the current IndexTuple is scanned, then
	 * you need to move on to the next one.
	 */
	bool			need_new_tuple;

	/*
	 * Parameters of the current key in the IndexTuple
	 * or the key for which the posting tree was built.
	 */
	OffsetNumber	cur_key_attnum;
	Datum			cur_key;
	RumNullCategory	cur_key_category;
	Oid				cur_key_oid;

	/* Information about the type of additional information */
	bool			cur_key_add_info_is_null;
	Oid				cur_key_add_info_oid;
	bool			cur_key_add_info_byval;

	/*
	 * To generate the results of each
	 * function call rum_page_items_info()
	 */
	Datum			result;
	HeapTuple		resultTuple;
	Datum			*values;
	bool			*nulls;
	FuncCallContext	*srf_fctx;

	/*
	 * A pointer to the RumState structure
	 * that describes the scanned index.
	 */
	RumState		*rum_state_ptr;
} rum_page_items_state;

static Datum get_datum_text_by_oid(Datum info, Oid info_id);
static Relation get_rel_from_name(text *relname);
static Page get_rel_page(Relation rel, BlockNumber blkno);
static Oid get_cur_tuple_key_oid(rum_page_items_state *inter_call_data);
static Datum category_get_datum_text(RumNullCategory category);
static Datum get_positions_to_text_datum(Datum add_info);
static char pos_get_weight(WordEntryPos position);
static void check_superuser(void);
static void check_page_opaque_data_size(Page page);
static void check_page_is_meta_page(RumPageOpaque opaq);
static void check_page_is_leaf_data_page(RumPageOpaque opaq);
static void check_page_is_internal_data_page(RumPageOpaque opaq);
static void check_page_is_leaf_entry_page(RumPageOpaque opaq);
static void check_page_is_internal_entry_page(RumPageOpaque opaq);
static bool prepare_scan(text *relname, uint32 blkno,
	rum_page_items_state **inter_call_data, FuncCallContext *srf_fctx,
	page_type_flags page_type);
static void data_page_get_next_result(rum_page_items_state *inter_call_data);
static void get_entry_index_tuple_values(rum_page_items_state *inter_call_data);
static void entry_internal_page_get_next_result(rum_page_items_state *inter_call_data);
static void entry_leaf_page_get_next_result(rum_page_items_state *inter_call_data);
static void get_entry_leaf_posting_list_result(rum_page_items_state *inter_call_data);
static void get_entry_leaf_posting_tree_result(rum_page_items_state *inter_call_data);
static void prepare_new_entry_leaf_posting_list(rum_page_items_state *inter_call_data);
static void write_res_cur_tuple_key_to_values(rum_page_items_state *inter_call_data);
static bool find_page_in_posting_tree(BlockNumber target_page_num,
	BlockNumber cur_page_num, RumState *rum_state_ptr);
static bool find_posting_tree_root(BlockNumber *cur_page_num,
	OffsetNumber *cur_tuple_num, OffsetNumber *cur_key_attnum,
	BlockNumber *posting_root_num, RumState *rum_state_ptr);
static BlockNumber find_min_entry_leaf_page(rum_page_items_state *inter_call_data);
static OffsetNumber find_attnum_posting_tree_key(rum_page_items_state *inter_call_data);

/*
 * The rum_metapage_info() function is used to retrieve
 * information stored on the meta page of the rum index.
 * To scan, need the index name and the page number.
 * (for the meta page blkno = 0).
 */
Datum
rum_metapage_info(PG_FUNCTION_ARGS)
{
	/* Reading input arguments */
	text				*relname = PG_GETARG_TEXT_PP(0);
	uint32				blkno = PG_GETARG_UINT32(1);

	Relation			rel;			/* needed to initialize the RumState structure */

	RumPageOpaque		opaq;			/* data from the opaque area of the page */
	RumMetaPageData		*metadata;		/* data stored on the meta page */
	Page				page;			/* the page to be scanned */

	TupleDesc			tupdesc;		/* description of the result tuple */
	HeapTuple			resultTuple;	/* for the results */
	Datum				values[10];		/* return values */
	bool				nulls[10];		/* true if the corresponding value is NULL */

	/*
	 * To output the index version.
	 * If you change the index version, you
	 * may need to increase the buffer size.
	 */
	char				version_buf[20];

	/* Only the superuser can use this */
	check_superuser();

	/* Getting rel by name and page by number */
	rel = get_rel_from_name(relname);
	page = get_rel_page(rel, blkno);
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
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Getting information from the meta page */
	metadata = RumPageGetMeta(page);

	memset(nulls, 0, sizeof(nulls));

	/*
	 * Writing data from metadata to values.
	 *
	 * The first five values are obsolete because the
	 * pending list was removed from the rum index.
	 */
	values[0] = Int64GetDatum(metadata->head);
	values[1] = Int64GetDatum(metadata->tail);
	values[2] = Int32GetDatum(metadata->tailFreeSize);
	values[3] = Int64GetDatum(metadata->nPendingPages);
	values[4] = Int64GetDatum(metadata->nPendingHeapTuples);
	values[5] = Int64GetDatum(metadata->nTotalPages);
	values[6] = Int64GetDatum(metadata->nEntryPages);
	values[7] = Int64GetDatum(metadata->nDataPages);
	values[8] = Int64GetDatum(metadata->nEntries);
	snprintf(version_buf, sizeof(version_buf), "0x%X", metadata->rumVersion);
	values[9] = CStringGetTextDatum(version_buf);

	pfree(page);

	/* Build and return the result tuple */
	resultTuple = heap_form_tuple(tupdesc, values, nulls);

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
	text				*relname = PG_GETARG_TEXT_PP(0);
	uint32				blkno = PG_GETARG_UINT32(1);

	Relation			rel;			/* needed to initialize the RumState structure */

	RumPageOpaque		opaq;			/* data from the opaque area of the page */
	Page				page;			/* the page to be scanned */

	HeapTuple			resultTuple;	/* for the results */
	TupleDesc			tupdesc;		/* description of the result tuple */

	Datum				values[5];		/* return values */
	bool				nulls[5];		/* true if the corresponding value is NULL */
	Datum				flags[16];		/* array with flags in text format */
	int					nflags = 0;		/* index in the array of flags */
	uint16				flagbits;		/* flags in the opaque area of the page */

	/* Only the superuser can use this */
	check_superuser();

	/* Getting rel by name and raw page by number */
	rel = get_rel_from_name(relname);
	page = get_rel_page(rel, blkno);
	relation_close(rel, AccessShareLock);

	/* If the page is new, the function should return NULL */
	if (PageIsNew(page))
		PG_RETURN_NULL();

	/* Checking the size of the opaque area of the page */
	check_page_opaque_data_size(page);

	/* Getting a page description from an opaque area */
	opaq = RumPageGetOpaque(page);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Convert the flags bitmask to an array of human-readable names */
	flagbits = opaq->flags;
	if (flagbits & RUM_DATA)
		flags[nflags++] = CStringGetTextDatum("data");
	if (flagbits & RUM_LEAF)
		flags[nflags++] = CStringGetTextDatum("leaf");
	if (flagbits & RUM_DELETED)
		flags[nflags++] = CStringGetTextDatum("deleted");
	if (flagbits & RUM_META)
		flags[nflags++] = CStringGetTextDatum("meta");
	if (flagbits & RUM_LIST)
		flags[nflags++] = CStringGetTextDatum("list");
	if (flagbits & RUM_LIST_FULLROW)
		flags[nflags++] = CStringGetTextDatum("list_fullrow");
	flagbits &= ~(RUM_DATA | RUM_LEAF | RUM_DELETED | RUM_META | RUM_LIST |
				RUM_LIST_FULLROW);
	if (flagbits)
	{
		/* any flags we don't recognize are printed in hex */
		flags[nflags++] = DirectFunctionCall1(to_hex32, Int32GetDatum(flagbits));
	}

	memset(nulls, 0, sizeof(nulls));

	/*
	 * Writing data from metadata to values.
	 */
	values[0] = Int64GetDatum(opaq->leftlink);
	values[1] = Int64GetDatum(opaq->rightlink);
	values[2] = Int32GetDatum(opaq->maxoff);
	values[3] = Int32GetDatum(opaq->freespace);

#if PG_VERSION_NUM >= 160000
	values[4] = PointerGetDatum(construct_array_builtin(flags, nflags, TEXTOID));
#elif PG_VERSION_NUM >= 130000
	values[4] = PointerGetDatum(construct_array(flags, nflags,
								TEXTOID, -1, false, TYPALIGN_INT));
#else
	values[4] = PointerGetDatum(construct_array(flags, nflags,
								TEXTOID, -1, false, 'i'));
#endif

	pfree(page);

	/* Build and return the result tuple. */
	resultTuple = heap_form_tuple(tupdesc, values, nulls);

	/* Returning the result */
	return HeapTupleGetDatum(resultTuple);
}

/*
 * The main universal function that is used to scan
 * all types of pages (except for the meta page).
 * There are four SQL wrappers made around this
 * function, each of which scans a specific type
 * of pages. page_type is used to select the type
 * of page to scan.
 */
Datum
rum_page_items_info(PG_FUNCTION_ARGS)
{
	/* Reading input arguments */
	text				*relname = PG_GETARG_TEXT_PP(0);
	uint32				blkno = PG_GETARG_UINT32(1);
	page_type_flags		page_type = PG_GETARG_UINT32(2);

	int					counter;

	/*
	 * The context of the function calls and the pointer
	 * to the long-lived inter_call_data structure.
	 */
	FuncCallContext				*fctx;
	rum_page_items_state		*inter_call_data;

	/* Only the superuser can use this */
	check_superuser();

	/*
	 * In the case of the first function call, it is necessary
	 * to get the page by its number and create a RumState
	 * structure for scanning the page.
	 */
	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc				tupdesc;	/* description of the result tuple */
		MemoryContext			oldmctx;	/* the old function memory context */

		/*
		 * Initializing the FuncCallContext structure and switching the memory
		 * context to the one needed for structures that must be saved during
		 * multiple calls.
		 */
		fctx = SRF_FIRSTCALL_INIT();
		oldmctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		/* Before scanning the page, you need to prepare inter_call_data */
		if (!prepare_scan(relname, blkno, &inter_call_data, fctx, page_type))
		{
			MemoryContextSwitchTo(oldmctx);
			PG_RETURN_NULL();
		}

		Assert(is_data_page(inter_call_data) || is_entry_page(inter_call_data));

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		/* Needed to for subsequent recording tupledesc in fctx */
		BlessTupleDesc(tupdesc);

		/*
		 * Save a pointer to a long-lived structure and
		 * tuple descriptor for our result type in fctx.
		 */
		fctx->user_fctx = inter_call_data;
		fctx->tuple_desc = tupdesc;

		/* Switching to the old memory context */
		MemoryContextSwitchTo(oldmctx);
	}

	/* Preparing to use the FuncCallContext */
	fctx = SRF_PERCALL_SETUP();

	/* In the current call, we are reading data from the previous one */
	inter_call_data = fctx->user_fctx;

	/* The counter is defined differently on different pages */
	if (is_data_page(inter_call_data))
		counter = fctx->call_cntr;
	else counter = inter_call_data->cur_tuple_num;

	/*
	 * Go through the page.
	 *
	 * In the case of scanning a Posting Tree page, the counter
	 * is fctx->call_cntr, which is 0 on the first call. The
	 * first call is special because it returns the high
	 * key from the pages of the Posting Tree (the high
	 * key is not counted in maxoff).
	 *
	 * On Entry tree pages, the high key is stored in
	 * the IndexTuple.
	 */
	if (counter <= inter_call_data->maxoff)
	{
		if (is_data_page(inter_call_data))
			data_page_get_next_result(inter_call_data);

		else if (inter_call_data->page_type == LEAF_ENTRY_PAGE)
			entry_leaf_page_get_next_result(inter_call_data);

		else entry_internal_page_get_next_result(inter_call_data);

		/* Returning the result of the current call */
		SRF_RETURN_NEXT(fctx, inter_call_data->result);
	}

	/* Completing the function */
	SRF_RETURN_DONE(fctx);
}

/*
 * An auxiliary function for preparing the scan.
 * Depending on the type of page, it fills in
 * inter_call_data and makes the necessary checks.
 */
static bool
prepare_scan(text *relname, uint32 blkno,
			 rum_page_items_state **inter_call_data,
			 FuncCallContext *srf_fctx,
			 page_type_flags page_type)
{
	Relation				rel;		/* needed to initialize the RumState structure */

	Page					page;		/* the page to be scanned */
	RumPageOpaque			opaq;		/* data from the opaque area of the page */

	int						res_size;

	/* Getting rel by name and page by number */
	rel = get_rel_from_name(relname);
	page = get_rel_page(rel, blkno);

	/* The page cannot be new */
	if (PageIsNew(page)) return false;

	/* Checking the size of the opaque area of the page */
	check_page_opaque_data_size(page);

	/* Getting a page description from an opaque area */
	opaq = RumPageGetOpaque(page);

	/* Allocating memory for a long-lived structure */
	*inter_call_data = palloc(sizeof(rum_page_items_state));

	/* Initializing the RumState structure */
	(*inter_call_data)->rum_state_ptr = palloc(sizeof(RumState));
	initRumState((*inter_call_data)->rum_state_ptr, rel);

	relation_close(rel, AccessShareLock);

	/*  Writing the page and page type into a long-lived structure */
	(*inter_call_data)->srf_fctx = srf_fctx;
	(*inter_call_data)->page = page;
	(*inter_call_data)->page_num = blkno;
	(*inter_call_data)->page_type = page_type;

	/* The number of results returned depends on the type of page */
	if ((*inter_call_data)->page_type == LEAF_DATA_PAGE)
		res_size = LEAF_DATA_PAGE_RES_SIZE;

	else if ((*inter_call_data)->page_type == INTERNAL_DATA_PAGE)
		res_size = INTERNAL_DATA_PAGE_RES_SIZE;

	else if ((*inter_call_data)->page_type == LEAF_ENTRY_PAGE)
		res_size = LEAF_ENTRY_PAGE_RES_SIZE;

	else res_size = INTERNAL_ENTRY_PAGE_RES_SIZE;

	/* Allocating memory for arrays of results */
	(*inter_call_data)->values = (Datum*) palloc(res_size * sizeof(Datum));
	(*inter_call_data)->nulls = (bool*) palloc(res_size * sizeof(bool));

	/*
	 * Depending on the type of page, it performs the
	 * necessary checks and writes the necessary data
	 * into a long-lived structure.
	 */
	if (is_data_page(*inter_call_data))
	{
		if ((*inter_call_data)->page_type == LEAF_DATA_PAGE)
			check_page_is_leaf_data_page(opaq);

		else check_page_is_internal_data_page(opaq);

		(*inter_call_data)->maxoff = opaq->maxoff;
		(*inter_call_data)->item_ptr = RumDataPageGetData(page);

		/*
		 * If the scanned page belongs to a posting tree,
		 * we do not know which key this posting tree was
		 * built for. However, we need to know the attribute
		 * number of the key in order to correctly determine
		 * the type of additional information that can be
		 * associated with it.
		 *
		 * The find_attnum_posting_tree_key() function is used
		 * to find the key attribute number. The function scans
		 * the index and searches for the page we are scanning
		 * in the posting tree, while remembering which key
		 * this posting tree was built for.
		 */
		(*inter_call_data)->cur_key_attnum =
			find_attnum_posting_tree_key(*inter_call_data);

		/* Error handling find_attnum_posting_tree_key() */
		if ((*inter_call_data)->cur_key_attnum == InvalidOffsetNumber)
			return false;

		if (get_add_info_attr(*inter_call_data))
		{
			(*inter_call_data)->cur_key_add_info_oid =
				get_add_info_attr(*inter_call_data)->atttypid;
			(*inter_call_data)->cur_key_add_info_byval =
				get_add_info_attr(*inter_call_data)->attbyval;
		}
	}

	else /* the entry tree page case */
	{
		if ((*inter_call_data)->page_type == LEAF_ENTRY_PAGE)
		{
			check_page_is_leaf_entry_page(opaq);

			(*inter_call_data)->need_new_tuple = true;
		}

		else check_page_is_internal_entry_page(opaq);

		(*inter_call_data)->maxoff = PageGetMaxOffsetNumber(page);
		(*inter_call_data)->cur_tuple_num = FirstOffsetNumber;
	}

	return true;
}

/*
 * An auxiliary function for reading information from leaf
 * and internal pages of the Posting Tree. For each call,
 * it returns the next result to be returned from the
 * rum_page_items_info() function.
 */
static void
data_page_get_next_result(rum_page_items_state *inter_call_data)
{
	int						counter = 0;

	/* Before returning the result, need to reset the nulls array */
	if (inter_call_data->page_type == LEAF_DATA_PAGE)
		memset(inter_call_data->nulls, 0,
		 LEAF_DATA_PAGE_RES_SIZE * sizeof(bool));
	else memset(inter_call_data->nulls, 0,
			 INTERNAL_DATA_PAGE_RES_SIZE * sizeof(bool));

	Assert(is_data_page(inter_call_data));

	/* Reading high key */
	if (inter_call_data->srf_fctx->call_cntr == 0)
		read_high_key_data_page(inter_call_data);

	/* Reading information from Posting List */
	else if (inter_call_data->page_type == LEAF_DATA_PAGE)
	{
		/*
		 * it is necessary for the correct reading of the
		 * tid (see the function rumdatapageleafread())
		 */
		if (inter_call_data->srf_fctx->call_cntr == 1)
			prepare_cur_pitem_to_posting_list(inter_call_data);

		/* Read new item */
		get_new_item_posting_list(inter_call_data);
	}

	/* Reading information from the internal data page */
	else read_key_data_page(inter_call_data);

	/* Write the read information into arrays of results */

	/*
	 * This means whether the result
	 * tuple is the high key or not
	 */
	if (inter_call_data->srf_fctx->call_cntr == 0)
	{
		inter_call_data->values[counter++] = BoolGetDatum(true);

		if (inter_call_data->page_type == INTERNAL_DATA_PAGE)
			inter_call_data->nulls[counter++] = true;
	}

	else /* if the result is not the high key */
	{
		inter_call_data->values[counter++] = BoolGetDatum(false);

		if (inter_call_data->page_type == INTERNAL_DATA_PAGE)
			write_res_blck_num_to_values(inter_call_data, counter++);
	}

	write_res_iptr_to_values(inter_call_data, counter++);
	write_res_addinfo_is_null_to_values(inter_call_data, counter++);

	/*
	 * Return of additional information depends on the
	 * type of page and the type of additional information
	 */
	if (cur_pitem_addinfo_is_normal(inter_call_data))
	{
		if (inter_call_data->page_type == LEAF_DATA_PAGE &&
			inter_call_data->srf_fctx->call_cntr != 0)
		{
			if (addinfo_is_positions(inter_call_data))
				write_res_addinfo_pos_to_values(inter_call_data, counter);

			else write_res_addinfo_to_values(inter_call_data, counter);
		}

		else /* If the page is internal or result is high key */
		{
			if (inter_call_data->cur_key_add_info_byval == false)
				inter_call_data->values[counter] =
					CStringGetTextDatum(VARLENA_MSG);

			else write_res_addinfo_to_values(inter_call_data, counter);
		}
	}

	/* If no additional information is available */
	else inter_call_data->nulls[counter] = true;

	/* Forming the returned tuple */
	prepare_result_tuple(inter_call_data);
}

/*
 * IndexTuples are located on the internal pages of the Etnry Tree.
 * Each IndexTuple contains a key and a link to a child page. This
 * function reads these values and generates the result tuple.
 */
static void
entry_internal_page_get_next_result(rum_page_items_state *inter_call_data)
{
	Assert(inter_call_data->page_type == INTERNAL_ENTRY_PAGE);

	/* Before returning the result, need to reset the nulls array */
	memset(inter_call_data->nulls, 0, INTERNAL_ENTRY_PAGE_RES_SIZE * sizeof(bool));

	/* Read the new IndexTuple */
	get_new_index_tuple(inter_call_data);

	/* Scanning the IndexTuple that we received earlier */
	get_entry_index_tuple_values(inter_call_data);

	/*
	 * On the rightmost page, in the last IndexTuple, there is a
	 * high key, which is assumed to be equal to +inf.
	 */
	if (is_entry_internal_high_key(inter_call_data))
	{
		inter_call_data->values[0] = CStringGetTextDatum("+inf");
		inter_call_data->nulls[1] = true;
		inter_call_data->nulls[2] = true;
		inter_call_data->values[3] =
		UInt32GetDatum(inter_call_data->cur_tuple_down_link);
	}

	/* Is not high key */
	else write_res_cur_tuple_key_to_values(inter_call_data);

	/* Forming the returned tuple */
	prepare_result_tuple(inter_call_data);

	/* Increase the counter before the next SRF call */
	inter_call_data->cur_tuple_num++;
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
entry_leaf_page_get_next_result(rum_page_items_state *inter_call_data)
{
	Assert(inter_call_data->page_type == LEAF_ENTRY_PAGE);

	/* Before returning the result, need to reset the nulls array */
	memset(inter_call_data->nulls, 0, LEAF_ENTRY_PAGE_RES_SIZE * sizeof(bool));

	if(inter_call_data->need_new_tuple)
	{
		/* Read the new IndexTuple */
		get_new_index_tuple(inter_call_data);

		/* Getting key and key attribute number */
		get_entry_index_tuple_values(inter_call_data);

		/* Getting the posting list */
		prepare_new_entry_leaf_posting_list(inter_call_data);

		/*
		 * The case when there is a posting tree
		 * instead of a compressed posting list
		 */
		if(RumIsPostingTree(inter_call_data->cur_itup))
		{
			get_entry_leaf_posting_tree_result(inter_call_data);
			return;
		}
	}

	get_entry_leaf_posting_list_result(inter_call_data);
}

/*
 * The Entry Tree leaf pages contain IndexTuples that contain
 * the key and either a compressed posting list or a link to
 * the root page of the Posting Tree. This function reads all
 * the values from posting list and generates the result tuple.
 */
static void
get_entry_leaf_posting_list_result(rum_page_items_state *inter_call_data)
{
	/*
	 * Start writing from 3, because the previous
	 * ones are occupied by a cur_tuple_key
	 */
	int						counter = 3;

	Assert(inter_call_data->page_type == LEAF_ENTRY_PAGE);

	/* Reading the RumItem structures from the IndexTuple */
	get_new_item_posting_list(inter_call_data);

	/* Write the read information into arrays of results */
	write_res_cur_tuple_key_to_values(inter_call_data);
	write_res_iptr_to_values(inter_call_data, counter++);
	write_res_addinfo_is_null_to_values(inter_call_data, counter++);

	if (cur_pitem_addinfo_is_normal(inter_call_data))
	{
		if (addinfo_is_positions(inter_call_data))
			write_res_addinfo_pos_to_values(inter_call_data, counter++);

		else write_res_addinfo_to_values(inter_call_data, counter++);
	}

	else inter_call_data->nulls[counter++] = true;

	/* The current IndexTuple does not contain a posting tree */
	inter_call_data->values[counter++] = BoolGetDatum(false);
	inter_call_data->nulls[counter] = true;

	/*
	 * If the current IndexTuple has ended, i.e. we have scanned all
	 * its RumItems, then we need to enable the need_new_tuple flag
	 * so that the next time the function is called, we can read
	 * a new IndexTuple from the page.
	 */
	inter_call_data->cur_tuple_item_num++;
	if(inter_call_data->cur_tuple_item_num >
		RumGetNPosting(inter_call_data->cur_itup))
		inter_call_data->need_new_tuple = true;

	/* Forming the returned tuple */
	prepare_result_tuple(inter_call_data);
}

/*
 * This function is used to prepare for scanning
 * the posting list on Entry Tree leaf pages.
 */
static void
prepare_new_entry_leaf_posting_list(rum_page_items_state *inter_call_data)
{
	Assert(inter_call_data->page_type == LEAF_ENTRY_PAGE);

	/* Getting the posting list */
	inter_call_data->item_ptr =
		RumGetPosting(inter_call_data->cur_itup);
	inter_call_data->cur_tuple_item_num = 1;
	inter_call_data->need_new_tuple = false;
	inter_call_data->cur_tuple_num++;

	/*
	 * Every time you read a new IndexTuple, you need to reset the
	 * tid for the rumDataPageLeafRead() function to work correctly.
	 */
	prepare_cur_pitem_to_posting_list(inter_call_data);
}

/*
 * The Entry Tree leaf pages contain IndexTuples that contain
 * the key and either a compressed posting list or a link to
 * the root page of the Posting Tree. This function reads all
 * the values from Posting Tree and generates the result tuple.
 */
static void
get_entry_leaf_posting_tree_result(rum_page_items_state *inter_call_data)
{
	/*
	 * Start writing from 3, because the previous
	 * ones are occupied by a cur_tuple_key
	 */
	int						counter = 3;

	Assert(inter_call_data->page_type == LEAF_ENTRY_PAGE);

	/* Returning the key value */
	write_res_cur_tuple_key_to_values(inter_call_data);

	/* Everything stored in the RumItem structure has a NULL value */
	inter_call_data->nulls[counter++] = true;
	inter_call_data->nulls[counter++] = true;
	inter_call_data->nulls[counter++] = true;

	/* Returning the root of the posting tree */
	inter_call_data->values[counter++] = true;
	inter_call_data->values[counter++] =
		UInt32GetDatum(RumGetPostingTree(inter_call_data->cur_itup));

	/* Forming the returned tuple */
	prepare_result_tuple(inter_call_data);

	/* The next call will require a new IndexTuple */
	inter_call_data->need_new_tuple = true;
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
write_res_cur_tuple_key_to_values(rum_page_items_state *inter_call_data)
{
	int						counter = 0;

	if(cur_key_is_norm(inter_call_data))
		inter_call_data->values[counter++] =
			get_datum_text_by_oid(inter_call_data->cur_key,
						inter_call_data->cur_key_oid);
	else inter_call_data->nulls[counter++] = true;

	inter_call_data->values[counter++] =
		UInt16GetDatum(inter_call_data->cur_key_attnum);

	inter_call_data->values[counter++] =
		category_get_datum_text(inter_call_data->cur_key_category);

	if (inter_call_data->page_type == INTERNAL_ENTRY_PAGE)
		inter_call_data->values[counter] =
			UInt32GetDatum(inter_call_data->cur_tuple_down_link);
}

/*
 * This function gets the attribute number of the
 * current tuple key from the RumState structure.
 */
static Oid
get_cur_tuple_key_oid(rum_page_items_state *inter_call_data)
{
	Oid						result;
	TupleDesc				orig_tuple_desc;
	OffsetNumber			attnum;
	FormData_pg_attribute	*attrs;

	attnum = inter_call_data->cur_key_attnum;
	orig_tuple_desc = inter_call_data->rum_state_ptr->origTupdesc;
	attrs = orig_tuple_desc->attrs;
	result = (attrs[attnum - 1]).atttypid;

	return result;
}

/*
 * The function is used to extract values
 * from a previously read IndexTuple.
 */
static void
get_entry_index_tuple_values(rum_page_items_state *inter_call_data)
{
	RumState *rum_state_ptr = inter_call_data->rum_state_ptr;

	/* Scanning the IndexTuple */
	inter_call_data->cur_key_attnum =
		rumtuple_get_attrnum(rum_state_ptr,
							inter_call_data->cur_itup);

	inter_call_data->cur_key =
		rumtuple_get_key(rum_state_ptr,
						inter_call_data->cur_itup,
						&(inter_call_data->cur_key_category));

	inter_call_data->cur_key_oid =
		get_cur_tuple_key_oid(inter_call_data);

	if (inter_call_data->page_type == INTERNAL_ENTRY_PAGE)
		inter_call_data->cur_tuple_down_link =
			RumGetDownlink(inter_call_data->cur_itup);

	if (inter_call_data->page_type == LEAF_ENTRY_PAGE &&
		get_add_info_attr(inter_call_data))
	{
		(inter_call_data)->cur_key_add_info_oid =
			get_add_info_attr(inter_call_data)->atttypid;
		(inter_call_data)->cur_key_add_info_byval =
			get_add_info_attr(inter_call_data)->attbyval;
	}
}

/*
 * This function and get_rel_raw_page() are derived
 * from the separation of the get_raw_page_internal()
 * function, which was copied from the pageinspect code.
 * It is needed in order to call the initRumState()
 * function if necessary.
 */
static Relation
get_rel_from_name(text *relname)
{
	RangeVar			*relrv;
	Relation			rel;

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
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
 * This function is used to get
 * a copy of the relation page.
 */
static Page
get_rel_page(Relation rel, BlockNumber blkno)
{
	bytea				*raw_page;
	char				*raw_page_data;
	int					raw_page_size;
	Buffer				buf;
	Page				page;

	if (blkno >= RelationGetNumberOfBlocksInFork(rel, MAIN_FORKNUM))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("block number %u is out of range for relation \"%s\"",
						blkno, RelationGetRelationName(rel))));

	/* Initialize buffer to copy to */
	raw_page = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(raw_page, BLCKSZ + VARHDRSZ);
	raw_page_data = VARDATA(raw_page);

	/* Take a verbatim copy of the page */
	buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_SHARE);

	memcpy(raw_page_data, BufferGetPage(buf), BLCKSZ);

	LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buf);

	raw_page_size = VARSIZE_ANY_EXHDR(raw_page);

	if (raw_page_size != BLCKSZ)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid page size"),
				 errdetail("Expected %d bytes, got %d.",
						   BLCKSZ, raw_page_size)));

	page = palloc(raw_page_size);

	memcpy(page, VARDATA_ANY(raw_page), raw_page_size);

	/* Cleaning the memory that raw_page occupies */
	pfree(raw_page);

	return page;
}

/*
 * In the case of scanning a posting tree page, we
 * do not know the key for which this posting tree
 * was built (as well as the key attribute number).
 * This function finds the attribute number of the
 * key for which the posting tree was built.
 *
 * First, the function descends to the leftmost
 * leaf page of the entry tree, after which it
 * searches for links to the posting tree there and
 * searches in each posting tree for the number of
 * the page we are scanning. If the page you are
 * looking for is found, then you need to return
 * the key attribute number that was contained in
 * the IndexTuple along with the link to the posting
 * tree. If nothing is found, the function returns
 * InvalidOffsetNumber.
 */
static OffsetNumber
find_attnum_posting_tree_key(rum_page_items_state *inter_call_data)
{
	BlockNumber			target_page_num = inter_call_data->page_num;
	RumState			*rum_state_ptr = inter_call_data->rum_state_ptr;

	/* Returned result*/
	OffsetNumber		key_attnum = InvalidOffsetNumber;

	/*
	 * The page search starts from the first
	 * internal page of the entry tree.
	 */
	BlockNumber			cur_page_num = 1;
	OffsetNumber		cur_tuple_num = FirstOffsetNumber;
	BlockNumber			posting_root_num = InvalidBlockNumber;

	/* Search for the leftmost leaf page of the entry tree */
	cur_page_num = find_min_entry_leaf_page(inter_call_data);

	/*
	 * At each iteration of the loop, we find the
	 * root of the posting tree, then we search for
	 * the desired page in this posting tree. The
	 * loop ends when a page is found, or when
	 * there is no longer a posting tree.
	 */
	while (find_posting_tree_root(&cur_page_num, &cur_tuple_num,
			&key_attnum, &posting_root_num, rum_state_ptr))
	{
		if (posting_root_num == target_page_num ||
			find_page_in_posting_tree(target_page_num,
					posting_root_num, rum_state_ptr))
			break;
	}

	return key_attnum;
}

/*
 * The function is used to find the number
 * of the leftmost leaf page of the entry tree.
 */
static BlockNumber
find_min_entry_leaf_page(rum_page_items_state *inter_call_data)
{
	RumState			*rum_state_ptr = inter_call_data->rum_state_ptr;

	/*
	 * The page search starts from the first
	 * internal page of the entry tree.
	 */
	BlockNumber			cur_page_num = 1;
	Page				cur_page;
	RumPageOpaque		cur_opaq;
	IndexTuple			cur_itup;

	for (;;)
	{
		/* Getting page by number */
		cur_page = get_rel_page(rum_state_ptr->index, cur_page_num);

		/* The page cannot be new */
		if (PageIsNew(cur_page)) return InvalidOffsetNumber;

		/* Getting a page description from an opaque area */
		cur_opaq = RumPageGetOpaque(cur_page);

		/* If the required page is found */
		if (cur_opaq->flags == RUM_LEAF && RumPageLeftMost(cur_page))
		{
			pfree(cur_page);
			return cur_page_num;
		}

		/* If cur_page is still an internal page */
		else if (cur_opaq->flags == 0)
		{
			/* Read the first IndexTuple */
			cur_itup = (IndexTuple) PageGetItem(cur_page,
								PageGetItemId(cur_page, 1));

			/* Step onto the child page */
			cur_page_num = RumGetDownlink(cur_itup);
			pfree(cur_page);
		}

		else /* Error case */
		{
			pfree(cur_page);
			return InvalidBlockNumber;
		}
	}
}

/*
 * This function is used to sequentially find
 * the roots of the posting tree. In the first
 * call, *cur_page_num should be the leftmost
 * leaf page of the entry tree, and *cur_tuple_num
 * should be equal to FirstOffsetNumber. Then, for
 * each call, the function will return the root
 * number of the posting tree until they exhaust.
 */
static bool
find_posting_tree_root(BlockNumber *cur_page_num,
					   OffsetNumber *cur_tuple_num,
					   OffsetNumber *cur_key_attnum,
					   BlockNumber *posting_root_num,
					   RumState *rum_state_ptr)
{
	Page				cur_page;
	RumPageOpaque		cur_opaq;
	IndexTuple			cur_itup;

	for (;;)
	{
		/* Getting rel by name and page by number */
		cur_page = get_rel_page(rum_state_ptr->index, *cur_page_num);

		/* The page cannot be new */
		if (PageIsNew(cur_page)) break;

		/* Getting a page description from an opaque area */
		cur_opaq = RumPageGetOpaque(cur_page);

		Assert(cur_opaq->flags == RUM_LEAF);

		/* Scanning current page */
		while (*cur_tuple_num <= PageGetMaxOffsetNumber(cur_page))
		{
			cur_itup = (IndexTuple)
				PageGetItem(cur_page, PageGetItemId(cur_page, *cur_tuple_num));

			(*cur_tuple_num)++;

			*cur_key_attnum = rumtuple_get_attrnum(rum_state_ptr, cur_itup);

			if (RumIsPostingTree(cur_itup))
			{
				*posting_root_num = RumGetPostingTree(cur_itup);
				pfree(cur_page);
				return true;
			}
		}

		/*
		 * If haven't found anything, need to move on
		 * or terminate the function if the pages are over.
		 */
		if (cur_opaq->rightlink == InvalidBlockNumber) break;
		else
		{
			*cur_page_num = cur_opaq->rightlink;
			*cur_tuple_num = FirstOffsetNumber;
			pfree(cur_page);
		}
	}

	/* Error case */
	*cur_page_num = InvalidBlockNumber;
	*posting_root_num = InvalidBlockNumber;
	*cur_tuple_num = InvalidOffsetNumber;
	*cur_key_attnum = InvalidOffsetNumber;
	pfree(cur_page);
	return false;
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
find_page_in_posting_tree(BlockNumber target_page_num,
						  BlockNumber cur_page_num,
						  RumState *rum_state_ptr)
{
	Page				cur_page;
	RumPageOpaque		cur_opaq;
	PostingItem			cur_pitem;
	BlockNumber			next_page_num;

	/* Page loop */
	for (;;)
	{
		cur_page = get_rel_page(rum_state_ptr->index, cur_page_num);

		/* The page cannot be new */
		if (PageIsNew(cur_page))
		{
			pfree(cur_page);
			return false;
		}

		/* Getting a page description from an opaque area */
		cur_opaq = RumPageGetOpaque(cur_page);

		/* If this is a leaf page, we stop the loop */
		if (cur_opaq->flags == (RUM_DATA | RUM_LEAF))
		{
			pfree(cur_page);
			return false;
		}

		/*
		 * Reading the first PostingItem from
		 * the current page. This is necessary
		 * to remember the link down.
		 */
		memcpy(&cur_pitem,
			RumDataPageGetItem(cur_page, 1), sizeof(PostingItem));
		next_page_num = PostingItemGetBlockNumber(&cur_pitem);

		/* The loop that scans the page */
		for (int i = 1; i <= cur_opaq->maxoff; i++)
		{
			/* Reading the PostingItem from the current page */
			memcpy(&cur_pitem,
				RumDataPageGetItem(cur_page, i), sizeof(PostingItem));

			if (target_page_num == PostingItemGetBlockNumber(&cur_pitem))
			{
				pfree(cur_page);
				return true;
			}
		}

		/* Go to the next page */

		/* If a step to the right is impossible, step down */
		if (cur_opaq->rightlink == InvalidBlockNumber)
			cur_page_num = next_page_num;

		/* Step to the right */
		else cur_page_num = cur_opaq->rightlink;

		pfree(cur_page);
	}
}

/*
 * A function for extracting the positions of lexemes
 * from additional information. Returns a string in
 * which the positions of the lexemes are recorded.
 */
static Datum
get_positions_to_text_datum(Datum add_info)
{
	bytea			*positions;
	char			*ptrt;
	WordEntryPos	position = 0;
	int32			npos;

	Datum			res;
	char			*positions_str;
	char			*positions_str_cur_ptr;
	int				cur_max_str_lenght;

	positions = DatumGetByteaP(add_info);
	ptrt = (char *) VARDATA_ANY(positions);
	npos = count_pos(VARDATA_ANY(positions),
					 VARSIZE_ANY_EXHDR(positions));

	/* Initialize the string */
	positions_str = (char*) palloc(POS_STR_BUF_LENGHT * sizeof(char));
	positions_str[0] = '\0';
	cur_max_str_lenght = POS_STR_BUF_LENGHT - 1;
	positions_str_cur_ptr = positions_str;

	/* Extract the positions of the lexemes and put them in the string */
	for (int i = 0; i < npos; i++)
	{
		/* At each iteration decode the position */
		ptrt = decompress_pos(ptrt, &position);

		/* Write this position and weight in the string */
		if(pos_get_weight(position) == 'D')
				sprintf(positions_str_cur_ptr, "%d,", WEP_GETPOS(position));
		else sprintf(positions_str_cur_ptr, "%d%c,",
				WEP_GETPOS(position), pos_get_weight(position));

		/* Moving the pointer forward */
		positions_str_cur_ptr += strlen(positions_str_cur_ptr);

		/*
		 * Check that there is not too little left to the
		 * end of the line and, if necessary, overspend
		 * the memory.
		 */
		if (cur_max_str_lenght -
			(positions_str_cur_ptr - positions_str) <= POS_MAX_VAL_LENGHT)
		{
			cur_max_str_lenght +=  POS_STR_BUF_LENGHT;
			positions_str = (char*) repalloc(positions_str,
									cur_max_str_lenght * sizeof(char));
			positions_str_cur_ptr = positions_str + strlen(positions_str);
		}
	}

	/*
	 * Delete the last comma if there has
	 * been at least one iteration of the loop
	 */
	if (npos > 0)
		positions_str[strlen(positions_str) - 1] = '\0';

	res = CStringGetTextDatum(positions_str);
	pfree(positions_str);
	return res;
}

/*
 * The function extracts the weight and
 * returns the corresponding letter.
 */
static char
pos_get_weight(WordEntryPos position)
{
	char res = 'D';

	switch(WEP_GETWEIGHT(position))
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
 * The function is used to output
 * information stored in Datum as text.
 */
static Datum
get_datum_text_by_oid(Datum info, Oid info_id)
{
	Oid		func_id;
	bool	is_varlena = false;
	char	*info_str;

	Assert(OidIsValid(info_id));

	getTypeOutputInfo(info_id, &func_id, &is_varlena);

	info_str = OidOutputFunctionCall(func_id, info);

	return CStringGetTextDatum(info_str);
}

/*
 * This function returns the key category as text.
 */
static Datum
category_get_datum_text(RumNullCategory category)
{
	char category_arr[][20] = {"RUM_CAT_NORM_KEY",
							"RUM_CAT_NULL_KEY",
							"RUM_CAT_EMPTY_ITEM",
							"RUM_CAT_NULL_ITEM",
							"RUM_CAT_EMPTY_QUERY"};

	switch(category)
	{
		case RUM_CAT_NORM_KEY:
			return CStringGetTextDatum(category_arr[0]);

		case RUM_CAT_NULL_KEY:
			return CStringGetTextDatum(category_arr[1]);

		case RUM_CAT_EMPTY_ITEM:
			return CStringGetTextDatum(category_arr[2]);

		case RUM_CAT_NULL_ITEM:
			return CStringGetTextDatum(category_arr[3]);

		case RUM_CAT_EMPTY_QUERY:
			return CStringGetTextDatum(category_arr[4]);
	}

	/* In case of an error, return 0 */
	return (Datum) 0;
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
