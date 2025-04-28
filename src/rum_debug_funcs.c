/*
 * rum_debug_funcs.c
 *		Functions to investigate the content of RUM indexes
 *
 * Copyright (c) 2025, Postgres Professional
 *
 * IDENTIFICATION
 *		contrib/rum/rum_debug_funcs.c
 *
 * LIST OF ISSUES:
 *
 * 1) Using obsolete macros in the get_datum_text_by_oid() function.
 *
 * 2) I/O functions were not available for all types in
 *	  in the get_datum_text_by_oid() function.
 *
 * 3) The output of lexeme positions in the high keys of the posting 
 * 	  tree is not supported.
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

PG_FUNCTION_INFO_V1(rum_metapage_info);
PG_FUNCTION_INFO_V1(rum_page_opaque_info);
PG_FUNCTION_INFO_V1(rum_leaf_data_page_items);
PG_FUNCTION_INFO_V1(rum_internal_data_page_items);
PG_FUNCTION_INFO_V1(rum_leaf_entry_page_items);
PG_FUNCTION_INFO_V1(rum_internal_entry_page_items);

/* 
 * A structure that stores information between calls to the 
 * rum_leaf_data_page_items(), rum_internal_data_page_items(),
 * rum_leaf_entry_page_items(), rum_internal_entry_page_items() 
 * functions. This information is necessary to scan the page.
 */
typedef struct rum_page_items_state
{
	/* Pointer to the beginning of the scanned page */
	Page 			page;

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
	int 			maxoff;

	/* Current IndexTuple on the page */
	IndexTuple 		cur_itup;

	/* The number of the current IndexTuple on the page */
	int 			cur_tuple_num;

	/* Pointer to the current scanning item */
	Pointer 		item_ptr; 	

	/* The number of the current element in the current IndexTuple */
	int 			cur_tuple_item;

	/* The attribute number of the current tuple key */
	OffsetNumber 	cur_tuple_key_attnum;

	/* The current tuple key */
	Datum 			cur_tuple_key;

	/* Current tuple key category (see rum.h) */
	RumNullCategory cur_tuple_key_category;

	/* The number of the child page that is stored in the current IndexTuple */
	BlockNumber 	cur_tuple_down_link;

	/* It is necessary to scan the posting list */
	RumItem 		cur_rum_item;

	/* 
	 * The Oid of the additional information in the index. 
	 * It is read from the RumConfig structure. 
	 */
	Oid 			add_info_oid;

	/* 
	 * If the current IndexTuple is scanned, then 
	 * you need to move on to the next one.
	 */
	bool 			need_new_tuple;

	/* A pointer to the RumState structure, which is needed to scan the page */
	RumState		*rum_state_ptr;
} rum_page_items_state;

static Page get_page_from_raw(bytea *raw_page);
static Datum get_datum_text_by_oid(Datum addInfo, Oid atttypid);
static Relation get_rel_from_name(text *relname);
static bytea *get_rel_raw_page(Relation rel, BlockNumber blkno);
static void get_new_index_tuple(rum_page_items_state *inter_call_data);
static Oid get_cur_attr_oid(rum_page_items_state *inter_call_data);
static Datum category_get_datum_text(RumNullCategory category);
static Oid find_add_info_oid(RumState *rum_state_ptr);
static OffsetNumber find_add_info_atrr_num(RumState *rum_state_ptr);
static Datum get_positions_to_text_datum(Datum add_info);
static char pos_get_weight(WordEntryPos position);

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
	text	   			*relname = PG_GETARG_TEXT_PP(0);
	uint32				blkno = PG_GETARG_UINT32(1);

	Relation 			rel;			/* needed to initialize the RumState structure */ 

	bytea	   			*raw_page;		/* the raw page obtained from rel */
	RumPageOpaque 		opaq;			/* data from the opaque area of the page */
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
	char 				version_buf[20];

	/* Only the superuser can use this */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use raw page functions")));

	/* Getting rel by name and raw page by number */
	rel = get_rel_from_name(relname);
	raw_page = get_rel_raw_page(rel, blkno);
	relation_close(rel, AccessShareLock);

	/* Getting a copy of the page from the raw page */
	page = get_page_from_raw(raw_page);

	/* If the page is new, the function should return NULL */
	if (PageIsNew(page))
		PG_RETURN_NULL();

	/* Checking the size of the opaque area of the page */
	if (PageGetSpecialSize(page) != MAXALIGN(sizeof(RumPageOpaqueData)))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("input page is not a valid RUM metapage"),
				 errdetail("Expected special size %d, got %d.",
						   (int) MAXALIGN(sizeof(RumPageOpaqueData)),
						   (int) PageGetSpecialSize(page))));

	/* Getting a page description from an opaque area */
	opaq = RumPageGetOpaque(page);

	/* Checking the flags */
	if (opaq->flags != RUM_META)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("input page is not a RUM metapage"),
				 errdetail("Flags %04X, expected %04X",
						   opaq->flags, RUM_META)));

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
	text	   			*relname = PG_GETARG_TEXT_PP(0);
	uint32				blkno = PG_GETARG_UINT32(1);

	Relation 			rel;			/* needed to initialize the RumState structure */ 

	bytea	   			*raw_page;		/* the raw page obtained from rel */
	RumPageOpaque 		opaq;			/* data from the opaque area of the page */
	Page				page;			/* the page to be scanned */

	HeapTuple			resultTuple;	/* for the results */
	TupleDesc			tupdesc;		/* description of the result tuple */

	Datum				values[5];		/* return values */
	bool				nulls[5];		/* true if the corresponding value is NULL */
	Datum				flags[16];		/* array with flags in text format */
	int					nflags = 0;		/* index in the array of flags */
	uint16				flagbits;		/* flags in the opaque area of the page */

	/* Only the superuser can use this */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use raw page functions")));

	/* Getting rel by name and raw page by number */
	rel = get_rel_from_name(relname);
	raw_page = get_rel_raw_page(rel, blkno);
	relation_close(rel, AccessShareLock);

	/* Getting a copy of the page from the raw page */
	page = get_page_from_raw(raw_page);

	/* If the page is new, the function should return NULL */
	if (PageIsNew(page))
		PG_RETURN_NULL();

	/* Checking the size of the opaque area of the page */
	if (PageGetSpecialSize(page) != MAXALIGN(sizeof(RumPageOpaqueData)))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("input page is not a valid RUM data leaf page"),
				 errdetail("Expected special size %d, got %d.",
						   (int) MAXALIGN(sizeof(RumPageOpaqueData)),
						   (int) PageGetSpecialSize(page))));

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
	values[4] = PointerGetDatum(construct_array_builtin(flags, nflags, TEXTOID));

	/* Build and return the result tuple. */
	resultTuple = heap_form_tuple(tupdesc, values, nulls);

	/* Returning the result */
	return HeapTupleGetDatum(resultTuple);
}

/*
 * The function rum_leaf_data_page_items() is designed
 * to view information that is located on the leaf
 * pages of the index's rum posting tree. On these pages, 
 * information is stored in a set of compressed posting lists 
 * (similar to entry tree leaf pages), but these posting lists 
 * are not in IndexTuples, but are located between the PageHeader 
 * and pd_lower. The space between pd_lower and pd_upper is not used.
 *
 * The function returns the bool variable is_high_key (true if 
 * the current returned tuple is the high key of the current page)
 * and the information contained in the corresponding structure of 
 * RumItem: tid, the bool variable add_info_is_null and add_info.
 *
 * It is a SRF function, i.e. it returns one tuple per call.
 */
Datum
rum_leaf_data_page_items(PG_FUNCTION_ARGS)
{
	/* Reading input arguments */
	text	   			*relname = PG_GETARG_TEXT_PP(0);
	uint32				blkno = PG_GETARG_UINT32(1);

	/* 
	 * The context of the function calls and the pointer 
	 * to the long-lived inter_call_data structure 
	 */
	FuncCallContext 			*fctx;
	rum_page_items_state 		*inter_call_data;

	/* Only the superuser can use this */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use this function")));

	/* 
	 * In the case of the first function call, it is necessary 
	 * to get the page by its number and create a Runstate 
	 * structure for scanning the page.
	 */
	if (SRF_IS_FIRSTCALL())
	{
		Relation 				rel;		/* needed to initialize the RumState structure */ 
		bytea	   			    *raw_page;	/* The raw page obtained from rel */

		TupleDesc			    tupdesc;	/* description of the result tuple */
		MemoryContext 			oldmctx;	/* the old function memory context */
		Page 					page;		/* the page to be scanned */
		RumPageOpaque 			opaq;		/* data from the opaque area of the page */

		/* 
		 * Initializing the FuncCallContext structure and switching the memory 
		 * context to the one needed for structures that must be saved during 
		 * multiple calls
		 */
		fctx = SRF_FIRSTCALL_INIT();
		oldmctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		/* Getting rel by name and raw page by number */
		rel = get_rel_from_name(relname);
		raw_page = get_rel_raw_page(rel, blkno);

		/* Allocating memory for a long-lived structure */
		inter_call_data = palloc(sizeof(rum_page_items_state));

		/* Getting a copy of the page from the raw page */
		page = get_page_from_raw(raw_page);

		/* If the page is new, the function should return NULL */
		if (PageIsNew(page))
		{
			MemoryContextSwitchTo(oldmctx);
			PG_RETURN_NULL();
		}

		/* Checking the size of the opaque area of the page */
		if (PageGetSpecialSize(page) != MAXALIGN(sizeof(RumPageOpaqueData)))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("input page is not a valid RUM page"),
					 errdetail("Expected special size %d, got %d.",
							   (int) MAXALIGN(sizeof(RumPageOpaqueData)),
							   (int) PageGetSpecialSize(page))));

		/* Getting a page description from an opaque area */
		opaq = RumPageGetOpaque(page);

		/* Checking the flags */
		if (opaq->flags != (RUM_DATA | RUM_LEAF))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("input page is not a RUM {data, leaf} page"),
					 errdetail("Flags %04X, expected %04X",
							   opaq->flags, (RUM_DATA | RUM_LEAF))));

		/* Initializing the RumState structure */
		inter_call_data->rum_state_ptr = palloc(sizeof(RumState));
		initRumState(inter_call_data->rum_state_ptr, rel);

		relation_close(rel, AccessShareLock);

		/* Build a tuple descriptor for our result type */ 
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		/* Needed to for subsequent recording tupledesc in fctx */
		BlessTupleDesc(tupdesc);

		/*
		 * Write to the long-lived structure the number of RumItem 
		 * structures on the page and a pointer to the data on the page.
		 */
		inter_call_data->page = page;
		inter_call_data->maxoff = opaq->maxoff;
		inter_call_data->item_ptr = RumDataPageGetData(page);
		inter_call_data->add_info_oid = find_add_info_oid(inter_call_data->rum_state_ptr);

		/* 
		 * It is necessary for the correct reading of the 
		 * tid (see the function rumDataPageLeafRead()) 
		 */
		memset(&(inter_call_data->cur_rum_item), 0, sizeof(RumItem));

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

	/* 
	 * Go through the page. It should be noted that fctx->call_cntr 
	 * on the first call is 0. The very first function call is intended 
	 * to return the high key of the scanned page (this is done because 
	 * the high key is not taken into account in inter_call_data->maxoff). 
	 */
	if(fctx->call_cntr <= inter_call_data->maxoff)
	{
		RumItem					*high_key_ptr; 		/* to read high key from a page */		
		RumItem 				*rum_item_ptr;		/* to read data from a page */
		Datum 					values[4];			/* return values */
		bool 					nulls[4];			/* true if the corresponding value is NULL */

		/* For the results */
		HeapTuple				resultTuple;
		Datum					result;

		memset(nulls, 0, sizeof(nulls));
		rum_item_ptr = &(inter_call_data->cur_rum_item);

		/* 
		 * The high key of the current page is the RumItem structure and 
		 * it is located immediately after the PageHeader. On the first 
		 * call, we return the information it stores.
		 */
		if (fctx->call_cntr == 0) 
		{
			high_key_ptr = RumDataPageGetRightBound(inter_call_data->page);
			values[0] = BoolGetDatum(true);
			values[1] = ItemPointerGetDatum(&(high_key_ptr->iptr));
			values[2] = BoolGetDatum(high_key_ptr->addInfoIsNull);

			/* Returning add info */
			if(!(high_key_ptr->addInfoIsNull) && inter_call_data->add_info_oid != InvalidOid 
				&& inter_call_data->add_info_oid != BYTEAOID)
			{
				values[3] = get_datum_text_by_oid(high_key_ptr->addInfo, 
								  inter_call_data->add_info_oid);
			}

			/* 
			 * In this case, we are dealing with the positions 
			 * of lexemes and they need to be decoded. 
			 */
			else if (!(high_key_ptr->addInfoIsNull) && inter_call_data->add_info_oid != InvalidOid 
					&& inter_call_data->add_info_oid == BYTEAOID) 
			{
				values[3] = CStringGetTextDatum("high key positions in posting tree is not supported");
			}

			else nulls[3] = true;

			/* Forming the returned tuple */
			resultTuple = heap_form_tuple(fctx->tuple_desc, values, nulls);
			result = HeapTupleGetDatum(resultTuple);

			/* Returning the result of the current call */
			SRF_RETURN_NEXT(fctx, result);
		}

		/* Reading information from the page in rum_item */
		inter_call_data->item_ptr = rumDataPageLeafRead(inter_call_data->item_ptr, 
									find_add_info_atrr_num(inter_call_data->rum_state_ptr),
									rum_item_ptr, false, inter_call_data->rum_state_ptr);

		/* Writing data from rum_item to values */
		values[0] = false;
		values[1] = ItemPointerGetDatum(&(rum_item_ptr->iptr)); 
		values[2] = BoolGetDatum(rum_item_ptr->addInfoIsNull);

		/* Returning add info */
		if(!(rum_item_ptr->addInfoIsNull) && inter_call_data->add_info_oid != InvalidOid
			&& inter_call_data->add_info_oid != BYTEAOID)
		{
			values[3] = get_datum_text_by_oid(rum_item_ptr->addInfo, 
									 inter_call_data->add_info_oid);
		}

		/* 
		 * In this case, we are dealing with the positions 
		 * of lexemes and they need to be decoded. 
		 */
		else if (!(rum_item_ptr->addInfoIsNull) && inter_call_data->add_info_oid != InvalidOid
				&& inter_call_data->add_info_oid == BYTEAOID) 
		{
			values[3] = get_positions_to_text_datum(rum_item_ptr->addInfo); 
		}

		else nulls[3] = true;

		/* Forming the returned tuple */
		resultTuple = heap_form_tuple(fctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(resultTuple);

		/* Returning the result of the current call */
		SRF_RETURN_NEXT(fctx, result);
	}

	/* Completing the function */
	SRF_RETURN_DONE(fctx);
}

/*
 * The function rum_internal_data_page_items() is designed
 * to view information that is located on the internal
 * pages of the index's rum posting tree. On these pages, 
 * information is stored in an array of PostingItem structures, 
 * which are located immediately after the PageHeader. Each 
 * PostingItem structure contains the child page number and 
 * RumItem in which iptr is the high key of the child page.
 *
 * The function returns the bool variable is_high_key (true if 
 * the current returned tuple is the high key of the current page);
 * the child page number; and the information contained in the 
 * corresponding structure of RumItem: tid, the bool variable 
 * add_info_is_null and add_info.
 *
 * It is a SRF function, i.e. it returns one tuple per call.
 */
Datum
rum_internal_data_page_items(PG_FUNCTION_ARGS)
{
	/* Reading input arguments */
	text	   			*relname = PG_GETARG_TEXT_PP(0);
	uint32				blkno = PG_GETARG_UINT32(1);

	/* 
	 * The context of the function calls and the pointer 
	 * to the long-lived inter_call_data structure 
	 */
	FuncCallContext 			*fctx;
	rum_page_items_state 		*inter_call_data;

	/* Only the superuser can use this */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use this function")));

	/* 
	 * In the case of the first function call, it is necessary 
	 * to get the page by its number and create a RumState 
	 * structure for scanning the page.
	 */
	if (SRF_IS_FIRSTCALL())
	{
		Relation 				rel;		/* needed to initialize the RumState structure */ 
		bytea 					*raw_page;	/* The raw page obtained from rel */

		TupleDesc 				tupdesc;	/* description of the result tuple */
		MemoryContext 			oldmctx;	/* the old function memory context */
		Page 					page;		/* the page to be scanned */
		RumPageOpaque 			opaq;		/* data from the opaque area of the page */

		/* 
		 * Initializing the FuncCallContext structure and switching the memory 
		 * context to the one needed for structures that must be saved during 
		 * multiple calls.
		 */
		fctx = SRF_FIRSTCALL_INIT();
		oldmctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		/* Getting rel by name and raw page by number */
		rel = get_rel_from_name(relname);
		raw_page = get_rel_raw_page(rel, blkno);

		/* Allocating memory for a long-lived structure */
		inter_call_data = palloc(sizeof(rum_page_items_state));

		/* Getting a copy of the page from the raw page */
		page = get_page_from_raw(raw_page);

		/* If the page is new, the function should return NULL */
		if (PageIsNew(page))
		{
			MemoryContextSwitchTo(oldmctx);
			PG_RETURN_NULL();
		}

		/* Checking the size of the opaque area of the page */
		if (PageGetSpecialSize(page) != MAXALIGN(sizeof(RumPageOpaqueData)))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("input page is not a valid RUM page"),
					 errdetail("Expected special size %d, got %d.",
							   (int) MAXALIGN(sizeof(RumPageOpaqueData)),
							   (int) PageGetSpecialSize(page))));

		/* Getting a page description from an opaque area */
		opaq = RumPageGetOpaque(page);

		/* Checking the flags */
		if (opaq->flags != (RUM_DATA & ~RUM_LEAF))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("input page is not a RUM {data} page"),
					 errdetail("Flags %04X, expected %04X",
							   opaq->flags, (RUM_DATA & ~RUM_LEAF))));

		/* Initializing the RumState structure */
		inter_call_data->rum_state_ptr = palloc(sizeof(RumState));
		initRumState(inter_call_data->rum_state_ptr, rel);

		relation_close(rel, AccessShareLock);

		/* Build a tuple descriptor for our result type */ 
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		/* Needed to for subsequent recording tupledesc in fctx */
		BlessTupleDesc(tupdesc);

		/*
		 * Write to the long-lived structure the number of RumItem 
		 * structures on the page and a pointer to the data on the page.
		 */
		inter_call_data->page = page;
		inter_call_data->maxoff = opaq->maxoff;
		inter_call_data->item_ptr = RumDataPageGetData(page);
		inter_call_data->add_info_oid = find_add_info_oid(inter_call_data->rum_state_ptr);

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

	/* 
	 * Go through the page. It should be noted that fctx->call_cntr 
	 * on the first call is 0. The very first function call is intended 
	 * to return the high key of the scanned page (this is done because 
	 * the high key is not taken into account in inter_call_data->maxoff). 
	 */
	if(fctx->call_cntr <= inter_call_data->maxoff)
	{
		RumItem					*high_key_ptr;		/* to read high key from a page */
		PostingItem 			*posting_item_ptr;	/* to read data from a page */
		Datum 					values[5];			/* returned values */
		bool 					nulls[5];			/* true if the corresponding returned value is NULL */

		/* For the results */
		HeapTuple 				resultTuple;
		Datum 					result;

		memset(nulls, 0, sizeof(nulls));

		/* 
		 * The high key of the current page is the RumItem structure and 
		 * it is located immediately after the PageHeader. On the first 
		 * call, we return the information it stores.
		 */
		if (fctx->call_cntr == 0) 
		{
			high_key_ptr = RumDataPageGetRightBound(inter_call_data->page);
			values[0] = BoolGetDatum(true);
			nulls[1] = true;
			values[2] = ItemPointerGetDatum(&(high_key_ptr->iptr));
			values[3] = BoolGetDatum(high_key_ptr->addInfoIsNull);

			/* Returning add info */
			if(!(high_key_ptr->addInfoIsNull) && inter_call_data->add_info_oid != InvalidOid
				&& inter_call_data->add_info_oid != BYTEAOID)
			{
				values[4] = get_datum_text_by_oid(high_key_ptr->addInfo, 
								  inter_call_data->add_info_oid);
			}

			/* 
			 * In this case, we are dealing with the positions 
			 * of lexemes and they need to be decoded. 
			 */
			else if (!(high_key_ptr->addInfoIsNull) && inter_call_data->add_info_oid != InvalidOid 
					&& inter_call_data->add_info_oid == BYTEAOID) 
			{
				values[4] = CStringGetTextDatum("high key positions in posting tree is not supported");
			}

			else nulls[4] = true;

			/* Forming the returned tuple */
			resultTuple = heap_form_tuple(fctx->tuple_desc, values, nulls);
			result = HeapTupleGetDatum(resultTuple);

			/* Returning the result of the current call */
			SRF_RETURN_NEXT(fctx, result);
		}

		/* Reading information from the page */
		posting_item_ptr = (PostingItem *) inter_call_data->item_ptr; 
		inter_call_data->item_ptr += sizeof(PostingItem);

		/* Writing data from posting_item_ptr to values */
		values[0] = BoolGetDatum(false);
		values[1] = UInt32GetDatum(PostingItemGetBlockNumber(posting_item_ptr)); 
		values[2] = ItemPointerGetDatum(&(posting_item_ptr->item.iptr)); 
		values[3] = BoolGetDatum(posting_item_ptr->item.addInfoIsNull);

		/* Returning add info */
		if(!posting_item_ptr->item.addInfoIsNull && inter_call_data->add_info_oid != InvalidOid
			&& inter_call_data->add_info_oid != BYTEAOID)
		{
			values[4] = get_datum_text_by_oid(posting_item_ptr->item.addInfo, 
							  inter_call_data->add_info_oid);
		}

		/* 
		 * In this case, we are dealing with the positions 
		 * of lexemes and they need to be decoded. 
		 */
		else if (!posting_item_ptr->item.addInfoIsNull && inter_call_data->add_info_oid != InvalidOid
				&& inter_call_data->add_info_oid == BYTEAOID) 
		{
			values[4] = CStringGetTextDatum("high key positions in posting tree is not supported");
		}

		else nulls[4] = true;

		/* Forming the returned tuple */
		resultTuple = heap_form_tuple(fctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(resultTuple);

		/* Returning the result of the current call */
		SRF_RETURN_NEXT(fctx, result);
	}

	/* Completing the function */
	SRF_RETURN_DONE(fctx);
}

/*
 * The function rum_leaf_entry_page_items() is designed 
 * to view information that is located on the leaf 
 * pages of the index's rum entry tree. On these pages, 
 * information is stored in compressed posting lists (which 
 * consist of RumItem structures), which conteins in the IndexTuples.
 *
 * The function returns the key; the key attribute number; 
 * the key category (see rum.h); and the information contained 
 * in the corresponding structure of RumItem: tid, the bool variable 
 * add_info_is_null and add_info. Also, in the case of the posting list, 
 * the bool variable is_postring_true = false and the page number of 
 * the root of posting tree is NULL.
 *
 * If posting tree is placed instead of posting list, the function 
 * returns the bool variable is_postring_true = true and the page 
 * number on which the root of the posting tree is located. The rest 
 * of the returned values contain NULL (except for the key itself).
 *
 * It is a SRF function, i.e. it returns one tuple per call.
 */
Datum
rum_leaf_entry_page_items(PG_FUNCTION_ARGS)
{
	/* Reading input arguments */
	text	   			*relname = PG_GETARG_TEXT_PP(0);
	uint32				blkno = PG_GETARG_UINT32(1);

	/* 
	 * The context of the function calls and the pointer 
	 * to the long-lived inter_call_data structure. 
	 */
	FuncCallContext 			*fctx;
	rum_page_items_state 		*inter_call_data;

	/* Only the superuser can use this */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use this function")));

	/* 
	 * In the case of the first function call, it is necessary 
	 * to get the page by its number and create a RumState 
	 * structure for scanning the page.
	 */
	if (SRF_IS_FIRSTCALL())
	{
		Relation 				rel;		/* needed to initialize the RumState structure */ 
		bytea 					*raw_page;	/* The raw page obtained from rel */

		TupleDesc 				tupdesc;	/* description of the result tuple */
		MemoryContext 			oldmctx;	/* the old function memory context */
		Page 					page;		/* the page to be scanned */
		RumPageOpaque 			opaq;		/* data from the opaque area of the page */

		/* 
		 * Initializing the FuncCallContext structure and switching the memory 
		 * context to the one needed for structures that must be saved during 
		 * multiple calls
		 */
		fctx = SRF_FIRSTCALL_INIT();
		oldmctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		/* Getting rel by name and raw page by number */
		rel = get_rel_from_name(relname);
		raw_page = get_rel_raw_page(rel, blkno);

		/* Allocating memory for a long-lived structure */
		inter_call_data = palloc(sizeof(rum_page_items_state));

		/* Getting a copy of the page from the raw page */
		page = get_page_from_raw(raw_page);

		/* If the page is new, the function should return NULL */
		if (PageIsNew(page))
		{
			MemoryContextSwitchTo(oldmctx);
			PG_RETURN_NULL();
		}

		/* Checking the size of the opaque area of the page */
		if (PageGetSpecialSize(page) != MAXALIGN(sizeof(RumPageOpaqueData)))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("input page is not a valid RUM page"),
					 errdetail("Expected special size %d, got %d.",
							   (int) MAXALIGN(sizeof(RumPageOpaqueData)),
							   (int) PageGetSpecialSize(page))));

		/* Getting a page description from an opaque area */
		opaq = RumPageGetOpaque(page);

		/* Checking the flags */
		if (opaq->flags != RUM_LEAF)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("input page is not a RUM {leaf} page"),
					 errdetail("Flags %04X, expected %04X",
							   opaq->flags, RUM_LEAF)));

		/* Initializing the RumState structure */
		inter_call_data->rum_state_ptr = palloc(sizeof(RumState));
		initRumState(inter_call_data->rum_state_ptr, rel);

		relation_close(rel, AccessShareLock);

		/* Build a tuple descriptor for our result type */ 
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		/* Needed to for subsequent recording tupledesc in fctx */
		BlessTupleDesc(tupdesc);

		/* 
		 * We save all the necessary information for 
		 * scanning the page in a long-lived structure. 
		 */
		inter_call_data->page = page;
		inter_call_data->maxoff = PageGetMaxOffsetNumber(page); 
		inter_call_data->need_new_tuple = true;
		inter_call_data->cur_tuple_num = FirstOffsetNumber;
		inter_call_data->add_info_oid = find_add_info_oid(inter_call_data->rum_state_ptr);

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

	/* Go through the page */
	if(inter_call_data->cur_tuple_num <= inter_call_data->maxoff)
	{
		RumItem 				*rum_item_ptr;	/* to read data from a page */
		Oid 					key_oid;		/* to convert key to text */

		Datum 					values[8];		/* returned values */
		bool 					nulls[8];		/* true if the corresponding returned value is NULL */

		/* For the results */
		HeapTuple 				resultTuple;
		Datum 					result;

		memset(nulls, 0, sizeof(nulls));
		rum_item_ptr = &(inter_call_data->cur_rum_item);

		/* 
		 * The function reads information from compressed Posting lists, 
		 * each of which is located in the corresponding IndexTuple. 
		 * Therefore, first, if the previous IndexTuple has ended, 
		 * the new one is read. After that, the current IndexTuple is 
		 * scanned until it runs out. The IndexTuple themselves are read 
		 * until they end on the page. 
		 */
		if(inter_call_data->need_new_tuple)
		{
			/* Read the new IndexTuple */
			get_new_index_tuple(inter_call_data);

			/* 
			 * Every time you read a new IndexTuple, you need to reset the 
			 * tid for the rumDataPageLeafRead() function to work correctly.
			 */
			memset(rum_item_ptr, 0, sizeof(RumItem));

			/* Getting the posting list */
			inter_call_data->item_ptr = RumGetPosting(inter_call_data->cur_itup);
			inter_call_data->cur_tuple_item = 1;
			inter_call_data->need_new_tuple = false;
			inter_call_data->cur_tuple_num++;

			/* Getting key and key attribute number */
			inter_call_data->cur_tuple_key_attnum = rumtuple_get_attrnum(inter_call_data->rum_state_ptr, 
																		  inter_call_data->cur_itup);
			inter_call_data->cur_tuple_key = rumtuple_get_key(inter_call_data->rum_state_ptr, 
														  inter_call_data->cur_itup,
														  &(inter_call_data->cur_tuple_key_category));

			/* The case when there is a posting tree instead of a compressed posting list */
			if(RumIsPostingTree(inter_call_data->cur_itup))
			{
				/* Returning the key value */
				key_oid = get_cur_attr_oid(inter_call_data);
				if(inter_call_data->cur_tuple_key_category == RUM_CAT_NORM_KEY)
					values[0] = get_datum_text_by_oid(inter_call_data->cur_tuple_key, key_oid);
				else nulls[0] = true;

				/* Returning the key attribute number */
				values[1] = UInt16GetDatum(inter_call_data->cur_tuple_key_attnum);

				/* Returning the key category */
				values[2] = category_get_datum_text(inter_call_data->cur_tuple_key_category);

				/* Everything stored in the RumItem structure has a NULL value */
				nulls[3] = true;
				nulls[4] = true;
				nulls[5] = true;

				/* Returning the root of the posting tree */
				values[6] = true;
				values[7] = UInt32GetDatum(RumGetPostingTree(inter_call_data->cur_itup));

				/* Forming the returned tuple */
				resultTuple = heap_form_tuple(fctx->tuple_desc, values, nulls);
				result = HeapTupleGetDatum(resultTuple);

				/* The next call will require a new IndexTuple */
				inter_call_data->need_new_tuple = true;

				/* Returning the result of the current call */
				SRF_RETURN_NEXT(fctx, result);
			}
		}

		/* Reading the RumItem structures from the IndexTuple */
		inter_call_data->item_ptr = rumDataPageLeafRead(inter_call_data->item_ptr, 
									inter_call_data->cur_tuple_key_attnum,
									rum_item_ptr, false, inter_call_data->rum_state_ptr);

		/* Returning the key value */
		key_oid = get_cur_attr_oid(inter_call_data);
		if(inter_call_data->cur_tuple_key_category == RUM_CAT_NORM_KEY)
			values[0] = get_datum_text_by_oid(inter_call_data->cur_tuple_key, key_oid);
		else nulls[0] = true;

		/* Returning the key attribute number */
		values[1] = UInt16GetDatum(inter_call_data->cur_tuple_key_attnum);

		/* Returning the key category */
		values[2] = category_get_datum_text(inter_call_data->cur_tuple_key_category);

		/* Writing data from rum_item to values */
		values[3] = ItemPointerGetDatum(&(rum_item_ptr->iptr)); 
		values[4] = BoolGetDatum(rum_item_ptr->addInfoIsNull);

		/* Returning add info */
		if (!(rum_item_ptr->addInfoIsNull) && inter_call_data->add_info_oid != InvalidOid && 
			inter_call_data->add_info_oid != BYTEAOID)
		{
			values[5] = get_datum_text_by_oid(rum_item_ptr->addInfo, inter_call_data->add_info_oid);
		}

		/* 
		 * In this case, we are dealing with the positions 
		 * of lexemes and they need to be decoded. 
		 */
		else if (!(rum_item_ptr->addInfoIsNull) && inter_call_data->add_info_oid != InvalidOid 
				&& inter_call_data->add_info_oid == BYTEAOID) 
		{
			values[5] = get_positions_to_text_datum(rum_item_ptr->addInfo); 
		}
		
		else nulls[5] = true;

		/* The current IndexTuple does not contain a posting tree */
		values[6] = BoolGetDatum(false);
		nulls[7] = true;

		/* 
		 * If the current IndexTuple has ended, i.e. we have scanned all 
		 * its RumItems, then we need to enable the need_new_tuple flag 
		 * so that the next time the function is called, we can read 
		 * a new IndexTuple from the page. 
		 */
		inter_call_data->cur_tuple_item++;
		if(inter_call_data->cur_tuple_item > RumGetNPosting(inter_call_data->cur_itup))
			inter_call_data->need_new_tuple = true;

		/* Forming the returned tuple */
		resultTuple = heap_form_tuple(fctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(resultTuple);

		/* Returning the result of the current call */
		SRF_RETURN_NEXT(fctx, result);
	}

	/* Completing the function */
	SRF_RETURN_DONE(fctx);
}

/* 
 * The function rum_internal_entry_page_items() is designed 
 * to view information that is located on the internal 
 * pages of the index's rum entry tree. On these pages, 
 * information is stored in the IndexTuples.
 *
 * The function returns the key, the key attribute number, 
 * the key category (see rum.h), and the child page number.
 *
 * It is a SRF function, i.e. it returns one tuple per call.
 */
Datum
rum_internal_entry_page_items(PG_FUNCTION_ARGS)
{
	/* Reading input arguments */
	text	   			*relname = PG_GETARG_TEXT_PP(0);
	uint32				blkno = PG_GETARG_UINT32(1);

	/* 
	 * The context of the function calls and the pointer 
	 * to the long-lived inter_call_data structure.
	 */
	FuncCallContext 			*fctx;
	rum_page_items_state 		*inter_call_data;

	/* Only the superuser can use this */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use this function")));

	/* 
	 * In the case of the first function call, it is necessary 
	 * to get the page by its number and create a RumState 
	 * structure for scanning the page.
	 */
	if (SRF_IS_FIRSTCALL())
	{
		Relation 				rel;		/* needed to initialize the RumState structure */ 
		bytea 					*raw_page;	/* the raw page obtained from rel */

		TupleDesc 				tupdesc;	/* description of the result tuple */
		MemoryContext 			oldmctx;	/* the old function memory context */
		Page 					page;		/* the page to be scanned */
		RumPageOpaque 			opaq;		/* data from the opaque area of the page */

		/* 
		 * Initializing the FuncCallContext structure and switching the memory 
		 * context to the one needed for structures that must be saved during 
		 * multiple calls.
		 */
		fctx = SRF_FIRSTCALL_INIT();
		oldmctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		/* Getting rel by name and raw page by number */
		rel = get_rel_from_name(relname);
		raw_page = get_rel_raw_page(rel, blkno);

		/* Allocating memory for a long-lived structure */
		inter_call_data = palloc(sizeof(rum_page_items_state));

		/* Getting a copy of the page from the raw page */
		page = get_page_from_raw(raw_page);

		/* If the page is new, the function should return NULL */
		if (PageIsNew(page))
		{
			MemoryContextSwitchTo(oldmctx);
			PG_RETURN_NULL();
		}

		/* Checking the size of the opaque area of the page */
		if (PageGetSpecialSize(page) != MAXALIGN(sizeof(RumPageOpaqueData)))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("input page is not a valid RUM page"),
					 errdetail("Expected special size %d, got %d.",
							   (int) MAXALIGN(sizeof(RumPageOpaqueData)),
							   (int) PageGetSpecialSize(page))));

		/* Getting a page description from an opaque area */
		opaq = RumPageGetOpaque(page);

		/* Checking the flags */
		if (opaq->flags != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("input page is not a RUM {} page"),
					 errdetail("Flags %04X, expected %04X",
							   opaq->flags, 0)));

		/* Initializing the RumState structure */
		inter_call_data->rum_state_ptr = palloc(sizeof(RumState));
		initRumState(inter_call_data->rum_state_ptr, rel);

		relation_close(rel, AccessShareLock);

		/* Build a tuple descriptor for our result type */ 
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		/* Needed to for subsequent recording tupledesc in fctx */
		BlessTupleDesc(tupdesc);

		/* 
		 * We save all the necessary information for 
		 * scanning the page in a long-lived structure. 
		 */
		inter_call_data->page = page;
		inter_call_data->maxoff = PageGetMaxOffsetNumber(page); 
		inter_call_data->cur_tuple_num = FirstOffsetNumber;

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

	/* Go through the page */
	if(inter_call_data->cur_tuple_num <= inter_call_data->maxoff)
	{
		/* returned values */
		Datum 					values[4];

		/* true if the corresponding returned value is NULL */
		bool 					nulls[4];

		/* For the results */
		HeapTuple 				resultTuple;
		Datum 					result;

		/* To convert a key to text */
		Oid key_oid;

		memset(nulls, 0, sizeof(nulls));

		/* Read the new IndexTuple and get the values that are stored in it */
		get_new_index_tuple(inter_call_data);

		/* Scanning the IndexTuple that we received earlier */
		inter_call_data->cur_tuple_key_attnum = rumtuple_get_attrnum(inter_call_data->rum_state_ptr, 
																	 inter_call_data->cur_itup);
		inter_call_data->cur_tuple_key = rumtuple_get_key(inter_call_data->rum_state_ptr, 
														  inter_call_data->cur_itup, 
														  &(inter_call_data->cur_tuple_key_category));
		inter_call_data->cur_tuple_down_link = RumGetDownlink(inter_call_data->cur_itup);

		/* 
		 * On the rightmost page, in the last IndexTuple, there is a 
		 * high key, which is assumed to be equal to +inf.
		 */
		if (RumPageRightMost(inter_call_data->page) && 
			inter_call_data->cur_tuple_num == inter_call_data->maxoff)
		{
			values[0] = CStringGetTextDatum("+inf");
			nulls[1] = true;
			nulls[2] = true;
			values[3] = UInt32GetDatum(inter_call_data->cur_tuple_down_link);

			/* Forming the returned tuple */
			resultTuple = heap_form_tuple(fctx->tuple_desc, values, nulls);
			result = HeapTupleGetDatum(resultTuple);

			/* Increase the counter before the next call */
			inter_call_data->cur_tuple_num++;

			/* Returning the result of the current call */
			SRF_RETURN_NEXT(fctx, result);
		}

		/* Increase the counter before the next call */
		inter_call_data->cur_tuple_num++;

		/* Getting the key attribute number */
		key_oid = get_cur_attr_oid(inter_call_data);

		/* Filling in the returned values */
		if (inter_call_data->cur_tuple_key_category == RUM_CAT_NORM_KEY) 
			values[0] = get_datum_text_by_oid(inter_call_data->cur_tuple_key, key_oid); 
		else nulls[0] = true;

		values[1] = UInt16GetDatum(inter_call_data->cur_tuple_key_attnum); 
		values[2] = category_get_datum_text(inter_call_data->cur_tuple_key_category);
		values[3] = UInt32GetDatum(inter_call_data->cur_tuple_down_link);

		/* Forming the returned tuple */
		resultTuple = heap_form_tuple(fctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(resultTuple);

		/* Returning the result of the current call */
		SRF_RETURN_NEXT(fctx, result);
	}

	/* Completing the function */
	SRF_RETURN_DONE(fctx);
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
 * This function places a pointer to a new IndexTuple 
 * in the rum_page_items_state structure.
 */
static void
get_new_index_tuple(rum_page_items_state *inter_call_data)
{
	inter_call_data->cur_itup = (IndexTuple) PageGetItem(inter_call_data->page, 
					PageGetItemId(inter_call_data->page, inter_call_data->cur_tuple_num));
}

/* 
 * This function gets the attribute number of the 
 * current key from the RumState structure.
 */
static Oid
get_cur_attr_oid(rum_page_items_state *inter_call_data)
{
	Oid result;
	TupleDesc orig_tuple_desc;
	OffsetNumber attnum;
	FormData_pg_attribute *attrs;

	attnum = inter_call_data->cur_tuple_key_attnum;
	orig_tuple_desc = inter_call_data->rum_state_ptr->origTupdesc;
	attrs = orig_tuple_desc->attrs; 
	result = (attrs[attnum - 1]).atttypid;

	return result;
}

/*
 * A copy of the get_page_from_raw() 
 * function from pageinspect.
 *
 * Get a palloc'd, maxalign'ed page image 
 * from the result of get_rel_raw_page() 
 */
static Page
get_page_from_raw(bytea *raw_page)
{
	Page		page;
	int			raw_page_size;

	raw_page_size = VARSIZE_ANY_EXHDR(raw_page);

	if (raw_page_size != BLCKSZ)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid page size"),
				 errdetail("Expected %d bytes, got %d.",
						   BLCKSZ, raw_page_size)));

	page = palloc(raw_page_size);

	memcpy(page, VARDATA_ANY(raw_page), raw_page_size);

	return page;
}

/*
 * An auxiliary function that is used to convert information 
 * (being a Datum) into text. This is a universal way to 
 * return any type.
 *
 * The following types of data are checked:
 * int2, int4, int8, float4, float8, money, oid, timestamp, 
 * timestamptz, time, timetz, date, interval, macaddr, inet, 
 * cidr, text, varchar, char, bytea, bit, varbit, numeric.
 */
static Datum
get_datum_text_by_oid(Datum info, Oid info_oid)
{
	char *str_info = NULL;

	/*
	 * Form a string depending on the type of info.
	 *
	 * TODO: The macros used below are taken from the 
	 * pg_type_d file.h, and it says not to use them 
	 * in the new code.
	 */
	switch (info_oid)
	{
		case INT2OID:
			str_info = OidOutputFunctionCall(F_INT2OUT, info);
			break;

		case INT4OID:
			str_info = OidOutputFunctionCall(F_INT4OUT, info);
			break;

		case INT8OID:
			str_info = OidOutputFunctionCall(F_INT8OUT, info);
			break;

		case FLOAT4OID:
			str_info = OidOutputFunctionCall(F_FLOAT4OUT, info);
			break;

		case FLOAT8OID:
			str_info = OidOutputFunctionCall(F_FLOAT8OUT, info);
			break;

		/* 
		 * TODO: The oid of the function for displaying this 
		 * type as text could not be found. 
		 */
		case MONEYOID:
			/* str_addInfo = OidOutputFunctionCall(, addInfo); */
			/* break; */
			return CStringGetTextDatum("MONEYOID is not supported");

		case OIDOID:
			str_info = OidOutputFunctionCall(F_OIDOUT, info);
			break;

		case TIMESTAMPOID:
			str_info = OidOutputFunctionCall(F_TIMESTAMP_OUT, info);
			break;

		case TIMESTAMPTZOID:
			str_info = OidOutputFunctionCall(F_TIMESTAMPTZ_OUT, info);
			break;

		case TIMEOID:
			str_info = OidOutputFunctionCall(F_TIME_OUT, info);
			break;

		case TIMETZOID:
			str_info = OidOutputFunctionCall(F_TIMETZ_OUT, info);
			break;

		case DATEOID:
			str_info = OidOutputFunctionCall(F_DATE_OUT, info);
			break;

		case INTERVALOID:
			str_info = OidOutputFunctionCall(F_INTERVAL_OUT, info);
			break;

		case MACADDROID:
			str_info = OidOutputFunctionCall(F_MACADDR_OUT, info);
			break;

		case INETOID:
			str_info = OidOutputFunctionCall(F_INET_OUT, info);
			break;

		case CIDROID:
			str_info = OidOutputFunctionCall(F_CIDR_OUT, info);
			break;

		case TEXTOID:
			return info;

		case VARCHAROID:
			str_info = OidOutputFunctionCall(F_VARCHAROUT, info);
			break;
		
		case CHAROID:
			str_info = OidOutputFunctionCall(F_CHAROUT, info);
			break;

		case BYTEAOID:
			str_info = OidOutputFunctionCall(F_BYTEAOUT, info);
			break;

		case BITOID:
			str_info = OidOutputFunctionCall(F_BIT_OUT, info);
			break;

		case VARBITOID:
			str_info = OidOutputFunctionCall(F_VARBIT_OUT, info);
			break;

		case NUMERICOID:
			str_info = OidOutputFunctionCall(F_NUMERIC_OUT, info);
			break;

		default:
			return CStringGetTextDatum("unsupported type");
	}

	return CStringGetTextDatum(str_info);
}

/*
 * This function and get_rel_raw_page() are derived from the separation 
 * of the get_raw_page_internal() function, which was copied from the pageinspect code.
 * It is needed in order to call the initRumState() function if necessary.
 */
static Relation
get_rel_from_name(text *relname)
{
	RangeVar   			*relrv;
	Relation			rel;

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname)); 
	rel = relation_openrv(relrv, AccessShareLock);

	if (!RELKIND_HAS_STORAGE(rel->rd_rel->relkind))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot get raw page from relation \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail_relkind_not_supported(rel->rd_rel->relkind)));

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
 * This function and get_rel_from_name() are derived from the separation 
 * of the get_raw_page_internal() function, which was copied from the pageinspect code.
 * It is needed in order to call the initRumState() function if necessary.
 */
static bytea *
get_rel_raw_page(Relation rel, BlockNumber blkno)
{
	bytea	   			*raw_page;
	char	   			*raw_page_data;
	Buffer				buf;

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

	return raw_page;
}

/*
 * This function looks through the addAttrs array and extracts 
 * the Oid of additional information for an attribute for 
 * which it is not NULL.
 *
 * The logic of the function assumes that there cannot 
 * be several types of additional information in the index, 
 * otherwise it will not work. 
 */
static Oid
find_add_info_oid(RumState *rum_state_ptr)
{
	Oid add_info_oid = InvalidOid;

	/* Number of index attributes */
	int num_attrs = rum_state_ptr->origTupdesc->natts;

	/* 
	 * For each of the attributes, we read the 
	 * oid of additional information.
	 */
	for (int i = 0; i < num_attrs; i++) 
	{
		if ((rum_state_ptr->addAttrs)[i] != NULL)
		{
			Assert(add_info_oid == InvalidOid);
			add_info_oid = ((rum_state_ptr->addAttrs)[i])->atttypid; 
		}
	}

	return add_info_oid;
}

/* 
 * This is an auxiliary function to get the attribute number 
 * for additional information. It is used in the rum_leaf_data_page_items() 
 * function to call the rumDataPageLeafRead() function.
 *
 * The logic of the function assumes that there cannot 
 * be several types of additional information in the index, 
 * otherwise it will not work. 
 */
static OffsetNumber
find_add_info_atrr_num(RumState *rum_state_ptr)
{
	OffsetNumber add_info_attr_num = InvalidOffsetNumber;

	/* Number of index attributes */
	int num_attrs = rum_state_ptr->origTupdesc->natts;

	/* Go through the addAttrs array */
	for (int i = 0; i < num_attrs; i++)
	{
		if ((rum_state_ptr->addAttrs)[i] != NULL)
		{
			Assert(add_info_attr_num == InvalidOffsetNumber);
			add_info_attr_num = i;
		}
	}

	/* Need to add 1 because the attributes are numbered from 1 */
	return add_info_attr_num + 1;
}

#define POS_STR_BUF_LENGHT 1024
#define POS_MAX_VAL_LENGHT 6

/*
 * A function for extracting the positions of lexemes from additional 
 * information. Returns a string in which the positions of the lexemes 
 * are recorded. The memory that the string occupies must be cleared later.
 */
static Datum
get_positions_to_text_datum(Datum add_info)
{
	bytea	   		*positions;
	char	   		*ptrt;
	WordEntryPos 	position = 0;
	int32			npos;

	Datum 			res;
	char 			*positions_str;
	char 			*positions_str_cur_ptr;
	int 			cur_max_str_lenght;

	positions = DatumGetByteaP(add_info);
	ptrt = (char *) VARDATA_ANY(positions);
	npos = count_pos(VARDATA_ANY(positions),
					 VARSIZE_ANY_EXHDR(positions));

	/* Initialize the string */
	positions_str = (char*) palloc(POS_STR_BUF_LENGHT * sizeof(char));
	positions_str[0] = '\0';
	cur_max_str_lenght = POS_STR_BUF_LENGHT;
	positions_str_cur_ptr = positions_str;

	/* Extract the positions of the lexemes and put them in the string */
	for (int i = 0; i < npos; i++)
	{
		/* At each iteration decode the position */
		ptrt = decompress_pos(ptrt, &position);

		/* Write this position and weight in the string */
		if(pos_get_weight(position) == 'D')
			sprintf(positions_str_cur_ptr, "%d,", WEP_GETPOS(position));
		else
			sprintf(positions_str_cur_ptr, "%d%c,", WEP_GETPOS(position), pos_get_weight(position));

		/* Moving the pointer forward */
		positions_str_cur_ptr += strlen(positions_str_cur_ptr);

		/* 
		 * Check that there is not too little left to the 
		 * end of the line and, if necessary, overspend 
		 * the memory. 
		 */
		if (cur_max_str_lenght - (positions_str_cur_ptr - positions_str) <= POS_MAX_VAL_LENGHT)
		{
			cur_max_str_lenght +=  POS_STR_BUF_LENGHT;
			positions_str = (char*) repalloc(positions_str, cur_max_str_lenght * sizeof(char));
			positions_str_cur_ptr = positions_str + strlen(positions_str);
		}
	}

	/* Delete the last comma if there has been at least one iteration of the loop */
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
