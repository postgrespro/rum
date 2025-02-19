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
 * 1) Strangely, the index version is returned in the 
 * 	  rum_metapage_info() function.
 *
 * 2) Using obsolete macros in the addInfoGetText() function.
 *
 * 3) I/O functions were not available for all types in
 *	  in the addInfoGetText() function.
 *
 * 4) The unclear logic of choosing the attribute number 
 *    that the addInfo corresponds to.
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "catalog/namespace.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "access/relation.h"
#include "utils/varlena.h"
#include "rum.h"

PG_FUNCTION_INFO_V1(rum_metapage_info);
PG_FUNCTION_INFO_V1(rum_page_opaque_info);
PG_FUNCTION_INFO_V1(rum_leaf_data_page_items);

static Page get_page_from_raw(bytea *raw_page);
static Datum addInfoGetText(Datum addInfo, Oid atttypid);
static Relation get_rel_from_name(text *relname);
static bytea *get_rel_raw_page(Relation rel, BlockNumber blkno);

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
	text	   	*relname = PG_GETARG_TEXT_PP(0);
	uint32		blkno = PG_GETARG_UINT32(1);

	Relation 	rel; 				/* needed to initialize the RumState 
									   structure */ 

	bytea	   	*raw_page; 			/* the raw page obtained from rel */
	RumPageOpaque opaq; 			/* data from the opaque area of the page */
	RumMetaPageData *metadata;		/* data stored on the meta page */
	Page		page; 				/* the page to be scanned */

	TupleDesc	tupdesc; 			/* description of the result tuple */
	HeapTuple	resultTuple;		/* for the results */
	Datum		values[10];			/* return values */
	bool		nulls[10];			/* true if the corresponding value is NULL */

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
	values[9] = Int64GetDatum(metadata->rumVersion);

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
	text	   	*relname = PG_GETARG_TEXT_PP(0);
	uint32		blkno = PG_GETARG_UINT32(1);

	Relation 	rel; 			/* needed to initialize the RumState 
								   structure */ 

	bytea	   	*raw_page; 		/* the raw page obtained from rel */
	RumPageOpaque opaq; 		/* data from the opaque area of the page */
	Page		page; 			/* the page to be scanned */

	HeapTuple	resultTuple; 	/* for the results */
	TupleDesc	tupdesc; 		/* description of the result tuple */

	Datum		values[5]; 		/* return values */
	bool		nulls[5]; 		/* true if the corresponding value is NULL */
	Datum		flags[16];		/* array with flags in text format */
	int			nflags = 0;		/* index in the array of flags */
	uint16		flagbits;		/* flags in the opaque area of the page */

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
 * A structure that stores information between calls to the 
 * rum_leaf_data_page_items() function. This information is 
 * necessary to scan the page.
 */
typedef struct rum_leafpage_items_state
{
	/* Number of RumItem structures per {leaf, data} page */
	int 		maxoff; 	

	/* Pointer to the current RumItem */
	Pointer 	item_ptr; 	

	/* A pointer to the RumState structure, which is needed to scan the page */
	RumState	*rum_state_ptr;

	/* A pointer to the description of the attribute, which is addInfo */
	Form_pg_attribute addInfo_att_ptr;
} rum_leafpage_items_state;

/*
 * The function rum_leaf_data_page_items() is designed to read 
 * information from the {leaf, data} pages of the rum index. 
 * The pages scanned by this function are the pages of the 
 * Posting Tree. The information on the page is stored in RumItem 
 * structures. The function returns tuple_id, add_info_is_null, 
 * addinfo. To scan, need the index name and the page number. 
 * It is an SRF function, i.e. it returns one value per call.
 */
Datum
rum_leaf_data_page_items(PG_FUNCTION_ARGS)
{
	/* Reading input arguments */
	text	   *relname = PG_GETARG_TEXT_PP(0);
	uint32		blkno = PG_GETARG_UINT32(1);

	/* 
	 * The context of the function calls and the pointer 
	 * to the long-lived inter_call_data structure 
	 */
	FuncCallContext 			*fctx;
	rum_leafpage_items_state 	*inter_call_data;

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
		
		Relation			rel; 		/* needed to initialize the RumState 
										   structure */ 
		bytea	   			*raw_page;	/* The raw page obtained from rel */

		TupleDesc			tupdesc; 	/* description of the result tuple */
		MemoryContext 		oldmctx;	/* the old function memory context */
		Page				page; 		/* the page to be scanned */
		RumPageOpaque 		opaq; 		/* data from the opaque area of the page */

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
		inter_call_data = palloc(sizeof(rum_leafpage_items_state));

		/* Initializing the RumState structure */
		inter_call_data->rum_state_ptr = palloc(sizeof(RumState));
		initRumState(inter_call_data->rum_state_ptr, rel);

		/* 
		 * It is convenient to save a pointer to an attribute 
		 * of addInfo in a long-lived structure for shorter 
		 * access in the future.
		 */
		inter_call_data->addInfo_att_ptr = inter_call_data->rum_state_ptr->
			addAttrs[inter_call_data->rum_state_ptr->attrnAddToColumn - 1];

		relation_close(rel, AccessShareLock);

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

		/* Build a tuple descriptor for our result type */ 
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		/* Needed to for subsequent recording tupledesc in fctx */
		BlessTupleDesc(tupdesc);

		/*
		 * Write to the long-lived structure the number of RumItem 
		 * structures on the page and a pointer to the data on the page.
		 */
		inter_call_data->maxoff = opaq->maxoff;
		inter_call_data->item_ptr = RumDataPageGetData(page);

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
	if((fctx->call_cntr + 1) <= inter_call_data->maxoff)
	{
		RumItem				rum_item;	/* to read data from a page */
		Datum				values[3]; 	/* return values */
		bool				nulls[3];	/* true if the corresponding value is NULL */

		/* For the results */
		HeapTuple	resultTuple;
		Datum		result;

		memset(nulls, 0, sizeof(nulls));

		/* Reading information from the page in rum_item */
		inter_call_data->item_ptr = rumDataPageLeafRead(inter_call_data->item_ptr, 
												  		inter_call_data->rum_state_ptr->attrnAddToColumn, 
												  		&rum_item, false, inter_call_data->rum_state_ptr);

		/* Writing data from rum_item to values */
		values[0] = ItemPointerGetDatum(&(rum_item.iptr)); 
		values[1] = BoolGetDatum(rum_item.addInfoIsNull);

		/*
		 * If addInfo is not NULL, you need to return it as text. 
		 * If addInfo is NULL, then you need to specify this in 
		 * the corresponding value of the nulls array.
		 *
		 * You don't have to worry about freeing up memory in the 
		 * addInfoGetText() function, because the memory context 
		 * in which the current SRF function is called is temporary 
		 * and it will be cleaned up between calls.
		 */
		if(!rum_item.addInfoIsNull)
			values[2] = addInfoGetText(rum_item.addInfo, 
							  inter_call_data->addInfo_att_ptr->atttypid);
		else nulls[2] = true;

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
 * An auxiliary function that is used to convert additional 
 * information into text. This is a universal way to return any type.
 *
 * The following types of data are checked:
 * int2, int4, int8, float4, float8, money, oid, timestamp, 
 * timestamptz, time, timetz, date, interval, macaddr, inet, 
 * cidr, text, varchar, char, bytea, bit, varbit, numeric.
 *
 * All types accepted by rum must be checked, but 
 * perhaps some types are missing or some are superfluous.
 */
static Datum  
addInfoGetText(Datum addInfo, Oid atttypid)
{
	char *str_addInfo = NULL;

	/* addInfo cannot be NULL */
	Assert(DatumGetPointer(addInfo) != NULL);

	/*
	 * Form a string depending on the type of addInfo.
	 *
	 * FIXME The macros used below are taken from the pg_type_d file.h, 
	 * and it says not to use them in the new code, then it's not 
	 * clear how to determine the attribute type. In addition, it 
	 * was not possible to find conversion functions for several 
	 * types below.
	 */
	switch (atttypid) 
	{
		case INT2OID:
			str_addInfo = OidOutputFunctionCall(F_INT2OUT, addInfo);
			break;

		case INT4OID:
			str_addInfo = OidOutputFunctionCall(F_INT4OUT, addInfo);
			break;

		case INT8OID:
			str_addInfo = OidOutputFunctionCall(F_INT8OUT, addInfo);
			break;

		case FLOAT4OID:
			str_addInfo = OidOutputFunctionCall(F_FLOAT4OUT, addInfo);
			break;

		case FLOAT8OID:
			str_addInfo = OidOutputFunctionCall(F_FLOAT8OUT, addInfo);
			break;

		/*case MONEYOID:*/
		/*	str_addInfo = OidOutputFunctionCall(, addInfo);*/
		/*	break;*/

		case OIDOID:
			str_addInfo = OidOutputFunctionCall(F_OIDOUT, addInfo);
			break;

		case TIMESTAMPOID:
			str_addInfo = OidOutputFunctionCall(F_TIMESTAMP_OUT, addInfo);
			break;

		case TIMESTAMPTZOID:
			str_addInfo = OidOutputFunctionCall(F_TIMESTAMPTZ_OUT, addInfo);
			break;

		case TIMEOID:
			str_addInfo = OidOutputFunctionCall(F_TIME_OUT, addInfo);
			break;

		case TIMETZOID:
			str_addInfo = OidOutputFunctionCall(F_TIMETZ_OUT, addInfo);
			break;

		case DATEOID:
			str_addInfo = OidOutputFunctionCall(F_DATE_OUT, addInfo);
			break;

		case INTERVALOID:
			str_addInfo = OidOutputFunctionCall(F_INTERVAL_OUT, addInfo);
			break;

		case MACADDROID:
			str_addInfo = OidOutputFunctionCall(F_MACADDR_OUT, addInfo);
			break;

		case INETOID:
			str_addInfo = OidOutputFunctionCall(F_INET_OUT, addInfo);
			break;

		case CIDROID:
			str_addInfo = OidOutputFunctionCall(F_CIDR_OUT, addInfo);
			break;

		case TEXTOID:
			str_addInfo = OidOutputFunctionCall(F_CIDR_OUT, addInfo);
			break;

		/*case VARCHAROID:*/
		/*	str_addInfo = OidOutputFunctionCall(, addInfo);*/
		/*	break;*/
		/**/
		/*case CHAROID:*/
		/*	str_addInfo = OidOutputFunctionCall(, addInfo);*/
		/*	break;*/
		/**/
		/*case BYTEAOID:*/
		/*	str_addInfo = OidOutputFunctionCall(, addInfo);*/
		/*	break;*/

		case BITOID:
			str_addInfo = OidOutputFunctionCall(F_BIT_OUT, addInfo);
			break;

		case VARBITOID:
			str_addInfo = OidOutputFunctionCall(F_VARBIT_OUT, addInfo);
			break;

		case NUMERICOID:
			str_addInfo = OidOutputFunctionCall(F_NUMERIC_OUT, addInfo);
			break;
	}

	return CStringGetTextDatum(str_addInfo);
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
