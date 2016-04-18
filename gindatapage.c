/*-------------------------------------------------------------------------
 *
 * gindatapage.c
 *	  page utilities routines for the postgres inverted index access method.
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/gin/gindatapage.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/datum.h"
#include "utils/rel.h"

#include "rum.h"

/* Does datatype allow packing into the 1-byte-header varlena format? */
#define TYPE_IS_PACKABLE(typlen, typstorage) \
	((typlen) == -1 && (typstorage) != 'p')

/*
 * Increment data_length by the space needed by the datum, including any
 * preceding alignment padding.
 */
static Size
ginComputeDatumSize(Size data_length, Datum val, bool typbyval, char typalign,
				   int16 typlen, char typstorage)
{
	if (TYPE_IS_PACKABLE(typlen, typstorage) &&
		VARATT_CAN_MAKE_SHORT(DatumGetPointer(val)))
	{
		/*
		 * we're anticipating converting to a short varlena header, so adjust
		 * length and don't count any alignment
		 */
		data_length += VARATT_CONVERTED_SHORT_SIZE(DatumGetPointer(val));
	}
	else
	{
		data_length = att_align_datum(data_length, typalign, typlen, val);
		data_length = att_addlength_datum(data_length, typlen, val);
	}

	return data_length;
}

/*
 * Write the given datum beginning at ptr (after advancing to correct
 * alignment, if needed). Setting padding bytes to zero if needed. Return the
 * pointer incremented by space used.
 */
static Pointer
ginDatumWrite(Pointer ptr, Datum datum, bool typbyval, char typalign,
			int16 typlen, char typstorage)
{
	Size		data_length;
	Pointer		prev_ptr = ptr;

	if (typbyval)
	{
		/* pass-by-value */
		ptr = (char *) att_align_nominal(ptr, typalign);
		store_att_byval(ptr, datum, typlen);
		data_length = typlen;
	}
	else if (typlen == -1)
	{
		/* varlena */
		Pointer		val = DatumGetPointer(datum);

		if (VARATT_IS_EXTERNAL(val))
		{
			/*
			 * Throw error, because we must never put a toast pointer inside a
			 * range object.  Caller should have detoasted it.
			 */
			elog(ERROR, "cannot store a toast pointer inside a range");
			data_length = 0;	/* keep compiler quiet */
		}
		else if (VARATT_IS_SHORT(val))
		{
			/* no alignment for short varlenas */
			data_length = VARSIZE_SHORT(val);
			memmove(ptr, val, data_length);
		}
		else if (TYPE_IS_PACKABLE(typlen, typstorage) &&
				 VARATT_CAN_MAKE_SHORT(val))
		{
			/* convert to short varlena -- no alignment */
			data_length = VARATT_CONVERTED_SHORT_SIZE(val);
			SET_VARSIZE_SHORT(ptr, data_length);
			memmove(ptr + 1, VARDATA(val), data_length - 1);
		}
		else
		{
			/* full 4-byte header varlena */
			ptr = (char *) att_align_nominal(ptr, typalign);
			data_length = VARSIZE(val);
			memmove(ptr, val, data_length);
		}
	}
	else if (typlen == -2)
	{
		/* cstring ... never needs alignment */
		Assert(typalign == 'c');
		data_length = strlen(DatumGetCString(datum)) + 1;
		memmove(ptr, DatumGetPointer(datum), data_length);
	}
	else
	{
		/* fixed-length pass-by-reference */
		ptr = (char *) att_align_nominal(ptr, typalign);
		Assert(typlen > 0);
		data_length = typlen;
		memmove(ptr, DatumGetPointer(datum), data_length);
	}

	if (ptr != prev_ptr)
		memset(prev_ptr, 0, ptr - prev_ptr);
	ptr += data_length;

	return ptr;
}

/*
 * Write item pointer into leaf data page using varbyte encoding. Since
 * BlockNumber is stored in incremental manner we also need a previous item
 * pointer. Also store addInfoIsNull flag using one bit of OffsetNumber.
 */
char *
ginDataPageLeafWriteItemPointer(char *ptr, ItemPointer iptr, ItemPointer prev,
															bool addInfoIsNull)
{
	uint32 blockNumberIncr = 0;
	uint16 offset = iptr->ip_posid;

	Assert(ginCompareItemPointers(iptr, prev) > 0);
	Assert(OffsetNumberIsValid(iptr->ip_posid));

	blockNumberIncr = iptr->ip_blkid.bi_lo + (iptr->ip_blkid.bi_hi << 16) -
					  (prev->ip_blkid.bi_lo + (prev->ip_blkid.bi_hi << 16));


	while (true)
	{
		*ptr = (blockNumberIncr & (~HIGHBIT)) |
								((blockNumberIncr >= HIGHBIT) ? HIGHBIT : 0);
		ptr++;
		if (blockNumberIncr < HIGHBIT)
			break;
		blockNumberIncr >>= 7;
	}

	while (true)
	{
		if (offset >= SEVENTHBIT)
		{
			*ptr = (offset & (~HIGHBIT)) | HIGHBIT;
			ptr++;
			offset >>= 7;
		}
		else
		{
			*ptr = offset | (addInfoIsNull ? SEVENTHBIT : 0);
			ptr++;
			break;
		}
	}

	return ptr;
}

/**
 * Place item pointer with additional information into leaf data page.
 */
Pointer
ginPlaceToDataPageLeaf(Pointer ptr, OffsetNumber attnum,
	ItemPointer iptr, Datum addInfo, bool addInfoIsNull, ItemPointer prev,
	GinState *ginstate)
{
	Form_pg_attribute attr;

	ptr = ginDataPageLeafWriteItemPointer(ptr, iptr, prev, addInfoIsNull);

	if (!addInfoIsNull)
	{
		attr = ginstate->addAttrs[attnum - 1];
		ptr = ginDatumWrite(ptr, addInfo, attr->attbyval, attr->attalign,
			attr->attlen, attr->attstorage);
	}
	return ptr;
}

/*
 * Calculate size of incremental varbyte encoding of item pointer.
 */
static int
ginDataPageLeafGetItemPointerSize(ItemPointer iptr, ItemPointer prev)
{
	uint32 blockNumberIncr = 0;
	uint16 offset = iptr->ip_posid;
	int size = 0;

	blockNumberIncr = iptr->ip_blkid.bi_lo + (iptr->ip_blkid.bi_hi << 16) -
					  (prev->ip_blkid.bi_lo + (prev->ip_blkid.bi_hi << 16));


	while (true)
	{
		size++;
		if (blockNumberIncr < HIGHBIT)
			break;
		blockNumberIncr >>= 7;
	}

	while (true)
	{
		size++;
		if (offset < SEVENTHBIT)
			break;
		offset >>= 7;
	}

	return size;
}

/*
 * Returns size of item pointers with additional information if leaf data page
 * after inserting another one.
 */
Size
ginCheckPlaceToDataPageLeaf(OffsetNumber attnum,
	ItemPointer iptr, Datum addInfo, bool addInfoIsNull, ItemPointer prev,
	GinState *ginstate, Size size)
{
	Form_pg_attribute attr;

	size += ginDataPageLeafGetItemPointerSize(iptr, prev);

	if (!addInfoIsNull)
	{
		attr = ginstate->addAttrs[attnum - 1];
		size = ginComputeDatumSize(size, addInfo, attr->attbyval,
			attr->attalign, attr->attlen, attr->attstorage);
	}

	return size;
}

int
ginCompareItemPointers(ItemPointer a, ItemPointer b)
{
	BlockNumber ba = GinItemPointerGetBlockNumber(a);
	BlockNumber bb = GinItemPointerGetBlockNumber(b);

	if (ba == bb)
	{
		OffsetNumber oa = GinItemPointerGetOffsetNumber(a);
		OffsetNumber ob = GinItemPointerGetOffsetNumber(b);

		if (oa == ob)
			return 0;
		return (oa > ob) ? 1 : -1;
	}

	return (ba > bb) ? 1 : -1;
}

/*
 * Merge two ordered arrays of itempointers, eliminating any duplicates.
 * Returns the number of items in the result.
 * Caller is responsible that there is enough space at *dst.
 */
uint32
ginMergeItemPointers(ItemPointerData *dst, Datum *dstAddInfo, bool *dstAddInfoIsNull,
					 ItemPointerData *a, Datum *aAddInfo, bool *aAddInfoIsNull, uint32 na,
					 ItemPointerData *b, Datum *bAddInfo, bool *bAddInfoIsNull, uint32 nb)
{
	ItemPointerData *dptr = dst;
	ItemPointerData *aptr = a,
			   *bptr = b;

	while (aptr - a < na && bptr - b < nb)
	{
		int			cmp = ginCompareItemPointers(aptr, bptr);

		if (cmp > 0)
		{
			*dptr++ = *bptr++;
			*dstAddInfo++ = *bAddInfo++;
			*dstAddInfoIsNull++ = *bAddInfoIsNull++;
		}
		else if (cmp == 0)
		{
			/* we want only one copy of the identical items */
			*dptr++ = *bptr++;
			*dstAddInfo++ = *bAddInfo++;
			*dstAddInfoIsNull++ = *bAddInfoIsNull++;
			aptr++;
			aAddInfo++;
			aAddInfoIsNull++;
		}
		else
		{
			*dptr++ = *aptr++;
			*dstAddInfo++ = *aAddInfo++;
			*dstAddInfoIsNull++ = *aAddInfoIsNull++;
		}
	}

	while (aptr - a < na)
	{
		*dptr++ = *aptr++;
		*dstAddInfo++ = *aAddInfo++;
		*dstAddInfoIsNull++ = *aAddInfoIsNull++;
	}

	while (bptr - b < nb)
	{
		*dptr++ = *bptr++;
		*dstAddInfo++ = *bAddInfo++;
		*dstAddInfoIsNull++ = *bAddInfoIsNull++;
	}

	return dptr - dst;
}

/*
 * Checks, should we move to right link...
 * Compares inserting itemp pointer with right bound of current page
 */
static bool
dataIsMoveRight(GinBtree btree, Page page)
{
	ItemPointer iptr = GinDataPageGetRightBound(page);

	if (GinPageRightMost(page))
		return FALSE;

	return (ginCompareItemPointers(btree->items + btree->curitem, iptr) > 0) ? TRUE : FALSE;
}

/*
 * Find correct PostingItem in non-leaf page. It supposed that page
 * correctly chosen and searching value SHOULD be on page
 */
static BlockNumber
dataLocateItem(GinBtree btree, GinBtreeStack *stack)
{
	OffsetNumber low,
				high,
				maxoff;
	PostingItem *pitem = NULL;
	int			result;
	Page		page = BufferGetPage(stack->buffer, NULL, NULL,
									 BGP_NO_SNAPSHOT_TEST);

	Assert(!GinPageIsLeaf(page));
	Assert(GinPageIsData(page));

	if (btree->fullScan)
	{
		stack->off = FirstOffsetNumber;
		stack->predictNumber *= GinPageGetOpaque(page)->maxoff;
		return btree->getLeftMostPage(btree, page);
	}

	low = FirstOffsetNumber;
	maxoff = high = GinPageGetOpaque(page)->maxoff;
	Assert(high >= low);

	high++;

	while (high > low)
	{
		OffsetNumber mid = low + ((high - low) / 2);

		pitem = (PostingItem *) GinDataPageGetItem(page, mid);

		if (mid == maxoff)
		{
			/*
			 * Right infinity, page already correctly chosen with a help of
			 * dataIsMoveRight
			 */
			result = -1;
		}
		else
		{
			pitem = (PostingItem *) GinDataPageGetItem(page, mid);
			result = ginCompareItemPointers(btree->items + btree->curitem, &(pitem->key));
		}

		if (result == 0)
		{
			stack->off = mid;
			return PostingItemGetBlockNumber(pitem);
		}
		else if (result > 0)
			low = mid + 1;
		else
			high = mid;
	}

	Assert(high >= FirstOffsetNumber && high <= maxoff);

	stack->off = high;
	pitem = (PostingItem *) GinDataPageGetItem(page, high);
	return PostingItemGetBlockNumber(pitem);
}

/**
 * Find item pointer in leaf data page. Returns true if given item pointer is
 * found and false if it's not. Sets offset and iptrOut to last item pointer
 * which is less than given one. Sets ptrOut ahead that item pointer.
 */
static bool
findInLeafPage(GinBtree btree, Page page, OffsetNumber *offset,
	ItemPointerData *iptrOut, Pointer *ptrOut)
{
	Pointer		ptr = GinDataPageGetData(page);
	OffsetNumber i, maxoff, first = FirstOffsetNumber;
	ItemPointerData iptr = {{0,0},0};
	int cmp;

	maxoff = GinPageGetOpaque(page)->maxoff;

	/*
	 * At first, search index at the end of page. As the result we narrow
	 * [first, maxoff] range.
	 */
	for (i = 0; i < GinDataLeafIndexCount; i++)
	{
		GinDataLeafItemIndex *index = &GinPageGetIndexes(page)[i];
		if (index->offsetNumer == InvalidOffsetNumber)
			break;

		cmp = ginCompareItemPointers(&index->iptr, btree->items + btree->curitem);
		if (cmp < 0)
		{
			ptr = GinDataPageGetData(page) + index->pageOffset;
			first = index->offsetNumer;
			iptr = index->iptr;
		}
		else
		{
			maxoff = index->offsetNumer - 1;
			break;
		}
	}

	/* Search page in [first, maxoff] range found by page index */
	for (i = first; i <= maxoff; i++)
	{
		*ptrOut = ptr;
		*iptrOut = iptr;
		ptr = ginDataPageLeafRead(ptr, btree->entryAttnum, &iptr,
			NULL, NULL, btree->ginstate);

		cmp = ginCompareItemPointers(btree->items + btree->curitem, &iptr);
		if (cmp == 0)
		{
			*offset = i;
			return true;
		}
		if (cmp < 0)
		{
			*offset = i;
			return false;
		}
	}

	*ptrOut = ptr;
	*iptrOut = iptr;
	*offset = GinPageGetOpaque(page)->maxoff + 1;
	return false;
}


/*
 * Searches correct position for value on leaf page.
 * Page should be correctly chosen.
 * Returns true if value found on page.
 */
static bool
dataLocateLeafItem(GinBtree btree, GinBtreeStack *stack)
{
	Page		page = BufferGetPage(stack->buffer, NULL, NULL,
									 BGP_NO_SNAPSHOT_TEST);
	ItemPointerData iptr;
	Pointer ptr;

	Assert(GinPageIsLeaf(page));
	Assert(GinPageIsData(page));

	if (btree->fullScan)
	{
		stack->off = FirstOffsetNumber;
		return TRUE;
	}

	return findInLeafPage(btree, page, &stack->off, &iptr, &ptr);

}

/*
 * Finds links to blkno on non-leaf page, returns
 * offset of PostingItem
 */
static OffsetNumber
dataFindChildPtr(GinBtree btree, Page page, BlockNumber blkno, OffsetNumber storedOff)
{
	OffsetNumber i,
				maxoff = GinPageGetOpaque(page)->maxoff;
	PostingItem *pitem;

	Assert(!GinPageIsLeaf(page));
	Assert(GinPageIsData(page));

	/* if page isn't changed, we return storedOff */
	if (storedOff >= FirstOffsetNumber && storedOff <= maxoff)
	{
		pitem = (PostingItem *) GinDataPageGetItem(page, storedOff);
		if (PostingItemGetBlockNumber(pitem) == blkno)
			return storedOff;

		/*
		 * we hope, that needed pointer goes to right. It's true if there
		 * wasn't a deletion
		 */
		for (i = storedOff + 1; i <= maxoff; i++)
		{
			pitem = (PostingItem *) GinDataPageGetItem(page, i);
			if (PostingItemGetBlockNumber(pitem) == blkno)
				return i;
		}

		maxoff = storedOff - 1;
	}

	/* last chance */
	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		pitem = (PostingItem *) GinDataPageGetItem(page, i);
		if (PostingItemGetBlockNumber(pitem) == blkno)
			return i;
	}

	return InvalidOffsetNumber;
}

/*
 * returns blkno of leftmost child
 */
static BlockNumber
dataGetLeftMostPage(GinBtree btree, Page page)
{
	PostingItem *pitem;

	Assert(!GinPageIsLeaf(page));
	Assert(GinPageIsData(page));
	Assert(GinPageGetOpaque(page)->maxoff >= FirstOffsetNumber);

	pitem = (PostingItem *) GinDataPageGetItem(page, FirstOffsetNumber);
	return PostingItemGetBlockNumber(pitem);
}

/*
 * add ItemPointer or PostingItem to page. data should point to
 * correct value! depending on leaf or non-leaf page
 */
void
GinDataPageAddItem(Page page, void *data, OffsetNumber offset)
{
	OffsetNumber maxoff = GinPageGetOpaque(page)->maxoff;
	char	   *ptr;

	if (offset == InvalidOffsetNumber)
	{
		ptr = GinDataPageGetItem(page, maxoff + 1);
	}
	else
	{
		ptr = GinDataPageGetItem(page, offset);
		if (maxoff + 1 - offset != 0)
			memmove(ptr + GinSizeOfDataPageItem(page),
					ptr,
					(maxoff - offset + 1) * GinSizeOfDataPageItem(page));
	}
	memcpy(ptr, data, GinSizeOfDataPageItem(page));

	GinPageGetOpaque(page)->maxoff++;
}

/*
 * Deletes posting item from non-leaf page
 */
void
GinPageDeletePostingItem(Page page, OffsetNumber offset)
{
	OffsetNumber maxoff = GinPageGetOpaque(page)->maxoff;

	Assert(!GinPageIsLeaf(page));
	Assert(offset >= FirstOffsetNumber && offset <= maxoff);

	if (offset != maxoff)
		memmove(GinDataPageGetItem(page, offset), GinDataPageGetItem(page, offset + 1),
				sizeof(PostingItem) * (maxoff - offset));

	GinPageGetOpaque(page)->maxoff--;
}

/*
 * checks space to install new value,
 * item pointer never deletes!
 */
static bool
dataIsEnoughSpace(GinBtree btree, Buffer buf, OffsetNumber off)
{
	Page		page = BufferGetPage(buf, NULL, NULL, BGP_NO_SNAPSHOT_TEST);

	Assert(GinPageIsData(page));
	Assert(!btree->isDelete);

	if (GinPageIsLeaf(page))
	{
		int n, j;
		ItemPointerData iptr = {{0,0},0};
		Size size = 0;

		/*
		 * Calculate additional size using worst case assumption: varbyte
		 * encoding from zero item pointer. Also use worst case assumption about
		 * alignment.
		 */
		n = GinPageGetOpaque(page)->maxoff;

		if (GinPageRightMost(page) && off > n)
		{
			for (j = btree->curitem; j < btree->nitem; j++)
			{
				size = ginCheckPlaceToDataPageLeaf(btree->entryAttnum,
					&btree->items[j], btree->addInfo[j], btree->addInfoIsNull[j],
					(j == btree->curitem) ? (&iptr) : &btree->items[j - 1],
					btree->ginstate, size);
			}
		}
		else
		{
			j = btree->curitem;
			size = ginCheckPlaceToDataPageLeaf(btree->entryAttnum,
				&btree->items[j], btree->addInfo[j], btree->addInfoIsNull[j],
				&iptr, btree->ginstate, size);
		}
		size += MAXIMUM_ALIGNOF;

		if (GinPageGetOpaque(page)->freespace >= size)
			return true;

	}
	else if (sizeof(PostingItem) <= GinDataPageGetFreeSpace(page))
		return true;

	return false;
}

/*
 * In case of previous split update old child blkno to
 * new right page
 * item pointer never deletes!
 */
static BlockNumber
dataPrepareData(GinBtree btree, Page page, OffsetNumber off)
{
	BlockNumber ret = InvalidBlockNumber;

	Assert(GinPageIsData(page));

	if (!GinPageIsLeaf(page) && btree->rightblkno != InvalidBlockNumber)
	{
		PostingItem *pitem = (PostingItem *) GinDataPageGetItem(page, off);

		PostingItemSetBlockNumber(pitem, btree->rightblkno);
		ret = btree->rightblkno;
	}

	btree->rightblkno = InvalidBlockNumber;

	return ret;
}

/*
 * Places keys to page and fills WAL record. In case leaf page and
 * build mode puts all ItemPointers to page.
 */
static void
dataPlaceToPage(GinBtree btree, Buffer buf, OffsetNumber off, XLogRecData **prdata)
{
	Page		page = BufferGetPage(buf, NULL, NULL, BGP_NO_SNAPSHOT_TEST);
	Form_pg_attribute attr = btree->ginstate->addAttrs[btree->entryAttnum - 1];

	/* these must be static so they can be returned to caller */
	static XLogRecData rdata[3];
	static ginxlogInsert data;
	static char insertData[BLCKSZ];

	*prdata = rdata;
	Assert(GinPageIsData(page));

	data.updateBlkno = dataPrepareData(btree, page, off);

	data.node = btree->index->rd_node;
	data.blkno = BufferGetBlockNumber(buf);
	data.offset = off;
	data.isDelete = FALSE;
	data.isData = TRUE;
	data.isLeaf = GinPageIsLeaf(page) ? TRUE : FALSE;

	if (attr)
	{
		data.typlen = attr->attlen;
		data.typalign = attr->attalign;
		data.typbyval = attr->attbyval;
		data.typstorage = attr->attstorage;
	}

	/*
	 * For incomplete-split tracking, we need updateBlkno information and the
	 * inserted item even when we make a full page image of the page, so put
	 * the buffer reference in a separate XLogRecData entry.
	 */
	rdata[0].buffer = buf;
	rdata[0].buffer_std = FALSE;
	rdata[0].data = NULL;
	rdata[0].len = 0;
	rdata[0].next = &rdata[1];
	XLogBeginInsert();

	rdata[1].buffer = InvalidBuffer;
	rdata[1].data = (char *) &data;
	rdata[1].len = sizeof(ginxlogInsert);
	rdata[1].next = &rdata[2];

	if (GinPageIsLeaf(page))
	{
		int			i = 0, j, max_j;
		Pointer		ptr = GinDataPageGetData(page),
					copy_ptr = NULL,
					insertStart;
		ItemPointerData iptr = {{0,0},0}, copy_iptr;
		char		pageCopy[BLCKSZ];
		Datum		addInfo = 0;
		bool		addInfoIsNull = false;
		int			maxoff = GinPageGetOpaque(page)->maxoff;

		/*
		 * We're going to prevent var-byte re-encoding of whole page.
		 * Find position in page using page indexes.
		 */
		findInLeafPage(btree, page, &off, &iptr, &ptr);

		Assert(GinDataPageFreeSpacePre(page,ptr) >= 0);

		if (off <= maxoff)
		{
			/*
			 * Read next item-pointer with additional information: we'll have
			 * to re-encode it. Copy previous part of page.
			 */
			memcpy(pageCopy, page, BLCKSZ);
			copy_ptr = pageCopy + (ptr - page);
			copy_iptr = iptr;
		}

		/* Check how many items we're going to add */
		if (GinPageRightMost(page) && off > maxoff)
			max_j = btree->nitem;
		else
			max_j = btree->curitem + 1;

		/* Place items to the page while we have enough of space */
		*((ItemPointerData *)insertData) = iptr;
		insertStart = ptr;
		i = 0;
		for (j = btree->curitem; j < max_j; j++)
		{
			Pointer ptr2;

			ptr2 = page + ginCheckPlaceToDataPageLeaf(btree->entryAttnum,
				&btree->items[j], btree->addInfo[j], btree->addInfoIsNull[j],
				&iptr, btree->ginstate, ptr - page);

			if (GinDataPageFreeSpacePre(page, ptr2) < 0)
				break;

			ptr = ginPlaceToDataPageLeaf(ptr, btree->entryAttnum,
				&btree->items[j], btree->addInfo[j], btree->addInfoIsNull[j],
				&iptr, btree->ginstate);
			Assert(GinDataPageFreeSpacePre(page,ptr) >= 0);

			iptr = btree->items[j];
			btree->curitem++;
			data.nitem++;
			i++;
		}

		/* Put WAL data */
		memcpy(insertData + sizeof(ItemPointerData), insertStart,
															ptr - insertStart);
		rdata[2].buffer = InvalidBuffer;
		rdata[2].data = insertData;
		rdata[2].len = sizeof(ItemPointerData) + (ptr - insertStart);
		rdata[2].next = NULL;
		data.nitem = ptr - insertStart;

		/* Place rest of the page back */
		if (off <= maxoff)
		{
			for (j = off; j <= maxoff; j++)
			{
				copy_ptr = ginDataPageLeafRead(copy_ptr, btree->entryAttnum,
						&copy_iptr, &addInfo, &addInfoIsNull, btree->ginstate);
				ptr = ginPlaceToDataPageLeaf(ptr, btree->entryAttnum,
					&copy_iptr, addInfo, addInfoIsNull,
					&iptr, btree->ginstate);
				Assert(GinDataPageFreeSpacePre(page,ptr) >= 0);
				iptr = copy_iptr;
			}
		}

		GinPageGetOpaque(page)->maxoff += i;

		if (GinDataPageFreeSpacePre(page,ptr) < 0)
			elog(ERROR, "Not enough of space in leaf page!");

		/* Update indexes in the end of page */
		updateItemIndexes(page, btree->entryAttnum, btree->ginstate);
	}
	else
	{
		rdata[2].buffer = InvalidBuffer;
		rdata[2].data = (char *) &(btree->pitem);
		rdata[2].len = sizeof(PostingItem);
		rdata[2].next = NULL;
		data.nitem = 1;

		GinDataPageAddItem(page, &(btree->pitem), off);
	}
}

/* Macro for leaf data page split: switch to right page if needed. */
#define CHECK_SWITCH_TO_RPAGE                    \
	do {                                         \
		if (ptr - GinDataPageGetData(page) >     \
			totalsize / 2 && page == lpage)      \
		{                                        \
			maxLeftIptr = iptr;                  \
			prevIptr.ip_blkid.bi_hi = 0;         \
			prevIptr.ip_blkid.bi_lo = 0;         \
			prevIptr.ip_posid = 0;               \
			GinPageGetOpaque(lpage)->maxoff = j; \
			page = rpage;                        \
			ptr = GinDataPageGetData(rpage);     \
			j = FirstOffsetNumber;               \
		}                                        \
		else                                     \
		{                                        \
			j++;                                 \
		}                                        \
	} while (0)



/*
 * Place tuple and split page, original buffer(lbuf) leaves untouched,
 * returns shadow page of lbuf filled new data.
 * Item pointers with additional information are distributed between pages by
 * equal size on its, not an equal number!
 */
static Page
dataSplitPageLeaf(GinBtree btree, Buffer lbuf, Buffer rbuf, OffsetNumber off,
														XLogRecData **prdata)
{
	OffsetNumber i, j,
				maxoff;
	Size		totalsize = 0, prevTotalsize;
	Pointer		ptr, copyPtr;
	Page		page;
	Page		lpage = PageGetTempPageCopy(BufferGetPage(lbuf, NULL, NULL,
														 BGP_NO_SNAPSHOT_TEST));
	Page		rpage = BufferGetPage(rbuf, NULL, NULL, BGP_NO_SNAPSHOT_TEST);
	Size		pageSize = PageGetPageSize(lpage);
	Size		maxItemSize = 0;
	Datum		addInfo = 0;
	bool		addInfoIsNull;
	ItemPointerData iptr, prevIptr, maxLeftIptr;
	int			totalCount = 0;
	int			maxItemIndex = btree->curitem;
	Form_pg_attribute attr = btree->ginstate->addAttrs[btree->entryAttnum - 1];

	/* these must be static so they can be returned to caller */
	static XLogRecData rdata[3];
	static ginxlogSplit data;
	static char lpageCopy[BLCKSZ];
	static char rpageCopy[BLCKSZ];

	*prdata = rdata;
	data.leftChildBlkno = (GinPageIsLeaf(lpage)) ?
		InvalidOffsetNumber : GinGetDownlink(btree->entry);
	data.updateBlkno = dataPrepareData(btree, lpage, off);

	maxoff = GinPageGetOpaque(lpage)->maxoff;

	/* Copy original data of the page */
	memcpy(lpageCopy, lpage, BLCKSZ);

	/* Reinitialize pages */
	GinInitPage(rpage, GinPageGetOpaque(lpage)->flags, pageSize);
	GinInitPage(lpage, GinPageGetOpaque(rpage)->flags, pageSize);

	GinPageGetOpaque(lpage)->maxoff = 0;
	GinPageGetOpaque(rpage)->maxoff = 0;

	/* Calculate the whole size we're going to place */
	copyPtr = GinDataPageGetData(lpageCopy);
	iptr.ip_blkid.bi_hi = 0;
	iptr.ip_blkid.bi_lo = 0;
	iptr.ip_posid = 0;
	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		if (i == off)
		{
			prevIptr = iptr;
			iptr = btree->items[maxItemIndex];

			prevTotalsize = totalsize;
			totalsize = ginCheckPlaceToDataPageLeaf(btree->entryAttnum,
				&iptr, btree->addInfo[maxItemIndex],
				btree->addInfoIsNull[maxItemIndex],
				&prevIptr, btree->ginstate, totalsize);

			maxItemIndex++;
			totalCount++;
			maxItemSize = Max(maxItemSize, totalsize - prevTotalsize);
		}

		prevIptr = iptr;
		copyPtr = ginDataPageLeafRead(copyPtr, btree->entryAttnum,
			&iptr, &addInfo, &addInfoIsNull, btree->ginstate);

		prevTotalsize = totalsize;
		totalsize = ginCheckPlaceToDataPageLeaf(btree->entryAttnum,
			&iptr, addInfo, addInfoIsNull,
			&prevIptr, btree->ginstate, totalsize);

		totalCount++;
		maxItemSize = Max(maxItemSize, totalsize - prevTotalsize);
	}

	if (off == maxoff + 1)
	{
		prevIptr = iptr;
		iptr = btree->items[maxItemIndex];
		if (GinPageRightMost(lpage))
		{
			Size newTotalsize;

			/*
			 * Found how many new item pointer we're going to add using
			 * worst case assumptions about odd placement and alignment.
			 */
			while (maxItemIndex < btree->nitem &&
				(newTotalsize = ginCheckPlaceToDataPageLeaf(btree->entryAttnum,
					&iptr, btree->addInfo[maxItemIndex],
					btree->addInfoIsNull[maxItemIndex],
					&prevIptr, btree->ginstate, totalsize)) <
					2 * GinDataPageSize - 2 * maxItemSize - 2 * MAXIMUM_ALIGNOF
			)
			{
				maxItemIndex++;
				totalCount++;
				maxItemSize = Max(maxItemSize, newTotalsize - totalsize);
				totalsize = newTotalsize;

				prevIptr = iptr;
				if (maxItemIndex < btree->nitem)
					iptr = btree->items[maxItemIndex];
			}
		}
		else
		{
			prevTotalsize = totalsize;
			totalsize = ginCheckPlaceToDataPageLeaf(btree->entryAttnum,
				&iptr, btree->addInfo[maxItemIndex],
				btree->addInfoIsNull[maxItemIndex],
				&prevIptr, btree->ginstate, totalsize);
			maxItemIndex++;

			totalCount++;
			maxItemSize = Max(maxItemSize, totalsize - prevTotalsize);
		}
	}

	/*
	 * Place item pointers with additional information to the pages using
	 * previous calculations.
	 */
	ptr = GinDataPageGetData(lpage);
	page = lpage;
	j = FirstOffsetNumber;
	iptr.ip_blkid.bi_hi = 0;
	iptr.ip_blkid.bi_lo = 0;
	iptr.ip_posid = 0;
	prevIptr = iptr;
	copyPtr = GinDataPageGetData(lpageCopy);
	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		if (i == off)
		{
			while (btree->curitem < maxItemIndex)
			{
				ptr = ginPlaceToDataPageLeaf(ptr, btree->entryAttnum,
					&btree->items[btree->curitem],
					btree->addInfo[btree->curitem],
					btree->addInfoIsNull[btree->curitem],
					&prevIptr, btree->ginstate);
				Assert(GinDataPageFreeSpacePre(page, ptr) >= 0);

				prevIptr = btree->items[btree->curitem];
				btree->curitem++;

				CHECK_SWITCH_TO_RPAGE;
			}
		}

		copyPtr = ginDataPageLeafRead(copyPtr, btree->entryAttnum,
			&iptr, &addInfo, &addInfoIsNull, btree->ginstate);

		ptr = ginPlaceToDataPageLeaf(ptr, btree->entryAttnum, &iptr,
			addInfo, addInfoIsNull, &prevIptr, btree->ginstate);
		Assert(GinDataPageFreeSpacePre(page, ptr) >= 0);

		prevIptr = iptr;

		CHECK_SWITCH_TO_RPAGE;
	}

	if (off == maxoff + 1)
	{
		while (btree->curitem < maxItemIndex)
		{
			ptr = ginPlaceToDataPageLeaf(ptr, btree->entryAttnum,
				&btree->items[btree->curitem],
				btree->addInfo[btree->curitem],
				btree->addInfoIsNull[btree->curitem],
				&prevIptr, btree->ginstate);
			Assert(GinDataPageFreeSpacePre(page, ptr) >= 0);

			prevIptr = btree->items[btree->curitem];
			btree->curitem++;

			CHECK_SWITCH_TO_RPAGE;
		}
	}

	GinPageGetOpaque(rpage)->maxoff = j - 1;

	PostingItemSetBlockNumber(&(btree->pitem), BufferGetBlockNumber(lbuf));
	btree->pitem.key = maxLeftIptr;
	btree->rightblkno = BufferGetBlockNumber(rbuf);

	*GinDataPageGetRightBound(rpage) = *GinDataPageGetRightBound(lpageCopy);
	*GinDataPageGetRightBound(lpage) = maxLeftIptr;

	/* Fill indexes at the end of pages */
	updateItemIndexes(lpage, btree->entryAttnum, btree->ginstate);
	updateItemIndexes(rpage, btree->entryAttnum, btree->ginstate);

	data.node = btree->index->rd_node;
	data.rootBlkno = InvalidBlockNumber;
	data.lblkno = BufferGetBlockNumber(lbuf);
	data.rblkno = BufferGetBlockNumber(rbuf);
	data.separator = GinPageGetOpaque(lpage)->maxoff;
	data.nitem = GinPageGetOpaque(lpage)->maxoff + GinPageGetOpaque(rpage)->maxoff;
	data.isData = TRUE;
	data.isLeaf = TRUE;
	data.isRootSplit = FALSE;
	if (attr)
	{
		data.typlen = attr->attlen;
		data.typalign = attr->attalign;
		data.typbyval = attr->attbyval;
		data.typstorage = attr->attstorage;
	}
	data.rightbound = *GinDataPageGetRightBound(rpage);

	rdata[0].buffer = InvalidBuffer;
	rdata[0].data = (char *) &data;
	rdata[0].len = MAXALIGN(sizeof(ginxlogSplit));
	rdata[0].next = &rdata[1];

	memcpy(lpageCopy, lpage, BLCKSZ);
	memcpy(rpageCopy, rpage, BLCKSZ);

	rdata[1].buffer = InvalidBuffer;
	rdata[1].data = GinDataPageGetData(lpageCopy);
	rdata[1].len = MAXALIGN((GinDataPageSize - GinPageGetOpaque(lpageCopy)->freespace));
	rdata[1].next = &rdata[2];

	rdata[2].buffer = InvalidBuffer;
	rdata[2].data = GinDataPageGetData(rpageCopy);
	rdata[2].len = MAXALIGN((GinDataPageSize - GinPageGetOpaque(rpageCopy)->freespace));
	rdata[2].next = NULL;

	return lpage;
}

/*
 * split page and fills WAL record. original buffer(lbuf) leaves untouched,
 * returns shadow page of lbuf filled new data. In leaf page and build mode puts all
 * ItemPointers to pages. Also, in build mode splits data by way to full fulled
 * left page
 */
static Page
dataSplitPageInternal(GinBtree btree, Buffer lbuf, Buffer rbuf,
										OffsetNumber off, XLogRecData **prdata)
{
	char	   *ptr;
	OffsetNumber separator;
	ItemPointer bound;
	Page		lpage = PageGetTempPageCopy(BufferGetPage(lbuf, NULL, NULL,
														 BGP_NO_SNAPSHOT_TEST));
	ItemPointerData oldbound = *GinDataPageGetRightBound(lpage);
	int			sizeofitem = GinSizeOfDataPageItem(lpage);
	OffsetNumber maxoff = GinPageGetOpaque(lpage)->maxoff;
	Page		rpage = BufferGetPage(rbuf, NULL, NULL, BGP_NO_SNAPSHOT_TEST);
	Size		pageSize = PageGetPageSize(lpage);
	Size		freeSpace;
	uint32		nCopied = 1;

	/* these must be static so they can be returned to caller */
	static ginxlogSplit data;
	static XLogRecData rdata[2];
	static char vector[2 * BLCKSZ];

	GinInitPage(rpage, GinPageGetOpaque(lpage)->flags, pageSize);
	freeSpace = GinDataPageGetFreeSpace(rpage);

	*prdata = rdata;
	data.leftChildBlkno = (GinPageIsLeaf(lpage)) ?
		InvalidOffsetNumber : PostingItemGetBlockNumber(&(btree->pitem));
	data.updateBlkno = dataPrepareData(btree, lpage, off);

	memcpy(vector, GinDataPageGetItem(lpage, FirstOffsetNumber),
		   maxoff * sizeofitem);

	if (GinPageIsLeaf(lpage) && GinPageRightMost(lpage) && off > GinPageGetOpaque(lpage)->maxoff)
	{
		nCopied = 0;
		while (btree->curitem < btree->nitem &&
			   maxoff * sizeof(ItemPointerData) < 2 * (freeSpace - sizeof(ItemPointerData)))
		{
			memcpy(vector + maxoff * sizeof(ItemPointerData),
				   btree->items + btree->curitem,
				   sizeof(ItemPointerData));
			maxoff++;
			nCopied++;
			btree->curitem++;
		}
	}
	else
	{
		ptr = vector + (off - 1) * sizeofitem;
		if (maxoff + 1 - off != 0)
			memmove(ptr + sizeofitem, ptr, (maxoff - off + 1) * sizeofitem);
		if (GinPageIsLeaf(lpage))
		{
			memcpy(ptr, btree->items + btree->curitem, sizeofitem);
			btree->curitem++;
		}
		else
			memcpy(ptr, &(btree->pitem), sizeofitem);

		maxoff++;
	}

	/*
	 * we suppose that during index creation table scaned from begin to end,
	 * so ItemPointers are monotonically increased..
	 */
	if (btree->isBuild && GinPageRightMost(lpage))
		separator = freeSpace / sizeofitem;
	else
		separator = maxoff / 2;

	GinInitPage(rpage, GinPageGetOpaque(lpage)->flags, pageSize);
	GinInitPage(lpage, GinPageGetOpaque(rpage)->flags, pageSize);

	memcpy(GinDataPageGetItem(lpage, FirstOffsetNumber), vector, separator * sizeofitem);
	GinPageGetOpaque(lpage)->maxoff = separator;
	memcpy(GinDataPageGetItem(rpage, FirstOffsetNumber),
		 vector + separator * sizeofitem, (maxoff - separator) * sizeofitem);
	GinPageGetOpaque(rpage)->maxoff = maxoff - separator;

	PostingItemSetBlockNumber(&(btree->pitem), BufferGetBlockNumber(lbuf));
	if (GinPageIsLeaf(lpage))
		btree->pitem.key = *(ItemPointerData *) GinDataPageGetItem(lpage,
											GinPageGetOpaque(lpage)->maxoff);
	else
		btree->pitem.key = ((PostingItem *) GinDataPageGetItem(lpage,
									  GinPageGetOpaque(lpage)->maxoff))->key;
	btree->rightblkno = BufferGetBlockNumber(rbuf);

	/* set up right bound for left page */
	bound = GinDataPageGetRightBound(lpage);
	*bound = btree->pitem.key;

	/* set up right bound for right page */
	bound = GinDataPageGetRightBound(rpage);
	*bound = oldbound;

	data.node = btree->index->rd_node;
	data.rootBlkno = InvalidBlockNumber;
	data.lblkno = BufferGetBlockNumber(lbuf);
	data.rblkno = BufferGetBlockNumber(rbuf);
	data.separator = separator;
	data.nitem = maxoff;
	data.isData = TRUE;
	data.isLeaf = FALSE;
	data.isRootSplit = FALSE;
	data.rightbound = oldbound;

	rdata[0].buffer = InvalidBuffer;
	rdata[0].data = (char *) &data;
	rdata[0].len = sizeof(ginxlogSplit);
	rdata[0].next = &rdata[1];

	rdata[1].buffer = InvalidBuffer;
	rdata[1].data = vector;
	rdata[1].len = MAXALIGN(maxoff * sizeofitem);
	rdata[1].next = NULL;

	return lpage;
}

/*
 * Split page of posting tree. Calls relevant function of internal of leaf page
 * because they are handled very different.
 */
static Page
dataSplitPage(GinBtree btree, Buffer lbuf, Buffer rbuf, OffsetNumber off,
														XLogRecData **prdata)
{
	if (GinPageIsLeaf(BufferGetPage(lbuf, NULL, NULL, BGP_NO_SNAPSHOT_TEST)))
		return dataSplitPageLeaf(btree, lbuf, rbuf, off, prdata);
	else
		return dataSplitPageInternal(btree, lbuf, rbuf, off, prdata);
}

/*
 * Updates indexes in the end of leaf page which are used for faster search.
 * Also updates freespace opaque field of page. Returns last item pointer of
 * page.
 */
ItemPointerData
updateItemIndexes(Page page, OffsetNumber attnum, GinState *ginstate)
{
	Pointer ptr;
	ItemPointerData iptr;
	int j = 0, maxoff, i;

	/* Iterate over page */

	maxoff = GinPageGetOpaque(page)->maxoff;
	ptr = GinDataPageGetData(page);
	iptr.ip_blkid.bi_lo = 0;
	iptr.ip_blkid.bi_hi = 0;
	iptr.ip_posid = 0;

	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		/* Place next page index entry if it's time to */
		if (i * (GinDataLeafIndexCount + 1) > (j + 1) * maxoff)
		{
			GinPageGetIndexes(page)[j].iptr = iptr;
			GinPageGetIndexes(page)[j].offsetNumer = i;
			GinPageGetIndexes(page)[j].pageOffset = ptr - GinDataPageGetData(page);
			j++;
		}
		ptr = ginDataPageLeafRead(ptr, attnum, &iptr, NULL, NULL, ginstate);
	}
	/* Fill rest of page indexes with InvalidOffsetNumber if any */
	for (; j < GinDataLeafIndexCount; j++)
	{
		GinPageGetIndexes(page)[j].offsetNumer = InvalidOffsetNumber;
	}
	/* Update freespace of page */
	GinPageGetOpaque(page)->freespace = GinDataPageFreeSpacePre(page, ptr);
	return iptr;
}

/*
 * Fills new root by right bound values from child.
 * Also called from ginxlog, should not use btree
 */
void
ginDataFillRoot(GinBtree btree, Buffer root, Buffer lbuf, Buffer rbuf)
{
	Page		page = BufferGetPage(root, NULL, NULL, BGP_NO_SNAPSHOT_TEST),
				lpage = BufferGetPage(lbuf, NULL, NULL, BGP_NO_SNAPSHOT_TEST),
				rpage = BufferGetPage(rbuf, NULL, NULL, BGP_NO_SNAPSHOT_TEST);
	PostingItem li,
				ri;

	li.key = *GinDataPageGetRightBound(lpage);
	PostingItemSetBlockNumber(&li, BufferGetBlockNumber(lbuf));
	GinDataPageAddItem(page, &li, InvalidOffsetNumber);

	ri.key = *GinDataPageGetRightBound(rpage);
	PostingItemSetBlockNumber(&ri, BufferGetBlockNumber(rbuf));
	GinDataPageAddItem(page, &ri, InvalidOffsetNumber);
}

void
ginPrepareDataScan(GinBtree btree, Relation index, OffsetNumber attnum, GinState *ginstate)
{
	memset(btree, 0, sizeof(GinBtreeData));

	btree->index = index;
	btree->ginstate = ginstate;

	btree->findChildPage = dataLocateItem;
	btree->isMoveRight = dataIsMoveRight;
	btree->findItem = dataLocateLeafItem;
	btree->findChildPtr = dataFindChildPtr;
	btree->getLeftMostPage = dataGetLeftMostPage;
	btree->isEnoughSpace = dataIsEnoughSpace;
	btree->placeToPage = dataPlaceToPage;
	btree->splitPage = dataSplitPage;
	btree->fillRoot = ginDataFillRoot;

	btree->isData = TRUE;
	btree->searchMode = FALSE;
	btree->isDelete = FALSE;
	btree->fullScan = FALSE;
	btree->isBuild = FALSE;

	btree->entryAttnum = attnum;
}

GinPostingTreeScan *
ginPrepareScanPostingTree(Relation index, BlockNumber rootBlkno,
					bool searchMode, OffsetNumber attnum, GinState *ginstate)
{
	GinPostingTreeScan *gdi = (GinPostingTreeScan *) palloc0(sizeof(GinPostingTreeScan));

	ginPrepareDataScan(&gdi->btree, index, attnum, ginstate);

	gdi->btree.searchMode = searchMode;
	gdi->btree.fullScan = searchMode;

	gdi->stack = ginPrepareFindLeafPage(&gdi->btree, rootBlkno);

	return gdi;
}

/*
 * Inserts array of item pointers, may execute several tree scan (very rare)
 */
void
ginInsertItemPointers(GinState *ginstate,
					  OffsetNumber attnum,
					  GinPostingTreeScan *gdi,
					  ItemPointerData *items,
					  Datum *addInfo,
					  bool *addInfoIsNull,
					  uint32 nitem,
					  GinStatsData *buildStats)
{
	BlockNumber rootBlkno = gdi->stack->blkno;


	gdi->btree.items = items;
	gdi->btree.addInfo = addInfo;
	gdi->btree.addInfoIsNull = addInfoIsNull;

	gdi->btree.nitem = nitem;
	gdi->btree.curitem = 0;

	while (gdi->btree.curitem < gdi->btree.nitem)
	{
		if (!gdi->stack)
			gdi->stack = ginPrepareFindLeafPage(&gdi->btree, rootBlkno);

		gdi->stack = ginFindLeafPage(&gdi->btree, gdi->stack);

		if (gdi->btree.findItem(&(gdi->btree), gdi->stack))
		{
			/*
			 * gdi->btree.items[gdi->btree.curitem] already exists in index
			 */
			gdi->btree.curitem++;
			LockBuffer(gdi->stack->buffer, GIN_UNLOCK);
			freeGinBtreeStack(gdi->stack);
		}
		else
			ginInsertValue(&(gdi->btree), gdi->stack, buildStats);

		gdi->stack = NULL;
	}
}

Buffer
ginScanBeginPostingTree(GinPostingTreeScan *gdi)
{
	gdi->stack = ginFindLeafPage(&gdi->btree, gdi->stack);
	return gdi->stack->buffer;
}
