/*-------------------------------------------------------------------------
 *
 * rumdatapage.c
 *	  page utilities routines for the postgres inverted index access method.
 *
 *
 * Portions Copyright (c) 2015-2016, Postgres Professional
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "rum.h"

/* Does datatype allow packing into the 1-byte-header varlena format? */
#define TYPE_IS_PACKABLE(typlen, typstorage) \
	((typlen) == -1 && (typstorage) != 'p')

/*
 * Increment data_length by the space needed by the datum, including any
 * preceding alignment padding.
 */
static Size
rumComputeDatumSize(Size data_length, Datum val, bool typbyval, char typalign,
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
rumDatumWrite(Pointer ptr, Datum datum, bool typbyval, char typalign,
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
rumDataPageLeafWriteItemPointer(char *ptr, ItemPointer iptr, ItemPointer prev,
															bool addInfoIsNull)
{
	uint32 blockNumberIncr = 0;
	uint16 offset = iptr->ip_posid;

	Assert(rumCompareItemPointers(iptr, prev) > 0);
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
rumPlaceToDataPageLeaf(Pointer ptr, OffsetNumber attnum,
	ItemPointer iptr, Datum addInfo, bool addInfoIsNull, ItemPointer prev,
	RumState *rumstate)
{
	Form_pg_attribute attr;

	ptr = rumDataPageLeafWriteItemPointer(ptr, iptr, prev, addInfoIsNull);

	if (!addInfoIsNull)
	{
		attr = rumstate->addAttrs[attnum - 1];
		ptr = rumDatumWrite(ptr, addInfo, attr->attbyval, attr->attalign,
			attr->attlen, attr->attstorage);
	}
	return ptr;
}

/*
 * Calculate size of incremental varbyte encoding of item pointer.
 */
static int
rumDataPageLeafGetItemPointerSize(ItemPointer iptr, ItemPointer prev)
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
rumCheckPlaceToDataPageLeaf(OffsetNumber attnum,
	ItemPointer iptr, Datum addInfo, bool addInfoIsNull, ItemPointer prev,
	RumState *rumstate, Size size)
{
	Form_pg_attribute attr;

	size += rumDataPageLeafGetItemPointerSize(iptr, prev);

	if (!addInfoIsNull)
	{
		attr = rumstate->addAttrs[attnum - 1];
		size = rumComputeDatumSize(size, addInfo, attr->attbyval,
			attr->attalign, attr->attlen, attr->attstorage);
	}

	return size;
}

int
rumCompareItemPointers(ItemPointer a, ItemPointer b)
{
	BlockNumber ba = RumItemPointerGetBlockNumber(a);
	BlockNumber bb = RumItemPointerGetBlockNumber(b);

	if (ba == bb)
	{
		OffsetNumber oa = RumItemPointerGetOffsetNumber(a);
		OffsetNumber ob = RumItemPointerGetOffsetNumber(b);

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
rumMergeItemPointers(ItemPointerData *dst, Datum *dstAddInfo, bool *dstAddInfoIsNull,
					 ItemPointerData *a, Datum *aAddInfo, bool *aAddInfoIsNull, uint32 na,
					 ItemPointerData *b, Datum *bAddInfo, bool *bAddInfoIsNull, uint32 nb)
{
	ItemPointerData *dptr = dst;
	ItemPointerData *aptr = a,
			   *bptr = b;

	while (aptr - a < na && bptr - b < nb)
	{
		int			cmp = rumCompareItemPointers(aptr, bptr);

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
dataIsMoveRight(RumBtree btree, Page page)
{
	ItemPointer iptr = RumDataPageGetRightBound(page);

	if (RumPageRightMost(page))
		return FALSE;

	return (rumCompareItemPointers(btree->items + btree->curitem, iptr) > 0) ? TRUE : FALSE;
}

/*
 * Find correct PostingItem in non-leaf page. It supposed that page
 * correctly chosen and searching value SHOULD be on page
 */
static BlockNumber
dataLocateItem(RumBtree btree, RumBtreeStack *stack)
{
	OffsetNumber low,
				high,
				maxoff;
	PostingItem *pitem = NULL;
	int			result;
	Page		page = BufferGetPage(stack->buffer, NULL, NULL,
									 BGP_NO_SNAPSHOT_TEST);

	Assert(!RumPageIsLeaf(page));
	Assert(RumPageIsData(page));

	if (btree->fullScan)
	{
		stack->off = FirstOffsetNumber;
		stack->predictNumber *= RumPageGetOpaque(page)->maxoff;
		return btree->getLeftMostPage(btree, page);
	}

	low = FirstOffsetNumber;
	maxoff = high = RumPageGetOpaque(page)->maxoff;
	Assert(high >= low);

	high++;

	while (high > low)
	{
		OffsetNumber mid = low + ((high - low) / 2);

		pitem = (PostingItem *) RumDataPageGetItem(page, mid);

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
			pitem = (PostingItem *) RumDataPageGetItem(page, mid);
			result = rumCompareItemPointers(btree->items + btree->curitem, &(pitem->key));
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
	pitem = (PostingItem *) RumDataPageGetItem(page, high);
	return PostingItemGetBlockNumber(pitem);
}

/**
 * Find item pointer in leaf data page. Returns true if given item pointer is
 * found and false if it's not. Sets offset and iptrOut to last item pointer
 * which is less than given one. Sets ptrOut ahead that item pointer.
 */
static bool
findInLeafPage(RumBtree btree, Page page, OffsetNumber *offset,
	ItemPointerData *iptrOut, Pointer *ptrOut)
{
	Pointer		ptr = RumDataPageGetData(page);
	OffsetNumber i, maxoff, first = FirstOffsetNumber;
	ItemPointerData iptr = {{0,0},0};
	int cmp;

	maxoff = RumPageGetOpaque(page)->maxoff;

	/*
	 * At first, search index at the end of page. As the result we narrow
	 * [first, maxoff] range.
	 */
	for (i = 0; i < RumDataLeafIndexCount; i++)
	{
		RumDataLeafItemIndex *index = &RumPageGetIndexes(page)[i];
		if (index->offsetNumer == InvalidOffsetNumber)
			break;

		cmp = rumCompareItemPointers(&index->iptr, btree->items + btree->curitem);
		if (cmp < 0)
		{
			ptr = RumDataPageGetData(page) + index->pageOffset;
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
		ptr = rumDataPageLeafRead(ptr, btree->entryAttnum, &iptr,
			NULL, NULL, btree->rumstate);

		cmp = rumCompareItemPointers(btree->items + btree->curitem, &iptr);
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
	*offset = RumPageGetOpaque(page)->maxoff + 1;
	return false;
}


/*
 * Searches correct position for value on leaf page.
 * Page should be correctly chosen.
 * Returns true if value found on page.
 */
static bool
dataLocateLeafItem(RumBtree btree, RumBtreeStack *stack)
{
	Page		page = BufferGetPage(stack->buffer, NULL, NULL,
									 BGP_NO_SNAPSHOT_TEST);
	ItemPointerData iptr;
	Pointer ptr;

	Assert(RumPageIsLeaf(page));
	Assert(RumPageIsData(page));

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
dataFindChildPtr(RumBtree btree, Page page, BlockNumber blkno, OffsetNumber storedOff)
{
	OffsetNumber i,
				maxoff = RumPageGetOpaque(page)->maxoff;
	PostingItem *pitem;

	Assert(!RumPageIsLeaf(page));
	Assert(RumPageIsData(page));

	/* if page isn't changed, we return storedOff */
	if (storedOff >= FirstOffsetNumber && storedOff <= maxoff)
	{
		pitem = (PostingItem *) RumDataPageGetItem(page, storedOff);
		if (PostingItemGetBlockNumber(pitem) == blkno)
			return storedOff;

		/*
		 * we hope, that needed pointer goes to right. It's true if there
		 * wasn't a deletion
		 */
		for (i = storedOff + 1; i <= maxoff; i++)
		{
			pitem = (PostingItem *) RumDataPageGetItem(page, i);
			if (PostingItemGetBlockNumber(pitem) == blkno)
				return i;
		}

		maxoff = storedOff - 1;
	}

	/* last chance */
	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		pitem = (PostingItem *) RumDataPageGetItem(page, i);
		if (PostingItemGetBlockNumber(pitem) == blkno)
			return i;
	}

	return InvalidOffsetNumber;
}

/*
 * returns blkno of leftmost child
 */
static BlockNumber
dataGetLeftMostPage(RumBtree btree, Page page)
{
	PostingItem *pitem;

	Assert(!RumPageIsLeaf(page));
	Assert(RumPageIsData(page));
	Assert(RumPageGetOpaque(page)->maxoff >= FirstOffsetNumber);

	pitem = (PostingItem *) RumDataPageGetItem(page, FirstOffsetNumber);
	return PostingItemGetBlockNumber(pitem);
}

/*
 * add ItemPointer or PostingItem to page. data should point to
 * correct value! depending on leaf or non-leaf page
 */
void
RumDataPageAddItem(Page page, void *data, OffsetNumber offset)
{
	OffsetNumber maxoff = RumPageGetOpaque(page)->maxoff;
	char	   *ptr;
	size_t		size;

	if (offset == InvalidOffsetNumber)
	{
		ptr = RumDataPageGetItem(page, maxoff + 1);
	}
	else
	{
		ptr = RumDataPageGetItem(page, offset);
		if (maxoff + 1 - offset != 0)
			memmove(ptr + RumSizeOfDataPageItem(page),
					ptr,
					(maxoff - offset + 1) * RumSizeOfDataPageItem(page));
	}
	size = RumSizeOfDataPageItem(page);
	memcpy(ptr, data, size);
	((PageHeader) page)->pd_lower = (ptr + size) - page;
	elog(INFO, "RumDataPageAddItem: %d, %d", ((PageHeader) page)->pd_lower, ((PageHeader) page)->pd_upper);

	RumPageGetOpaque(page)->maxoff++;
}

/*
 * Deletes posting item from non-leaf page
 */
void
RumPageDeletePostingItem(Page page, OffsetNumber offset)
{
	OffsetNumber maxoff = RumPageGetOpaque(page)->maxoff;

	Assert(!RumPageIsLeaf(page));
	Assert(offset >= FirstOffsetNumber && offset <= maxoff);

	if (offset != maxoff)
		memmove(RumDataPageGetItem(page, offset), RumDataPageGetItem(page, offset + 1),
				sizeof(PostingItem) * (maxoff - offset));

	RumPageGetOpaque(page)->maxoff--;
}

/*
 * checks space to install new value,
 * item pointer never deletes!
 */
static bool
dataIsEnoughSpace(RumBtree btree, Buffer buf, OffsetNumber off)
{
	Page		page = BufferGetPage(buf, NULL, NULL, BGP_NO_SNAPSHOT_TEST);

	Assert(RumPageIsData(page));
	Assert(!btree->isDelete);

	if (RumPageIsLeaf(page))
	{
		int n, j;
		ItemPointerData iptr = {{0,0},0};
		Size size = 0;

		/*
		 * Calculate additional size using worst case assumption: varbyte
		 * encoding from zero item pointer. Also use worst case assumption about
		 * alignment.
		 */
		n = RumPageGetOpaque(page)->maxoff;

		if (RumPageRightMost(page) && off > n)
		{
			for (j = btree->curitem; j < btree->nitem; j++)
			{
				size = rumCheckPlaceToDataPageLeaf(btree->entryAttnum,
					&btree->items[j], btree->addInfo[j], btree->addInfoIsNull[j],
					(j == btree->curitem) ? (&iptr) : &btree->items[j - 1],
					btree->rumstate, size);
			}
		}
		else
		{
			j = btree->curitem;
			size = rumCheckPlaceToDataPageLeaf(btree->entryAttnum,
				&btree->items[j], btree->addInfo[j], btree->addInfoIsNull[j],
				&iptr, btree->rumstate, size);
		}
		size += MAXIMUM_ALIGNOF;

		if (RumPageGetOpaque(page)->freespace >= size)
			return true;

	}
	else if (sizeof(PostingItem) <= RumDataPageGetFreeSpace(page))
		return true;

	return false;
}

/*
 * In case of previous split update old child blkno to
 * new right page
 * item pointer never deletes!
 */
static BlockNumber
dataPrepareData(RumBtree btree, Page page, OffsetNumber off)
{
	BlockNumber ret = InvalidBlockNumber;

	Assert(RumPageIsData(page));

	if (!RumPageIsLeaf(page) && btree->rightblkno != InvalidBlockNumber)
	{
		PostingItem *pitem = (PostingItem *) RumDataPageGetItem(page, off);

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
dataPlaceToPage(RumBtree btree, Page page, OffsetNumber off)
{
	Assert(RumPageIsData(page));

// 	dataPrepareData(btree, page, off);

	if (RumPageIsLeaf(page))
	{
		int			i = 0, j, max_j;
		Pointer		ptr = RumDataPageGetData(page),
					copy_ptr = NULL;
		ItemPointerData iptr = {{0,0},0}, copy_iptr;
		char		pageCopy[BLCKSZ];
		Datum		addInfo = 0;
		bool		addInfoIsNull = false;
		int			maxoff = RumPageGetOpaque(page)->maxoff;

		/*
		 * We're going to prevent var-byte re-encoding of whole page.
		 * Find position in page using page indexes.
		 */
		findInLeafPage(btree, page, &off, &iptr, &ptr);

		Assert(RumDataPageFreeSpacePre(page,ptr) >= 0);

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
		if (RumPageRightMost(page) && off > maxoff)
			max_j = btree->nitem;
		else
			max_j = btree->curitem + 1;

		/* Place items to the page while we have enough of space */
		i = 0;
		for (j = btree->curitem; j < max_j; j++)
		{
			Pointer ptr2;

			ptr2 = page + rumCheckPlaceToDataPageLeaf(btree->entryAttnum,
				&btree->items[j], btree->addInfo[j], btree->addInfoIsNull[j],
				&iptr, btree->rumstate, ptr - page);

			if (RumDataPageFreeSpacePre(page, ptr2) < 0)
				break;

			ptr = rumPlaceToDataPageLeaf(ptr, btree->entryAttnum,
				&btree->items[j], btree->addInfo[j], btree->addInfoIsNull[j],
				&iptr, btree->rumstate);
			Assert(RumDataPageFreeSpacePre(page,ptr) >= 0);

			iptr = btree->items[j];
			btree->curitem++;
			i++;
		}

		/* Place rest of the page back */
		if (off <= maxoff)
		{
			for (j = off; j <= maxoff; j++)
			{
				copy_ptr = rumDataPageLeafRead(copy_ptr, btree->entryAttnum,
						&copy_iptr, &addInfo, &addInfoIsNull, btree->rumstate);
				ptr = rumPlaceToDataPageLeaf(ptr, btree->entryAttnum,
					&copy_iptr, addInfo, addInfoIsNull,
					&iptr, btree->rumstate);
				Assert(RumDataPageFreeSpacePre(page,ptr) >= 0);
				iptr = copy_iptr;
			}
		}

		RumPageGetOpaque(page)->maxoff += i;

		if (RumDataPageFreeSpacePre(page,ptr) < 0)
			elog(ERROR, "Not enough of space in leaf page!");

		/* Update indexes in the end of page */
		updateItemIndexes(page, btree->entryAttnum, btree->rumstate);
	}
	else
	{
		elog(INFO, "dataPlaceToPage: %d", PostingItemGetBlockNumber(&(btree->pitem)));
		RumDataPageAddItem(page, &(btree->pitem), off);
	}
}

/* Macro for leaf data page split: switch to right page if needed. */
#define CHECK_SWITCH_TO_RPAGE                       \
	do {                                            \
		if (ptr - RumDataPageGetData(page) >        \
			totalsize / 2 && page == newlPage)      \
		{                                           \
			maxLeftIptr = iptr;                     \
			prevIptr.ip_blkid.bi_hi = 0;            \
			prevIptr.ip_blkid.bi_lo = 0;            \
			prevIptr.ip_posid = 0;                  \
			RumPageGetOpaque(newlPage)->maxoff = j; \
			page = rPage;                           \
			ptr = RumDataPageGetData(rPage);        \
			j = FirstOffsetNumber;                  \
		}                                           \
		else                                        \
		{                                           \
			j++;                                    \
		}                                           \
	} while (0)



/*
 * Place tuple and split page, original buffer(lbuf) leaves untouched,
 * returns shadow page of lbuf filled new data.
 * Item pointers with additional information are distributed between pages by
 * equal size on its, not an equal number!
 */
static Page
dataSplitPageLeaf(RumBtree btree, Buffer lbuf, Buffer rbuf,
				  Page lPage, Page rPage, OffsetNumber off)
{
	OffsetNumber i, j,
				maxoff;
	Size		totalsize = 0, prevTotalsize;
	Pointer		ptr, copyPtr;
	Page		page;
	Page		newlPage = PageGetTempPageCopy(lPage);
	Size		pageSize = PageGetPageSize(newlPage);
	Size		maxItemSize = 0;
	Datum		addInfo = 0;
	bool		addInfoIsNull;
	ItemPointerData iptr, prevIptr, maxLeftIptr;
	int			totalCount = 0;
	int			maxItemIndex = btree->curitem;

	static char lpageCopy[BLCKSZ];

// 	dataPrepareData(btree, newlPage, off);
	maxoff = RumPageGetOpaque(newlPage)->maxoff;

	/* Copy original data of the page */
	memcpy(lpageCopy, newlPage, BLCKSZ);

	/* Reinitialize pages */
	RumInitPage(rPage, RumPageGetOpaque(newlPage)->flags, pageSize);
	RumInitPage(newlPage, RumPageGetOpaque(rPage)->flags, pageSize);

	RumPageGetOpaque(newlPage)->maxoff = 0;
	RumPageGetOpaque(rPage)->maxoff = 0;

	/* Calculate the whole size we're going to place */
	copyPtr = RumDataPageGetData(lpageCopy);
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
			totalsize = rumCheckPlaceToDataPageLeaf(btree->entryAttnum,
				&iptr, btree->addInfo[maxItemIndex],
				btree->addInfoIsNull[maxItemIndex],
				&prevIptr, btree->rumstate, totalsize);

			maxItemIndex++;
			totalCount++;
			maxItemSize = Max(maxItemSize, totalsize - prevTotalsize);
		}

		prevIptr = iptr;
		copyPtr = rumDataPageLeafRead(copyPtr, btree->entryAttnum,
			&iptr, &addInfo, &addInfoIsNull, btree->rumstate);

		prevTotalsize = totalsize;
		totalsize = rumCheckPlaceToDataPageLeaf(btree->entryAttnum,
			&iptr, addInfo, addInfoIsNull,
			&prevIptr, btree->rumstate, totalsize);

		totalCount++;
		maxItemSize = Max(maxItemSize, totalsize - prevTotalsize);
	}

	if (off == maxoff + 1)
	{
		prevIptr = iptr;
		iptr = btree->items[maxItemIndex];
		if (RumPageRightMost(newlPage))
		{
			Size newTotalsize;

			/*
			 * Found how many new item pointer we're going to add using
			 * worst case assumptions about odd placement and alignment.
			 */
			while (maxItemIndex < btree->nitem &&
				(newTotalsize = rumCheckPlaceToDataPageLeaf(btree->entryAttnum,
					&iptr, btree->addInfo[maxItemIndex],
					btree->addInfoIsNull[maxItemIndex],
					&prevIptr, btree->rumstate, totalsize)) <
					2 * RumDataPageSize - 2 * maxItemSize - 2 * MAXIMUM_ALIGNOF
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
			totalsize = rumCheckPlaceToDataPageLeaf(btree->entryAttnum,
				&iptr, btree->addInfo[maxItemIndex],
				btree->addInfoIsNull[maxItemIndex],
				&prevIptr, btree->rumstate, totalsize);
			maxItemIndex++;

			totalCount++;
			maxItemSize = Max(maxItemSize, totalsize - prevTotalsize);
		}
	}

	/*
	 * Place item pointers with additional information to the pages using
	 * previous calculations.
	 */
	ptr = RumDataPageGetData(newlPage);
	page = newlPage;
	j = FirstOffsetNumber;
	iptr.ip_blkid.bi_hi = 0;
	iptr.ip_blkid.bi_lo = 0;
	iptr.ip_posid = 0;
	prevIptr = iptr;
	copyPtr = RumDataPageGetData(lpageCopy);
	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		if (i == off)
		{
			while (btree->curitem < maxItemIndex)
			{
				ptr = rumPlaceToDataPageLeaf(ptr, btree->entryAttnum,
					&btree->items[btree->curitem],
					btree->addInfo[btree->curitem],
					btree->addInfoIsNull[btree->curitem],
					&prevIptr, btree->rumstate);
				Assert(RumDataPageFreeSpacePre(page, ptr) >= 0);

				prevIptr = btree->items[btree->curitem];
				btree->curitem++;

				CHECK_SWITCH_TO_RPAGE;
			}
		}

		copyPtr = rumDataPageLeafRead(copyPtr, btree->entryAttnum,
			&iptr, &addInfo, &addInfoIsNull, btree->rumstate);

		ptr = rumPlaceToDataPageLeaf(ptr, btree->entryAttnum, &iptr,
			addInfo, addInfoIsNull, &prevIptr, btree->rumstate);
		Assert(RumDataPageFreeSpacePre(page, ptr) >= 0);

		prevIptr = iptr;

		CHECK_SWITCH_TO_RPAGE;
	}

	if (off == maxoff + 1)
	{
		while (btree->curitem < maxItemIndex)
		{
			ptr = rumPlaceToDataPageLeaf(ptr, btree->entryAttnum,
				&btree->items[btree->curitem],
				btree->addInfo[btree->curitem],
				btree->addInfoIsNull[btree->curitem],
				&prevIptr, btree->rumstate);
			Assert(RumDataPageFreeSpacePre(page, ptr) >= 0);

			prevIptr = btree->items[btree->curitem];
			btree->curitem++;

			CHECK_SWITCH_TO_RPAGE;
		}
	}

	RumPageGetOpaque(rPage)->maxoff = j - 1;

	elog(INFO, "dataSplitPageLeaf: %d, %d", lbuf, BufferGetBlockNumber(lbuf));
	PostingItemSetBlockNumber(&(btree->pitem), BufferGetBlockNumber(lbuf));
	btree->pitem.key = maxLeftIptr;
	btree->rightblkno = BufferGetBlockNumber(rbuf);

	*RumDataPageGetRightBound(rPage) = *RumDataPageGetRightBound(lpageCopy);
	*RumDataPageGetRightBound(newlPage) = maxLeftIptr;

	/* Fill indexes at the end of pages */
	updateItemIndexes(newlPage, btree->entryAttnum, btree->rumstate);
	updateItemIndexes(rPage, btree->entryAttnum, btree->rumstate);

	return newlPage;
}

/*
 * split page and fills WAL record. original buffer(lbuf) leaves untouched,
 * returns shadow page of lbuf filled new data. In leaf page and build mode puts all
 * ItemPointers to pages. Also, in build mode splits data by way to full fulled
 * left page
 */
static Page
dataSplitPageInternal(RumBtree btree, Buffer lbuf, Buffer rbuf,
					  Page lPage, Page rPage, OffsetNumber off)
{
	char	   *ptr;
	OffsetNumber separator;
	ItemPointer bound;
	Page		newlPage = PageGetTempPageCopy(BufferGetPage(lbuf, NULL, NULL,
														 BGP_NO_SNAPSHOT_TEST));
	ItemPointerData oldbound = *RumDataPageGetRightBound(newlPage);
	int			sizeofitem = RumSizeOfDataPageItem(newlPage);
	OffsetNumber maxoff = RumPageGetOpaque(newlPage)->maxoff;
	Size		pageSize = PageGetPageSize(newlPage);
	Size		freeSpace;
	uint32		nCopied = 1;

	static char vector[2 * BLCKSZ];

	RumInitPage(rPage, RumPageGetOpaque(newlPage)->flags, pageSize);
	freeSpace = RumDataPageGetFreeSpace(rPage);
// 	dataPrepareData(btree, newlPage, off);

	memcpy(vector, RumDataPageGetItem(newlPage, FirstOffsetNumber),
		   maxoff * sizeofitem);

	if (RumPageIsLeaf(newlPage) && RumPageRightMost(newlPage) &&
		off > RumPageGetOpaque(newlPage)->maxoff)
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
		if (RumPageIsLeaf(newlPage))
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
	if (btree->isBuild && RumPageRightMost(newlPage))
		separator = freeSpace / sizeofitem;
	else
		separator = maxoff / 2;

	RumInitPage(rPage, RumPageGetOpaque(newlPage)->flags, pageSize);
	RumInitPage(newlPage, RumPageGetOpaque(rPage)->flags, pageSize);

	memcpy(RumDataPageGetItem(newlPage, FirstOffsetNumber), vector, separator * sizeofitem);
	RumPageGetOpaque(newlPage)->maxoff = separator;
	memcpy(RumDataPageGetItem(rPage, FirstOffsetNumber),
		 vector + separator * sizeofitem, (maxoff - separator) * sizeofitem);
	RumPageGetOpaque(rPage)->maxoff = maxoff - separator;

	PostingItemSetBlockNumber(&(btree->pitem), BufferGetBlockNumber(lbuf));
	if (RumPageIsLeaf(newlPage))
		btree->pitem.key = *(ItemPointerData *) RumDataPageGetItem(newlPage,
											RumPageGetOpaque(newlPage)->maxoff);
	else
		btree->pitem.key = ((PostingItem *) RumDataPageGetItem(newlPage,
									  RumPageGetOpaque(newlPage)->maxoff))->key;
	btree->rightblkno = BufferGetBlockNumber(rbuf);

	/* set up right bound for left page */
	bound = RumDataPageGetRightBound(newlPage);
	*bound = btree->pitem.key;

	/* set up right bound for right page */
	bound = RumDataPageGetRightBound(rPage);
	*bound = oldbound;

	return newlPage;
}

/*
 * Split page of posting tree. Calls relevant function of internal of leaf page
 * because they are handled very different.
 */
static Page
dataSplitPage(RumBtree btree, Buffer lbuf, Buffer rbuf,
			  Page lpage, Page rpage, OffsetNumber off)
{
	if (RumPageIsLeaf(BufferGetPage(lbuf, NULL, NULL, BGP_NO_SNAPSHOT_TEST)))
		return dataSplitPageLeaf(btree, lbuf, rbuf, lpage, rpage, off);
	else
		return dataSplitPageInternal(btree, lbuf, rbuf, lpage, rpage, off);
}

/*
 * Updates indexes in the end of leaf page which are used for faster search.
 * Also updates freespace opaque field of page. Returns last item pointer of
 * page.
 */
ItemPointerData
updateItemIndexes(Page page, OffsetNumber attnum, RumState *rumstate)
{
	Pointer ptr;
	ItemPointerData iptr;
	int j = 0, maxoff, i;

	/* Iterate over page */

	maxoff = RumPageGetOpaque(page)->maxoff;
	ptr = RumDataPageGetData(page);
	iptr.ip_blkid.bi_lo = 0;
	iptr.ip_blkid.bi_hi = 0;
	iptr.ip_posid = 0;

	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		/* Place next page index entry if it's time to */
		if (i * (RumDataLeafIndexCount + 1) > (j + 1) * maxoff)
		{
			RumPageGetIndexes(page)[j].iptr = iptr;
			RumPageGetIndexes(page)[j].offsetNumer = i;
			RumPageGetIndexes(page)[j].pageOffset = ptr - RumDataPageGetData(page);
			j++;
		}
		ptr = rumDataPageLeafRead(ptr, attnum, &iptr, NULL, NULL, rumstate);
	}
	/* Fill rest of page indexes with InvalidOffsetNumber if any */
	for (; j < RumDataLeafIndexCount; j++)
	{
		RumPageGetIndexes(page)[j].offsetNumer = InvalidOffsetNumber;
	}
	/* Update freespace of page */
	RumPageGetOpaque(page)->freespace = RumDataPageFreeSpacePre(page, ptr);
	/* Adjust pd_lower */
	((PageHeader) page)->pd_lower = ptr - page;
	return iptr;
}

/*
 * Fills new root by right bound values from child.
 * Also called from rumxlog, should not use btree
 */
void
rumDataFillRoot(RumBtree btree, Buffer root, Buffer lbuf, Buffer rbuf,
				Page page, Page lpage, Page rpage)
{
	PostingItem li,
				ri;

	li.key = *RumDataPageGetRightBound(lpage);
	PostingItemSetBlockNumber(&li, BufferGetBlockNumber(lbuf));
	RumDataPageAddItem(page, &li, InvalidOffsetNumber);

	ri.key = *RumDataPageGetRightBound(rpage);
	PostingItemSetBlockNumber(&ri, BufferGetBlockNumber(rbuf));
	RumDataPageAddItem(page, &ri, InvalidOffsetNumber);
}

void
rumPrepareDataScan(RumBtree btree, Relation index, OffsetNumber attnum, RumState *rumstate)
{
	memset(btree, 0, sizeof(RumBtreeData));

	btree->index = index;
	btree->rumstate = rumstate;

	btree->findChildPage = dataLocateItem;
	btree->isMoveRight = dataIsMoveRight;
	btree->findItem = dataLocateLeafItem;
	btree->findChildPtr = dataFindChildPtr;
	btree->getLeftMostPage = dataGetLeftMostPage;
	btree->isEnoughSpace = dataIsEnoughSpace;
	btree->placeToPage = dataPlaceToPage;
	btree->splitPage = dataSplitPage;
	btree->fillRoot = rumDataFillRoot;

	btree->isData = TRUE;
	btree->searchMode = FALSE;
	btree->isDelete = FALSE;
	btree->fullScan = FALSE;
	btree->isBuild = FALSE;

	btree->entryAttnum = attnum;
}

RumPostingTreeScan *
rumPrepareScanPostingTree(Relation index, BlockNumber rootBlkno,
					bool searchMode, OffsetNumber attnum, RumState *rumstate)
{
	RumPostingTreeScan *gdi = (RumPostingTreeScan *) palloc0(sizeof(RumPostingTreeScan));

	rumPrepareDataScan(&gdi->btree, index, attnum, rumstate);

	gdi->btree.searchMode = searchMode;
	gdi->btree.fullScan = searchMode;

	gdi->stack = rumPrepareFindLeafPage(&gdi->btree, rootBlkno);

	return gdi;
}

/*
 * Inserts array of item pointers, may execute several tree scan (very rare)
 */
void
rumInsertItemPointers(RumState *rumstate,
					  OffsetNumber attnum,
					  RumPostingTreeScan *gdi,
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
			gdi->stack = rumPrepareFindLeafPage(&gdi->btree, rootBlkno);

		gdi->stack = rumFindLeafPage(&gdi->btree, gdi->stack);

		if (gdi->btree.findItem(&(gdi->btree), gdi->stack))
		{
			/*
			 * gdi->btree.items[gdi->btree.curitem] already exists in index
			 */
			gdi->btree.curitem++;
			LockBuffer(gdi->stack->buffer, RUM_UNLOCK);
			freeRumBtreeStack(gdi->stack);
		}
		else
			rumInsertValue(rumstate->index, &(gdi->btree), gdi->stack, buildStats);

		gdi->stack = NULL;
	}
}

Buffer
rumScanBeginPostingTree(RumPostingTreeScan *gdi)
{
	gdi->stack = rumFindLeafPage(&gdi->btree, gdi->stack);
	return gdi->stack->buffer;
}
