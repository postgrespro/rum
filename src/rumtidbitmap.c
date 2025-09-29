/*-------------------------------------------------------------------------
 *
 * rumtidbitmap.c
 *	  PostgreSQL tuple-id (TID) bitmap package
 *
 * This module contains copies of static functions from
 * src/backend/nodes/tidbitmap.c, which are needed to store
 * and quickly search for tids.
 *
 * Portions Copyright (c) 2025, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "rumtidbitmap.h"

#if PG_VERSION_NUM >= 170000
#include "tidbitmap17.c"
#elif PG_VERSION_NUM >= 160000
#include "tidbitmap16.c"
#elif PG_VERSION_NUM >= 150000
#include "tidbitmap15.c"
#elif PG_VERSION_NUM >= 140000
#include "tidbitmap14.c"
#elif PG_VERSION_NUM >= 130000
#include "tidbitmap13.c"
#elif PG_VERSION_NUM >= 120000
#include "tidbitmap12.c"
#endif

RumTIDBitmap *
rum_tbm_create(long maxbytes, dsa_area *dsa)
{
	return tbm_create(maxbytes, dsa);
}

void
rum_tbm_free(RumTIDBitmap *tbm)
{
	tbm_free(tbm);
}

void
rum_tbm_add_tuples(RumTIDBitmap *tbm,
				   const ItemPointer tids,
				   int ntids, bool recheck)
{
	tbm_add_tuples(tbm, tids, ntids, recheck);
}

void
rum_tbm_add_page(RumTIDBitmap *tbm, BlockNumber pageno)
{
	tbm_add_page(tbm, pageno);
}

void
rum_tbm_union(RumTIDBitmap *a, const RumTIDBitmap *b)
{
	tbm_union(a, b);
}

void rum_tbm_intersect(RumTIDBitmap *a, const RumTIDBitmap *b)
{
	tbm_intersect(a, b);
}

bool
rum_tbm_is_empty(const RumTIDBitmap *tbm)
{
	return tbm_is_empty(tbm);
}

RumTBMIterator *
rum_tbm_begin_iterate(RumTIDBitmap *tbm)
{
	return tbm_begin_iterate(tbm);
}

RumTBMIterateResult *
rum_tbm_iterate(RumTBMIterator *iterator)
{
	return tbm_iterate(iterator);
}

void
rum_tbm_end_iterate(RumTBMIterator *iterator)
{
	tbm_end_iterate(iterator);
}

long
rum_tbm_calculate_entries(double maxbytes)
{
	return tbm_calculate_entries(maxbytes);
}

/*
 * The function is needed to search for tid inside RumTIDBitmap.
 * If the page is marked as lossy, it writes true to *recheck.
 */
bool
rum_tbm_contains_tid(RumTIDBitmap *tbm, ItemPointer tid, bool *recheck)
{
	BlockNumber blkn = ItemPointerGetBlockNumber(tid);
	OffsetNumber off = ItemPointerGetOffsetNumber(tid);

	const PagetableEntry *page = NULL;

	*recheck = false;

	if (tbm_page_is_lossy(tbm, blkn))
	{
		*recheck = true;
		return true;
	}

	else if ((page = tbm_find_pageentry(tbm, blkn)) != NULL)
	{
		int wn = WORDNUM(off - 1);
		int bn = BITNUM(off - 1);
		return (page->words[wn] & ((bitmapword) 1 << bn)) != 0;
	}

	else
		return false;
}
