/*-------------------------------------------------------------------------
 *
 * rumtidbitmap.h
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
#ifndef RUMTIDBITMAP_H
#define RUMTIDBITMAP_H

#include "postgres.h"
#include "nodes/tidbitmap.h"

typedef struct TIDBitmap RumTIDBitmap;

/* Likewise, RumTIDBitmap is private */
typedef struct TBMIterator RumTBMIterator;

/* Result structure for rum_tbm_iterate */
typedef struct TBMIterateResult RumTBMIterateResult;

/* function prototypes in rumtidbitmap.c */

extern RumTIDBitmap *rum_tbm_create(long maxbytes, dsa_area *dsa);
extern void rum_tbm_free(RumTIDBitmap *tbm);

extern void rum_tbm_add_tuples(RumTIDBitmap *tbm,
						   const ItemPointer tids, int ntids,
						   bool recheck);
extern void rum_tbm_add_page(RumTIDBitmap *tbm, BlockNumber pageno);

extern void rum_tbm_union(RumTIDBitmap *a, const RumTIDBitmap *b);
extern void rum_tbm_intersect(RumTIDBitmap *a, const RumTIDBitmap *b);

extern bool rum_tbm_is_empty(const RumTIDBitmap *tbm);

#if PG_VERSION_NUM >= 180000
extern RumTBMIterator rum_tbm_begin_iterate(RumTIDBitmap *tbm,
											dsa_area *dsa, dsa_pointer dsp);
extern bool rum_tbm_iterate(RumTBMIterator *iterator, RumTBMIterateResult *tbmres);
#else
extern RumTBMIterateResult *rum_tbm_iterate(RumTBMIterator *iterator);
extern RumTBMIterator *rum_tbm_begin_iterate(RumTIDBitmap *tbm);
#endif

extern void rum_tbm_end_iterate(RumTBMIterator *iterator);

extern long rum_tbm_calculate_entries(double maxbytes);

extern bool
rum_tbm_contains_tid(RumTIDBitmap *tbm, ItemPointer tid, bool *recheck);

#endif							/* RUMTIDBITMAP_H */
