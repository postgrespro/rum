/*-------------------------------------------------------------------------
 *
 * rumsort.h
 *	Generalized tuple sorting routines.
 *
 * This module handles sorting of heap tuples, index tuples, or single
 * Datums (and could easily support other kinds of sortable objects,
 * if necessary).  It works efficiently for both small and large amounts
 * of data.  Small amounts are sorted in-memory using qsort().  Large
 * amounts are sorted using temporary files and a standard external sort
 * algorithm.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#ifndef TUPLESORT_H
/* Hide tuplesort.h and tuplesort.c */
#define TUPLESORT_H

#include "postgres.h"
#include "fmgr.h"

#include "access/itup.h"
#include "executor/tuptable.h"
#include "utils/relcache.h"

/* Tuplesortstate is an opaque type whose details are not known outside
 * tuplesort.c.
 */
typedef struct Tuplesortstate Tuplesortstate;
struct RumKey;

/*
 * We provide multiple interfaces to what is essentially the same code,
 * since different callers have different data to be sorted and want to
 * specify the sort key information differently.  There are two APIs for
 * sorting HeapTuples and two more for sorting IndexTuples.  Yet another
 * API supports sorting bare Datums.
 *
 * The "heap" API actually stores/sorts MinimalTuples, which means it doesn't
 * preserve the system columns (tuple identity and transaction visibility
 * info).  The sort keys are specified by column numbers within the tuples
 * and sort operator OIDs.  We save some cycles by passing and returning the
 * tuples in TupleTableSlots, rather than forming actual HeapTuples (which'd
 * have to be converted to MinimalTuples).  This API works well for sorts
 * executed as parts of plan trees.
 *
 * The "cluster" API stores/sorts full HeapTuples including all visibility
 * info. The sort keys are specified by reference to a btree index that is
 * defined on the relation to be sorted.  Note that putheaptuple/getheaptuple
 * go with this API, not the "begin_heap" one!
 *
 * The "index_btree" API stores/sorts IndexTuples (preserving all their
 * header fields).  The sort keys are specified by a btree index definition.
 *
 * The "index_hash" API is similar to index_btree, but the tuples are
 * actually sorted by their hash codes not the raw data.
 */

typedef struct
{
	ItemPointerData iptr;
	bool		recheck;
	float8		data[FLEXIBLE_ARRAY_MEMBER];
}	RumSortItem;

#define RumSortItemSize(nKeys) (offsetof(RumSortItem,data)+(nKeys)*sizeof(float8))

extern MemoryContext rum_tuplesort_get_memorycontext(Tuplesortstate *state);
extern Tuplesortstate *rum_tuplesort_begin_heap(TupleDesc tupDesc,
						 int nkeys, AttrNumber *attNums,
						 Oid *sortOperators, Oid *sortCollations,
						 bool *nullsFirstFlags,
						 int workMem, bool randomAccess);
extern Tuplesortstate *rum_tuplesort_begin_cluster(TupleDesc tupDesc,
							Relation indexRel,
							int workMem, bool randomAccess);
extern Tuplesortstate *rum_tuplesort_begin_index_btree(Relation heapRel,
								Relation indexRel,
								bool enforceUnique,
								int workMem, bool randomAccess);
extern Tuplesortstate *rum_tuplesort_begin_index_hash(Relation heapRel,
							   Relation indexRel,
							   uint32 hash_mask,
							   int workMem, bool randomAccess);
extern Tuplesortstate *rum_tuplesort_begin_datum(Oid datumType,
						  Oid sortOperator, Oid sortCollation,
						  bool nullsFirstFlag,
						  int workMem, bool randomAccess);
extern Tuplesortstate *rum_tuplesort_begin_rum(int workMem,
						int nKeys, bool randomAccess, bool compareItemPointer);
extern Tuplesortstate	*rum_tuplesort_begin_rumkey(int workMem,
													FmgrInfo *cmp);

extern void rum_tuplesort_set_bound(Tuplesortstate *state, int64 bound);

extern void rum_tuplesort_puttupleslot(Tuplesortstate *state,
						   TupleTableSlot *slot);
extern void rum_tuplesort_putheaptuple(Tuplesortstate *state, HeapTuple tup);
extern void rum_tuplesort_putindextuple(Tuplesortstate *state, IndexTuple tuple);
extern void rum_tuplesort_putdatum(Tuplesortstate *state, Datum val,
					   bool isNull);
extern void rum_tuplesort_putrum(Tuplesortstate *state, RumSortItem * item);
extern void rum_tuplesort_putrumkey(Tuplesortstate *state, struct RumKey * item);

extern void rum_tuplesort_performsort(Tuplesortstate *state);

extern bool rum_tuplesort_gettupleslot(Tuplesortstate *state, bool forward,
						   TupleTableSlot *slot);
extern HeapTuple rum_tuplesort_getheaptuple(Tuplesortstate *state, bool forward,
						   bool *should_free);
extern IndexTuple rum_tuplesort_getindextuple(Tuplesortstate *state, bool forward,
							bool *should_free);
extern bool rum_tuplesort_getdatum(Tuplesortstate *state, bool forward,
					   Datum *val, bool *isNull);
extern RumSortItem *rum_tuplesort_getrum(Tuplesortstate *state, bool forward,
					 bool *should_free);
extern struct RumKey *rum_tuplesort_getrumkey(Tuplesortstate *state, bool forward,
					 bool *should_free);

extern void rum_tuplesort_end(Tuplesortstate *state);

extern void rum_tuplesort_get_stats(Tuplesortstate *state,
						const char **sortMethod,
						const char **spaceType,
						long *spaceUsed);

extern int	rum_tuplesort_merge_order(long allowedMem);

/*
 * These routines may only be called if randomAccess was specified 'true'.
 * Likewise, backwards scan in gettuple/getdatum is only allowed if
 * randomAccess was specified.
 */

extern void rum_tuplesort_rescan(Tuplesortstate *state);
extern void rum_tuplesort_markpos(Tuplesortstate *state);
extern void rum_tuplesort_restorepos(Tuplesortstate *state);

#endif   /* TUPLESORT_H */
