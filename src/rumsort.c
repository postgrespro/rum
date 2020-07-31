/*-------------------------------------------------------------------------
 *
 * rumsort.c
 *	  Generalized tuple sorting routines.
 *
 * This module handles sorting of RumSortItem or RumScanItem structures.
 * It contains copy of static functions from
 * src/backend/utils/sort/tuplesort.c.
 *
 *
 * Portions Copyright (c) 2015-2019, Postgres Professional
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "rumsort.h"

#include "commands/tablespace.h"
#include "executor/executor.h"
#include "utils/guc.h"
#include "utils/logtape.h"
#include "utils/pg_rusage.h"

#include "rum.h" /* RumItem */

/* sort-type codes for sort__start probes */
#define HEAP_SORT		0
#define INDEX_SORT		1
#define DATUM_SORT		2
#define CLUSTER_SORT	3

#if PG_VERSION_NUM < 100000
/* Provide fallback for old version of tape interface for 9.6 */
#define LogicalTapeRewindForRead(x, y, z) LogicalTapeRewind((x), (y), false)
#define LogicalTapeRewindForWrite(x, y) LogicalTapeRewind((x), (y), true)
#endif

#if PG_VERSION_NUM >= 110000
#define LogicalTapeSetCreate(X) LogicalTapeSetCreate(X, NULL, NULL, 1)
#define LogicalTapeFreeze(X, Y) LogicalTapeFreeze(X, Y, NULL)
#endif

/*
 * Below are copied definitions from src/backend/utils/sort/tuplesort.c.
 */

/* GUC variables */
#ifdef TRACE_SORT
bool		trace_sort = false;
#endif

typedef struct
{
	void	   *tuple;			/* the tuple proper */
	Datum		datum1;			/* value of first key column */
	bool		isnull1;		/* is first key column NULL? */
	int			tupindex;		/* see notes above */
} SortTuple;

typedef enum
{
	TSS_INITIAL,				/* Loading tuples; still within memory limit */
	TSS_BOUNDED,				/* Loading tuples into bounded-size heap */
	TSS_BUILDRUNS,				/* Loading tuples; writing to tape */
	TSS_SORTEDINMEM,			/* Sort completed entirely in memory */
	TSS_SORTEDONTAPE,			/* Sort completed, final run is on tape */
	TSS_FINALMERGE				/* Performing final merge on-the-fly */
} TupSortStatus;

#define MINORDER		6		/* minimum merge order */
#define TAPE_BUFFER_OVERHEAD		(BLCKSZ * 3)
#define MERGE_BUFFER_SIZE			(BLCKSZ * 32)

typedef int (*SortTupleComparator) (const SortTuple *a, const SortTuple *b,
												RumTuplesortstate *state);

/*
 * Renamed copy of Tuplesortstate.
 */
struct RumTuplesortstate
{
	TupSortStatus status;		/* enumerated value as shown above */
	int			nKeys;			/* number of columns in sort key */
	bool		randomAccess;	/* did caller request random access? */
	bool		bounded;		/* did caller specify a maximum number of
								 * tuples to return? */
	bool		boundUsed;		/* true if we made use of a bounded heap */
	int			bound;			/* if bounded, the maximum number of tuples */
	long		availMem;		/* remaining memory available, in bytes */
	long		allowedMem;		/* total memory allowed, in bytes */
	int			maxTapes;		/* number of tapes (Knuth's T) */
	int			tapeRange;		/* maxTapes-1 (Knuth's P) */
	MemoryContext sortcontext;	/* memory context holding all sort data */
	LogicalTapeSet *tapeset;	/* logtape.c object for tapes in a temp file */

	/*
	 * These function pointers decouple the routines that must know what kind
	 * of tuple we are sorting from the routines that don't need to know it.
	 * They are set up by the rum_tuplesort_begin_xxx routines.
	 *
	 * Function to compare two tuples; result is per qsort() convention, ie:
	 * <0, 0, >0 according as a<b, a=b, a>b.  The API must match
	 * qsort_arg_comparator.
	 */
	SortTupleComparator comparetup;

	/*
	 * Function to copy a supplied input tuple into palloc'd space and set up
	 * its SortTuple representation (ie, set tuple/datum1/isnull1).  Also,
	 * state->availMem must be decreased by the amount of space used for the
	 * tuple copy (note the SortTuple struct itself is not counted).
	 */
	void		(*copytup) (RumTuplesortstate *state, SortTuple *stup, void *tup);

	/*
	 * Function to write a stored tuple onto tape.  The representation of the
	 * tuple on tape need not be the same as it is in memory; requirements on
	 * the tape representation are given below.  After writing the tuple,
	 * pfree() the out-of-line data (not the SortTuple struct!), and increase
	 * state->availMem by the amount of memory space thereby released.
	 */
	void		(*writetup) (RumTuplesortstate *state, int tapenum,
										 SortTuple *stup);

	/*
	 * Function to read a stored tuple from tape back into memory. 'len' is
	 * the already-read length of the stored tuple.  Create a palloc'd copy,
	 * initialize tuple/datum1/isnull1 in the target SortTuple struct, and
	 * decrease state->availMem by the amount of memory space consumed.
	 */
	void		(*readtup) (RumTuplesortstate *state, SortTuple *stup,
										int tapenum, unsigned int len);

	/*
	 * Function to reverse the sort direction from its current state. (We
	 * could dispense with this if we wanted to enforce that all variants
	 * represent the sort key information alike.)
	 */
	void		(*reversedirection) (RumTuplesortstate *state);

	/*
	 * This array holds the tuples now in sort memory.  If we are in state
	 * INITIAL, the tuples are in no particular order; if we are in state
	 * SORTEDINMEM, the tuples are in final sorted order; in states BUILDRUNS
	 * and FINALMERGE, the tuples are organized in "heap" order per Algorithm
	 * H.  (Note that memtupcount only counts the tuples that are part of the
	 * heap --- during merge passes, memtuples[] entries beyond tapeRange are
	 * never in the heap and are used to hold pre-read tuples.)  In state
	 * SORTEDONTAPE, the array is not used.
	 */
	SortTuple  *memtuples;		/* array of SortTuple structs */
	int			memtupcount;	/* number of tuples currently present */
	int			memtupsize;		/* allocated length of memtuples array */
	bool		growmemtuples;	/* memtuples' growth still underway? */

	/* Buffer size to use for reading input tapes, during merge. */
	size_t		read_buffer_size;

	/*
	 * While building initial runs, this is the current output run number
	 * (starting at 0).  Afterwards, it is the number of initial runs we made.
	 */
	int			currentRun;

	/*
	 * Unless otherwise noted, all pointer variables below are pointers to
	 * arrays of length maxTapes, holding per-tape data.
	 */

	/*
	 * These variables are only used during merge passes.  mergeactive[i] is
	 * true if we are reading an input run from (actual) tape number i and
	 * have not yet exhausted that run.  mergenext[i] is the memtuples index
	 * of the next pre-read tuple (next to be loaded into the heap) for tape
	 * i, or 0 if we are out of pre-read tuples.  mergelast[i] similarly
	 * points to the last pre-read tuple from each tape.  mergeavailslots[i]
	 * is the number of unused memtuples[] slots reserved for tape i, and
	 * mergeavailmem[i] is the amount of unused space allocated for tape i.
	 * mergefreelist and mergefirstfree keep track of unused locations in the
	 * memtuples[] array.  The memtuples[].tupindex fields link together
	 * pre-read tuples for each tape as well as recycled locations in
	 * mergefreelist. It is OK to use 0 as a null link in these lists, because
	 * memtuples[0] is part of the merge heap and is never a pre-read tuple.
	 */
	bool	   *mergeactive;	/* active input run source? */
	int		   *mergenext;		/* first preread tuple for each source */
	int		   *mergelast;		/* last preread tuple for each source */
	int		   *mergeavailslots;	/* slots left for prereading each tape */
	long	   *mergeavailmem;	/* availMem for prereading each tape */
	int			mergefreelist;	/* head of freelist of recycled slots */
	int			mergefirstfree; /* first slot never used in this merge */

	/*
	 * Variables for Algorithm D.  Note that destTape is a "logical" tape
	 * number, ie, an index into the tp_xxx[] arrays.  Be careful to keep
	 * "logical" and "actual" tape numbers straight!
	 */
	int			Level;			/* Knuth's l */
	int			destTape;		/* current output tape (Knuth's j, less 1) */
	int		   *tp_fib;			/* Target Fibonacci run counts (A[]) */
	int		   *tp_runs;		/* # of real runs on each tape */
	int		   *tp_dummy;		/* # of dummy runs for each tape (D[]) */
	int		   *tp_tapenum;		/* Actual tape numbers (TAPE[]) */
	int			activeTapes;	/* # of active input tapes in merge pass */

	/*
	 * These variables are used after completion of sorting to keep track of
	 * the next tuple to return.  (In the tape case, the tape's current read
	 * position is also critical state.)
	 */
	int			result_tape;	/* actual tape number of finished output */
	int			current;		/* array index (only used if SORTEDINMEM) */
	bool		eof_reached;	/* reached EOF (needed for cursors) */

	/* markpos_xxx holds marked position for mark and restore */
	long		markpos_block;	/* tape block# (only used if SORTEDONTAPE) */
	int			markpos_offset; /* saved "current", or offset in tape block */
	bool		markpos_eof;	/* saved "eof_reached" */

	/*
	 * These variables are specific to the MinimalTuple case; they are set by
	 * rum_tuplesort_begin_heap and used only by the MinimalTuple routines.
	 */
	TupleDesc	tupDesc;
	SortSupport sortKeys;		/* array of length nKeys */

	/*
	 * This variable is shared by the single-key MinimalTuple case and the
	 * Datum case (which both use qsort_ssup()).  Otherwise it's NULL.
	 */
	SortSupport onlyKey;

	/*
	 * These variables are specific to the CLUSTER case; they are set by
	 * rum_tuplesort_begin_cluster.  Note CLUSTER also uses tupDesc and
	 * indexScanKey.
	 */
	IndexInfo  *indexInfo;		/* info about index being used for reference */
	EState	   *estate;			/* for evaluating index expressions */

	/*
	 * These variables are specific to the IndexTuple case; they are set by
	 * rum_tuplesort_begin_index_xxx and used only by the IndexTuple routines.
	 */
	Relation	heapRel;		/* table the index is being built on */
	Relation	indexRel;		/* index being built */

	/* These are specific to the index_btree subcase: */
	ScanKey		indexScanKey;
	bool		enforceUnique;	/* complain if we find duplicate tuples */

	/* These are specific to the index_hash subcase: */
	uint32		hash_mask;		/* mask for sortable part of hash code */

	/*
	 * These variables are specific to the Datum case; they are set by
	 * rum_tuplesort_begin_datum and used only by the DatumTuple routines.
	 */
	Oid			datumType;
	/* we need typelen and byval in order to know how to copy the Datums. */
	int			datumTypeLen;
	bool		datumTypeByVal;

	bool		reverse;

	/* Do we need ItemPointer comparison in comparetup_rum()? */
	bool		compareItemPointer;

	/* compare_rumitem */
	FmgrInfo	*cmp;

	/*
	 * Resource snapshot for time of sort start.
	 */
#ifdef TRACE_SORT
	PGRUsage	ru_start;
#endif
};

#define COMPARETUP(state,a,b)	((*(state)->comparetup) (a, b, state))
#define COPYTUP(state,stup,tup) ((*(state)->copytup) (state, stup, tup))
#define WRITETUP(state,tape,stup)	((*(state)->writetup) (state, tape, stup))
#define READTUP(state,stup,tape,len) ((*(state)->readtup) (state, stup, tape, len))
#define REVERSEDIRECTION(state) ((*(state)->reversedirection) (state))
#define LACKMEM(state)		((state)->availMem < 0)
#define USEMEM(state,amt)	((state)->availMem -= (amt))
#define FREEMEM(state,amt)	((state)->availMem += (amt))

/* When using this macro, beware of double evaluation of len */
#define LogicalTapeReadExact(tapeset, tapenum, ptr, len) \
	do { \
		if (LogicalTapeRead(tapeset, tapenum, ptr, len) != (size_t) (len)) \
			elog(ERROR, "unexpected end of data"); \
	} while(0)


static RumTuplesortstate *rum_tuplesort_begin_common(int workMem, bool randomAccess);
static void puttuple_common(RumTuplesortstate *state, SortTuple *tuple);
static void inittapes(RumTuplesortstate *state);
static void selectnewtape(RumTuplesortstate *state);
static void mergeruns(RumTuplesortstate *state);
static void mergeonerun(RumTuplesortstate *state);
static void beginmerge(RumTuplesortstate *state);
static void mergepreread(RumTuplesortstate *state);
static void mergeprereadone(RumTuplesortstate *state, int srcTape);
static void dumptuples(RumTuplesortstate *state, bool alltuples);
static void make_bounded_heap(RumTuplesortstate *state);
static void sort_bounded_heap(RumTuplesortstate *state);
static void rum_tuplesort_heap_insert(RumTuplesortstate *state, SortTuple *tuple,
						  int tupleindex, bool checkIndex);
static void rum_tuplesort_heap_siftup(RumTuplesortstate *state, bool checkIndex);
static unsigned int getlen(RumTuplesortstate *state, int tapenum, bool eofOK);
static void markrunend(RumTuplesortstate *state, int tapenum);
static void free_sort_tuple(RumTuplesortstate *state, SortTuple *stup);
static int comparetup_rum(const SortTuple *a, const SortTuple *b,
			   RumTuplesortstate *state);
static void copytup_rum(RumTuplesortstate *state, SortTuple *stup, void *tup);
static void writetup_rum(RumTuplesortstate *state, int tapenum,
			 SortTuple *stup);
static void readtup_rum(RumTuplesortstate *state, SortTuple *stup,
			int tapenum, unsigned int len);
static void reversedirection_rum(RumTuplesortstate *state);
static int comparetup_rumitem(const SortTuple *a, const SortTuple *b,
			   RumTuplesortstate *state);
static void copytup_rumitem(RumTuplesortstate *state, SortTuple *stup, void *tup);
static void writetup_rumitem(RumTuplesortstate *state, int tapenum,
			 SortTuple *stup);
static void readtup_rumitem(RumTuplesortstate *state, SortTuple *stup,
			int tapenum, unsigned int len);

/*
 * Special versions of qsort just for SortTuple objects.  qsort_tuple() sorts
 * any variant of SortTuples, using the appropriate comparetup function.
 * qsort_ssup() is specialized for the case where the comparetup function
 * reduces to ApplySortComparator(), that is single-key MinimalTuple sorts
 * and Datum sorts.
 */
/* #include "qsort_tuple.c" */

static void
swapfunc(SortTuple *a, SortTuple *b, size_t n)
{
	do
	{
		SortTuple	t = *a;

		*a++ = *b;
		*b++ = t;
	} while (--n > 0);
}

#define cmp_ssup(a, b, ssup) \
	ApplySortComparator((a)->datum1, (a)->isnull1, \
						(b)->datum1, (b)->isnull1, ssup)

#define swap(a, b)						\
	do {								\
		SortTuple t = *(a);				\
		*(a) = *(b);					\
		*(b) = t;						\
	} while (0);

#define vecswap(a, b, n) if ((n) > 0) swapfunc(a, b, n)

static SortTuple *
med3_tuple(SortTuple *a, SortTuple *b, SortTuple *c, SortTupleComparator cmp_tuple, RumTuplesortstate *state)
{
	return cmp_tuple(a, b, state) < 0 ?
		(cmp_tuple(b, c, state) < 0 ? b :
		 (cmp_tuple(a, c, state) < 0 ? c : a))
		: (cmp_tuple(b, c, state) > 0 ? b :
		   (cmp_tuple(a, c, state) < 0 ? a : c));
}

static SortTuple *
med3_ssup(SortTuple *a, SortTuple *b, SortTuple *c, SortSupport ssup)
{
	return cmp_ssup(a, b, ssup) < 0 ?
		(cmp_ssup(b, c, ssup) < 0 ? b :
		 (cmp_ssup(a, c, ssup) < 0 ? c : a))
		: (cmp_ssup(b, c, ssup) > 0 ? b :
		   (cmp_ssup(a, c, ssup) < 0 ? a : c));
}

static void
qsort_ssup(SortTuple *a, size_t n, SortSupport ssup)
{
	SortTuple  *pa,
			   *pb,
			   *pc,
			   *pd,
			   *pl,
			   *pm,
			   *pn;
	size_t		d1,
				d2;
	int			r,
				presorted;

loop:
	CHECK_FOR_INTERRUPTS();
	if (n < 7)
	{
		for (pm = a + 1; pm < a + n; pm++)
			for (pl = pm; pl > a && cmp_ssup(pl - 1, pl, ssup) > 0; pl--)
				swap(pl, pl - 1);
		return;
	}
	presorted = 1;
	for (pm = a + 1; pm < a + n; pm++)
	{
		CHECK_FOR_INTERRUPTS();
		if (cmp_ssup(pm - 1, pm, ssup) > 0)
		{
			presorted = 0;
			break;
		}
	}
	if (presorted)
		return;
	pm = a + (n / 2);
	if (n > 7)
	{
		pl = a;
		pn = a + (n - 1);
		if (n > 40)
		{
			size_t		d = (n / 8);

			pl = med3_ssup(pl, pl + d, pl + 2 * d, ssup);
			pm = med3_ssup(pm - d, pm, pm + d, ssup);
			pn = med3_ssup(pn - 2 * d, pn - d, pn, ssup);
		}
		pm = med3_ssup(pl, pm, pn, ssup);
	}
	swap(a, pm);
	pa = pb = a + 1;
	pc = pd = a + (n - 1);
	for (;;)
	{
		while (pb <= pc && (r = cmp_ssup(pb, a, ssup)) <= 0)
		{
			if (r == 0)
			{
				swap(pa, pb);
				pa++;
			}
			pb++;
			CHECK_FOR_INTERRUPTS();
		}
		while (pb <= pc && (r = cmp_ssup(pc, a, ssup)) >= 0)
		{
			if (r == 0)
			{
				swap(pc, pd);
				pd--;
			}
			pc--;
			CHECK_FOR_INTERRUPTS();
		}
		if (pb > pc)
			break;
		swap(pb, pc);
		pb++;
		pc--;
	}
	pn = a + n;
	d1 = Min(pa - a, pb - pa);
	vecswap(a, pb - d1, d1);
	d1 = Min(pd - pc, pn - pd - 1);
	vecswap(pb, pn - d1, d1);
	d1 = pb - pa;
	d2 = pd - pc;
	if (d1 <= d2)
	{
		/* Recurse on left partition, then iterate on right partition */
		if (d1 > 1)
			qsort_ssup(a, d1, ssup);
		if (d2 > 1)
		{
			/* Iterate rather than recurse to save stack space */
			/* qsort_ssup(pn - d2, d2, ssup); */
			a = pn - d2;
			n = d2;
			goto loop;
		}
	}
	else
	{
		/* Recurse on right partition, then iterate on left partition */
		if (d2 > 1)
			qsort_ssup(pn - d2, d2, ssup);
		if (d1 > 1)
		{
			/* Iterate rather than recurse to save stack space */
			/* qsort_ssup(a, d1, ssup); */
			n = d1;
			goto loop;
		}
	}
}

static void
qsort_tuple(SortTuple *a, size_t n, SortTupleComparator cmp_tuple, RumTuplesortstate *state)
{
	SortTuple  *pa,
			   *pb,
			   *pc,
			   *pd,
			   *pl,
			   *pm,
			   *pn;
	size_t		d1,
				d2;
	int			r,
				presorted;

loop:
	CHECK_FOR_INTERRUPTS();
	if (n < 7)
	{
		for (pm = a + 1; pm < a + n; pm++)
			for (pl = pm; pl > a && cmp_tuple(pl - 1, pl, state) > 0; pl--)
				swap(pl, pl - 1);
		return;
	}
	presorted = 1;
	for (pm = a + 1; pm < a + n; pm++)
	{
		CHECK_FOR_INTERRUPTS();
		if (cmp_tuple(pm - 1, pm, state) > 0)
		{
			presorted = 0;
			break;
		}
	}
	if (presorted)
		return;
	pm = a + (n / 2);
	if (n > 7)
	{
		pl = a;
		pn = a + (n - 1);
		if (n > 40)
		{
			size_t		d = (n / 8);

			pl = med3_tuple(pl, pl + d, pl + 2 * d, cmp_tuple, state);
			pm = med3_tuple(pm - d, pm, pm + d, cmp_tuple, state);
			pn = med3_tuple(pn - 2 * d, pn - d, pn, cmp_tuple, state);
		}
		pm = med3_tuple(pl, pm, pn, cmp_tuple, state);
	}
	swap(a, pm);
	pa = pb = a + 1;
	pc = pd = a + (n - 1);
	for (;;)
	{
		while (pb <= pc && (r = cmp_tuple(pb, a, state)) <= 0)
		{
			if (r == 0)
			{
				swap(pa, pb);
				pa++;
			}
			pb++;
			CHECK_FOR_INTERRUPTS();
		}
		while (pb <= pc && (r = cmp_tuple(pc, a, state)) >= 0)
		{
			if (r == 0)
			{
				swap(pc, pd);
				pd--;
			}
			pc--;
			CHECK_FOR_INTERRUPTS();
		}
		if (pb > pc)
			break;
		swap(pb, pc);
		pb++;
		pc--;
	}
	pn = a + n;
	d1 = Min(pa - a, pb - pa);
	vecswap(a, pb - d1, d1);
	d1 = Min(pd - pc, pn - pd - 1);
	vecswap(pb, pn - d1, d1);
	d1 = pb - pa;
	d2 = pd - pc;
	if (d1 <= d2)
	{
		/* Recurse on left partition, then iterate on right partition */
		if (d1 > 1)
			qsort_tuple(a, d1, cmp_tuple, state);
		if (d2 > 1)
		{
			/* Iterate rather than recurse to save stack space */
			/* qsort_tuple(pn - d2, d2, cmp_tuple, state); */
			a = pn - d2;
			n = d2;
			goto loop;
		}
	}
	else
	{
		/* Recurse on right partition, then iterate on left partition */
		if (d2 > 1)
			qsort_tuple(pn - d2, d2, cmp_tuple, state);
		if (d1 > 1)
		{
			/* Iterate rather than recurse to save stack space */
			/* qsort_tuple(a, d1, cmp_tuple, state); */
			n = d1;
			goto loop;
		}
	}
}

/*
 *		rum_tuplesort_begin_xxx
 *
 * Initialize for a tuple sort operation.
 *
 * After calling rum_tuplesort_begin, the caller should call rum_tuplesort_putXXX
 * zero or more times, then call rum_tuplesort_performsort when all the tuples
 * have been supplied.  After performsort, retrieve the tuples in sorted
 * order by calling rum_tuplesort_getXXX until it returns false/NULL.  (If random
 * access was requested, rescan, markpos, and restorepos can also be called.)
 * Call rum_tuplesort_end to terminate the operation and release memory/disk space.
 *
 * Each variant of rum_tuplesort_begin has a workMem parameter specifying the
 * maximum number of kilobytes of RAM to use before spilling data to disk.
 * (The normal value of this parameter is work_mem, but some callers use
 * other values.)  Each variant also has a randomAccess parameter specifying
 * whether the caller needs non-sequential access to the sort result.
 */

static RumTuplesortstate *
rum_tuplesort_begin_common(int workMem, bool randomAccess)
{
	RumTuplesortstate *state;
	MemoryContext sortcontext;
	MemoryContext oldcontext;

	/*
	 * Create a working memory context for this sort operation. All data
	 * needed by the sort will live inside this context.
	 */
	sortcontext = RumContextCreate(CurrentMemoryContext, "TupleSort");

	/*
	 * Make the Tuplesortstate within the per-sort context.  This way, we
	 * don't need a separate pfree() operation for it at shutdown.
	 */
	oldcontext = MemoryContextSwitchTo(sortcontext);

	state = (RumTuplesortstate *) palloc0(sizeof(RumTuplesortstate));

#ifdef TRACE_SORT
	if (trace_sort)
		pg_rusage_init(&state->ru_start);
#endif

	state->status = TSS_INITIAL;
	state->randomAccess = randomAccess;
	state->bounded = false;
	state->boundUsed = false;
	state->allowedMem = workMem * 1024L;
	state->availMem = state->allowedMem;
	state->sortcontext = sortcontext;
	state->tapeset = NULL;

	state->memtupcount = 0;

	/*
	 * Initial size of array must be more than ALLOCSET_SEPARATE_THRESHOLD;
	 * see comments in grow_memtuples().
	 */
	state->memtupsize = Max(1024,
						ALLOCSET_SEPARATE_THRESHOLD / sizeof(SortTuple) + 1);

	state->growmemtuples = true;
	state->memtuples = (SortTuple *) palloc(state->memtupsize * sizeof(SortTuple));

	USEMEM(state, GetMemoryChunkSpace(state->memtuples));

	/* workMem must be large enough for the minimal memtuples array */
	if (LACKMEM(state))
		elog(ERROR, "insufficient memory allowed for sort");

	state->currentRun = 0;

	/*
	 * maxTapes, tapeRange, and Algorithm D variables will be initialized by
	 * inittapes(), if needed
	 */

	state->result_tape = -1;	/* flag that result tape has not been formed */

	MemoryContextSwitchTo(oldcontext);

	return state;
}

/*
 * Get sort state memory context.  Currently it is used only to allocate
 * RumSortItem.
 */
MemoryContext
rum_tuplesort_get_memorycontext(RumTuplesortstate *state)
{
	return state->sortcontext;
}

RumTuplesortstate *
rum_tuplesort_begin_rum(int workMem, int nKeys, bool randomAccess,
						bool compareItemPointer)
{
	RumTuplesortstate *state = rum_tuplesort_begin_common(workMem, randomAccess);
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(state->sortcontext);

#ifdef TRACE_SORT
	if (trace_sort)
		elog(LOG,
			 "begin rum sort: nKeys = %d, workMem = %d, randomAccess = %c",
			 nKeys, workMem, randomAccess ? 't' : 'f');
#endif

	state->nKeys = nKeys;

	state->comparetup = comparetup_rum;
	state->copytup = copytup_rum;
	state->writetup = writetup_rum;
	state->readtup = readtup_rum;
	state->reversedirection = reversedirection_rum;
	state->reverse = false;
	state->compareItemPointer = compareItemPointer;

	MemoryContextSwitchTo(oldcontext);

	return state;
}

RumTuplesortstate *
rum_tuplesort_begin_rumitem(int workMem, FmgrInfo *cmp)
{
	RumTuplesortstate *state = rum_tuplesort_begin_common(workMem, false);
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(state->sortcontext);

#ifdef TRACE_SORT
	if (trace_sort)
		elog(LOG,
			 "begin rumitem sort: workMem = %d", workMem);
#endif

	state->cmp = cmp;
	state->comparetup = comparetup_rumitem;
	state->copytup = copytup_rumitem;
	state->writetup = writetup_rumitem;
	state->readtup = readtup_rumitem;
	state->reversedirection = reversedirection_rum;
	state->reverse = false;
	state->compareItemPointer = false;

	MemoryContextSwitchTo(oldcontext);

	return state;
}

/*
 * rum_tuplesort_end
 *
 *	Release resources and clean up.
 *
 * NOTE: after calling this, any pointers returned by rum_tuplesort_getXXX are
 * pointing to garbage.  Be careful not to attempt to use or free such
 * pointers afterwards!
 */
void
rum_tuplesort_end(RumTuplesortstate *state)
{
	/* context swap probably not needed, but let's be safe */
	MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);

#ifdef TRACE_SORT
	long		spaceUsed;

	if (state->tapeset)
		spaceUsed = LogicalTapeSetBlocks(state->tapeset);
	else
		spaceUsed = (state->allowedMem - state->availMem + 1023) / 1024;
#endif

	/*
	 * Delete temporary "tape" files, if any.
	 *
	 * Note: want to include this in reported total cost of sort, hence need
	 * for two #ifdef TRACE_SORT sections.
	 */
	if (state->tapeset)
		LogicalTapeSetClose(state->tapeset);

#ifdef TRACE_SORT
	if (trace_sort)
	{
		if (state->tapeset)
			elog(LOG, "external sort ended, %ld disk blocks used: %s",
				 spaceUsed, pg_rusage_show(&state->ru_start));
		else
			elog(LOG, "internal sort ended, %ld KB used: %s",
				 spaceUsed, pg_rusage_show(&state->ru_start));
	}
#endif

	/* Free any execution state created for CLUSTER case */
	if (state->estate != NULL)
	{
		ExprContext *econtext = GetPerTupleExprContext(state->estate);

		ExecDropSingleTupleTableSlot(econtext->ecxt_scantuple);
		FreeExecutorState(state->estate);
	}

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Free the per-sort memory context, thereby releasing all working memory,
	 * including the Tuplesortstate struct itself.
	 */
	MemoryContextDelete(state->sortcontext);
}

/*
 * Grow the memtuples[] array, if possible within our memory constraint.
 * Return true if we were able to enlarge the array, false if not.
 *
 * Normally, at each increment we double the size of the array.  When we no
 * longer have enough memory to do that, we attempt one last, smaller increase
 * (and then clear the growmemtuples flag so we don't try any more).  That
 * allows us to use allowedMem as fully as possible; sticking to the pure
 * doubling rule could result in almost half of allowedMem going unused.
 * Because availMem moves around with tuple addition/removal, we need some
 * rule to prevent making repeated small increases in memtupsize, which would
 * just be useless thrashing.  The growmemtuples flag accomplishes that and
 * also prevents useless recalculations in this function.
 */
static bool
grow_memtuples(RumTuplesortstate *state)
{
	int			newmemtupsize;
	int			memtupsize = state->memtupsize;
	long		memNowUsed = state->allowedMem - state->availMem;

	/* Forget it if we've already maxed out memtuples, per comment above */
	if (!state->growmemtuples)
		return false;

	/* Select new value of memtupsize */
	if (memNowUsed <= state->availMem)
	{
		/*
		 * It is surely safe to double memtupsize if we've used no more than
		 * half of allowedMem.
		 *
		 * Note: it might seem that we need to worry about memtupsize * 2
		 * overflowing an int, but the MaxAllocSize clamp applied below
		 * ensures the existing memtupsize can't be large enough for that.
		 */
		newmemtupsize = memtupsize * 2;
	}
	else
	{
		/*
		 * This will be the last increment of memtupsize.  Abandon doubling
		 * strategy and instead increase as much as we safely can.
		 *
		 * To stay within allowedMem, we can't increase memtupsize by more
		 * than availMem / sizeof(SortTuple) elements.  In practice, we want
		 * to increase it by considerably less, because we need to leave some
		 * space for the tuples to which the new array slots will refer.  We
		 * assume the new tuples will be about the same size as the tuples
		 * we've already seen, and thus we can extrapolate from the space
		 * consumption so far to estimate an appropriate new size for the
		 * memtuples array.  The optimal value might be higher or lower than
		 * this estimate, but it's hard to know that in advance.
		 *
		 * This calculation is safe against enlarging the array so much that
		 * LACKMEM becomes true, because the memory currently used includes
		 * the present array; thus, there would be enough allowedMem for the
		 * new array elements even if no other memory were currently used.
		 *
		 * We do the arithmetic in float8, because otherwise the product of
		 * memtupsize and allowedMem could overflow.  (A little algebra shows
		 * that grow_ratio must be less than 2 here, so we are not risking
		 * integer overflow this way.)	Any inaccuracy in the result should be
		 * insignificant; but even if we computed a completely insane result,
		 * the checks below will prevent anything really bad from happening.
		 */
		double		grow_ratio;

		grow_ratio = (double) state->allowedMem / (double) memNowUsed;
		newmemtupsize = (int) (memtupsize * grow_ratio);

		/* We won't make any further enlargement attempts */
		state->growmemtuples = false;
	}

	/* Must enlarge array by at least one element, else report failure */
	if (newmemtupsize <= memtupsize)
		goto noalloc;

	/*
	 * On a 64-bit machine, allowedMem could be more than MaxAllocSize.  Clamp
	 * to ensure our request won't be rejected by palloc.
	 */
	if ((Size) newmemtupsize >= MaxAllocSize / sizeof(SortTuple))
	{
		newmemtupsize = (int) (MaxAllocSize / sizeof(SortTuple));
		state->growmemtuples = false;	/* can't grow any more */
	}

	/*
	 * We need to be sure that we do not cause LACKMEM to become true, else
	 * the space management algorithm will go nuts.  The code above should
	 * never generate a dangerous request, but to be safe, check explicitly
	 * that the array growth fits within availMem.  (We could still cause
	 * LACKMEM if the memory chunk overhead associated with the memtuples
	 * array were to increase.  That shouldn't happen because we chose the
	 * initial array size large enough to ensure that palloc will be treating
	 * both old and new arrays as separate chunks.  But we'll check LACKMEM
	 * explicitly below just in case.)
	 */
	if (state->availMem < (long) ((newmemtupsize - memtupsize) * sizeof(SortTuple)))
		goto noalloc;

	/* OK, do it */
	FREEMEM(state, GetMemoryChunkSpace(state->memtuples));
	state->memtupsize = newmemtupsize;
	state->memtuples = (SortTuple *)
		repalloc(state->memtuples,
				 state->memtupsize * sizeof(SortTuple));
	USEMEM(state, GetMemoryChunkSpace(state->memtuples));
	if (LACKMEM(state))
		elog(ERROR, "unexpected out-of-memory situation in tuplesort");
	return true;

noalloc:
	/* If for any reason we didn't realloc, shut off future attempts */
	state->growmemtuples = false;
	return false;
}

void
rum_tuplesort_putrum(RumTuplesortstate *state, RumSortItem * item)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);
	SortTuple	stup;

	/*
	 * Copy the given tuple into memory we control, and decrease availMem.
	 * Then call the common code.
	 */
	COPYTUP(state, &stup, (void *) item);

	puttuple_common(state, &stup);

	MemoryContextSwitchTo(oldcontext);
}

void
rum_tuplesort_putrumitem(RumTuplesortstate *state, RumScanItem * item)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);
	SortTuple	stup;

	/*
	 * Copy the given tuple into memory we control, and decrease availMem.
	 * Then call the common code.
	 */
	COPYTUP(state, &stup, (void *) item);

	puttuple_common(state, &stup);

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Shared code for tuple and datum cases.
 */
static void
puttuple_common(RumTuplesortstate *state, SortTuple *tuple)
{
	switch (state->status)
	{
		case TSS_INITIAL:

			/*
			 * Save the tuple into the unsorted array.  First, grow the array
			 * as needed.  Note that we try to grow the array when there is
			 * still one free slot remaining --- if we fail, there'll still be
			 * room to store the incoming tuple, and then we'll switch to
			 * tape-based operation.
			 */
			if (state->memtupcount >= state->memtupsize - 1)
			{
				(void) grow_memtuples(state);
				Assert(state->memtupcount < state->memtupsize);
			}
			state->memtuples[state->memtupcount++] = *tuple;

			/*
			 * Check if it's time to switch over to a bounded heapsort. We do
			 * so if the input tuple count exceeds twice the desired tuple
			 * count (this is a heuristic for where heapsort becomes cheaper
			 * than a quicksort), or if we've just filled workMem and have
			 * enough tuples to meet the bound.
			 *
			 * Note that once we enter TSS_BOUNDED state we will always try to
			 * complete the sort that way.  In the worst case, if later input
			 * tuples are larger than earlier ones, this might cause us to
			 * exceed workMem significantly.
			 */
			if (state->bounded &&
				(state->memtupcount > state->bound * 2 ||
				 (state->memtupcount > state->bound && LACKMEM(state))))
			{
#ifdef TRACE_SORT
				if (trace_sort)
					elog(LOG, "switching to bounded heapsort at %d tuples: %s",
						 state->memtupcount,
						 pg_rusage_show(&state->ru_start));
#endif
				make_bounded_heap(state);
				return;
			}

			/*
			 * Done if we still fit in available memory and have array slots.
			 */
			if (state->memtupcount < state->memtupsize && !LACKMEM(state))
				return;

			/*
			 * Nope; time to switch to tape-based operation.
			 */
			inittapes(state);

			/*
			 * Dump tuples until we are back under the limit.
			 */
			dumptuples(state, false);
			break;

		case TSS_BOUNDED:

			/*
			 * We don't want to grow the array here, so check whether the new
			 * tuple can be discarded before putting it in.  This should be a
			 * good speed optimization, too, since when there are many more
			 * input tuples than the bound, most input tuples can be discarded
			 * with just this one comparison.  Note that because we currently
			 * have the sort direction reversed, we must check for <= not >=.
			 */
			if (COMPARETUP(state, tuple, &state->memtuples[0]) <= 0)
			{
				/* new tuple <= top of the heap, so we can discard it */
				free_sort_tuple(state, tuple);
				CHECK_FOR_INTERRUPTS();
			}
			else
			{
				/* discard top of heap, sift up, insert new tuple */
				free_sort_tuple(state, &state->memtuples[0]);
				rum_tuplesort_heap_siftup(state, false);
				rum_tuplesort_heap_insert(state, tuple, 0, false);
			}
			break;

		case TSS_BUILDRUNS:

			/*
			 * Insert the tuple into the heap, with run number currentRun if
			 * it can go into the current run, else run number currentRun+1.
			 * The tuple can go into the current run if it is >= the first
			 * not-yet-output tuple.  (Actually, it could go into the current
			 * run if it is >= the most recently output tuple ... but that
			 * would require keeping around the tuple we last output, and it's
			 * simplest to let writetup free each tuple as soon as it's
			 * written.)
			 *
			 * Note there will always be at least one tuple in the heap at
			 * this point; see dumptuples.
			 */
			Assert(state->memtupcount > 0);
			if (COMPARETUP(state, tuple, &state->memtuples[0]) >= 0)
				rum_tuplesort_heap_insert(state, tuple, state->currentRun, true);
			else
				rum_tuplesort_heap_insert(state, tuple, state->currentRun + 1, true);

			/*
			 * If we are over the memory limit, dump tuples till we're under.
			 */
			dumptuples(state, false);
			break;

		default:
			elog(ERROR, "invalid tuplesort state");
			break;
	}
}

/*
 * All tuples have been provided; finish the sort.
 */
void
rum_tuplesort_performsort(RumTuplesortstate *state)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);

#ifdef TRACE_SORT
	if (trace_sort)
		elog(LOG, "performsort starting: %s",
			 pg_rusage_show(&state->ru_start));
#endif

	switch (state->status)
	{
		case TSS_INITIAL:

			/*
			 * We were able to accumulate all the tuples within the allowed
			 * amount of memory.  Just qsort 'em and we're done.
			 */
			if (state->memtupcount > 1)
			{
				/* Can we use the single-key sort function? */
				if (state->onlyKey != NULL)
					qsort_ssup(state->memtuples, state->memtupcount,
							   state->onlyKey);
				else
					qsort_tuple(state->memtuples,
								state->memtupcount,
								state->comparetup,
								state);
			}
			state->current = 0;
			state->eof_reached = false;
			state->markpos_offset = 0;
			state->markpos_eof = false;
			state->status = TSS_SORTEDINMEM;
			break;

		case TSS_BOUNDED:

			/*
			 * We were able to accumulate all the tuples required for output
			 * in memory, using a heap to eliminate excess tuples.  Now we
			 * have to transform the heap to a properly-sorted array.
			 */
			sort_bounded_heap(state);
			state->current = 0;
			state->eof_reached = false;
			state->markpos_offset = 0;
			state->markpos_eof = false;
			state->status = TSS_SORTEDINMEM;
			break;

		case TSS_BUILDRUNS:

			/*
			 * Finish tape-based sort.  First, flush all tuples remaining in
			 * memory out to tape; then merge until we have a single remaining
			 * run (or, if !randomAccess, one run per tape). Note that
			 * mergeruns sets the correct state->status.
			 */
			dumptuples(state, true);
			mergeruns(state);
			state->eof_reached = false;
			state->markpos_block = 0L;
			state->markpos_offset = 0;
			state->markpos_eof = false;
			break;

		default:
			elog(ERROR, "invalid tuplesort state");
			break;
	}

#ifdef TRACE_SORT
	if (trace_sort)
	{
		if (state->status == TSS_FINALMERGE)
			elog(LOG, "performsort done (except %d-way final merge): %s",
				 state->activeTapes,
				 pg_rusage_show(&state->ru_start));
		else
			elog(LOG, "performsort done: %s",
				 pg_rusage_show(&state->ru_start));
	}
#endif

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Internal routine to fetch the next tuple in either forward or back
 * direction into *stup.  Returns false if no more tuples.
 * If *should_free is set, the caller must pfree stup.tuple when done with it.
 */
static bool
rum_tuplesort_gettuple_common(RumTuplesortstate *state, bool forward,
							  SortTuple *stup, bool *should_free)
{
	unsigned int tuplen;

	switch (state->status)
	{
		case TSS_SORTEDINMEM:
			Assert(forward || state->randomAccess);
			*should_free = false;
			if (forward)
			{
				if (state->current < state->memtupcount)
				{
					*stup = state->memtuples[state->current++];
					return true;
				}
				state->eof_reached = true;

				/*
				 * Complain if caller tries to retrieve more tuples than
				 * originally asked for in a bounded sort.  This is because
				 * returning EOF here might be the wrong thing.
				 */
				if (state->bounded && state->current >= state->bound)
					elog(ERROR, "retrieved too many tuples in a bounded sort");

				return false;
			}
			else
			{
				if (state->current <= 0)
					return false;

				/*
				 * if all tuples are fetched already then we return last
				 * tuple, else - tuple before last returned.
				 */
				if (state->eof_reached)
					state->eof_reached = false;
				else
				{
					state->current--;	/* last returned tuple */
					if (state->current <= 0)
						return false;
				}
				*stup = state->memtuples[state->current - 1];
				return true;
			}
			break;

		case TSS_SORTEDONTAPE:
			Assert(forward || state->randomAccess);
			*should_free = true;
			if (forward)
			{
				if (state->eof_reached)
					return false;
				if ((tuplen = getlen(state, state->result_tape, true)) != 0)
				{
					READTUP(state, stup, state->result_tape, tuplen);
					return true;
				}
				else
				{
					state->eof_reached = true;
					return false;
				}
			}

			/*
			 * Backward.
			 *
			 * if all tuples are fetched already then we return last tuple,
			 * else - tuple before last returned.
			 */
			if (state->eof_reached)
			{
				/*
				 * Seek position is pointing just past the zero tuplen at the
				 * end of file; back up to fetch last tuple's ending length
				 * word.  If seek fails we must have a completely empty file.
				 */
				if (!LogicalTapeBackspace(state->tapeset,
										  state->result_tape,
										  2 * sizeof(unsigned int)))
					return false;
				state->eof_reached = false;
			}
			else
			{
				/*
				 * Back up and fetch previously-returned tuple's ending length
				 * word.  If seek fails, assume we are at start of file.
				 */
				if (!LogicalTapeBackspace(state->tapeset,
										  state->result_tape,
										  sizeof(unsigned int)))
					return false;
				tuplen = getlen(state, state->result_tape, false);

				/*
				 * Back up to get ending length word of tuple before it.
				 */
				if (!LogicalTapeBackspace(state->tapeset,
										  state->result_tape,
										  tuplen + 2 * sizeof(unsigned int)))
				{
					/*
					 * If that fails, presumably the prev tuple is the first
					 * in the file.  Back up so that it becomes next to read
					 * in forward direction (not obviously right, but that is
					 * what in-memory case does).
					 */
					if (!LogicalTapeBackspace(state->tapeset,
											  state->result_tape,
											  tuplen + sizeof(unsigned int)))
						elog(ERROR, "bogus tuple length in backward scan");
					return false;
				}
			}

			tuplen = getlen(state, state->result_tape, false);

			/*
			 * Now we have the length of the prior tuple, back up and read it.
			 * Note: READTUP expects we are positioned after the initial
			 * length word of the tuple, so back up to that point.
			 */
			if (!LogicalTapeBackspace(state->tapeset,
									  state->result_tape,
									  tuplen))
				elog(ERROR, "bogus tuple length in backward scan");
			READTUP(state, stup, state->result_tape, tuplen);
			return true;

		case TSS_FINALMERGE:
			Assert(forward);
			*should_free = true;

			/*
			 * This code should match the inner loop of mergeonerun().
			 */
			if (state->memtupcount > 0)
			{
				int			srcTape = state->memtuples[0].tupindex;
				Size		tuplen;
				int			tupIndex;
				SortTuple  *newtup;

				*stup = state->memtuples[0];
				/* returned tuple is no longer counted in our memory space */
				if (stup->tuple)
				{
					tuplen = GetMemoryChunkSpace(stup->tuple);
					state->availMem += tuplen;
					state->mergeavailmem[srcTape] += tuplen;
				}
				rum_tuplesort_heap_siftup(state, false);
				if ((tupIndex = state->mergenext[srcTape]) == 0)
				{
					/*
					 * out of preloaded data on this tape, try to read more
					 *
					 * Unlike mergeonerun(), we only preload from the single
					 * tape that's run dry.  See mergepreread() comments.
					 */
					mergeprereadone(state, srcTape);

					/*
					 * if still no data, we've reached end of run on this tape
					 */
					if ((tupIndex = state->mergenext[srcTape]) == 0)
						return true;
				}
				/* pull next preread tuple from list, insert in heap */
				newtup = &state->memtuples[tupIndex];
				state->mergenext[srcTape] = newtup->tupindex;
				if (state->mergenext[srcTape] == 0)
					state->mergelast[srcTape] = 0;
				rum_tuplesort_heap_insert(state, newtup, srcTape, false);
				/* put the now-unused memtuples entry on the freelist */
				newtup->tupindex = state->mergefreelist;
				state->mergefreelist = tupIndex;
				state->mergeavailslots[srcTape]++;
				return true;
			}
			return false;

		default:
			elog(ERROR, "invalid tuplesort state");
			return false;		/* keep compiler quiet */
	}
}

RumSortItem *
rum_tuplesort_getrum(RumTuplesortstate *state, bool forward, bool *should_free)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);
	SortTuple	stup;

	if (!rum_tuplesort_gettuple_common(state, forward, &stup, should_free))
		stup.tuple = NULL;

	MemoryContextSwitchTo(oldcontext);

	return (RumSortItem *) stup.tuple;
}

RumScanItem *
rum_tuplesort_getrumitem(RumTuplesortstate *state, bool forward, bool *should_free)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);
	SortTuple	stup;

	if (!rum_tuplesort_gettuple_common(state, forward, &stup, should_free))
		stup.tuple = NULL;

	MemoryContextSwitchTo(oldcontext);

	return (RumScanItem *) stup.tuple;
}

/*
 * rum_tuplesort_merge_order - report merge order we'll use for given memory
 * (note: "merge order" just means the number of input tapes in the merge).
 *
 * This is exported for use by the planner.  allowedMem is in bytes.
 */
int
rum_tuplesort_merge_order(long allowedMem)
{
	int			mOrder;

	/*
	 * We need one tape for each merge input, plus another one for the output,
	 * and each of these tapes needs buffer space.  In addition we want
	 * MERGE_BUFFER_SIZE workspace per input tape (but the output tape doesn't
	 * count).
	 *
	 * Note: you might be thinking we need to account for the memtuples[]
	 * array in this calculation, but we effectively treat that as part of the
	 * MERGE_BUFFER_SIZE workspace.
	 */
	mOrder = (allowedMem - TAPE_BUFFER_OVERHEAD) /
		(MERGE_BUFFER_SIZE + TAPE_BUFFER_OVERHEAD);

	/* Even in minimum memory, use at least a MINORDER merge */
	mOrder = Max(mOrder, MINORDER);

	return mOrder;
}

/*
 * inittapes - initialize for tape sorting.
 *
 * This is called only if we have found we don't have room to sort in memory.
 */
static void
inittapes(RumTuplesortstate *state)
{
	int			maxTapes,
				ntuples,
				j;
	long		tapeSpace;

	/* Compute number of tapes to use: merge order plus 1 */
	maxTapes = rum_tuplesort_merge_order(state->allowedMem) + 1;

	/*
	 * We must have at least 2*maxTapes slots in the memtuples[] array, else
	 * we'd not have room for merge heap plus preread.  It seems unlikely that
	 * this case would ever occur, but be safe.
	 */
	maxTapes = Min(maxTapes, state->memtupsize / 2);

	state->maxTapes = maxTapes;
	state->tapeRange = maxTapes - 1;

#ifdef TRACE_SORT
	if (trace_sort)
		elog(LOG, "switching to external sort with %d tapes: %s",
			 maxTapes, pg_rusage_show(&state->ru_start));
#endif

	/*
	 * Decrease availMem to reflect the space needed for tape buffers; but
	 * don't decrease it to the point that we have no room for tuples. (That
	 * case is only likely to occur if sorting pass-by-value Datums; in all
	 * other scenarios the memtuples[] array is unlikely to occupy more than
	 * half of allowedMem.  In the pass-by-value case it's not important to
	 * account for tuple space, so we don't care if LACKMEM becomes
	 * inaccurate.)
	 */
	tapeSpace = (long) maxTapes *TAPE_BUFFER_OVERHEAD;

	if (tapeSpace + GetMemoryChunkSpace(state->memtuples) < state->allowedMem)
		USEMEM(state, tapeSpace);

	/*
	 * Make sure that the temp file(s) underlying the tape set are created in
	 * suitable temp tablespaces.
	 */
	PrepareTempTablespaces();

	/*
	 * Create the tape set and allocate the per-tape data arrays.
	 */
	state->tapeset = LogicalTapeSetCreate(maxTapes);

	state->mergeactive = (bool *) palloc0(maxTapes * sizeof(bool));
	state->mergenext = (int *) palloc0(maxTapes * sizeof(int));
	state->mergelast = (int *) palloc0(maxTapes * sizeof(int));
	state->mergeavailslots = (int *) palloc0(maxTapes * sizeof(int));
	state->mergeavailmem = (long *) palloc0(maxTapes * sizeof(long));
	state->tp_fib = (int *) palloc0(maxTapes * sizeof(int));
	state->tp_runs = (int *) palloc0(maxTapes * sizeof(int));
	state->tp_dummy = (int *) palloc0(maxTapes * sizeof(int));
	state->tp_tapenum = (int *) palloc0(maxTapes * sizeof(int));

	/*
	 * Convert the unsorted contents of memtuples[] into a heap. Each tuple is
	 * marked as belonging to run number zero.
	 *
	 * NOTE: we pass false for checkIndex since there's no point in comparing
	 * indexes in this step, even though we do intend the indexes to be part
	 * of the sort key...
	 */
	ntuples = state->memtupcount;
	state->memtupcount = 0;		/* make the heap empty */
	for (j = 0; j < ntuples; j++)
	{
		/* Must copy source tuple to avoid possible overwrite */
		SortTuple	stup = state->memtuples[j];

		rum_tuplesort_heap_insert(state, &stup, 0, false);
	}
	Assert(state->memtupcount == ntuples);

	state->currentRun = 0;

	/*
	 * Initialize variables of Algorithm D (step D1).
	 */
	for (j = 0; j < maxTapes; j++)
	{
		state->tp_fib[j] = 1;
		state->tp_runs[j] = 0;
		state->tp_dummy[j] = 1;
		state->tp_tapenum[j] = j;
	}
	state->tp_fib[state->tapeRange] = 0;
	state->tp_dummy[state->tapeRange] = 0;

	state->Level = 1;
	state->destTape = 0;

	state->status = TSS_BUILDRUNS;
}

/*
 * selectnewtape -- select new tape for new initial run.
 *
 * This is called after finishing a run when we know another run
 * must be started.  This implements steps D3, D4 of Algorithm D.
 */
static void
selectnewtape(RumTuplesortstate *state)
{
	int			j;
	int			a;

	/* Step D3: advance j (destTape) */
	if (state->tp_dummy[state->destTape] < state->tp_dummy[state->destTape + 1])
	{
		state->destTape++;
		return;
	}
	if (state->tp_dummy[state->destTape] != 0)
	{
		state->destTape = 0;
		return;
	}

	/* Step D4: increase level */
	state->Level++;
	a = state->tp_fib[0];
	for (j = 0; j < state->tapeRange; j++)
	{
		state->tp_dummy[j] = a + state->tp_fib[j + 1] - state->tp_fib[j];
		state->tp_fib[j] = a + state->tp_fib[j + 1];
	}
	state->destTape = 0;
}

/*
 * mergeruns -- merge all the completed initial runs.
 *
 * This implements steps D5, D6 of Algorithm D.  All input data has
 * already been written to initial runs on tape (see dumptuples).
 */
static void
mergeruns(RumTuplesortstate *state)
{
	int			tapenum,
				svTape,
				svRuns,
				svDummy;
	int			numTapes;
	int			numInputTapes;

	Assert(state->status == TSS_BUILDRUNS);
	Assert(state->memtupcount == 0);

	/*
	 * If we produced only one initial run (quite likely if the total data
	 * volume is between 1X and 2X workMem), we can just use that tape as the
	 * finished output, rather than doing a useless merge.  (This obvious
	 * optimization is not in Knuth's algorithm.)
	 */
	if (state->currentRun == 1)
	{
		state->result_tape = state->tp_tapenum[state->destTape];
		/* must freeze and rewind the finished output tape */
		LogicalTapeFreeze(state->tapeset, state->result_tape);
		state->status = TSS_SORTEDONTAPE;
		return;
	}

	/*
	 * If we had fewer runs than tapes, refund the memory that we imagined we
	 * would need for the tape buffers of the unused tapes.
	 *
	 * numTapes and numInputTapes reflect the actual number of tapes we will
	 * use.  Note that the output tape's tape number is maxTapes - 1, so the
	 * tape numbers of the used tapes are not consecutive, and you cannot just
	 * loop from 0 to numTapes to visit all used tapes!
	 */
	if (state->Level == 1)
	{
		numInputTapes = state->currentRun;
		numTapes = numInputTapes + 1;
		FREEMEM(state, (state->maxTapes - numTapes) * TAPE_BUFFER_OVERHEAD);
	}
	else
	{
		numInputTapes = state->tapeRange;
	}

	state->read_buffer_size = Max(state->availMem / numInputTapes, 0);
	USEMEM(state, state->read_buffer_size * numInputTapes);

	/* End of step D2: rewind all output tapes to prepare for merging */
	for (tapenum = 0; tapenum < state->tapeRange; tapenum++)
		LogicalTapeRewindForRead(state->tapeset, tapenum, state->read_buffer_size);

	for (;;)
	{
		/*
		 * At this point we know that tape[T] is empty.  If there's just one
		 * (real or dummy) run left on each input tape, then only one merge
		 * pass remains.  If we don't have to produce a materialized sorted
		 * tape, we can stop at this point and do the final merge on-the-fly.
		 */
		if (!state->randomAccess)
		{
			bool		allOneRun = true;

			Assert(state->tp_runs[state->tapeRange] == 0);
			for (tapenum = 0; tapenum < state->tapeRange; tapenum++)
			{
				if (state->tp_runs[tapenum] + state->tp_dummy[tapenum] != 1)
				{
					allOneRun = false;
					break;
				}
			}
			if (allOneRun)
			{
				/* Tell logtape.c we won't be writing anymore */
				LogicalTapeSetForgetFreeSpace(state->tapeset);
				/* Initialize for the final merge pass */
				beginmerge(state);
				state->status = TSS_FINALMERGE;
				return;
			}
		}

		/* Step D5: merge runs onto tape[T] until tape[P] is empty */
		while (state->tp_runs[state->tapeRange - 1] ||
			   state->tp_dummy[state->tapeRange - 1])
		{
			bool		allDummy = true;

			for (tapenum = 0; tapenum < state->tapeRange; tapenum++)
			{
				if (state->tp_dummy[tapenum] == 0)
				{
					allDummy = false;
					break;
				}
			}

			if (allDummy)
			{
				state->tp_dummy[state->tapeRange]++;
				for (tapenum = 0; tapenum < state->tapeRange; tapenum++)
					state->tp_dummy[tapenum]--;
			}
			else
				mergeonerun(state);
		}

		/* Step D6: decrease level */
		if (--state->Level == 0)
			break;
		/* rewind output tape T to use as new input */
		LogicalTapeRewindForRead(state->tapeset, state->tp_tapenum[state->tapeRange],
								 state->read_buffer_size);
		/* rewind used-up input tape P, and prepare it for write pass */
		LogicalTapeRewindForWrite(state->tapeset, state->tp_tapenum[state->tapeRange - 1]);
		state->tp_runs[state->tapeRange - 1] = 0;

		/*
		 * reassign tape units per step D6; note we no longer care about A[]
		 */
		svTape = state->tp_tapenum[state->tapeRange];
		svDummy = state->tp_dummy[state->tapeRange];
		svRuns = state->tp_runs[state->tapeRange];
		for (tapenum = state->tapeRange; tapenum > 0; tapenum--)
		{
			state->tp_tapenum[tapenum] = state->tp_tapenum[tapenum - 1];
			state->tp_dummy[tapenum] = state->tp_dummy[tapenum - 1];
			state->tp_runs[tapenum] = state->tp_runs[tapenum - 1];
		}
		state->tp_tapenum[0] = svTape;
		state->tp_dummy[0] = svDummy;
		state->tp_runs[0] = svRuns;
	}

	/*
	 * Done.  Knuth says that the result is on TAPE[1], but since we exited
	 * the loop without performing the last iteration of step D6, we have not
	 * rearranged the tape unit assignment, and therefore the result is on
	 * TAPE[T].  We need to do it this way so that we can freeze the final
	 * output tape while rewinding it.  The last iteration of step D6 would be
	 * a waste of cycles anyway...
	 */
	state->result_tape = state->tp_tapenum[state->tapeRange];
	LogicalTapeFreeze(state->tapeset, state->result_tape);
	state->status = TSS_SORTEDONTAPE;
}

/*
 * Merge one run from each input tape, except ones with dummy runs.
 *
 * This is the inner loop of Algorithm D step D5.  We know that the
 * output tape is TAPE[T].
 */
static void
mergeonerun(RumTuplesortstate *state)
{
	int			destTape = state->tp_tapenum[state->tapeRange];
	int			srcTape;
	int			tupIndex;
	SortTuple  *tup;
	long		priorAvail,
				spaceFreed;

	/*
	 * Start the merge by loading one tuple from each active source tape into
	 * the heap.  We can also decrease the input run/dummy run counts.
	 */
	beginmerge(state);

	/*
	 * Execute merge by repeatedly extracting lowest tuple in heap, writing it
	 * out, and replacing it with next tuple from same tape (if there is
	 * another one).
	 */
	while (state->memtupcount > 0)
	{
		/* write the tuple to destTape */
		priorAvail = state->availMem;
		srcTape = state->memtuples[0].tupindex;
		WRITETUP(state, destTape, &state->memtuples[0]);
		/* writetup adjusted total free space, now fix per-tape space */
		spaceFreed = state->availMem - priorAvail;
		state->mergeavailmem[srcTape] += spaceFreed;
		/* compact the heap */
		rum_tuplesort_heap_siftup(state, false);
		if ((tupIndex = state->mergenext[srcTape]) == 0)
		{
			/* out of preloaded data on this tape, try to read more */
			mergepreread(state);
			/* if still no data, we've reached end of run on this tape */
			if ((tupIndex = state->mergenext[srcTape]) == 0)
				continue;
		}
		/* pull next preread tuple from list, insert in heap */
		tup = &state->memtuples[tupIndex];
		state->mergenext[srcTape] = tup->tupindex;
		if (state->mergenext[srcTape] == 0)
			state->mergelast[srcTape] = 0;
		rum_tuplesort_heap_insert(state, tup, srcTape, false);
		/* put the now-unused memtuples entry on the freelist */
		tup->tupindex = state->mergefreelist;
		state->mergefreelist = tupIndex;
		state->mergeavailslots[srcTape]++;
	}

	/*
	 * When the heap empties, we're done.  Write an end-of-run marker on the
	 * output tape, and increment its count of real runs.
	 */
	markrunend(state, destTape);
	state->tp_runs[state->tapeRange]++;

#ifdef TRACE_SORT
	if (trace_sort)
		elog(LOG, "finished %d-way merge step: %s", state->activeTapes,
			 pg_rusage_show(&state->ru_start));
#endif
}

/*
 * beginmerge - initialize for a merge pass
 *
 * We decrease the counts of real and dummy runs for each tape, and mark
 * which tapes contain active input runs in mergeactive[].  Then, load
 * as many tuples as we can from each active input tape, and finally
 * fill the merge heap with the first tuple from each active tape.
 */
static void
beginmerge(RumTuplesortstate *state)
{
	int			activeTapes;
	int			tapenum;
	int			srcTape;
	int			slotsPerTape;
	long		spacePerTape;

	/* Heap should be empty here */
	Assert(state->memtupcount == 0);

	/* Adjust run counts and mark the active tapes */
	memset(state->mergeactive, 0,
		   state->maxTapes * sizeof(*state->mergeactive));
	activeTapes = 0;
	for (tapenum = 0; tapenum < state->tapeRange; tapenum++)
	{
		if (state->tp_dummy[tapenum] > 0)
			state->tp_dummy[tapenum]--;
		else
		{
			Assert(state->tp_runs[tapenum] > 0);
			state->tp_runs[tapenum]--;
			srcTape = state->tp_tapenum[tapenum];
			state->mergeactive[srcTape] = true;
			activeTapes++;
		}
	}
	state->activeTapes = activeTapes;

	/* Clear merge-pass state variables */
	memset(state->mergenext, 0,
		   state->maxTapes * sizeof(*state->mergenext));
	memset(state->mergelast, 0,
		   state->maxTapes * sizeof(*state->mergelast));
	state->mergefreelist = 0;	/* nothing in the freelist */
	state->mergefirstfree = activeTapes;		/* 1st slot avail for preread */

	/*
	 * Initialize space allocation to let each active input tape have an equal
	 * share of preread space.
	 */
	Assert(activeTapes > 0);
	slotsPerTape = (state->memtupsize - state->mergefirstfree) / activeTapes;
	Assert(slotsPerTape > 0);
	spacePerTape = state->availMem / activeTapes;
	for (srcTape = 0; srcTape < state->maxTapes; srcTape++)
	{
		if (state->mergeactive[srcTape])
		{
			state->mergeavailslots[srcTape] = slotsPerTape;
			state->mergeavailmem[srcTape] = spacePerTape;
		}
	}

	/*
	 * Preread as many tuples as possible (and at least one) from each active
	 * tape
	 */
	mergepreread(state);

	/* Load the merge heap with the first tuple from each input tape */
	for (srcTape = 0; srcTape < state->maxTapes; srcTape++)
	{
		int			tupIndex = state->mergenext[srcTape];
		SortTuple  *tup;

		if (tupIndex)
		{
			tup = &state->memtuples[tupIndex];
			state->mergenext[srcTape] = tup->tupindex;
			if (state->mergenext[srcTape] == 0)
				state->mergelast[srcTape] = 0;
			rum_tuplesort_heap_insert(state, tup, srcTape, false);
			/* put the now-unused memtuples entry on the freelist */
			tup->tupindex = state->mergefreelist;
			state->mergefreelist = tupIndex;
			state->mergeavailslots[srcTape]++;
		}
	}
}

/*
 * mergepreread - load tuples from merge input tapes
 *
 * This routine exists to improve sequentiality of reads during a merge pass,
 * as explained in the header comments of this file.  Load tuples from each
 * active source tape until the tape's run is exhausted or it has used up
 * its fair share of available memory.  In any case, we guarantee that there
 * is at least one preread tuple available from each unexhausted input tape.
 *
 * We invoke this routine at the start of a merge pass for initial load,
 * and then whenever any tape's preread data runs out.  Note that we load
 * as much data as possible from all tapes, not just the one that ran out.
 * This is because logtape.c works best with a usage pattern that alternates
 * between reading a lot of data and writing a lot of data, so whenever we
 * are forced to read, we should fill working memory completely.
 *
 * In FINALMERGE state, we *don't* use this routine, but instead just preread
 * from the single tape that ran dry.  There's no read/write alternation in
 * that state and so no point in scanning through all the tapes to fix one.
 * (Moreover, there may be quite a lot of inactive tapes in that state, since
 * we might have had many fewer runs than tapes.  In a regular tape-to-tape
 * merge we can expect most of the tapes to be active.)
 */
static void
mergepreread(RumTuplesortstate *state)
{
	int			srcTape;

	for (srcTape = 0; srcTape < state->maxTapes; srcTape++)
		mergeprereadone(state, srcTape);
}

/*
 * mergeprereadone - load tuples from one merge input tape
 *
 * Read tuples from the specified tape until it has used up its free memory
 * or array slots; but ensure that we have at least one tuple, if any are
 * to be had.
 */
static void
mergeprereadone(RumTuplesortstate *state, int srcTape)
{
	unsigned int tuplen;
	SortTuple	stup;
	int			tupIndex;
	long		priorAvail,
				spaceUsed;

	if (!state->mergeactive[srcTape])
		return;					/* tape's run is already exhausted */
	priorAvail = state->availMem;
	state->availMem = state->mergeavailmem[srcTape];
	while ((state->mergeavailslots[srcTape] > 0 && !LACKMEM(state)) ||
		   state->mergenext[srcTape] == 0)
	{
		/* read next tuple, if any */
		if ((tuplen = getlen(state, srcTape, true)) == 0)
		{
			state->mergeactive[srcTape] = false;
			break;
		}
		READTUP(state, &stup, srcTape, tuplen);
		/* find a free slot in memtuples[] for it */
		tupIndex = state->mergefreelist;
		if (tupIndex)
			state->mergefreelist = state->memtuples[tupIndex].tupindex;
		else
		{
			tupIndex = state->mergefirstfree++;
			Assert(tupIndex < state->memtupsize);
		}
		state->mergeavailslots[srcTape]--;
		/* store tuple, append to list for its tape */
		stup.tupindex = 0;
		state->memtuples[tupIndex] = stup;
		if (state->mergelast[srcTape])
			state->memtuples[state->mergelast[srcTape]].tupindex = tupIndex;
		else
			state->mergenext[srcTape] = tupIndex;
		state->mergelast[srcTape] = tupIndex;
	}
	/* update per-tape and global availmem counts */
	spaceUsed = state->mergeavailmem[srcTape] - state->availMem;
	state->mergeavailmem[srcTape] = state->availMem;
	state->availMem = priorAvail - spaceUsed;
}

/*
 * dumptuples - remove tuples from heap and write to tape
 *
 * This is used during initial-run building, but not during merging.
 *
 * When alltuples = false, dump only enough tuples to get under the
 * availMem limit (and leave at least one tuple in the heap in any case,
 * since puttuple assumes it always has a tuple to compare to).  We also
 * insist there be at least one free slot in the memtuples[] array.
 *
 * When alltuples = true, dump everything currently in memory.
 * (This case is only used at end of input data.)
 *
 * If we empty the heap, close out the current run and return (this should
 * only happen at end of input data).  If we see that the tuple run number
 * at the top of the heap has changed, start a new run.
 */
static void
dumptuples(RumTuplesortstate *state, bool alltuples)
{
	while (alltuples ||
		   (LACKMEM(state) && state->memtupcount > 1) ||
		   state->memtupcount >= state->memtupsize)
	{
		/*
		 * Dump the heap's frontmost entry, and sift up to remove it from the
		 * heap.
		 */
		Assert(state->memtupcount > 0);
		WRITETUP(state, state->tp_tapenum[state->destTape],
				 &state->memtuples[0]);
		rum_tuplesort_heap_siftup(state, true);

		/*
		 * If the heap is empty *or* top run number has changed, we've
		 * finished the current run.
		 */
		if (state->memtupcount == 0 ||
			state->currentRun != state->memtuples[0].tupindex)
		{
			markrunend(state, state->tp_tapenum[state->destTape]);
			state->currentRun++;
			state->tp_runs[state->destTape]++;
			state->tp_dummy[state->destTape]--; /* per Alg D step D2 */

#ifdef TRACE_SORT
			if (trace_sort)
				elog(LOG, "finished writing%s run %d to tape %d: %s",
					 (state->memtupcount == 0) ? " final" : "",
					 state->currentRun, state->destTape,
					 pg_rusage_show(&state->ru_start));
#endif

			/*
			 * Done if heap is empty, else prepare for new run.
			 */
			if (state->memtupcount == 0)
				break;
			Assert(state->currentRun == state->memtuples[0].tupindex);
			selectnewtape(state);
		}
	}
}


/*
 * Heap manipulation routines, per Knuth's Algorithm 5.2.3H.
 *
 * Compare two SortTuples.  If checkIndex is true, use the tuple index
 * as the front of the sort key; otherwise, no.
 */

#define HEAPCOMPARE(tup1,tup2) \
	(checkIndex && ((tup1)->tupindex != (tup2)->tupindex) ? \
	 ((tup1)->tupindex) - ((tup2)->tupindex) : \
	 COMPARETUP(state, tup1, tup2))

/*
 * Convert the existing unordered array of SortTuples to a bounded heap,
 * discarding all but the smallest "state->bound" tuples.
 *
 * When working with a bounded heap, we want to keep the largest entry
 * at the root (array entry zero), instead of the smallest as in the normal
 * sort case.  This allows us to discard the largest entry cheaply.
 * Therefore, we temporarily reverse the sort direction.
 *
 * We assume that all entries in a bounded heap will always have tupindex
 * zero; it therefore doesn't matter that HEAPCOMPARE() doesn't reverse
 * the direction of comparison for tupindexes.
 */
static void
make_bounded_heap(RumTuplesortstate *state)
{
	int			tupcount = state->memtupcount;
	int			i;

	Assert(state->status == TSS_INITIAL);
	Assert(state->bounded);
	Assert(tupcount >= state->bound);

	/* Reverse sort direction so largest entry will be at root */
	REVERSEDIRECTION(state);

	state->memtupcount = 0;		/* make the heap empty */
	for (i = 0; i < tupcount; i++)
	{
		if (state->memtupcount >= state->bound &&
		  COMPARETUP(state, &state->memtuples[i], &state->memtuples[0]) <= 0)
		{
			/* New tuple would just get thrown out, so skip it */
			free_sort_tuple(state, &state->memtuples[i]);
			CHECK_FOR_INTERRUPTS();
		}
		else
		{
			/* Insert next tuple into heap */
			/* Must copy source tuple to avoid possible overwrite */
			SortTuple	stup = state->memtuples[i];

			rum_tuplesort_heap_insert(state, &stup, 0, false);

			/* If heap too full, discard largest entry */
			if (state->memtupcount > state->bound)
			{
				free_sort_tuple(state, &state->memtuples[0]);
				rum_tuplesort_heap_siftup(state, false);
			}
		}
	}

	Assert(state->memtupcount == state->bound);
	state->status = TSS_BOUNDED;
}

/*
 * Convert the bounded heap to a properly-sorted array
 */
static void
sort_bounded_heap(RumTuplesortstate *state)
{
	int			tupcount = state->memtupcount;

	Assert(state->status == TSS_BOUNDED);
	Assert(state->bounded);
	Assert(tupcount == state->bound);

	/*
	 * We can unheapify in place because each sift-up will remove the largest
	 * entry, which we can promptly store in the newly freed slot at the end.
	 * Once we're down to a single-entry heap, we're done.
	 */
	while (state->memtupcount > 1)
	{
		SortTuple	stup = state->memtuples[0];

		/* this sifts-up the next-largest entry and decreases memtupcount */
		rum_tuplesort_heap_siftup(state, false);
		state->memtuples[state->memtupcount] = stup;
	}
	state->memtupcount = tupcount;

	/*
	 * Reverse sort direction back to the original state.  This is not
	 * actually necessary but seems like a good idea for tidiness.
	 */
	REVERSEDIRECTION(state);

	state->status = TSS_SORTEDINMEM;
	state->boundUsed = true;
}

/*
 * Insert a new tuple into an empty or existing heap, maintaining the
 * heap invariant.  Caller is responsible for ensuring there's room.
 *
 * Note: we assume *tuple is a temporary variable that can be scribbled on.
 * For some callers, tuple actually points to a memtuples[] entry above the
 * end of the heap.  This is safe as long as it's not immediately adjacent
 * to the end of the heap (ie, in the [memtupcount] array entry) --- if it
 * is, it might get overwritten before being moved into the heap!
 */
static void
rum_tuplesort_heap_insert(RumTuplesortstate *state, SortTuple *tuple,
						  int tupleindex, bool checkIndex)
{
	SortTuple  *memtuples;
	int			j;

	/*
	 * Save the tupleindex --- see notes above about writing on *tuple. It's a
	 * historical artifact that tupleindex is passed as a separate argument
	 * and not in *tuple, but it's notationally convenient so let's leave it
	 * that way.
	 */
	tuple->tupindex = tupleindex;

	memtuples = state->memtuples;
	Assert(state->memtupcount < state->memtupsize);

	CHECK_FOR_INTERRUPTS();

	/*
	 * Sift-up the new entry, per Knuth 5.2.3 exercise 16. Note that Knuth is
	 * using 1-based array indexes, not 0-based.
	 */
	j = state->memtupcount++;
	while (j > 0)
	{
		int			i = (j - 1) >> 1;

		if (HEAPCOMPARE(tuple, &memtuples[i]) >= 0)
			break;
		memtuples[j] = memtuples[i];
		j = i;
	}
	memtuples[j] = *tuple;
}

/*
 * The tuple at state->memtuples[0] has been removed from the heap.
 * Decrement memtupcount, and sift up to maintain the heap invariant.
 */
static void
rum_tuplesort_heap_siftup(RumTuplesortstate *state, bool checkIndex)
{
	SortTuple  *memtuples = state->memtuples;
	SortTuple  *tuple;
	int			i,
				n;

	if (--state->memtupcount <= 0)
		return;

	CHECK_FOR_INTERRUPTS();

	n = state->memtupcount;
	tuple = &memtuples[n];		/* tuple that must be reinserted */
	i = 0;						/* i is where the "hole" is */
	for (;;)
	{
		int			j = 2 * i + 1;

		if (j >= n)
			break;
		if (j + 1 < n &&
			HEAPCOMPARE(&memtuples[j], &memtuples[j + 1]) > 0)
			j++;
		if (HEAPCOMPARE(tuple, &memtuples[j]) <= 0)
			break;
		memtuples[i] = memtuples[j];
		i = j;
	}
	memtuples[i] = *tuple;
}


/*
 * Tape interface routines
 */

static unsigned int
getlen(RumTuplesortstate *state, int tapenum, bool eofOK)
{
	unsigned int len;

	if (LogicalTapeRead(state->tapeset, tapenum,
						&len, sizeof(len)) != sizeof(len))
		elog(ERROR, "unexpected end of tape");
	if (len == 0 && !eofOK)
		elog(ERROR, "unexpected end of data");
	return len;
}

static void
markrunend(RumTuplesortstate *state, int tapenum)
{
	unsigned int len = 0;

	LogicalTapeWrite(state->tapeset, tapenum, (void *) &len, sizeof(len));
}


/*
 * Convenience routine to free a tuple previously loaded into sort memory
 */
static void
free_sort_tuple(RumTuplesortstate *state, SortTuple *stup)
{
	FREEMEM(state, GetMemoryChunkSpace(stup->tuple));
	pfree(stup->tuple);
}

static int
comparetup_rum(const SortTuple *a, const SortTuple *b, RumTuplesortstate *state)
{
	RumSortItem *i1,
			   *i2;
	float8		v1 = DatumGetFloat8(a->datum1);
	float8		v2 = DatumGetFloat8(b->datum1);
	int			i;

	if (v1 < v2)
		return -1;
	else if (v1 > v2)
		return 1;

	i1 = (RumSortItem *) a->tuple;
	i2 = (RumSortItem *) b->tuple;
	for (i = 1; i < state->nKeys; i++)
	{
		if (i1->data[i] < i2->data[i])
			return -1;
		else if (i1->data[i] > i2->data[i])
			return 1;
	}

	if (!state->compareItemPointer)
		return 0;

	/*
	 * If key values are equal, we sort on ItemPointer.
	 */
	if (i1->iptr.ip_blkid.bi_hi < i2->iptr.ip_blkid.bi_hi)
		return -1;
	else if (i1->iptr.ip_blkid.bi_hi > i2->iptr.ip_blkid.bi_hi)
		return 1;

	if (i1->iptr.ip_blkid.bi_lo < i2->iptr.ip_blkid.bi_lo)
		return -1;
	else if (i1->iptr.ip_blkid.bi_lo > i2->iptr.ip_blkid.bi_lo)
		return 1;

	if (i1->iptr.ip_posid < i2->iptr.ip_posid)
		return -1;
	else if (i1->iptr.ip_posid > i2->iptr.ip_posid)
		return 1;

	return 0;
}

static void
copytup_rum(RumTuplesortstate *state, SortTuple *stup, void *tup)
{
	RumSortItem *item = (RumSortItem *) tup;

	stup->datum1 = Float8GetDatum(state->nKeys > 0 ? item->data[0] : 0);
	stup->isnull1 = false;
	stup->tuple = tup;
	USEMEM(state, GetMemoryChunkSpace(tup));
}

static void
writetup_rum(RumTuplesortstate *state, int tapenum, SortTuple *stup)
{
	RumSortItem *item = (RumSortItem *) stup->tuple;
	unsigned int writtenlen = RumSortItemSize(state->nKeys) + sizeof(unsigned int);


	LogicalTapeWrite(state->tapeset, tapenum,
					 (void *) &writtenlen, sizeof(writtenlen));
	LogicalTapeWrite(state->tapeset, tapenum,
					 (void *) item, RumSortItemSize(state->nKeys));
	if (state->randomAccess)	/* need trailing length word? */
		LogicalTapeWrite(state->tapeset, tapenum,
						 (void *) &writtenlen, sizeof(writtenlen));

	FREEMEM(state, GetMemoryChunkSpace(item));
	pfree(item);
}

static void
readtup_rum(RumTuplesortstate *state, SortTuple *stup,
			int tapenum, unsigned int len)
{
	unsigned int tuplen = len - sizeof(unsigned int);
	RumSortItem *item = (RumSortItem *) palloc(RumSortItemSize(state->nKeys));

	Assert(tuplen == RumSortItemSize(state->nKeys));

	USEMEM(state, GetMemoryChunkSpace(item));
	LogicalTapeReadExact(state->tapeset, tapenum,
						 (void *) item, RumSortItemSize(state->nKeys));
	stup->datum1 = Float8GetDatum(state->nKeys > 0 ? item->data[0] : 0);
	stup->isnull1 = false;
	stup->tuple = item;

	if (state->randomAccess)	/* need trailing length word? */
		LogicalTapeReadExact(state->tapeset, tapenum,
							 &tuplen, sizeof(tuplen));
}

static void
reversedirection_rum(RumTuplesortstate *state)
{
	state->reverse = !state->reverse;
}

static int
comparetup_rumitem(const SortTuple *a, const SortTuple *b, RumTuplesortstate *state)
{
	RumItem	   *i1, *i2;

	/* Extract RumItem from RumScanItem */
	i1 = (RumItem *) a->tuple;
	i2 = (RumItem *) b->tuple;

	if (state->cmp)
	{
		if (i1->addInfoIsNull || i2->addInfoIsNull)
		{
			if (!(i1->addInfoIsNull && i2->addInfoIsNull))
				return (i1->addInfoIsNull) ? 1 : -1;
			/* go to itempointer compare */
		}
		else
		{
			int r;

			r = DatumGetInt32(FunctionCall2(state->cmp,
											i1->addInfo,
											i2->addInfo));

			if (r != 0)
				return r;
		}
	}

	/*
	 * If key values are equal, we sort on ItemPointer.
	 */
	if (i1->iptr.ip_blkid.bi_hi < i2->iptr.ip_blkid.bi_hi)
		return -1;
	else if (i1->iptr.ip_blkid.bi_hi > i2->iptr.ip_blkid.bi_hi)
		return 1;

	if (i1->iptr.ip_blkid.bi_lo < i2->iptr.ip_blkid.bi_lo)
		return -1;
	else if (i1->iptr.ip_blkid.bi_lo > i2->iptr.ip_blkid.bi_lo)
		return 1;

	if (i1->iptr.ip_posid < i2->iptr.ip_posid)
		return -1;
	else if (i1->iptr.ip_posid > i2->iptr.ip_posid)
		return 1;

	return 0;
}

static void
copytup_rumitem(RumTuplesortstate *state, SortTuple *stup, void *tup)
{
	stup->isnull1 = true;
	stup->tuple = palloc(sizeof(RumScanItem));
	memcpy(stup->tuple, tup, sizeof(RumScanItem));
	USEMEM(state, GetMemoryChunkSpace(stup->tuple));
}

static void
writetup_rumitem(RumTuplesortstate *state, int tapenum, SortTuple *stup)
{
	RumScanItem *item = (RumScanItem *) stup->tuple;
	unsigned int writtenlen = sizeof(*item) + sizeof(unsigned int);

	LogicalTapeWrite(state->tapeset, tapenum,
					 (void *) &writtenlen, sizeof(writtenlen));
	LogicalTapeWrite(state->tapeset, tapenum,
					 (void *) item, sizeof(*item));
	if (state->randomAccess)	/* need trailing length word? */
		LogicalTapeWrite(state->tapeset, tapenum,
						 (void *) &writtenlen, sizeof(writtenlen));

	FREEMEM(state, GetMemoryChunkSpace(item));
	pfree(item);
}

static void
readtup_rumitem(RumTuplesortstate *state, SortTuple *stup,
			int tapenum, unsigned int len)
{
	unsigned int tuplen = len - sizeof(unsigned int);
	RumScanItem *item = (RumScanItem *) palloc(sizeof(RumScanItem));

	Assert(tuplen == sizeof(RumScanItem));

	USEMEM(state, GetMemoryChunkSpace(item));
	LogicalTapeReadExact(state->tapeset, tapenum,
						 (void *) item, tuplen);
	stup->isnull1 = true;
	stup->tuple = item;

	if (state->randomAccess)	/* need trailing length word? */
		LogicalTapeReadExact(state->tapeset, tapenum,
							 &tuplen, sizeof(tuplen));
}

