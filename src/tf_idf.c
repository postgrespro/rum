/*-------------------------------------------------------------------------
 *
 * tf_idf.c
 *		Implementation of TD/IDF statistics calculation.
 *
 * Portions Copyright (c) 2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/namespace.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

#include "rum.h"

/*
 * FIXME:
 *  * cache IDF for ts_query (non-prefix search?)
 *  * calculate IDF from RUM index
 */

/* lookup table type for binary searching through MCELEMs */
typedef struct
{
	text	   *element;
	float4		frequency;
} TextFreq;

/* type of keys for bsearch'ing through an array of TextFreqs */
typedef struct
{
	char	   *lexeme;
	int			length;
} LexemeKey;

typedef struct
{
	TextFreq   *lookup;
	int			nmcelem;
	float4		minfreq;
} MCelemStats;

typedef struct
{
	Oid			relId;
	AttrNumber	attrno;
} RelAttrInfo;

char				   *TFIDFSource;
static RelAttrInfo		TFIDFSourceParsed;
static bool				TDIDFLoaded = false;
static MemoryContext	TFIDFContext = NULL;
static MCelemStats		TDIDFStats;

#define EXIT_CHECK_TF_IDF_SOURCE(error) \
	do { \
		GUC_check_errdetail(error); \
		pfree(rawname); \
		list_free(namelist); \
		if (rel) \
			RelationClose(rel); \
		return false; \
	} while (false);

static void load_tf_idf_source(void);
static void check_load_tf_idf_source(void);
static void forget_tf_idf_stats(void);
static int	compare_lexeme_textfreq(const void *e1, const void *e2);

bool
check_tf_idf_source(char **newval, void **extra, GucSource source)
{
	char			   *rawname;
	char			   *attname;
	List			   *namelist;
	Oid					namespaceId;
	Oid					relId;
	Relation			rel = NULL;
	AttrNumber			attrno;
	int					i;
	RelAttrInfo		   *myextra;

	/* Need a modifiable copy of string */
	rawname = pstrdup(*newval);

	/* Parse string into list of identifiers */
	if (!SplitIdentifierString(rawname, '.', &namelist))
	{
		/* syntax error in name list */
		EXIT_CHECK_TF_IDF_SOURCE("List syntax is invalid.");
	}

	switch (list_length(namelist))
	{
		case 0:
			return true;
		case 1:
			EXIT_CHECK_TF_IDF_SOURCE("improper column name (there should be at least 2 dotted names)");
		case 2:
			relId = RelnameGetRelid(linitial(namelist));
			attname = lsecond(namelist);
			break;
		case 3:
			/* use exact schema given */
			namespaceId = LookupExplicitNamespace(linitial(namelist), true);
			if (!OidIsValid(namespaceId))
				relId = InvalidOid;
			else
				relId = get_relname_relid(lsecond(namelist), namespaceId);
			attname = lthird(namelist);
			break;
		default:
			EXIT_CHECK_TF_IDF_SOURCE("improper column name (too many dotted names)");
	}

	if (!OidIsValid(relId))
		EXIT_CHECK_TF_IDF_SOURCE("relation not found");

	rel = RelationIdGetRelation(relId);
	if (rel->rd_rel->relkind == RELKIND_INDEX)
	{
		int		exprnum = 0;

		attrno = pg_atoi(attname, sizeof(attrno), 10);
		if (attrno <= 0 || attrno > rel->rd_index->indnatts)
			EXIT_CHECK_TF_IDF_SOURCE("wrong index attribute number");
		if (rel->rd_index->indkey.values[attrno - 1] != InvalidAttrNumber)
			EXIT_CHECK_TF_IDF_SOURCE("regular indexed column is specified");
		for (i = 0; i < attrno - 1; i++)
		{
			if (rel->rd_index->indkey.values[i] == InvalidAttrNumber)
				exprnum++;
		}
		RelationGetIndexExpressions(rel);
		if (exprType((Node *) list_nth(rel->rd_indexprs, exprnum)) != TSVECTOROID)
			EXIT_CHECK_TF_IDF_SOURCE("indexed expression should be of tsvector type");
	}
	else
	{
		TupleDesc	tupDesc = rel->rd_att;

		attrno = InvalidAttrNumber;
		for (i = 0; i < tupDesc->natts; i++)
		{
			if (namestrcmp(&(tupDesc->attrs[i]->attname), attname) == 0)
			{
				attrno = tupDesc->attrs[i]->attnum;
				break;
			}
		}
		if (attrno == InvalidAttrNumber)
			EXIT_CHECK_TF_IDF_SOURCE("attribute not found");
		if (tupDesc->attrs[attrno - 1]->atttypid != TSVECTOROID)
			EXIT_CHECK_TF_IDF_SOURCE("attribute should be of tsvector type");
	}


	myextra = (RelAttrInfo *) malloc(sizeof(RelAttrInfo));
	myextra->relId = relId;
	myextra->attrno = attrno;
	*extra = (void *) myextra;

	pfree(rawname);
	list_free(namelist);
	RelationClose(rel);
	return true;
}


void
assign_tf_idf_source(const char *newval, void *extra)
{
	RelAttrInfo  *myextra = (RelAttrInfo *) extra;

	if (myextra)
	{
		TFIDFSourceParsed = *myextra;
	}
	else
	{
		TFIDFSourceParsed.relId = InvalidOid;
		TFIDFSourceParsed.attrno = InvalidAttrNumber;
	}

	forget_tf_idf_stats();
}

static void
load_tf_idf_source(void)
{
	HeapTuple		statsTuple;
	AttStatsSlot	sslot;
	MemoryContext	oldContext;
	int				i;

	if (!TFIDFContext)
		TFIDFContext = AllocSetContextCreate(TopMemoryContext,
											 "Memory context for TF/IDF statistics",
											 ALLOCSET_DEFAULT_SIZES);

	if (!OidIsValid(TFIDFSourceParsed.relId)
		|| TFIDFSourceParsed.attrno == InvalidAttrNumber)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("statistics for TD/IDF is not defined"),
				 errhint("consider setting tf_idf_source GUC")));
	}

	statsTuple = SearchSysCache3(STATRELATTINH,
								 ObjectIdGetDatum(TFIDFSourceParsed.relId),
								 Int16GetDatum(TFIDFSourceParsed.attrno),
								 BoolGetDatum(true));

	if (!statsTuple)
		statsTuple = SearchSysCache3(STATRELATTINH,
									 ObjectIdGetDatum(TFIDFSourceParsed.relId),
									 Int16GetDatum(TFIDFSourceParsed.attrno),
									 BoolGetDatum(false));

	MemoryContextReset(TFIDFContext);
	TDIDFLoaded = false;

	oldContext = MemoryContextSwitchTo(TFIDFContext);

	if (!statsTuple
		|| !get_attstatsslot(&sslot, statsTuple,
							 STATISTIC_KIND_MCELEM, InvalidOid,
							 ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS)
		|| sslot.nnumbers != sslot.nvalues + 2)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("statistics for TD/IDF is not found"),
				 errhint("consider running ANALYZE")));
	}

	TDIDFStats.nmcelem = sslot.nvalues;
	TDIDFStats.minfreq = sslot.numbers[sslot.nnumbers - 2];
	/*
	 * Transpose the data into a single array so we can use bsearch().
	 */
	TDIDFStats.lookup = (TextFreq *) palloc(sizeof(TextFreq) * TDIDFStats.nmcelem);
	for (i = 0; i < TDIDFStats.nmcelem; i++)
	{
		/*
		 * The text Datums came from an array, so it cannot be compressed or
		 * stored out-of-line -- it's safe to use VARSIZE_ANY*.
		 */
		Assert(!VARATT_IS_COMPRESSED(sslot.values[i]) && !VARATT_IS_EXTERNAL(sslot.values[i]));
		TDIDFStats.lookup[i].element = (text *) DatumGetPointer(sslot.values[i]);
		TDIDFStats.lookup[i].frequency = sslot.numbers[i];
	}

	MemoryContextSwitchTo(oldContext);

	TDIDFLoaded = true;

	ReleaseSysCache(statsTuple);
}

static void
check_load_tf_idf_source(void)
{
	if (!TDIDFLoaded)
		load_tf_idf_source();
}

static void
forget_tf_idf_stats(void)
{
	if (TFIDFContext)
		MemoryContextReset(TFIDFContext);
	TDIDFLoaded = false;
}

/*
 * bsearch() comparator for a lexeme (non-NULL terminated string with length)
 * and a TextFreq. Use length, then byte-for-byte comparison, because that's
 * how ANALYZE code sorted data before storing it in a statistic tuple.
 * See ts_typanalyze.c for details.
 */
static int
compare_lexeme_textfreq(const void *e1, const void *e2)
{
	const LexemeKey *key = (const LexemeKey *) e1;
	const TextFreq *t = (const TextFreq *) e2;
	int			len1,
				len2;

	len1 = key->length;
	len2 = VARSIZE_ANY_EXHDR(t->element);

	/* Compare lengths first, possibly avoiding a strncmp call */
	if (len1 > len2)
		return 1;
	else if (len1 < len2)
		return -1;

	/* Fall back on byte-for-byte comparison */
	return strncmp(key->lexeme, VARDATA_ANY(t->element), len1);
}

float4
estimate_idf(char *lexeme, int length)
{
	TextFreq   *searchres;
	LexemeKey	key;
	float4		selec;

	check_load_tf_idf_source();

	key.lexeme = lexeme;
	key.length = length;

	searchres = (TextFreq *) bsearch(&key, TDIDFStats.lookup, TDIDFStats.nmcelem,
									 sizeof(TextFreq),
									 compare_lexeme_textfreq);

	if (searchres)
	{
		/*
		 * The element is in MCELEM.  Return precise selectivity (or
		 * at least as precise as ANALYZE could find out).
		 */
		selec = searchres->frequency;
	}
	else
	{
		/*
		 * The element is not in MCELEM.  Punt, but assume that the
		 * selectivity cannot be more than minfreq / 2.
		 */
		selec = TDIDFStats.minfreq / 2;
	}

	return 1.0f / selec;
}
