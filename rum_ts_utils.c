/*-------------------------------------------------------------------------
 *
 * rum_ts_utils.c
 *		various text-search functions
 *
 * Portions Copyright (c) 2015-2016, Postgres Professional
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "tsearch/ts_type.h"
#include "tsearch/ts_utils.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/typcache.h"

#include "rum.h"

#include <math.h>

PG_FUNCTION_INFO_V1(rum_extract_tsvector);
PG_FUNCTION_INFO_V1(rum_extract_tsquery);
PG_FUNCTION_INFO_V1(rum_tsvector_config);
PG_FUNCTION_INFO_V1(rum_tsquery_pre_consistent);
PG_FUNCTION_INFO_V1(rum_tsquery_consistent);
PG_FUNCTION_INFO_V1(rum_tsquery_timestamp_consistent);
PG_FUNCTION_INFO_V1(rum_tsquery_distance);
PG_FUNCTION_INFO_V1(rum_ts_distance_tt);
PG_FUNCTION_INFO_V1(rum_ts_distance_ttf);
PG_FUNCTION_INFO_V1(rum_ts_distance_td);
PG_FUNCTION_INFO_V1(rum_ts_join_pos);

PG_FUNCTION_INFO_V1(tsquery_to_distance_query);

static int	count_pos(char *ptr, int len);
static char *decompress_pos(char *ptr, WordEntryPos *pos);

typedef struct
{
	QueryItem  *first_item;
	int		   *map_item_operand;
	bool	   *check;
	bool	   *need_recheck;
	Datum	   *addInfo;
	bool	   *addInfoIsNull;
	bool		recheckPhrase;
}	RumChkVal;

typedef struct
{
	union
	{
		/* Used in rum_ts_distance() */
		struct
		{
			QueryItem **item;
			int16		nitem;
		} item;
		/* Used in rum_tsquery_distance() */
		struct
		{
			QueryItem  *item_first;
			int32		keyn;
		} key;
	} data;
	uint8		wclass;
	int32		pos;
} DocRepresentation;

typedef struct
{
	TSQuery		query;
	/* Used in rum_tsquery_distance() */
	int		   *map_item_operand;

	bool	   *operandexist;
	int			lenght;
} QueryRepresentation;

typedef struct
{
	int			pos;
	int			p;
	int			q;
	DocRepresentation *begin;
	DocRepresentation *end;
} Extention;

static float weights[] = {1.0/0.1f, 1.0/0.2f, 1.0/0.4f, 1.0/1.0f};

/* A dummy WordEntryPos array to use when haspos is false */
static WordEntryPosVector POSNULL = {
	1,							/* Number of elements that follow */
	{0}
};

#define RANK_NO_NORM			0x00
#define RANK_NORM_LOGLENGTH		0x01
#define RANK_NORM_LENGTH		0x02
#define RANK_NORM_EXTDIST		0x04
#define RANK_NORM_UNIQ			0x08
#define RANK_NORM_LOGUNIQ		0x10
#define RANK_NORM_RDIVRPLUS1	0x20
#define DEF_NORM_METHOD			RANK_NO_NORM

#define QR_GET_OPERAND_EXISTS(q, v)		( (q)->operandexist[ ((QueryItem*)(v)) - GETQUERY((q)->query) ] )
#define QR_SET_OPERAND_EXISTS(q, v)  QR_GET_OPERAND_EXISTS(q,v) = true

static bool
pre_checkcondition_rum(void *checkval, QueryOperand *val, ExecPhraseData *data)
{
	RumChkVal  *gcv = (RumChkVal *) checkval;
	int			j;

	/* if any val requiring a weight is used, set recheck flag */
	if (val->weight != 0 || data != NULL)
		*(gcv->need_recheck) = true;

	/* convert item's number to corresponding entry's (operand's) number */
	j = gcv->map_item_operand[((QueryItem *) val) - gcv->first_item];

	/* return presence of current entry in indexed value */
	return gcv->check[j];
}

Datum
rum_tsquery_pre_consistent(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);

	TSQuery		query = PG_GETARG_TSQUERY(2);

	Pointer    *extra_data = (Pointer *) PG_GETARG_POINTER(4);
	bool		recheck;
	bool		res = FALSE;

	if (query->size > 0)
	{
		QueryItem  *item;
		RumChkVal	gcv;

		/*
		 * check-parameter array has one entry for each value (operand) in the
		 * query.
		 */
		gcv.first_item = item = GETQUERY(query);
		gcv.check = check;
		gcv.map_item_operand = (int *) (extra_data[0]);
		gcv.need_recheck = &recheck;

		res = TS_execute(GETQUERY(query),
						 &gcv,
						 TS_EXEC_PHRASE_AS_AND,
						 pre_checkcondition_rum);
	}

	PG_RETURN_BOOL(res);
}

static bool
checkcondition_rum(void *checkval, QueryOperand *val, ExecPhraseData *data)
{
	RumChkVal  *gcv = (RumChkVal *) checkval;
	int			j;

	/* if any val requiring a weight is used, set recheck flag */
	if (val->weight != 0)
		*(gcv->need_recheck) = true;

	/* convert item's number to corresponding entry's (operand's) number */
	j = gcv->map_item_operand[((QueryItem *) val) - gcv->first_item];

	/* return presence of current entry in indexed value */
	if (!gcv->check[j])
		return false;

	/*
	 * Fill position list for phrase operator if it's needed end it exists
	 */
	if (data)
	{
		/* caller wants an array of positions (phrase search) */

		if (gcv->recheckPhrase)
		{
			/*
			 * we don't have a positions because we store a timestamp in
			 * addInfo
			 */
			*(gcv->need_recheck) = true;
		}
		else if (gcv->addInfo && gcv->addInfoIsNull[j] == false)
		{
			bytea	   *positions;
			int32		i;
			char	   *ptrt;
			WordEntryPos post;

			positions = DatumGetByteaP(gcv->addInfo[j]);
			data->npos = count_pos(VARDATA_ANY(positions),
								   VARSIZE_ANY_EXHDR(positions));
			data->pos = palloc(sizeof(*data->pos) * data->npos);
			data->allocated = true;

			ptrt = (char *) VARDATA_ANY(positions);
			post = 0;

			for (i = 0; i < data->npos; i++)
			{
				ptrt = decompress_pos(ptrt, &post);
				data->pos[i] = post;
			}
		}
	}

	return true;
}

Datum
rum_tsquery_consistent(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);

	/* StrategyNumber strategy = PG_GETARG_UINT16(1); */
	TSQuery		query = PG_GETARG_TSQUERY(2);

	/* int32	nkeys = PG_GETARG_INT32(3); */
	Pointer    *extra_data = (Pointer *) PG_GETARG_POINTER(4);
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);
	Datum	   *addInfo = (Datum *) PG_GETARG_POINTER(8);
	bool	   *addInfoIsNull = (bool *) PG_GETARG_POINTER(9);
	bool		res = FALSE;

	/*
	 * The query requires recheck only if it involves weights
	 */
	*recheck = false;

	if (query->size > 0)
	{
		QueryItem  *item;
		RumChkVal	gcv;

		/*
		 * check-parameter array has one entry for each value (operand) in the
		 * query.
		 */
		gcv.first_item = item = GETQUERY(query);
		gcv.check = check;
		gcv.map_item_operand = (int *) (extra_data[0]);
		gcv.need_recheck = recheck;
		gcv.addInfo = addInfo;
		gcv.addInfoIsNull = addInfoIsNull;
		gcv.recheckPhrase = false;

		res = TS_execute(GETQUERY(query), &gcv,
						 TS_EXEC_CALC_NOT,
						 checkcondition_rum);
	}

	PG_RETURN_BOOL(res);
}

Datum
rum_tsquery_timestamp_consistent(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);

	/* StrategyNumber strategy = PG_GETARG_UINT16(1); */
	TSQuery		query = PG_GETARG_TSQUERY(2);

	/* int32	nkeys = PG_GETARG_INT32(3); */
	Pointer    *extra_data = (Pointer *) PG_GETARG_POINTER(4);
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);
	Datum	   *addInfo = (Datum *) PG_GETARG_POINTER(8);
	bool	   *addInfoIsNull = (bool *) PG_GETARG_POINTER(9);
	bool		res = FALSE;

	/*
	 * The query requires recheck only if it involves weights
	 */
	*recheck = false;

	if (query->size > 0)
	{
		QueryItem  *item;
		RumChkVal	gcv;

		/*
		 * check-parameter array has one entry for each value (operand) in the
		 * query.
		 */
		gcv.first_item = item = GETQUERY(query);
		gcv.check = check;
		gcv.map_item_operand = (int *) (extra_data[0]);
		gcv.need_recheck = recheck;
		gcv.addInfo = addInfo;
		gcv.addInfoIsNull = addInfoIsNull;
		gcv.recheckPhrase = true;

		res = TS_execute(GETQUERY(query), &gcv,
						 TS_EXEC_CALC_NOT | TS_EXEC_PHRASE_AS_AND,
						 checkcondition_rum);
	}

	PG_RETURN_BOOL(res);
}

#define SIXTHBIT 0x20
#define LOWERMASK 0x1F

static int
compress_pos(char *target, WordEntryPos *pos, int npos)
{
	int			i;
	uint16		prev = 0,
				delta;
	char	   *ptr;

	ptr = target;
	for (i = 0; i < npos; i++)
	{
		delta = WEP_GETPOS(pos[i]) - WEP_GETPOS(prev);

		while (true)
		{
			if (delta >= SIXTHBIT)
			{
				*ptr = (delta & (~HIGHBIT)) | HIGHBIT;
				ptr++;
				delta >>= 7;
			}
			else
			{
				*ptr = delta | (WEP_GETWEIGHT(pos[i]) << 5);
				ptr++;
				break;
			}
		}
		prev = pos[i];
	}
	return ptr - target;
}

static char *
decompress_pos(char *ptr, WordEntryPos *pos)
{
	int			i;
	uint8		v;
	uint16		delta = 0;

	i = 0;
	while (true)
	{
		v = *ptr;
		ptr++;
		if (v & HIGHBIT)
		{
			delta |= (v & (~HIGHBIT)) << i;
		}
		else
		{
			delta |= (v & LOWERMASK) << i;
			*pos += delta;
			WEP_SETWEIGHT(*pos, v >> 5);
			return ptr;
		}
		i += 7;
	}
}

static int
count_pos(char *ptr, int len)
{
	int			count = 0,
				i;

	for (i = 0; i < len; i++)
	{
		if (!(ptr[i] & HIGHBIT))
			count++;
	}
	return count;
}

static uint32
count_length(TSVector t)
{
	WordEntry  *ptr = ARRPTR(t),
			   *end = (WordEntry *) STRPTR(t);
	uint32		len = 0;

	while (ptr < end)
	{
		uint32		clen = POSDATALEN(t, ptr);

		if (clen == 0)
			len += 1;
		else
			len += clen;

		ptr++;
	}

	return len;
}

/*
 * sort QueryOperands by (length, word)
 */
static int
compareQueryOperand(const void *a, const void *b, void *arg)
{
	char	   *operand = (char *) arg;
	QueryOperand *qa = (*(QueryOperand *const *) a);
	QueryOperand *qb = (*(QueryOperand *const *) b);

	return tsCompareString(operand + qa->distance, qa->length,
						   operand + qb->distance, qb->length,
						   false);
}

/*
 * Returns a sorted, de-duplicated array of QueryOperands in a query.
 * The returned QueryOperands are pointers to the original QueryOperands
 * in the query.
 *
 * Length of the returned array is stored in *size
 */
static QueryOperand **
SortAndUniqItems(TSQuery q, int *size)
{
	char	   *operand = GETOPERAND(q);
	QueryItem  *item = GETQUERY(q);
	QueryOperand **res,
			  **ptr,
			  **prevptr;

	ptr = res = (QueryOperand **) palloc(sizeof(QueryOperand *) * *size);

	/* Collect all operands from the tree to res */
	while ((*size)--)
	{
		if (item->type == QI_VAL)
		{
			*ptr = (QueryOperand *) item;
			ptr++;
		}
		item++;
	}

	*size = ptr - res;
	if (*size < 2)
		return res;

	qsort_arg(res, *size, sizeof(QueryOperand *), compareQueryOperand, (void *) operand);

	ptr = res + 1;
	prevptr = res;

	/* remove duplicates */
	while (ptr - res < *size)
	{
		if (compareQueryOperand((void *) ptr, (void *) prevptr, (void *) operand) != 0)
		{
			prevptr++;
			*prevptr = *ptr;
		}
		ptr++;
	}

	*size = prevptr + 1 - res;
	return res;
}

Datum
rum_extract_tsvector(PG_FUNCTION_ARGS)
{
	TSVector	vector = PG_GETARG_TSVECTOR(0);
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	Datum	  **addInfo = (Datum **) PG_GETARG_POINTER(3);
	bool	  **addInfoIsNull = (bool **) PG_GETARG_POINTER(4);
	Datum	   *entries = NULL;

	*nentries = vector->size;
	if (vector->size > 0)
	{
		int			i;
		WordEntry  *we = ARRPTR(vector);
		WordEntryPosVector *posVec;

		entries = (Datum *) palloc(sizeof(Datum) * vector->size);
		*addInfo = (Datum *) palloc(sizeof(Datum) * vector->size);
		*addInfoIsNull = (bool *) palloc(sizeof(bool) * vector->size);

		for (i = 0; i < vector->size; i++)
		{
			text	   *txt;
			bytea	   *posData;
			int			posDataSize;

			txt = cstring_to_text_with_len(STRPTR(vector) + we->pos, we->len);
			entries[i] = PointerGetDatum(txt);

			if (we->haspos)
			{
				posVec = _POSVECPTR(vector, we);
				posDataSize = VARHDRSZ + 2 * posVec->npos * sizeof(WordEntryPos);
				posData = (bytea *) palloc(posDataSize);
				posDataSize = compress_pos(posData->vl_dat, posVec->pos, posVec->npos) + VARHDRSZ;
				SET_VARSIZE(posData, posDataSize);

				(*addInfo)[i] = PointerGetDatum(posData);
				(*addInfoIsNull)[i] = false;
			}
			else
			{
				(*addInfo)[i] = (Datum) 0;
				(*addInfoIsNull)[i] = true;
			}
			we++;
		}
	}

	PG_FREE_IF_COPY(vector, 0);
	PG_RETURN_POINTER(entries);
}

Datum
rum_extract_tsquery(PG_FUNCTION_ARGS)
{
	TSQuery		query = PG_GETARG_TSQUERY(0);
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);

	/* StrategyNumber strategy = PG_GETARG_UINT16(2); */
	bool	  **ptr_partialmatch = (bool **) PG_GETARG_POINTER(3);
	Pointer   **extra_data = (Pointer **) PG_GETARG_POINTER(4);

	/* bool   **nullFlags = (bool **) PG_GETARG_POINTER(5); */
	int32	   *searchMode = (int32 *) PG_GETARG_POINTER(6);
	Datum	   *entries = NULL;

	*nentries = 0;

	if (query->size > 0)
	{
		QueryItem  *item = GETQUERY(query);
		int32		i,
					j;
		bool	   *partialmatch;
		int		   *map_item_operand;
		char	   *operand = GETOPERAND(query);
		QueryOperand **operands;

		/*
		 * If the query doesn't have any required positive matches (for
		 * instance, it's something like '! foo'), we have to do a full index
		 * scan.
		 */
		if (tsquery_requires_match(item))
			*searchMode = GIN_SEARCH_MODE_DEFAULT;
		else
			*searchMode = GIN_SEARCH_MODE_ALL;

		*nentries = query->size;
		operands = SortAndUniqItems(query, nentries);

		entries = (Datum *) palloc(sizeof(Datum) * (*nentries));
		partialmatch = *ptr_partialmatch = (bool *) palloc(sizeof(bool) * (*nentries));

		/*
		 * Make map to convert item's number to corresponding operand's (the
		 * same, entry's) number. Entry's number is used in check array in
		 * consistent method. We use the same map for each entry.
		 */
		*extra_data = (Pointer *) palloc(sizeof(Pointer) * (*nentries));
		map_item_operand = (int *) palloc0(sizeof(int) * query->size);

		for (i = 0; i < (*nentries); i++)
		{
			text	   *txt;

			txt = cstring_to_text_with_len(GETOPERAND(query) + operands[i]->distance,
										   operands[i]->length);
			entries[i] = PointerGetDatum(txt);
			partialmatch[i] = operands[i]->prefix;
			(*extra_data)[i] = (Pointer) map_item_operand;
		}

		/* Now rescan the VAL items and fill in the arrays */
		for (j = 0; j < query->size; j++)
		{
			if (item[j].type == QI_VAL)
			{
				QueryOperand *val = &item[j].qoperand;
				bool		found = false;

				for (i = 0; i < (*nentries); i++)
				{
					if (!tsCompareString(operand + operands[i]->distance, operands[i]->length,
										 operand + val->distance, val->length,
										 false))
					{
						map_item_operand[j] = i;
						found = true;
						break;
					}
				}

				if (!found)
					elog(ERROR, "Operand not found!");
			}
		}
	}

	PG_FREE_IF_COPY(query, 0);

	PG_RETURN_POINTER(entries);
}

static int
compareDocR(const void *va, const void *vb)
{
	DocRepresentation *a = (DocRepresentation *) va;
	DocRepresentation *b = (DocRepresentation *) vb;

	if (a->pos == b->pos)
		return 0;
	return (a->pos > b->pos) ? 1 : -1;
}

static bool
checkcondition_QueryOperand(void *checkval, QueryOperand *val,
							ExecPhraseData *data)
{
	QueryRepresentation *qr = (QueryRepresentation *) checkval;

	/* Check for rum_tsquery_distance() */
	if (qr->map_item_operand != NULL)
	{
		int		i = (QueryItem *) val - GETQUERY(qr->query);
		return qr->operandexist[qr->map_item_operand[i]];
	}

	return QR_GET_OPERAND_EXISTS(qr, val);
}

static bool
Cover(DocRepresentation *doc, uint32 len, QueryRepresentation *qr,
	  Extention *ext)
{
	DocRepresentation *ptr;
	int			lastpos;
	int			i;
	bool		found;

restart:
	lastpos = ext->pos;
	found = false;

	memset(qr->operandexist, 0, sizeof(bool) * qr->lenght);

	ext->p = 0x7fffffff;
	ext->q = 0;
	ptr = doc + ext->pos;

	/* find upper bound of cover from current position, move up */
	while (ptr - doc < len)
	{
		if (qr->map_item_operand != NULL)
		{
			qr->operandexist[ptr->data.key.keyn] = true;
		}
		else
		{
			for (i = 0; i < ptr->data.item.nitem; i++)
				QR_SET_OPERAND_EXISTS(qr, ptr->data.item.item[i]);
		}
		if (TS_execute(GETQUERY(qr->query), (void *) qr, false,
					   checkcondition_QueryOperand))
		{
			if (ptr->pos > ext->q)
			{
				ext->q = ptr->pos;
				ext->end = ptr;
				lastpos = ptr - doc;
				found = true;
			}
			break;
		}
		ptr++;
	}

	if (!found)
		return false;

	memset(qr->operandexist, 0, sizeof(bool) * qr->lenght);

	ptr = doc + lastpos;

	/* find lower bound of cover from found upper bound, move down */
	while (ptr >= doc + ext->pos)
	{
		if (qr->map_item_operand != NULL)
		{
			qr->operandexist[ptr->data.key.keyn] = true;
		}
		else
		{
			for (i = 0; i < ptr->data.item.nitem; i++)
				QR_SET_OPERAND_EXISTS(qr, ptr->data.item.item[i]);
		}
		if (TS_execute(GETQUERY(qr->query), (void *) qr, true,
					   checkcondition_QueryOperand))
		{
			if (ptr->pos < ext->p)
			{
				ext->begin = ptr;
				ext->p = ptr->pos;
			}
			break;
		}
		ptr--;
	}

	if (ext->p <= ext->q)
	{
		/*
		 * set position for next try to next lexeme after begining of founded
		 * cover
		 */
		ext->pos = (ptr - doc) + 1;
		return true;
	}

	ext->pos++;
	goto restart;
}

static DocRepresentation *
get_docrep_addinfo(bool *check, QueryRepresentation *qr,
				   Datum *addInfo, bool *addInfoIsNull, uint32 *doclen)
{
	QueryItem  *item = GETQUERY(qr->query);
	int32		dimt,
				j,
				i;
	int			len = qr->query->size * 4,
				cur = 0;
	DocRepresentation *doc;
	char	   *ptrt;

	doc = (DocRepresentation *) palloc(sizeof(DocRepresentation) * len);

	for (i = 0; i < qr->query->size; i++)
	{
		int			keyN;
		WordEntryPos post = 0;

		if (item[i].type != QI_VAL)
			continue;

		keyN = qr->map_item_operand[i];
		if (!check[keyN])
			continue;

		/*
		 * entries could be repeated in tsquery, do not visit them twice
		 * or more. Modifying of check array (entryRes) is safe
		 */
		check[keyN] = false;

		if (!addInfoIsNull[keyN])
		{
			dimt = count_pos(VARDATA_ANY(addInfo[keyN]),
							 VARSIZE_ANY_EXHDR(addInfo[keyN]));
			ptrt = (char *) VARDATA_ANY(addInfo[keyN]);
		}
		else
			continue;

		while (cur + dimt >= len)
		{
			len *= 2;
			doc = (DocRepresentation *) repalloc(doc, sizeof(DocRepresentation) * len);
		}

		for (j = 0; j < dimt; j++)
		{
			ptrt = decompress_pos(ptrt, &post);

			doc[cur].data.key.item_first = item + i;
			doc[cur].data.key.keyn = keyN;
			doc[cur].pos = WEP_GETPOS(post);
			doc[cur].wclass = WEP_GETWEIGHT(post);
			cur++;
		}
	}

	*doclen = cur;

	if (cur > 0)
	{
		qsort((void *) doc, cur, sizeof(DocRepresentation), compareDocR);
		return doc;
	}

	pfree(doc);
	return NULL;
}

#define WordECompareQueryItem(e,q,p,i,m)				\
	tsCompareString((q) + (i)->distance, (i)->length,	\
					(e) + (p)->pos, (p)->len, (m))

/*
 * Returns a pointer to a WordEntry's array corresponding to 'item' from
 * tsvector 't'. 'q' is the TSQuery containing 'item'.
 * Returns NULL if not found.
 */
static WordEntry *
find_wordentry(TSVector t, TSQuery q, QueryOperand *item, int32 *nitem)
{
	WordEntry  *StopLow = ARRPTR(t);
	WordEntry  *StopHigh = (WordEntry *) STRPTR(t);
	WordEntry  *StopMiddle = StopHigh;
	int			difference;

	*nitem = 0;

	/* Loop invariant: StopLow <= item < StopHigh */
	while (StopLow < StopHigh)
	{
		StopMiddle = StopLow + (StopHigh - StopLow) / 2;
		difference = WordECompareQueryItem(STRPTR(t), GETOPERAND(q), StopMiddle, item, false);
		if (difference == 0)
		{
			StopHigh = StopMiddle;
			*nitem = 1;
			break;
		}
		else if (difference > 0)
			StopLow = StopMiddle + 1;
		else
			StopHigh = StopMiddle;
	}

	if (item->prefix == true)
	{
		if (StopLow >= StopHigh)
			StopMiddle = StopHigh;

		*nitem = 0;

		while (StopMiddle < (WordEntry *) STRPTR(t) &&
			   WordECompareQueryItem(STRPTR(t), GETOPERAND(q), StopMiddle, item, true) == 0)
		{
			(*nitem)++;
			StopMiddle++;
		}
	}

	return (*nitem > 0) ? StopHigh : NULL;
}

static DocRepresentation *
get_docrep(TSVector txt, QueryRepresentation *qr, uint32 *doclen)
{
	QueryItem  *item = GETQUERY(qr->query);
	WordEntry  *entry,
			   *firstentry;
	WordEntryPos *post;
	int32		dimt,
				j,
				i,
				nitem;
	int			len = qr->query->size * 4,
				cur = 0;
	DocRepresentation *doc;
	char	   *operand;

	doc = (DocRepresentation *) palloc(sizeof(DocRepresentation) * len);
	operand = GETOPERAND(qr->query);

	for (i = 0; i < qr->query->size; i++)
	{
		QueryOperand *curoperand;

		if (item[i].type != QI_VAL)
			continue;

		curoperand = &item[i].qoperand;

		if (QR_GET_OPERAND_EXISTS(qr, &item[i]))
			continue;

		firstentry = entry = find_wordentry(txt, qr->query, curoperand, &nitem);
		if (!entry)
			continue;

		while (entry - firstentry < nitem)
		{
			if (entry->haspos)
			{
				dimt = POSDATALEN(txt, entry);
				post = POSDATAPTR(txt, entry);
			}
			else
			{
				dimt = POSNULL.npos;
				post = POSNULL.pos;
			}

			while (cur + dimt >= len)
			{
				len *= 2;
				doc = (DocRepresentation *) repalloc(doc, sizeof(DocRepresentation) * len);
			}

			for (j = 0; j < dimt; j++)
			{
				if (j == 0)
				{
					int			k;

					doc[cur].data.item.nitem = 0;
					doc[cur].data.item.item = (QueryItem **) palloc(
								sizeof(QueryItem *) * qr->query->size);

					for (k = 0; k < qr->query->size; k++)
					{
						QueryOperand *kptr = &item[k].qoperand;
						QueryOperand *iptr = &item[i].qoperand;

						if (k == i ||
							(item[k].type == QI_VAL &&
							 compareQueryOperand(&kptr, &iptr, operand) == 0))
						{
							/*
							 * if k == i, we've already checked above that
							 * it's type == Q_VAL
							 */
							doc[cur].data.item.item[doc[cur].data.item.nitem] =
									item + k;
							doc[cur].data.item.nitem++;
							QR_SET_OPERAND_EXISTS(qr, item + k);
						}
					}
				}
				else
				{
					doc[cur].data.item.nitem = doc[cur - 1].data.item.nitem;
					doc[cur].data.item.item = doc[cur - 1].data.item.item;
				}
				doc[cur].pos = WEP_GETPOS(post[j]);
				doc[cur].wclass = WEP_GETWEIGHT(post[j]);
				cur++;
			}

			entry++;
		}
	}

	*doclen = cur;

	if (cur > 0)
	{
		qsort((void *) doc, cur, sizeof(DocRepresentation), compareDocR);
		return doc;
	}

	pfree(doc);
	return NULL;
}

static double
calc_score_docr(float4 *arrdata, DocRepresentation *doc, uint32 doclen,
				QueryRepresentation *qr, int method)
{
	int32		i;
	Extention	ext;
	double		Wdoc = 0.0;
	double		SumDist = 0.0,
				PrevExtPos = 0.0,
				CurExtPos = 0.0;
	int			NExtent = 0;

	/* Added by SK */
	int		   *cover_keys = (int *)palloc(0);
	int		   *cover_lengths = (int *)palloc(0);
	double	   *cover_ranks = (double *)palloc(0);
	int			ncovers = 0;

	MemSet(&ext, 0, sizeof(Extention));
	while (Cover(doc, doclen, qr, &ext))
	{
		double		Cpos = 0.0;
		double		InvSum = 0.0;
		int			nNoise;
		DocRepresentation *ptr = ext.begin;
		/* Added by SK */
		int			new_cover_idx = 0;
		int			new_cover_key = 0;
		int			nitems = 0;

		while (ptr <= ext.end)
		{
			InvSum += arrdata[ptr->wclass];
			/* SK: Quick and dirty hash key. Hope collisions will be not too frequent. */
			new_cover_key = new_cover_key << 1;
			/* For rum_ts_distance() */
			if (qr->map_item_operand == NULL)
				new_cover_key += (int)(uintptr_t)ptr->data.item.item;
			/* For rum_tsquery_distance() */
			else
				new_cover_key += (int)(uintptr_t)ptr->data.key.item_first;
			ptr++;
		}

		/* Added by SK */
		/* TODO: use binary tree?.. */
		while(new_cover_idx < ncovers)
		{
			if(new_cover_key == cover_keys[new_cover_idx])
				break;
			new_cover_idx ++;
		}

		if(new_cover_idx == ncovers)
		{
			cover_keys = (int *) repalloc(cover_keys, sizeof(int) *
										  (ncovers + 1));
			cover_lengths = (int *) repalloc(cover_lengths, sizeof(int) *
											 (ncovers + 1));
			cover_ranks = (double *) repalloc(cover_ranks, sizeof(double) *
											  (ncovers + 1));

			cover_lengths[ncovers] = 0;
			cover_ranks[ncovers] = 0;

			ncovers ++;
		}

		cover_keys[new_cover_idx] = new_cover_key;

		/* Compute the number of query terms in the cover */
		for (i = 0; i < qr->lenght; i++)
			if (qr->operandexist[i])
				nitems++;

		Cpos = ((double) (ext.end - ext.begin + 1)) / InvSum;

		if (nitems > 0)
			Cpos *= nitems;

		/*
		 * if doc are big enough then ext.q may be equal to ext.p due to limit
		 * of posional information. In this case we approximate number of
		 * noise word as half cover's length
		 */
		nNoise = (ext.q - ext.p) - (ext.end - ext.begin);
		if (nNoise < 0)
			nNoise = (ext.end - ext.begin) / 2;
		/* SK: Wdoc += Cpos / ((double) (1 + nNoise)); */
		cover_lengths[new_cover_idx] ++;
		cover_ranks[new_cover_idx] += Cpos / ((double) (1 + nNoise))
			/ cover_lengths[new_cover_idx] / cover_lengths[new_cover_idx]
				/ 1.64493406685;

		CurExtPos = ((double) (ext.q + ext.p)) / 2.0;
		if (NExtent > 0 && CurExtPos > PrevExtPos		/* prevent devision by
														 * zero in a case of
				multiple lexize */ )
			SumDist += 1.0 / (CurExtPos - PrevExtPos);

		PrevExtPos = CurExtPos;
		NExtent++;
	}

	/* Added by SK */
	for(i = 0; i < ncovers; i++)
		Wdoc += cover_ranks[i];

	if ((method & RANK_NORM_EXTDIST) && NExtent > 0 && SumDist > 0)
		Wdoc /= ((double) NExtent) / SumDist;

	if (method & RANK_NORM_RDIVRPLUS1)
		Wdoc /= (Wdoc + 1);

	pfree(cover_keys);
	pfree(cover_lengths);
	pfree(cover_ranks);

	return (float4) Wdoc;
}

static float4
calc_score_addinfo(float4 *arrdata, bool *check, TSQuery query,
				   int *map_item_operand, Datum *addInfo, bool *addInfoIsNull,
				   int nkeys)
{
	DocRepresentation *doc;
	uint32		doclen = 0;
	double		Wdoc = 0.0;
	QueryRepresentation qr;

	qr.query = query;
	qr.map_item_operand = map_item_operand;
	qr.operandexist = (bool *) palloc0(sizeof(bool) * nkeys);
	qr.lenght = nkeys;

	doc = get_docrep_addinfo(check, &qr, addInfo, addInfoIsNull, &doclen);
	if (!doc)
	{
		pfree(qr.operandexist);
		return 0.0;
	}

	Wdoc = calc_score_docr(arrdata, doc, doclen, &qr, DEF_NORM_METHOD);

	pfree(doc);
	pfree(qr.operandexist);

	return (float4) Wdoc;
}

static float4
calc_score(float4 *arrdata, TSVector txt, TSQuery query, int method)
{
	DocRepresentation *doc;
	uint32		len,
				doclen = 0;
	double		Wdoc = 0.0;
	QueryRepresentation qr;

	qr.query = query;
	qr.map_item_operand = NULL;
	qr.operandexist = (bool *) palloc0(sizeof(bool) * query->size);
	qr.lenght = query->size;

	doc = get_docrep(txt, &qr, &doclen);
	if (!doc)
	{
		pfree(qr.operandexist);
		return 0.0;
	}

	Wdoc = calc_score_docr(arrdata, doc, doclen, &qr, method);

	if ((method & RANK_NORM_LOGLENGTH) && txt->size > 0)
		Wdoc /= log((double) (count_length(txt) + 1));

	if (method & RANK_NORM_LENGTH)
	{
		len = count_length(txt);
		if (len > 0)
			Wdoc /= (double) len;
	}

	if ((method & RANK_NORM_UNIQ) && txt->size > 0)
		Wdoc /= (double) (txt->size);

	if ((method & RANK_NORM_LOGUNIQ) && txt->size > 0)
		Wdoc /= log((double) (txt->size + 1)) / log(2.0);

	pfree(doc);
	pfree(qr.operandexist);

	return (float4) Wdoc;
}

Datum
rum_tsquery_distance(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);

	TSQuery		query = PG_GETARG_TSQUERY(2);
	int			nkeys = PG_GETARG_INT32(3);
	Pointer	   *extra_data = (Pointer *) PG_GETARG_POINTER(4);
	Datum	   *addInfo = (Datum *) PG_GETARG_POINTER(8);
	bool	   *addInfoIsNull = (bool *) PG_GETARG_POINTER(9);
	float8		res;
	int		   *map_item_operand = (int *) (extra_data[0]);

	res = calc_score_addinfo(weights, check, query, map_item_operand,
							 addInfo, addInfoIsNull, nkeys);

	PG_FREE_IF_COPY(query, 2);
	if (res == 0)
		PG_RETURN_FLOAT8(get_float8_infinity());
	else
		PG_RETURN_FLOAT8(1.0 / res);
}

Datum
rum_ts_distance_tt(PG_FUNCTION_ARGS)
{
	TSVector	txt = PG_GETARG_TSVECTOR(0);
	TSQuery		query = PG_GETARG_TSQUERY(1);
	float4		res;

	res = calc_score(weights, txt, query, DEF_NORM_METHOD);

	PG_FREE_IF_COPY(txt, 0);
	PG_FREE_IF_COPY(query, 1);
	if (res == 0)
		PG_RETURN_FLOAT4(get_float4_infinity());
	else
		PG_RETURN_FLOAT4(1.0 / res);
}

Datum
rum_ts_distance_ttf(PG_FUNCTION_ARGS)
{
	TSVector	txt = PG_GETARG_TSVECTOR(0);
	TSQuery		query = PG_GETARG_TSQUERY(1);
	int			method = PG_GETARG_INT32(2);
	float4		res;

	res = calc_score(weights, txt, query, method);

	PG_FREE_IF_COPY(txt, 0);
	PG_FREE_IF_COPY(query, 1);
	if (res == 0)
		PG_RETURN_FLOAT4(get_float4_infinity());
	else
		PG_RETURN_FLOAT4(1.0 / res);
}

Datum
rum_ts_distance_td(PG_FUNCTION_ARGS)
{
	TSVector	txt = PG_GETARG_TSVECTOR(0);
	HeapTupleHeader d = PG_GETARG_HEAPTUPLEHEADER(1);

	Oid			tupType = HeapTupleHeaderGetTypeId(d);
	int32		tupTypmod = HeapTupleHeaderGetTypMod(d);
	TupleDesc	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
	HeapTupleData tuple;

	TSQuery		query;
	int			method;
	bool		isnull;
	float4		res;

	tuple.t_len = HeapTupleHeaderGetDatumLength(d);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = d;

	query = DatumGetTSQuery(fastgetattr(&tuple, 1, tupdesc, &isnull));
	if (isnull)
	{
		ReleaseTupleDesc(tupdesc);
		PG_FREE_IF_COPY(txt, 0);
		PG_FREE_IF_COPY(d, 1);
		elog(ERROR, "NULL query value is not allowed");
	}

	method = DatumGetInt32(fastgetattr(&tuple, 2, tupdesc, &isnull));
	if (isnull)
		method = 0;

	res = calc_score(weights, txt, query, method);

	ReleaseTupleDesc(tupdesc);
	PG_FREE_IF_COPY(txt, 0);
	PG_FREE_IF_COPY(d, 1);

	if (res == 0)
		PG_RETURN_FLOAT4(get_float4_infinity());
	else
		PG_RETURN_FLOAT4(1.0 / res);
}

Datum
tsquery_to_distance_query(PG_FUNCTION_ARGS)
{
	TSQuery		query = PG_GETARG_TSQUERY(0);

	TupleDesc	tupdesc;
	HeapTuple	htup;
	Datum		values[2];
	bool		nulls[2];

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupdesc = BlessTupleDesc(tupdesc);

	MemSet(nulls, 0, sizeof(nulls));
	values[0] = TSQueryGetDatum(query);
	values[1] = Int32GetDatum(DEF_NORM_METHOD);

	htup = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}

Datum
rum_tsvector_config(PG_FUNCTION_ARGS)
{
	RumConfig  *config = (RumConfig *) PG_GETARG_POINTER(0);

	config->addInfoTypeOid = BYTEAOID;
	config->strategyInfo[0].strategy = InvalidStrategy;

	PG_RETURN_VOID();
}

Datum
rum_ts_join_pos(PG_FUNCTION_ARGS)
{
	Datum		addInfo1 = PG_GETARG_DATUM(0);
	Datum		addInfo2 = PG_GETARG_DATUM(1);
	char	   *in1 = VARDATA_ANY(addInfo1),
			   *in2 = VARDATA_ANY(addInfo2);
	bytea	   *result;
	int			count1 = count_pos(in1, VARSIZE_ANY_EXHDR(addInfo1)),
				count2 = count_pos(in2, VARSIZE_ANY_EXHDR(addInfo2)),
				countRes = 0,
				i1 = 0, i2 = 0, size;
	WordEntryPos pos1 = 0,
				pos2 = 0,
			   *pos;

	result = palloc(VARHDRSZ + sizeof(WordEntryPos) * (count1 + count2));
	pos = palloc(sizeof(WordEntryPos) * (count1 + count2));

	Assert(count1 > 0 && count2 > 0);

	in1 = decompress_pos(in1, &pos1);
	in2 = decompress_pos(in2, &pos2);

	while(i1 < count1 && i2 < count2)
	{
		if (WEP_GETPOS(pos1) > WEP_GETPOS(pos2))
		{
			pos[countRes++] = pos2;
			if (i2 < count2)
				in2 = decompress_pos(in2, &pos2);
			i2++;
		}
		else if (WEP_GETPOS(pos1) < WEP_GETPOS(pos2))
		{
			pos[countRes++] = pos1;
			if (i1 < count1)
				in1 = decompress_pos(in1, &pos1);
			i1++;
		}
		else
		{
			pos[countRes++] = pos1;
			if (i1 < count1)
				in1 = decompress_pos(in1, &pos1);
			if (i2 < count2)
				in2 = decompress_pos(in2, &pos2);
			i1++;
			i2++;
		}
	}

	while(i1 < count1)
	{
		pos[countRes++] = pos1;
		if (i1 < count1)
			in1 = decompress_pos(in1, &pos1);
		i1++;
	}

	while(i2 < count2)
	{
		pos[countRes++] = pos2;
		if (i2 < count2)
			in2 = decompress_pos(in2, &pos2);
		i2++;
	}

	size = compress_pos(result->vl_dat, pos, countRes) + VARHDRSZ;
	SET_VARSIZE(result, size);

	PG_RETURN_BYTEA_P(result);
}
