/*-------------------------------------------------------------------------
 *
 * rum_ts_utils.c
 *		various support functions
 *
 * Portions Copyright (c) 2015-2016, Postgres Professional
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "tsearch/ts_type.h"
#include "tsearch/ts_utils.h"
#include "utils/array.h"
#include "utils/builtins.h"

#include "rum.h"

#include <math.h>

PG_FUNCTION_INFO_V1(rum_extract_tsvector);
PG_FUNCTION_INFO_V1(rum_extract_tsquery);
PG_FUNCTION_INFO_V1(rum_tsvector_config);
PG_FUNCTION_INFO_V1(rum_tsquery_pre_consistent);
PG_FUNCTION_INFO_V1(rum_tsquery_consistent);
PG_FUNCTION_INFO_V1(rum_tsquery_timestamp_consistent);
PG_FUNCTION_INFO_V1(rum_tsquery_distance);
PG_FUNCTION_INFO_V1(rum_ts_distance);

static int	count_pos(char *ptr, int len);
static char *decompress_pos(char *ptr, uint16 *pos);

typedef struct
{
	QueryItem  *first_item;
	int		   *map_item_operand;
	bool	   *check;
	bool	   *need_recheck;
	Datum	   *addInfo;
	bool	   *addInfoIsNull;
	bool		notPhrase;
}	RumChkVal;

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
						 false,
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
	if (data && gcv->addInfo && gcv->addInfoIsNull[j] == false)
	{
		bytea	   *positions;
		int32		i;
		char	   *ptrt;
		WordEntryPos post;

		if (gcv->notPhrase)
			elog(ERROR, "phrase search isn't supported yet");

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
		gcv.notPhrase = false;

		res = TS_execute(GETQUERY(query), &gcv, true, checkcondition_rum);
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
		gcv.notPhrase = true;

		res = TS_execute(GETQUERY(query), &gcv, true, checkcondition_rum);
	}

	PG_RETURN_BOOL(res);
}

#define SIXTHBIT 0x20
#define LOWERMASK 0x1F

static int
compress_pos(char *target, uint16 *pos, int npos)
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
decompress_pos(char *ptr, uint16 *pos)
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

/*
 * reconstruct partial tsvector from set of index entries
 */
static TSVector
rum_reconstruct_tsvector(bool *check, TSQuery query, int *map_item_operand,
						 Datum *addInfo, bool *addInfoIsNull)
{
	TSVector	tsv;
	int			cntwords = 0;
	int			i = 0;
	QueryItem  *item = GETQUERY(query);
	char	   *operandData = GETOPERAND(query);
	struct
	{
		char		*word;
		char		*posptr;
		int32		npos;
		int32		wordlen;
	} *restoredWordEntry;
	int			len = 0, totallen;
	bool	   *visited;
	WordEntry  *ptr;
	char	   *str;
	int			stroff;


	restoredWordEntry = palloc(sizeof(*restoredWordEntry) * query->size);
	visited = palloc0(sizeof(*visited) * query->size);

	/*
	 * go through query to collect lexemes and add to them
	 * positions from addInfo. Here we believe that keys are
	 * ordered in the same order as in tsvector (see SortAndUniqItems)
	 */
	for(i=0; i<query->size; i++)
	{
		if (item->type == QI_VAL)
		{
			int	keyN = map_item_operand[i];

			if (check[keyN] == true && visited[keyN] == false)
			{
				/*
				 * entries could be repeated in tsquery, do not visit them twice
				 * or more
				 */
				visited[keyN] = true;

				restoredWordEntry[cntwords].word = operandData + item->qoperand.distance;
				restoredWordEntry[cntwords].wordlen = item->qoperand.length;

				len += item->qoperand.length;

				if (addInfoIsNull[keyN] == false)
				{
					bytea	*positions = DatumGetByteaP(addInfo[keyN]);

					restoredWordEntry[cntwords].npos = count_pos(VARDATA_ANY(positions),
												   VARSIZE_ANY_EXHDR(positions));
					restoredWordEntry[cntwords].posptr = VARDATA_ANY(positions);

					len = SHORTALIGN(len);
					len += sizeof(uint16) +
								restoredWordEntry[cntwords].npos * sizeof(WordEntryPos);
				}
				else
				{
					restoredWordEntry[cntwords].npos = 0;
				}

				cntwords++;
			}
		}
		item++;
	}

	totallen = CALCDATASIZE(cntwords, len);
	tsv = palloc(totallen);
	SET_VARSIZE(tsv, totallen);
	tsv->size = cntwords;

	ptr = ARRPTR(tsv);
	str = STRPTR(tsv);
	stroff = 0;

	for (i=0; i<cntwords; i++)
	{
		ptr->len = restoredWordEntry[i].wordlen;
		ptr->pos = stroff;
		memcpy(str + stroff, restoredWordEntry[i].word, ptr->len);
		stroff += ptr->len;

		if (restoredWordEntry[i].npos)
		{
			WordEntryPos	*wptr,
							post = 0;
			int				j;

			ptr->haspos = 1;

			stroff = SHORTALIGN(stroff);
			*(uint16 *) (str + stroff) = restoredWordEntry[i].npos;
			wptr = POSDATAPTR(tsv, ptr);

			for (j=0; j<restoredWordEntry[i].npos; j++)
			{
				restoredWordEntry[i].posptr = decompress_pos(restoredWordEntry[i].posptr, &post);
				wptr[j] = post;
			}
			stroff += sizeof(uint16) + restoredWordEntry[i].npos * sizeof(WordEntryPos);
		}
		else
		{
			ptr->haspos = 0;
		}

		ptr++;
	}

	pfree(restoredWordEntry);
	pfree(visited);

	return tsv;
}

Datum
rum_tsquery_distance(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);

	/* StrategyNumber strategy = PG_GETARG_UINT16(1); */
	TSQuery		query = PG_GETARG_TSQUERY(2);
	/* int32		nkeys = PG_GETARG_INT32(3); */
	Pointer	   *extra_data = (Pointer *) PG_GETARG_POINTER(4);
	Datum	   *addInfo = (Datum *) PG_GETARG_POINTER(8);
	bool	   *addInfoIsNull = (bool *) PG_GETARG_POINTER(9);
	float8		res;
	int		   *map_item_operand = (int *) (extra_data[0]);
	TSVector	tsv;

	tsv = rum_reconstruct_tsvector(check, query, map_item_operand,
								   addInfo, addInfoIsNull);

	res = DatumGetFloat4(DirectFunctionCall2Coll(ts_rank_tt,
												 PG_GET_COLLATION(),
												 TSVectorGetDatum(tsv),
												 TSQueryGetDatum(query)));

	pfree(tsv);

	if (res == 0)
		PG_RETURN_FLOAT8(get_float8_infinity());
	else
		PG_RETURN_FLOAT8(1.0 / res);
}

Datum
rum_ts_distance(PG_FUNCTION_ARGS)
{
	float4		r = DatumGetFloat4(DirectFunctionCall2Coll(ts_rank_tt,
														   PG_GET_COLLATION(),
														   PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	if (r == 0)
		PG_RETURN_FLOAT4(get_float4_infinity());
	else
		PG_RETURN_FLOAT4(1.0 / r);
}

Datum
rum_tsvector_config(PG_FUNCTION_ARGS)
{
	RumConfig  *config = (RumConfig *) PG_GETARG_POINTER(0);

	config->addInfoTypeOid = BYTEAOID;
	PG_RETURN_VOID();
}
