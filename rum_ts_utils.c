/*-------------------------------------------------------------------------
 *
 * rum_ts_utils.c
 *		various support functions
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "tsearch/ts_type.h"
#include "tsearch/ts_utils.h"

#include "rum.h"

#include <math.h>

PG_FUNCTION_INFO_V1(gin_tsvector_config);
PG_FUNCTION_INFO_V1(gin_tsquery_pre_consistent);
PG_FUNCTION_INFO_V1(gin_tsquery_distance);

static float calc_rank_and(float *w, Datum *addInfo, bool *addInfoIsNull,
						   int size);
static float calc_rank_or(float *w, Datum *addInfo, bool *addInfoIsNull,
						  int size);

typedef struct
{
	QueryItem  *first_item;
	bool	   *check;
	int		   *map_item_operand;
	bool	   *need_recheck;
} GinChkVal;

static bool
checkcondition_gin(void *checkval, QueryOperand *val, ExecPhraseData *data)
{
	GinChkVal  *gcv = (GinChkVal *) checkval;
	int			j;

	/* if any val requiring a weight is used, set recheck flag */
	if (val->weight != 0)
		*(gcv->need_recheck) = true;

	/* convert item's number to corresponding entry's (operand's) number */
	j = gcv->map_item_operand[((QueryItem *) val) - gcv->first_item];

	/* return presence of current entry in indexed value */
	return gcv->check[j];
}

Datum
gin_tsquery_pre_consistent(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);

	TSQuery		query = PG_GETARG_TSQUERY(2);

	Pointer    *extra_data = (Pointer *) PG_GETARG_POINTER(4);
	bool	    recheck;
	bool		res = FALSE;

	if (query->size > 0)
	{
		QueryItem  *item;
		GinChkVal	gcv;

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
						 checkcondition_gin);
	}

	PG_RETURN_BOOL(res);
}

static float weights[] = {0.1f, 0.2f, 0.4f, 1.0f};

#define wpos(wep)	( w[ WEP_GETWEIGHT(wep) ] )
/* A dummy WordEntryPos array to use when haspos is false */
static WordEntryPosVector POSNULL = {
	1,							/* Number of elements that follow */
	{0}
};

#define LOWERMASK 0x1F

/*
 * Returns a weight of a word collocation
 */
static float4
word_distance(int32 w)
{
	if (w > 100)
		return 1e-30f;

	return 1.0 / (1.005 + 0.05 * exp(((float4) w) / 1.5 - 2));
}

static char *
decompress_pos(char *ptr, uint16 *pos)
{
	int i;
	uint8 v;
	uint16 delta = 0;

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
	int count = 0, i;
	for (i = 0; i < len; i++)
	{
		if (!(ptr[i] & HIGHBIT))
			count++;
	}
	return count;
}

static float
calc_rank_and(float *w, Datum *addInfo, bool *addInfoIsNull, int size)
{
	int			i,
				k,
				l,
				p;
	WordEntryPos post,
			   ct;
	int32		dimt,
				lenct,
				dist;
	float		res = -1.0;
	char		*ptrt, *ptrc;

	if (size < 2)
	{
		return calc_rank_or(w, addInfo, addInfoIsNull, size);
	}
	WEP_SETPOS(POSNULL.pos[0], MAXENTRYPOS - 1);

	for (i = 0; i < size; i++)
	{
		if (!addInfoIsNull[i])
		{
			dimt = count_pos(VARDATA_ANY(addInfo[i]), VARSIZE_ANY_EXHDR(addInfo[i]));
			ptrt = (char *)VARDATA_ANY(addInfo[i]);
		}
		else
		{
			dimt = POSNULL.npos;
			ptrt = (char *)POSNULL.pos;
		}
		for (k = 0; k < i; k++)
		{
			if (!addInfoIsNull[k])
				lenct = count_pos(VARDATA_ANY(addInfo[k]), VARSIZE_ANY_EXHDR(addInfo[k]));
			else
				lenct = POSNULL.npos;
			post = 0;
			for (l = 0; l < dimt; l++)
			{
				ptrt = decompress_pos(ptrt, &post);
				ct = 0;
				if (!addInfoIsNull[k])
					ptrc = (char *)VARDATA_ANY(addInfo[k]);
				else
					ptrc = (char *)POSNULL.pos;
				for (p = 0; p < lenct; p++)
				{
					ptrc = decompress_pos(ptrc, &ct);
					dist = Abs((int) WEP_GETPOS(post) - (int) WEP_GETPOS(ct));
					if (dist || (dist == 0 && (ptrt == (char *)POSNULL.pos || ptrc == (char *)POSNULL.pos)))
					{
						float		curw;

						if (!dist)
							dist = MAXENTRYPOS;
						curw = sqrt(wpos(post) * wpos(ct) * word_distance(dist));
						res = (res < 0) ? curw : 1.0 - (1.0 - res) * (1.0 - curw);
					}
				}
			}
		}

	}
	return res;
}

static float
calc_rank_or(float *w, Datum *addInfo, bool *addInfoIsNull, int size)
{
	WordEntryPos post;
	int32		dimt,
				j,
				i;
	float		res = 0.0;
	char *ptrt;

	for (i = 0; i < size; i++)
	{
		float		resj,
					wjm;
		int32		jm;

		if (!addInfoIsNull[i])
		{
			dimt = count_pos(VARDATA_ANY(addInfo[i]), VARSIZE_ANY_EXHDR(addInfo[i]));
			ptrt = (char *)VARDATA_ANY(addInfo[i]);
		}
		else
		{
			dimt = POSNULL.npos;
			ptrt = (char *)POSNULL.pos;
		}

		resj = 0.0;
		wjm = -1.0;
		jm = 0;
		post = 0;
		for (j = 0; j < dimt; j++)
		{
			ptrt = decompress_pos(ptrt, &post);
			resj = resj + wpos(post) / ((j + 1) * (j + 1));
			if (wpos(post) > wjm)
			{
				wjm = wpos(post);
				jm = j;
			}
		}
/*
		limit (sum(i/i^2),i->inf) = pi^2/6
		resj = sum(wi/i^2),i=1,noccurence,
		wi - should be sorted desc,
		don't sort for now, just choose maximum weight. This should be corrected
		Oleg Bartunov
*/
		res = res + (wjm + resj - wjm / ((jm + 1) * (jm + 1))) / 1.64493406685;

	}
	if (size > 0)
		res = res / size;
	return res;
}

static float
calc_rank(float *w, TSQuery q, Datum *addInfo, bool *addInfoIsNull, int size)
{
	QueryItem  *item = GETQUERY(q);
	float		res = 0.0;

	if (!size || !q->size)
		return 0.0;

	/* XXX: What about NOT? */
	res = (item->type == QI_OPR && item->qoperator.oper == OP_AND) ?
		calc_rank_and(w, addInfo, addInfoIsNull, size) : calc_rank_or(w, addInfo, addInfoIsNull, size);

	if (res < 0)
		res = 1e-20f;

	return res;
}

Datum
gin_tsquery_distance(PG_FUNCTION_ARGS)
{
	/* bool	   *check = (bool *) PG_GETARG_POINTER(0); */

	/* StrategyNumber strategy = PG_GETARG_UINT16(1); */
	TSQuery		query = PG_GETARG_TSQUERY(2);

	int32	nkeys = PG_GETARG_INT32(3);
	/* Pointer    *extra_data = (Pointer *) PG_GETARG_POINTER(4); */
	Datum	   *addInfo = (Datum *) PG_GETARG_POINTER(8);
	bool	   *addInfoIsNull = (bool *) PG_GETARG_POINTER(9);
	float8 res;

	res = 1.0 / (float8)calc_rank(weights, query, addInfo, addInfoIsNull, nkeys);

	PG_RETURN_FLOAT8(res);
}

Datum
gin_tsvector_config(PG_FUNCTION_ARGS)
{
	GinConfig *config = (GinConfig *)PG_GETARG_POINTER(0);
	config->addInfoTypeOid = BYTEAOID;
	PG_RETURN_VOID();
}
