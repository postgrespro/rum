/*-------------------------------------------------------------------------
 *
 * rumtsquery.c
 *		Inverted fulltext search: indexing tsqueries.
 *
 * Portions Copyright (c) 2015-2025, Postgres Professional
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
#include "utils/bytea.h"

#include "rum.h"

/*
 * A "wrapper" over tsquery item.  More suitable representation for pocessing.
 */
typedef struct QueryItemWrap
{
	QueryItemType type;
	int8		oper;
	bool		not;
	List	   *operands;
	struct QueryItemWrap *parent;
	int			distance,
				length;
	int			sum;
	int			num;
}	QueryItemWrap;

/*
 * Add child to tsquery item wrap.
 */
static QueryItemWrap *
add_child(QueryItemWrap *parent)
{
	QueryItemWrap *result;

	result = (QueryItemWrap *) palloc0(sizeof(QueryItemWrap));

	if (parent)
	{
		result->parent = parent;
		parent->operands = lappend(parent->operands, result);
	}
	return result;
}

/*
 * Make wrapper over tsquery item.  Flattern tree if needed.
 */
static QueryItemWrap *
make_query_item_wrap(QueryItem *item, QueryItemWrap *parent, bool not)
{
	if (item->type == QI_VAL)
	{
		QueryOperand *operand = (QueryOperand *) item;
		QueryItemWrap *wrap = add_child(parent);

		if (operand->prefix)
			elog(ERROR, "Indexing of prefix tsqueries isn't supported yet");

		wrap->type = QI_VAL;
		wrap->distance = operand->distance;
		wrap->length = operand->length;
		wrap->not = not;
		return wrap;
	}

	switch (item->qoperator.oper)
	{
		case OP_NOT:
			return make_query_item_wrap(item + 1, parent, !not);

		case OP_AND:
		case OP_OR:
			{
				uint8		oper = item->qoperator.oper;

				if (not)
					oper = (oper == OP_AND) ? OP_OR : OP_AND;

				if (!parent || oper != parent->oper)
				{
					QueryItemWrap *wrap = add_child(parent);

					wrap->type = QI_OPR;
					wrap->oper = oper;

					make_query_item_wrap(item + item->qoperator.left, wrap, not);
					make_query_item_wrap(item + 1, wrap, not);
					return wrap;
				}
				else
				{
					make_query_item_wrap(item + item->qoperator.left, parent, not);
					make_query_item_wrap(item + 1, parent, not);
					return NULL;
				}
			}
		case OP_PHRASE:
			elog(ERROR, "Indexing of phrase tsqueries isn't supported yet");
			break;
		default:
			elog(ERROR, "Invalid tsquery operator");
	}

	/* not reachable, but keep compiler quiet */
	return NULL;
}

/*
 * Recursively calculate "sum" for tsquery item wraps.
 */
static int
calc_wraps(QueryItemWrap *wrap, int *num)
{
	int			notCount = 0,
				result;
	ListCell   *lc;

	foreach(lc, wrap->operands)
	{
		QueryItemWrap *item = (QueryItemWrap *) lfirst(lc);

		if (item->not)
			notCount++;
	}

	if (wrap->type == QI_OPR)
	{
		wrap->num = (*num)++;
		if (wrap->oper == OP_AND)
			wrap->sum = notCount + 1 - list_length(wrap->operands);
		if (wrap->oper == OP_OR)
			wrap->sum = notCount;
	}
	else if (wrap->type == QI_VAL)
	{
		return 1;
	}

	result = 0;
	foreach(lc, wrap->operands)
	{
		QueryItemWrap *item = (QueryItemWrap *) lfirst(lc);

		result += calc_wraps(item, num);
	}
	return result;
}

/*
 * Check if tsquery doesn't need any positive lexeme occurence for satisfaction.
 * That is this funciton returns true when tsquery maches empty tsvector.
 */
static bool
check_allnegative(QueryItemWrap * wrap)
{
	if (wrap->type == QI_VAL)
	{
		return wrap->not;
	}
	else if (wrap->oper == OP_AND)
	{
		ListCell   *lc;

		foreach(lc, wrap->operands)
		{
			QueryItemWrap *item = (QueryItemWrap *) lfirst(lc);

			if (!check_allnegative(item))
				return false;
		}
		return true;
	}
	else if (wrap->oper == OP_OR)
	{
		ListCell   *lc;

		foreach(lc, wrap->operands)
		{
			QueryItemWrap *item = (QueryItemWrap *) lfirst(lc);

			if (check_allnegative(item))
				return true;
		}
		return false;
	}
	else
	{
		elog(ERROR, "check_allnegative: invalid node");
		return false;
	}

}

/* Max length of variable-length encoded 32-bit integer */
#define MAX_ENCODED_LEN 5

/*
 * Varbyte-encode 'val' into *ptr. *ptr is incremented to next integer.
 */
static void
encode_varbyte(uint32 val, unsigned char **ptr)
{
	unsigned char *p = *ptr;

	while (val > 0x7F)
	{
		*(p++) = 0x80 | (val & 0x7F);
		val >>= 7;
	}
	*(p++) = (unsigned char) val;

	*ptr = p;
}

/*
 * Decode varbyte-encoded integer at *ptr. *ptr is incremented to next integer.
 */
static uint32
decode_varbyte(unsigned char **ptr)
{
	uint32		val;
	unsigned char *p = *ptr;
	uint32		c;

	c = *(p++);
	val = c & 0x7F;
	if (c & 0x80)
	{
		c = *(p++);
		val |= (c & 0x7F) << 7;
		if (c & 0x80)
		{
			c = *(p++);
			val |= (c & 0x7F) << 14;
			if (c & 0x80)
			{
				c = *(p++);
				val |= (c & 0x7F) << 21;
				if (c & 0x80)
				{
					c = *(p++);
					val |= (c & 0x7F) << 28;
				}
			}
		}
	}

	*ptr = p;

	return val;
}

typedef struct
{
	Datum	   *addInfo;
	bool	   *addInfoIsNull;
	Datum	   *entries;
	int			index;
	char	   *operand;
}	ExtractContext;

/*
 * Recursively extract entries from tsquery wraps.  Encode paths into addInfos.
 */
static void
extract_wraps(QueryItemWrap *wrap, ExtractContext *context, int level)
{
	if (wrap->type == QI_VAL)
	{
		bytea	   *addinfo;
		unsigned char *ptr;
		int			index;

		/* Check if given lexeme was already extracted */
		for (index = 0; index < context->index; index++)
		{
			text	   *entry;

			entry = DatumGetByteaP(context->entries[index]);
			if (VARSIZE_ANY_EXHDR(entry) == wrap->length &&
				!memcmp(context->operand + wrap->distance, VARDATA_ANY(entry), wrap->length))
				break;
		}

		/* Either allocate new addInfo or extend existing addInfo */
		if (index >= context->index)
		{
			index = context->index;
			addinfo = (bytea *) palloc(VARHDRSZ + 2 * Max(level, 1) * MAX_ENCODED_LEN);
			ptr = (unsigned char *) VARDATA(addinfo);
			context->entries[index] = PointerGetDatum(cstring_to_text_with_len(context->operand + wrap->distance, wrap->length));
			context->addInfo[index] = PointerGetDatum(addinfo);
			context->addInfoIsNull[index] = false;
			context->index++;
		}
		else
		{
			addinfo = DatumGetByteaP(context->addInfo[index]);
			addinfo = (bytea *) repalloc(addinfo,
					 VARSIZE(addinfo) + 2 * Max(level, 1) * MAX_ENCODED_LEN);
			context->addInfo[index] = PointerGetDatum(addinfo);
			ptr = (unsigned char *) VARDATA(addinfo) + VARSIZE_ANY_EXHDR(addinfo);
		}

		/* Encode path into addInfo */
		while (wrap->parent)
		{
			QueryItemWrap *parent = wrap->parent;
			uint32		sum;

			encode_varbyte((uint32) parent->num, &ptr);
			sum = (uint32) abs(parent->sum);
			sum <<= 2;
			if (parent->sum < 0)
				sum |= 2;
			if (wrap->not)
				sum |= 1;
			encode_varbyte(sum, &ptr);
			wrap = parent;
		}
		if (level == 0 && wrap->not)
		{
			encode_varbyte(1, &ptr);
			encode_varbyte(4 | 1, &ptr);
		}
		SET_VARSIZE(addinfo, ptr - (unsigned char *) addinfo);
	}
	else if (wrap->type == QI_OPR)
	{
		ListCell   *lc;

		foreach(lc, wrap->operands)
		{
			QueryItemWrap *item = (QueryItemWrap *) lfirst(lc);

			extract_wraps(item, context, level + 1);
		}
	}
}

PG_FUNCTION_INFO_V1(ruminv_extract_tsquery);
Datum
ruminv_extract_tsquery(PG_FUNCTION_ARGS)
{
	TSQuery		query = PG_GETARG_TSQUERY(0);
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	bool	  **nullFlags = (bool **) PG_GETARG_POINTER(2);
	Datum	  **addInfo = (Datum **) PG_GETARG_POINTER(3);
	bool	  **addInfoIsNull = (bool **) PG_GETARG_POINTER(4);
	Datum	   *entries = NULL;
	QueryItem  *item = GETQUERY(query);
	QueryItemWrap *wrap;
	ExtractContext context;
	int			num = 1,
				count;
	bool		extractNull;

	wrap = make_query_item_wrap(item, NULL, false);
	count = calc_wraps(wrap, &num);
	extractNull = check_allnegative(wrap);
	if (extractNull)
		count++;

	entries = (Datum *) palloc(sizeof(Datum) * count);
	*addInfo = (Datum *) palloc(sizeof(Datum) * count);
	*addInfoIsNull = (bool *) palloc(sizeof(bool) * count);

	context.addInfo = *addInfo;
	context.addInfoIsNull = *addInfoIsNull;
	context.entries = entries;
	context.operand = GETOPERAND(query);
	context.index = 0;

	extract_wraps(wrap, &context, 0);

	count = context.index;
	if (extractNull)
	{
		int			i;

		count++;
		*nullFlags = (bool *) palloc(sizeof(bool) * count);
		for (i = 0; i < count - 1; i++)
			(*nullFlags)[i] = false;
		(*nullFlags)[count - 1] = true;
		(*addInfoIsNull)[count - 1] = true;
	}
	*nentries = count;

	PG_FREE_IF_COPY(query, 0);
	PG_RETURN_POINTER(entries);
}

PG_FUNCTION_INFO_V1(ruminv_extract_tsvector);
Datum
ruminv_extract_tsvector(PG_FUNCTION_ARGS)
{
	TSVector	vector = PG_GETARG_TSVECTOR(0);
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);

	/* StrategyNumber strategy = PG_GETARG_UINT16(2); */
	bool	  **ptr_partialmatch = (bool **) PG_GETARG_POINTER(3);
	Pointer   **extra_data = (Pointer **) PG_GETARG_POINTER(4);

	bool	  **nullFlags = (bool **) PG_GETARG_POINTER(5);
	int32	   *searchMode = (int32 *) PG_GETARG_POINTER(6);
	Datum	   *entries = NULL;

	*searchMode = GIN_SEARCH_MODE_DEFAULT;

	if (vector->size > 0)
	{
		int			i;
		WordEntry  *we = ARRPTR(vector);

		*nentries = vector->size + 1;
		*extra_data = NULL;
		*ptr_partialmatch = NULL;

		entries = (Datum *) palloc(sizeof(Datum) * (*nentries));
		*nullFlags = (bool *) palloc(sizeof(bool) * (*nentries));

		for (i = 0; i < vector->size; i++)
		{
			text	   *txt;

			txt = cstring_to_text_with_len(STRPTR(vector) + we[i].pos, we[i].len);
			entries[i] = PointerGetDatum(txt);
			(*nullFlags)[i] = false;
		}
		(*nullFlags)[*nentries - 1] = true;
	}
	else
	{
		*nentries = 0;
	}
	PG_FREE_IF_COPY(vector, 0);
	PG_RETURN_POINTER(entries);
}

typedef struct
{
	int			sum;
	int			parent;
	bool		not;
}	TmpNode;

PG_FUNCTION_INFO_V1(ruminv_tsvector_consistent);
Datum
ruminv_tsvector_consistent(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);

	/* StrategyNumber strategy = PG_GETARG_UINT16(1); */
	/* TSVector vector = PG_GETARG_TSVECTOR(2); */
	int32		nkeys = PG_GETARG_INT32(3);

	/* Pointer	   *extra_data = (Pointer *) PG_GETARG_POINTER(4); */
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);
	Datum	   *addInfo = (Datum *) PG_GETARG_POINTER(8);
	bool	   *addInfoIsNull = (bool *) PG_GETARG_POINTER(9);
	bool		res = false,
				allFalse = true;
	int			i,
				lastIndex = 0;
	TmpNode		nodes[256];

	*recheck = false;

	for (i = 0; i < nkeys - 1; i++)
	{
		unsigned char *ptr,
				   *ptrEnd;
		int			size;
		TmpNode    *child = NULL;

		if (!check[i])
			continue;

		allFalse = false;

		if (addInfoIsNull[i])
			elog(ERROR, "Unexpected addInfoIsNull");

		/* Iterate path making corresponding calculation */
		ptr = (unsigned char *) VARDATA_ANY(DatumGetPointer(addInfo[i]));
		size = VARSIZE_ANY_EXHDR(DatumGetPointer(addInfo[i]));

		if (size == 0)
		{
			res = true;
			break;
		}

		ptrEnd = ptr + size;
		while (ptr < ptrEnd)
		{
			uint32		num = decode_varbyte(&ptr),
						sumVal = decode_varbyte(&ptr);
			int			sum,
						index;
			bool		not;

			not = (sumVal & 1) ? true : false;
			sum = sumVal >> 2;
			sum = (sumVal & 2) ? (-sum) : (sum);

			index = num - 1;

			if (child)
			{
				child->parent = index;
				child->not = not;
			}

			while (num > lastIndex)
			{
				nodes[lastIndex].parent = -2;
				lastIndex++;
			}

			if (nodes[index].parent == -2)
			{
				nodes[index].sum = sum;
				nodes[index].parent = -1;
				nodes[index].not = false;
			}
			if (!child)
			{
				if (not)
					nodes[index].sum--;
				else
					nodes[index].sum++;
			}

			if (index == 0)
				child = NULL;
			else
				child = &nodes[index];
		}
	}

	/* Iterate over nodes */
	if (allFalse && check[nkeys - 1])
	{
		res = true;
	}
	else
	{
		for (i = lastIndex - 1; i >= 0; i--)
		{
			if (nodes[i].parent != -2)
			{
				if (nodes[i].sum > 0)
				{
					if (nodes[i].parent == -1)
					{
						res = true;
						break;
					}
					else
					{
						int			parent = nodes[i].parent;

						nodes[parent].sum += nodes[i].not ? -1 : 1;
					}
				}
			}
		}
	}

	PG_RETURN_BOOL(res);
}

PG_FUNCTION_INFO_V1(ruminv_tsquery_config);
Datum
ruminv_tsquery_config(PG_FUNCTION_ARGS)
{
	RumConfig  *config = (RumConfig *) PG_GETARG_POINTER(0);

	config->addInfoTypeOid = BYTEAOID;
	config->strategyInfo[0].strategy = InvalidStrategy;

	PG_RETURN_VOID();
}
