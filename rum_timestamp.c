#include "postgres.h"

#include <limits.h>

#include "access/stratnum.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"

typedef struct QueryInfo
{
	StrategyNumber	strategy;
	Datum			datum;
} QueryInfo;


PG_FUNCTION_INFO_V1(rum_timestamp_extract_value);
Datum
rum_timestamp_extract_value(PG_FUNCTION_ARGS)
{
	Datum	datum = PG_GETARG_DATUM(0);
	int32	*nentries = (int32 *) PG_GETARG_POINTER(1);
	Datum	*entries = (Datum *) palloc(sizeof(Datum));

	entries[0] = datum;
	*nentries = 1;

	PG_RETURN_POINTER(entries);
}

PG_FUNCTION_INFO_V1(rum_timestamp_extract_query);
Datum
rum_timestamp_extract_query(PG_FUNCTION_ARGS)
{
	Datum	   datum = PG_GETARG_DATUM(0);
	int32	  *nentries = (int32 *) PG_GETARG_POINTER(1);
	StrategyNumber strategy = PG_GETARG_UINT16(2);
	bool	  **partialmatch = (bool **) PG_GETARG_POINTER(3);
	Pointer   **extra_data = (Pointer **) PG_GETARG_POINTER(4);
	Datum	  *entries = (Datum *) palloc(sizeof(Datum));
	QueryInfo  *data = (QueryInfo *) palloc(sizeof(QueryInfo));
	bool	   *ptr_partialmatch;

	*nentries = 1;
	ptr_partialmatch = *partialmatch = (bool *) palloc(sizeof(bool));
	*ptr_partialmatch = false;
	data->strategy = strategy;
	data->datum = datum;
	*extra_data = (Pointer *) palloc(sizeof(Pointer));
	**extra_data = (Pointer) data;

	switch(strategy)
	{
		case BTLessStrategyNumber:
		case BTLessEqualStrategyNumber:
			entries[0] = TimestampGetDatum(DT_NOBEGIN); /* leftmost */
			*ptr_partialmatch = true;
			break;
		case BTGreaterEqualStrategyNumber:
		case BTGreaterStrategyNumber:
			*ptr_partialmatch = true;
		case BTEqualStrategyNumber:
			entries[0] = datum;
			break;
		default:
			elog(ERROR, "unrecognized strategy number: %d", strategy);
	}

	PG_RETURN_POINTER(entries);
}

PG_FUNCTION_INFO_V1(rum_timestamp_compare_prefix);
Datum
rum_timestamp_compare_prefix(PG_FUNCTION_ARGS)
{
	Datum	   a = PG_GETARG_DATUM(0);
	Datum	   b = PG_GETARG_DATUM(1);
	QueryInfo  *data = (QueryInfo *) PG_GETARG_POINTER(3);
	int32	   res, cmp;

	cmp = DatumGetInt32(DirectFunctionCall2Coll(timestamp_cmp,
												PG_GET_COLLATION(),
								(data->strategy == BTLessStrategyNumber ||
								 data->strategy == BTLessEqualStrategyNumber)
										?  data->datum : a, b));

	switch (data->strategy)
	{
		case BTLessStrategyNumber:
			/* If original datum > indexed one then return match */
			if (cmp > 0)
				res = 0;
			else
				res = 1;
			break;
		case BTLessEqualStrategyNumber:
			/* The same except equality */
			if (cmp >= 0)
				res = 0;
			else
				res = 1;
			break;
		case BTEqualStrategyNumber:
			if (cmp != 0)
				res = 1;
			else
				res = 0;
			break;
		case BTGreaterEqualStrategyNumber:
			/* If original datum <= indexed one then return match */
			if (cmp <= 0)
				res = 0;
			else
				res = 1;
			break;
		case BTGreaterStrategyNumber:
			/* If original datum <= indexed one then return match */
			/* If original datum == indexed one then continue scan */
			if (cmp < 0)
				res = 0;
			else if (cmp == 0)
				res = -1;
			else
				res = 1;
			break;
		default:
			elog(ERROR, "unrecognized strategy number: %d", data->strategy);
			res = 0;
	}

	PG_RETURN_INT32(res);
}

PG_FUNCTION_INFO_V1(rum_timestamp_consistent);
Datum
rum_timestamp_consistent(PG_FUNCTION_ARGS)
{
	bool	*recheck = (bool *) PG_GETARG_POINTER(5);

	*recheck = false;
	PG_RETURN_BOOL(true);
}
