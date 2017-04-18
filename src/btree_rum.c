#include "postgres.h"

#include <limits.h>

#include "access/stratnum.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/cash.h"
#include "utils/date.h"
#include "utils/inet.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"
#include "utils/varbit.h"

#include "rum.h"

typedef struct QueryInfo
{
	StrategyNumber strategy;
	Datum		datum;
	bool		is_varlena;
	Datum		(*typecmp) (FunctionCallInfo);
} QueryInfo;


/*** RUM support functions shared by all datatypes ***/

static Datum
rum_btree_extract_value(FunctionCallInfo fcinfo, bool is_varlena)
{
	Datum		datum = PG_GETARG_DATUM(0);
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	Datum	   *entries = (Datum *) palloc(sizeof(Datum));

	if (is_varlena)
		datum = PointerGetDatum(PG_DETOAST_DATUM(datum));
	entries[0] = datum;
	*nentries = 1;

	PG_RETURN_POINTER(entries);
}

/*
 * For BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber, and
 * BTEqualStrategyNumber we want to start the index scan at the
 * supplied query datum, and work forward. For BTLessStrategyNumber
 * and BTLessEqualStrategyNumber, we need to start at the leftmost
 * key, and work forward until the supplied query datum (which must be
 * sent along inside the QueryInfo structure).
 */
static Datum
rum_btree_extract_query(FunctionCallInfo fcinfo,
						bool is_varlena,
						Datum (*leftmostvalue) (void),
						Datum (*typecmp) (FunctionCallInfo))
{
	Datum		datum = PG_GETARG_DATUM(0);
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	StrategyNumber strategy = PG_GETARG_UINT16(2);
	bool	  **partialmatch = (bool **) PG_GETARG_POINTER(3);
	Pointer   **extra_data = (Pointer **) PG_GETARG_POINTER(4);
	Datum	   *entries = (Datum *) palloc(sizeof(Datum));
	QueryInfo  *data = (QueryInfo *) palloc(sizeof(QueryInfo));
	bool	   *ptr_partialmatch;

	*nentries = 1;
	ptr_partialmatch = *partialmatch = (bool *) palloc(sizeof(bool));
	*ptr_partialmatch = false;
	if (is_varlena)
		datum = PointerGetDatum(PG_DETOAST_DATUM(datum));
	data->strategy = strategy;
	data->datum = datum;
	data->is_varlena = is_varlena;
	data->typecmp = typecmp;
	*extra_data = (Pointer *) palloc(sizeof(Pointer));
	**extra_data = (Pointer) data;

	switch (strategy)
	{
		case BTLessStrategyNumber:
		case BTLessEqualStrategyNumber:
			entries[0] = leftmostvalue();
			*ptr_partialmatch = true;
			break;
		case BTGreaterEqualStrategyNumber:
		case BTGreaterStrategyNumber:
			*ptr_partialmatch = true;
		case BTEqualStrategyNumber:
		case RUM_DISTANCE:
		case RUM_LEFT_DISTANCE:
		case RUM_RIGHT_DISTANCE:
			entries[0] = datum;
			break;
		default:
			elog(ERROR, "unrecognized strategy number: %d", strategy);
	}

	PG_RETURN_POINTER(entries);
}

/*
 * Datum a is a value from extract_query method and for BTLess*
 * strategy it is a left-most value.  So, use original datum from QueryInfo
 * to decide to stop scanning or not.  Datum b is always from index.
 */
static Datum
rum_btree_compare_prefix(FunctionCallInfo fcinfo)
{
	Datum		a = PG_GETARG_DATUM(0);
	Datum		b = PG_GETARG_DATUM(1);
	QueryInfo  *data = (QueryInfo *) PG_GETARG_POINTER(3);
	int32		res,
				cmp;

	cmp = DatumGetInt32(DirectFunctionCall2Coll(
												data->typecmp,
												PG_GET_COLLATION(),
								   (data->strategy == BTLessStrategyNumber ||
								 data->strategy == BTLessEqualStrategyNumber)
												? data->datum : a,
												b));

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
			elog(ERROR, "unrecognized strategy number: %d",
				 data->strategy);
			res = 0;
	}

	PG_RETURN_INT32(res);
}

PG_FUNCTION_INFO_V1(rum_btree_consistent);
Datum
rum_btree_consistent(PG_FUNCTION_ARGS)
{
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);

	*recheck = false;
	PG_RETURN_BOOL(true);
}


/*** RUM_SUPPORT macro defines the datatype specific functions ***/

#define RUM_SUPPORT(type, is_varlena, leftmostvalue, typecmp)				\
PG_FUNCTION_INFO_V1(rum_##type##_extract_value);							\
Datum																		\
rum_##type##_extract_value(PG_FUNCTION_ARGS)								\
{																			\
	return rum_btree_extract_value(fcinfo, is_varlena);						\
}	\
PG_FUNCTION_INFO_V1(rum_##type##_extract_query);							\
Datum																		\
rum_##type##_extract_query(PG_FUNCTION_ARGS)								\
{																			\
	return rum_btree_extract_query(fcinfo,									\
								   is_varlena, leftmostvalue, typecmp);		\
}	\
PG_FUNCTION_INFO_V1(rum_##type##_compare_prefix);							\
Datum																		\
rum_##type##_compare_prefix(PG_FUNCTION_ARGS)								\
{																			\
	return rum_btree_compare_prefix(fcinfo);								\
}

#define RUM_SUPPORT_DIST(type, is_varlena, leftmostvalue, typecmp, isinfinite, subtract) \
RUM_SUPPORT(type, is_varlena, leftmostvalue, typecmp)						\
PG_FUNCTION_INFO_V1(rum_##type##_config);									\
Datum																		\
rum_##type##_config(PG_FUNCTION_ARGS)										\
{																			\
	RumConfig  *config = (RumConfig *) PG_GETARG_POINTER(0);				\
																			\
	config->addInfoTypeOid = InvalidOid;									\
																			\
	config->strategyInfo[0].strategy = RUM_LEFT_DISTANCE;					\
	config->strategyInfo[0].direction = BackwardScanDirection;				\
																			\
	config->strategyInfo[1].strategy = RUM_RIGHT_DISTANCE;					\
	config->strategyInfo[1].direction = ForwardScanDirection;				\
																			\
	config->strategyInfo[2].strategy = InvalidStrategy;						\
																			\
	PG_RETURN_VOID();														\
}	\
PG_FUNCTION_INFO_V1(rum_##type##_distance);									\
Datum																		\
rum_##type##_distance(PG_FUNCTION_ARGS)										\
{																			\
	Datum	a = PG_GETARG_DATUM(0);											\
	Datum	b = PG_GETARG_DATUM(1);											\
	double	diff;															\
																			\
	if (isinfinite(a) || isinfinite(b))										\
	{																		\
		if (isinfinite(a) && isinfinite(b))									\
			diff = 0;														\
		else																\
			diff = get_float8_infinity();									\
	}																		\
	else																	\
	{																		\
		int r = DatumGetInt32(DirectFunctionCall2Coll(						\
					typecmp, PG_GET_COLLATION(), a, b));					\
																			\
		diff = (r > 0)	? subtract(a, b) : subtract(b, a);					\
	}																		\
																			\
	PG_RETURN_FLOAT8(diff);													\
}																			\
PG_FUNCTION_INFO_V1(rum_##type##_left_distance);							\
Datum																		\
rum_##type##_left_distance(PG_FUNCTION_ARGS)								\
{																			\
	Datum	a = PG_GETARG_DATUM(0);											\
	Datum	b = PG_GETARG_DATUM(1);											\
	double	diff;															\
																			\
	if (isinfinite(a) || isinfinite(b))										\
	{																		\
		if (isinfinite(a) && isinfinite(b))									\
			diff = 0;														\
		else																\
			diff = get_float8_infinity();									\
	}																		\
	else																	\
	{																		\
		int r = DatumGetInt32(DirectFunctionCall2Coll(						\
					typecmp, PG_GET_COLLATION(), a, b));					\
																			\
		diff = (r > 0)	? get_float8_infinity() : subtract(b, a);			\
	}																		\
																			\
	PG_RETURN_FLOAT8(diff);													\
}																			\
PG_FUNCTION_INFO_V1(rum_##type##_right_distance);							\
Datum																		\
rum_##type##_right_distance(PG_FUNCTION_ARGS)								\
{																			\
	Datum	a = PG_GETARG_DATUM(0);											\
	Datum	b = PG_GETARG_DATUM(1);											\
	double	diff;															\
																			\
	if (isinfinite(a) || isinfinite(b))										\
	{																		\
		if (isinfinite(a) && isinfinite(b))									\
			diff = 0;														\
		else																\
			diff = get_float8_infinity();									\
	}																		\
	else																	\
	{																		\
		int r = DatumGetInt32(DirectFunctionCall2Coll(						\
					typecmp, PG_GET_COLLATION(), a, b));					\
																			\
		diff = (r > 0)	? subtract(a, b) : get_float8_infinity();			\
	}																		\
																			\
	PG_RETURN_FLOAT8(diff);													\
}																			\
PG_FUNCTION_INFO_V1(rum_##type##_outer_distance);							\
Datum																		\
rum_##type##_outer_distance(PG_FUNCTION_ARGS)								\
{																			\
	StrategyNumber	strategy = PG_GETARG_UINT16(2);							\
	Datum			diff;													\
																			\
	switch (strategy)														\
	{																		\
		 case RUM_DISTANCE:													\
			diff = DirectFunctionCall2(rum_##type##_distance,				\
									   PG_GETARG_DATUM(0),					\
									   PG_GETARG_DATUM(1));					\
			break;															\
		 case RUM_LEFT_DISTANCE:											\
			diff = DirectFunctionCall2(rum_##type##_left_distance,			\
									   PG_GETARG_DATUM(0),					\
									   PG_GETARG_DATUM(1));					\
			break;															\
		 case RUM_RIGHT_DISTANCE:											\
			diff = DirectFunctionCall2(rum_##type##_right_distance,			\
									   PG_GETARG_DATUM(0),					\
									   PG_GETARG_DATUM(1));					\
			break;															\
		default:															\
			elog(ERROR, "rum_outer_distance_%s: unknown strategy %u",		\
						#type, strategy);									\
	}																		\
																			\
	PG_RETURN_DATUM(diff);													\
}

static bool
always_false(Datum a)
{
	return false;
}

/*** Datatype specifications ***/

static Datum
leftmostvalue_int2(void)
{
	return Int16GetDatum(SHRT_MIN);
}

static float8
int2subtract(Datum a, Datum b)
{
	return ((float8)DatumGetInt16(a)) - ((float8)DatumGetInt16(b));
}

RUM_SUPPORT_DIST(int2, false, leftmostvalue_int2, btint2cmp, always_false, int2subtract)

static Datum
leftmostvalue_int4(void)
{
	return Int32GetDatum(INT_MIN);
}

static float8
int4subtract(Datum a, Datum b)
{
	return ((float8)DatumGetInt32(a)) - ((float8)DatumGetInt32(b));
}

RUM_SUPPORT_DIST(int4, false, leftmostvalue_int4, btint4cmp, always_false, int4subtract)

static Datum
leftmostvalue_int8(void)
{
	return Int64GetDatum(PG_INT64_MIN);
}

static float8
int8subtract(Datum a, Datum b)
{
	return ((float8)DatumGetInt64(a)) - ((float8)DatumGetInt64(b));
}

RUM_SUPPORT_DIST(int8, false, leftmostvalue_int8, btint8cmp, always_false, int8subtract)

static Datum
leftmostvalue_float4(void)
{
	return Float4GetDatum(-get_float4_infinity());
}

static bool
float4_is_infinite(Datum a)
{
	return !isfinite(DatumGetFloat4(a));
}

static float8
float4subtract(Datum a, Datum b)
{
	return ((float8)DatumGetFloat4(a)) - ((float8)DatumGetFloat4(b));
}

RUM_SUPPORT_DIST(float4, false, leftmostvalue_float4, btfloat4cmp,
			float4_is_infinite, float4subtract)

static Datum
leftmostvalue_float8(void)
{
	return Float8GetDatum(-get_float8_infinity());
}

static bool
float8_is_infinite(Datum a)
{
	return !isfinite(DatumGetFloat8(a));
}

static float8
float8subtract(Datum a, Datum b)
{
	return DatumGetFloat8(a) - DatumGetFloat8(b);
}

RUM_SUPPORT_DIST(float8, false, leftmostvalue_float8, btfloat8cmp,
			float8_is_infinite, float8subtract)

static Datum
leftmostvalue_money(void)
{
	return Int64GetDatum(PG_INT64_MIN);
}

RUM_SUPPORT_DIST(money, false, leftmostvalue_money, cash_cmp, always_false, int8subtract)

static Datum
leftmostvalue_oid(void)
{
	return ObjectIdGetDatum(0);
}

static float8
oidsubtract(Datum a, Datum b)
{
	return ((float8)DatumGetObjectId(a)) - ((float8)DatumGetObjectId(b));
}

RUM_SUPPORT_DIST(oid, false, leftmostvalue_oid, btoidcmp, always_false, oidsubtract)

static Datum
leftmostvalue_timestamp(void)
{
	return TimestampGetDatum(DT_NOBEGIN);
}

static bool
timestamp_is_infinite(Datum a)
{
	return TIMESTAMP_NOT_FINITE(DatumGetTimestamp(a));
}

static float8
timestamp_subtract(Datum a, Datum b)
{
	return	(DatumGetTimestamp(a) - DatumGetTimestamp(b)) / 1e6;
}

RUM_SUPPORT_DIST(timestamp, false, leftmostvalue_timestamp, timestamp_cmp,
				 timestamp_is_infinite, timestamp_subtract)

RUM_SUPPORT_DIST(timestamptz, false, leftmostvalue_timestamp, timestamp_cmp,
				 timestamp_is_infinite, timestamp_subtract)


static Datum
leftmostvalue_time(void)
{
	return TimeADTGetDatum(0);
}

RUM_SUPPORT(time, false, leftmostvalue_time, time_cmp)

static Datum
leftmostvalue_timetz(void)
{
	TimeTzADT  *v = palloc(sizeof(TimeTzADT));

	v->time = 0;
	v->zone = -24 * 3600;		/* XXX is that true? */

	return TimeTzADTPGetDatum(v);
}

RUM_SUPPORT(timetz, false, leftmostvalue_timetz, timetz_cmp)

static Datum
leftmostvalue_date(void)
{
	return DateADTGetDatum(DATEVAL_NOBEGIN);
}

RUM_SUPPORT(date, false, leftmostvalue_date, date_cmp)

static Datum
leftmostvalue_interval(void)
{
	Interval   *v = palloc(sizeof(Interval));

	v->time = DT_NOBEGIN;
	v->day = 0;
	v->month = 0;
	return IntervalPGetDatum(v);
}

RUM_SUPPORT(interval, false, leftmostvalue_interval, interval_cmp)

static Datum
leftmostvalue_macaddr(void)
{
	macaddr    *v = palloc0(sizeof(macaddr));

	return MacaddrPGetDatum(v);
}

RUM_SUPPORT(macaddr, false, leftmostvalue_macaddr, macaddr_cmp)

static Datum
leftmostvalue_inet(void)
{
	return DirectFunctionCall1(inet_in, CStringGetDatum("0.0.0.0/0"));
}

RUM_SUPPORT(inet, true, leftmostvalue_inet, network_cmp)

RUM_SUPPORT(cidr, true, leftmostvalue_inet, network_cmp)

static Datum
leftmostvalue_text(void)
{
	return PointerGetDatum(cstring_to_text_with_len("", 0));
}

RUM_SUPPORT(text, true, leftmostvalue_text, bttextcmp)

static Datum
leftmostvalue_char(void)
{
	return CharGetDatum(SCHAR_MIN);
}

RUM_SUPPORT(char, false, leftmostvalue_char, btcharcmp)

RUM_SUPPORT(bytea, true, leftmostvalue_text, byteacmp)

static Datum
leftmostvalue_bit(void)
{
	return DirectFunctionCall3(bit_in,
							   CStringGetDatum(""),
							   ObjectIdGetDatum(0),
							   Int32GetDatum(-1));
}

RUM_SUPPORT(bit, true, leftmostvalue_bit, bitcmp)

static Datum
leftmostvalue_varbit(void)
{
	return DirectFunctionCall3(varbit_in,
							   CStringGetDatum(""),
							   ObjectIdGetDatum(0),
							   Int32GetDatum(-1));
}

RUM_SUPPORT(varbit, true, leftmostvalue_varbit, bitcmp)

/*
 * Numeric type hasn't a real left-most value, so we use PointerGetDatum(NULL)
 * (*not* a SQL NULL) to represent that.  We can get away with that because
 * the value returned by our leftmostvalue function will never be stored in
 * the index nor passed to anything except our compare and prefix-comparison
 * functions.  The same trick could be used for other pass-by-reference types.
 */

#define NUMERIC_IS_LEFTMOST(x)	((x) == NULL)

PG_FUNCTION_INFO_V1(rum_numeric_cmp);

Datum
rum_numeric_cmp(PG_FUNCTION_ARGS)
{
	Numeric		a = (Numeric) PG_GETARG_POINTER(0);
	Numeric		b = (Numeric) PG_GETARG_POINTER(1);
	int			res = 0;

	if (NUMERIC_IS_LEFTMOST(a))
	{
		res = (NUMERIC_IS_LEFTMOST(b)) ? 0 : -1;
	}
	else if (NUMERIC_IS_LEFTMOST(b))
	{
		res = 1;
	}
	else
	{
		res = DatumGetInt32(DirectFunctionCall2(numeric_cmp,
												NumericGetDatum(a),
												NumericGetDatum(b)));
	}

	PG_RETURN_INT32(res);
}

static Datum
leftmostvalue_numeric(void)
{
	return PointerGetDatum(NULL);
}

RUM_SUPPORT(numeric, true, leftmostvalue_numeric, rum_numeric_cmp)

/* Compatibility with rum-1.0, but see gen_rum_sql--1.0--1.1.pl */
PG_FUNCTION_INFO_V1(rum_timestamp_consistent);
Datum
rum_timestamp_consistent(PG_FUNCTION_ARGS)
{
	bool	*recheck = (bool *) PG_GETARG_POINTER(5);

	*recheck = false;
	PG_RETURN_BOOL(true);
}

