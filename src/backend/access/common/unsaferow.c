
#include "postgres.h"

#include "fmgr.h"
#include "access/unsaferow.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_type_d.h"
#include "mb/pg_wchar.h"
#include "utils/numeric.h"
#include "utils/date.h"
#include "utils/timestamp.h"

void UnsafeRowBuilderInit(UnsafeRowBuilder * urb, int colcnt)
{
	urb->col_cnt = colcnt;
	urb->bmp = palloc0(UnsafeRowBmpSize(colcnt));
	urb->fixed_data = palloc0(colcnt * 8);
	urb->var_data = NULL;
	urb->vardata_cur_len = 0;
	urb->vardata_max_len = 0;

	urb->next_append_idx = 0;
}

void UnsafeRowBuilderAppendNull(UnsafeRowBuilder * urb)
{
	uint64 idx = urb->next_append_idx;
	uint64 offset = (idx >> 6) * 8;
	uint64 mask = 1ULL << (idx & 0x3F);
	uint64 v = *(uint64 *)(urb->bmp + offset);

	Assert(urb->next_append_idx < urb->col_cnt);

	v |= mask;
	*(uint64 *)(urb->bmp + offset) = v;
}

static void AppendVarData(UnsafeRowBuilder * urb, const char * data, size_t sz)
{
	size_t sz2 = ((sz + 7) / 8) * 8;
	if (urb->vardata_max_len - urb->vardata_cur_len < sz2)
	{
		size_t new_vardata_max_len = urb->vardata_cur_len + sz2;
		urb->var_data = repalloc(urb->var_data, new_vardata_max_len);
		urb->vardata_max_len = new_vardata_max_len;
	}
	memcpy(urb->var_data + urb->vardata_cur_len, data, sz);
	urb->vardata_cur_len += sz;

	for (size_t i = sz; i < sz2; ++i)
		urb->var_data[urb->vardata_cur_len++] = '\0';
}

#if defined(PG_INT128_TYPE)
static int128 strtoi128(const char * s);
#endif

void UnsafeRowBuilderAppend(UnsafeRowBuilder * urb, Datum attr, Form_pg_attribute attrdef)
{
	char * addr = urb->fixed_data + urb->next_append_idx * 8;
	Oid typid = attrdef->atttypid;

	Assert(urb->next_append_idx < urb->col_cnt);

	if (typid == INT2OID)
		*(int16 *)addr = DatumGetInt16(attr);
	else if (typid == INT4OID)
		*(int32 *)addr = DatumGetInt32(attr);
	else if (typid == INT8OID)
		*(int64 *)addr = DatumGetInt64(attr);
	else if (typid == FLOAT4OID)
		*(float4 *)addr = DatumGetFloat4(attr);
	else if (typid == FLOAT8OID)
		*(float8 *)addr = DatumGetFloat8(attr);
	else if (typid == BYTEAOID)
	{
		size_t offset = UnsafeRowSize(urb);
		bytea * v = DatumGetByteaPP(attr);
		const char * data = VARDATA_ANY(v);
		size_t sz = VARSIZE_ANY_EXHDR(v);
		AppendVarData(urb, data, sz);
		*(uint64 *)addr = offset << 32 | sz;
	}
	else if (typid == TEXTOID)
	{
		size_t offset = UnsafeRowSize(urb);
		text * v = DatumGetTextPP(attr);
		const char * data = VARDATA_ANY(v);
		size_t sz = VARSIZE_ANY_EXHDR(v);
		char * data2 = pg_server_to_client(data, (int)sz);
		if (data2 != data)
		{
			sz = strlen(data2);
			AppendVarData(urb, data2, sz);
			pfree(data2);
		}
		else
		{
			AppendVarData(urb, data, sz);
		}
		*(uint64 *)addr = offset << 32 | sz;
	}
	else if (typid == NUMERICOID)
	{
		char * str;
		int32 p = 0;
		Numeric v = DatumGetNumeric(attr);

		if (numeric_is_nan(v))
			ereport(ERROR, 
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsaferow does not supoort NaN for numeric")));
		if (attrdef->atttypmod < 0)
			ereport(ERROR, 
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsaferow does not support numeric type without percision/scale")));
		parse_numeric_typmod(attrdef->atttypmod, &p, NULL);
		str = numeric_as_intstr(v);
		if (p <= 18) // max precision for int64
		{
			char * end;
			int64 val;
			val = strtoll(str, &end, 10);
			if (*end != '\0')
				ereport(ERROR, 
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid int64 value:%s", str)));
			*(int64 *)addr = val;
		}
		else if (p <= 38) // max precision is 38 for int128
		{
#if defined(PG_INT128_TYPE)
			size_t offset = UnsafeRowSize(urb);
			int128 val = strtoi128(str);
			const char * s = (const char *)&val;

			AppendVarData(urb, s, 16);
			*(uint64 *)addr = offset << 32 | 16;
#else
			ereport(ERROR, 
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), 
					 errmsg("do not support int128 for numeric")));
#endif
		}
		else 
		{
			ereport(ERROR, 
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("unsaferow does not support numeric with precision(%d)", p)));
		}
	}
	else if (typid == DATEOID)
	{
		DateADT v = DatumGetDateADT(attr);
		v += POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE;
		*(int32 *)addr = v;
	}
	else if (typid == TIMESTAMPTZOID)
	{
		TimestampTz v = DatumGetTimestampTz(attr);
		v += (POSTGRES_EPOCH_JDATE - POSTGRES_EPOCH_JDATE) * USECS_PER_DAY;
		*(int64 *)addr = v;
	}
	else
	{
		ereport(ERROR, 
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
				errmsg("unsupported col type(%d) for unsaferow", typid)));
	}

	urb->next_append_idx++;
}

#if defined(PG_INT128_TYPE)
static int128 strtoi128(const char * s)
{
	bool isneg = false;
	int128 v = 0;
	int idx = 0;

	isneg = (s[0] == '-');
	if (s[0] == '-' || s[0] == '+')
		idx = 1;
	while (s[idx])
	{
		if (s[idx] < '0' || s[idx] > '9')
			ereport(ERROR, 
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid int128 value:%s", s)));
		v = v * 10 + (s[idx] - '0');
		++idx;
	}
	if (isneg)
		v *= -1;
	return v;
}
#endif
