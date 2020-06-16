
#ifndef UNSAFE_ROW_H
#define UNSAFE_ROW_H

#include "postgres.h"
#include "catalog/pg_attribute.h"

typedef struct UnsafeRowBuilder 
{
	int col_cnt;
	char * bmp;
	char * fixed_data;
	char * var_data;
	size_t vardata_cur_len;
	size_t vardata_max_len;

	int next_append_idx;
} UnsafeRowBuilder;

extern void UnsafeRowBuilderInit(UnsafeRowBuilder * urb, int colcnt);
extern void UnsafeRowBuilderAppendNull(UnsafeRowBuilder * urb);
extern void UnsafeRowBuilderAppend(UnsafeRowBuilder * urb, Datum attr, Form_pg_attribute attrdef);

static inline size_t UnsafeRowBmpSize(int colcnt)
{
	return ((colcnt + 63) / 64) * 8;
}
static inline size_t UnsafeRowBmpSize2(UnsafeRowBuilder * urb)
{
	return UnsafeRowBmpSize(urb->col_cnt);
}
static inline size_t UnsafeRowFixedDataSize(UnsafeRowBuilder * urb)
{
	return urb->col_cnt * 8;
}
static inline size_t UnsafeRowVarDataSize(UnsafeRowBuilder * urb)
{
	return urb->vardata_cur_len;
}
static inline size_t UnsafeRowSize(UnsafeRowBuilder * urb)
{
	return UnsafeRowBmpSize2(urb) + UnsafeRowFixedDataSize(urb) + UnsafeRowVarDataSize(urb);
}

#endif // end of UNSAFE_ROW_H
