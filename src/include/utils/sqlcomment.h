
#ifndef SQL_COMMENT_H
#define SQL_COMMENT_H

#include <string.h>
#include "utils/elog.h"

typedef struct SqlComment 
{
	bool unsaferow_format;
	bool use_idx_in_plain;
} SqlComment;

static inline SqlComment ParseSqlComment(const char * sql)
{
	int idx = 2;
	int sidx, eidx;
	SqlComment sc = { false, false };

	if (sql[0] != '/' || sql[1] != '*')
		return sc;
	while (sql[idx])
	{
		if (sql[idx] == '*' && sql[idx + 1] == '/')
			break;
		++idx;
	}
	if (sql[idx] != '*')
		return sc;
	sidx = eidx = 2;
	while (eidx < idx)
	{
		if (sql[eidx] != ',')
		{
			++eidx;
			continue;
		}

		if (strncmp(&sql[sidx], "ur", 2) == 0)
			sc.unsaferow_format = true;
		else if (strncmp(&sql[sidx], "idx", 3) == 0)
			sc.use_idx_in_plain = true;
		else
			ereport(ERROR, 
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
					errmsg("unknown parameter in sql comment")));

		sidx = ++eidx;
	}
	return sc;
}

#endif // end of SQL_COMMENT_H
