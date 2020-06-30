
#ifndef SQL_COMMENT_H
#define SQL_COMMENT_H

#include <string.h>
#include "utils/elog.h"
#include "nodes/pg_list.h"

typedef struct SqlComment 
{
	bool unsaferow_format;
	bool use_idx_in_plain;
	List * nodeids;
} SqlComment;

static inline void ParseOneItemInSqlComment(const char * sql, int sidx, int eidx, SqlComment * sc)
{
	if (eidx <= sidx)
		return;

	if (strncmp(&sql[sidx], "ur", 2) == 0)
		sc->unsaferow_format = true;
	else if (strncmp(&sql[sidx], "idx", 3) == 0)
		sc->use_idx_in_plain = true;
	else if (strncmp(&sql[sidx], "node=", 5) == 0)
	{
		char * nodeid;
		if (eidx - sidx <= 5)
			ereport(ERROR, 
			        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
				     errmsg("nodeid is empty in sql comment")));

		nodeid = pnstrdup(sql + sidx + 5, eidx - sidx - 5);
		if (!sc->nodeids)
			sc->nodeids = list_make1(nodeid);
		else
			lappend(sc->nodeids, nodeid);
	}
	/*else
		ereport(ERROR,
		        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			     errmsg("unknown parameter in sql comment. sql:\"%s\"; sidx:%d; eidx:%d", sql, sidx, eidx)));*/
}
static inline SqlComment ParseSqlComment(const char * sql)
{
	int idx = 2;
	int sidx, eidx;
	SqlComment sc = { false, false, NULL };

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

		ParseOneItemInSqlComment(sql, sidx, eidx, &sc);

		sidx = ++eidx;
	}
	ParseOneItemInSqlComment(sql, sidx, eidx, &sc);
	return sc;
}

#endif // end of SQL_COMMENT_H
