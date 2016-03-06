#pragma once

#include <terark/db/db_table.hpp>
#include <wiredtiger.h>

namespace terark { namespace db { namespace wt {

/*
class TERARK_DB_DLL WtContext : public DbContext {
public:
	WT_SESSION*        wtSession;
	WT_CURSOR*         wtStoreCursor;
	WT_CURSOR*         wtStoreAppend;
//	WT_CURSOR*         wtStoreReplace; // reuse default cursor
	valvec<WT_CURSOR*> wtIndexCursor;

	WtContext(const CompositeTable* tab);
	~WtContext();

	WT_CURSOR* getStoreCursor(fstring uri);
	WT_CURSOR* getStoreAppend(fstring uri);
	WT_CURSOR* getStoreReplace(fstring uri);
	WT_CURSOR* getIndexCursor(size_t indexId, fstring indexUri);
};
*/

}}} // namespace terark::db::wt

