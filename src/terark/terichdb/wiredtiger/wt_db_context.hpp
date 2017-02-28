#pragma once

#include <terark/terichdb/db_table.hpp>
#include <wiredtiger.h>

namespace terark { namespace terichdb { namespace wt {

struct WtCursor {
	WT_CURSOR* cursor;
	WtCursor() : cursor(NULL) {}
	~WtCursor();
	void close();
#if !defined(NDEBUG)
	WtCursor(const WtCursor& y) : cursor(NULL) { assert(NULL == y.cursor); }
	WtCursor& operator=(const WtCursor& y) { assert(NULL == y.cursor); }
#endif
	operator WT_CURSOR*() const { return cursor; }
	WT_CURSOR* operator->() const { return cursor; }
	void reset() const;
};

struct WtSession {
	WT_SESSION* ses; // WT_SESSION is not thread safe
	WtSession() : ses(NULL) {}
	~WtSession();
	void close();
#if !defined(NDEBUG)
	WtSession(const WtSession& y) : ses(NULL) { assert(NULL == y.ses); }
	WtSession& operator=(const WtSession& y) { assert(NULL == y.ses); }
#endif
	operator WT_SESSION*() const { return ses; }
	WT_SESSION* operator->() const { return ses; }
};

/*
class TERICHDB_DLL WtContext : public DbContext {
public:
	WT_SESSION*        wtSession;
	WT_CURSOR*         wtStoreCursor;
	WT_CURSOR*         wtStoreAppend;
//	WT_CURSOR*         wtStoreReplace; // reuse default cursor
	valvec<WT_CURSOR*> wtIndexCursor;

	WtContext(const DbTable* tab);
	~WtContext();

	WT_CURSOR* getStoreCursor(fstring uri);
	WT_CURSOR* getStoreAppend(fstring uri);
	WT_CURSOR* getStoreReplace(fstring uri);
	WT_CURSOR* getIndexCursor(size_t indexId, fstring indexUri);
};
*/

}}} // namespace terark::terichdb::wt

