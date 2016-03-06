#include "wt_db_context.hpp"

namespace terark { namespace db { namespace wt {

namespace fs = boost::filesystem;
/*
WtContext::WtContext(const CompositeTable* tab) : DbContext(tab) {
	wtSession = NULL;
	wtStoreCursor = NULL;
	wtStoreAppend = NULL;
	wtIndexCursor.resize(tab->indexNum());
}

WtContext::~WtContext() {
	for (auto cursor : wtIndexCursor) {
		if (cursor)
			cursor->close(cursor);
	}
	if (wtStoreCursor)
		wtStoreCursor->close(wtStoreCursor);
}

WT_CURSOR* WtContext::getStoreCursor(fstring uri) {
	if (NULL == wtStoreCursor) {
		int err = wtSession->open_cursor(wtSession, uri.c_str(), NULL, NULL, &wtStoreCursor);
		if (err) {
			auto msg = wtSession->strerror(wtSession, err);
			THROW_STD(invalid_argument, "ERROR: wiredtiger store open_cursor: %s", msg);
		}
	}
	return wtStoreCursor;
}

WT_CURSOR* WtContext::getStoreAppend(fstring uri) {
	if (NULL == wtStoreAppend) {
		int err = wtSession->open_cursor(wtSession, uri.c_str(), NULL, "append", &wtStoreAppend);
		if (err) {
			auto msg = wtSession->strerror(wtSession, err);
			THROW_STD(invalid_argument, "ERROR: wiredtiger store append open_cursor: %s", msg);
		}
	}
	return wtStoreAppend;
}

WT_CURSOR* WtContext::getStoreReplace(fstring uri) {
	return getStoreCursor(uri);
}

WT_CURSOR* WtContext::getIndexCursor(size_t indexId, fstring indexUri) {
	TERARK_RT_assert(wtIndexCursor.size() == m_tab->indexNum(), std::invalid_argument);
	TERARK_RT_assert(indexId < wtIndexCursor.size(), std::invalid_argument);
	auto& cursor = wtIndexCursor[indexId];
	if (!cursor) {
		int err = wtSession->open_cursor(wtSession, indexUri.c_str(), NULL, NULL, &cursor);
		if (err) {
			auto msg = wtSession->strerror(wtSession, err);
			THROW_STD(invalid_argument, "ERROR: wiredtiger index open_cursor: %s", msg);
		}
	}
	return cursor;
}
*/

}}} // namespace terark::db::wt
