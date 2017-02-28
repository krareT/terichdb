#include "wt_db_context.hpp"

namespace terark { namespace terichdb { namespace wt {

namespace fs = boost::filesystem;

WtCursor::~WtCursor() {
	this->close();
}

void WtCursor::close() {
	if (NULL == cursor) {
		return;
	}
	WT_SESSION* ses = cursor->session;
	const char* uri = cursor->uri;
	int err = cursor->close(cursor);
	if (err) {
		const char* szErr = ses->strerror(ses, err);
		fprintf(stderr, "ERROR: WT_CURSOR.close(%s, flags=0x%X) fail: %s\n"
			, uri, cursor->flags, szErr);
	}
	cursor = NULL;
}

void WtCursor::reset() const {
	assert(NULL != cursor);
	int err = cursor->reset(cursor);
	if (err) {
		WT_SESSION* ses = cursor->session;
		const char* uri = cursor->uri;
		const char* szErr = ses->strerror(ses, err);
		fprintf(stderr, "ERROR: WT_CURSOR.reset(%s, flags=0x%X) fail: %s\n"
			, uri, cursor->flags, szErr);
	}
}

WtSession::~WtSession() {
	this->close();
}

void WtSession::close() {
	if (NULL == ses) {
		return;
	}
	WT_CONNECTION* conn = ses->connection;
	const char* home = conn->get_home(conn);
	int err = ses->close(ses, NULL);
	if (err) {
		const char* szErr = wiredtiger_strerror(err);
		fprintf(stderr, "ERROR: WT_SESSION.close(%s) fail: %s\n", home, szErr);
	}
	ses = NULL;
}

/*
WtContext::WtContext(const DbTable* tab) : DbContext(tab) {
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

}}} // namespace terark::terichdb::wt
