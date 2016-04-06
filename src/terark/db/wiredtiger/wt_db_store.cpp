#include "wt_db_store.hpp"
#include "wt_db_context.hpp"
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <boost/filesystem.hpp>

namespace terark { namespace db { namespace wt {

namespace fs = boost::filesystem;

static const char g_dataStoreUri[] = "table:__BlobStore__";

//////////////////////////////////////////////////////////////////
class WtWritableStoreIterBase : public StoreIterator {
	WT_CURSOR* m_cursor;
public:
	WtWritableStoreIterBase(const WtWritableStore* store, WT_CONNECTION* conn) {
		WT_SESSION* ses; // WT_SESSION is not thread safe
		int err = conn->open_session(conn, NULL, NULL, &ses);
		if (err) {
			THROW_STD(invalid_argument
				, "FATAL: wiredtiger open session(dir=%s) = %s"
				, conn->get_home(conn), wiredtiger_strerror(err)
				);
		}
		WT_CURSOR* cursor;
		err = ses->open_cursor(ses, g_dataStoreUri, NULL, NULL, &cursor);
		if (err) {
			std::string msg = ses->strerror(ses, err);
			ses->close(ses, NULL);
			THROW_STD(invalid_argument,
				"ERROR: wiredtiger store open forward cursor: %s", msg.c_str());
		}
		m_store.reset(const_cast<WtWritableStore*>(store));
		m_cursor = cursor;
	}
	~WtWritableStoreIterBase() {
		WT_SESSION* session = m_cursor->session;
		m_cursor->close(m_cursor);
		session->close(session, NULL);
	}
	virtual int advance(WT_CURSOR*) = 0;

	bool increment(llong* id, valvec<byte>* val) override {
		int err = advance(m_cursor);
		if (0 == err) {
			WT_ITEM item;
			llong recno;
			m_cursor->get_key(m_cursor, &recno);
			m_cursor->get_value(m_cursor, &item);
			if (0 == item.size) {
				*id = *id;
			}
			*id = recno - 1;
			val->assign((const byte*)item.data, item.size);
			return true;
		}
		if (WT_NOTFOUND == err) {
			return false;
		}
		WT_CONNECTION* conn = m_cursor->session->connection;
		THROW_STD(invalid_argument
			, "FATAL: wiredtiger search(%s, dir=%s) = %s"
			, g_dataStoreUri, conn->get_home(conn)
			, wiredtiger_strerror(err)
			);
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		llong recno = id + 1;
		m_cursor->set_key(m_cursor, recno);
		int err = m_cursor->search(m_cursor);
		if (err == 0) {
			WT_ITEM item;
			m_cursor->get_value(m_cursor, &item);
			val->assign((const byte*)item.data, item.size);
			return true;
		}
		if (WT_NOTFOUND == err) {
			return false;
		}
		WT_CONNECTION* conn = m_cursor->session->connection;
		THROW_STD(invalid_argument
			, "FATAL: wiredtiger search(%s, dir=%s) = %s"
			, g_dataStoreUri, conn->get_home(conn)
			, wiredtiger_strerror(err)
			);
	}
	void reset() override {
		m_cursor->reset(m_cursor);
	}
};
class WtWritableStoreIterForward : public WtWritableStoreIterBase {
public:
	virtual int advance(WT_CURSOR* cursor) override {
		return cursor->next(cursor);
	}
	WtWritableStoreIterForward(const WtWritableStore* store, WT_CONNECTION* conn)
		: WtWritableStoreIterBase(store, conn) {}
};
class WtWritableStoreIterBackward : public WtWritableStoreIterBase {
public:
	virtual int advance(WT_CURSOR* cursor) override {
		return cursor->prev(cursor);
	}
	WtWritableStoreIterBackward(const WtWritableStore* store, WT_CONNECTION* conn)
		: WtWritableStoreIterBase(store, conn) {}
};

WtWritableStore::WtWritableStore(WT_CONNECTION* conn) {
	WT_SESSION* session;
	int err = conn->open_session(conn, NULL, NULL, &session);
	if (err) {
		THROW_STD(invalid_argument
			, "FATAL: wiredtiger open session(dir=%s) = %s"
			, conn->get_home(conn), wiredtiger_strerror(err)
			);
	}
	err = session->create(session, g_dataStoreUri, "key_format=r,value_format=u");
	if (err) {
		session->close(session, NULL);
		THROW_STD(invalid_argument, "FATAL: wiredtiger create(%s, dir=%s) = %s"
			, g_dataStoreUri
			, conn->get_home(conn), wiredtiger_strerror(err)
			);
	}
	m_wtSession = session;
	m_wtCursor = NULL;
	m_wtAppend = NULL;
	m_dataSize = 0;
}
WtWritableStore::~WtWritableStore() {
	if (m_wtCursor)
		m_wtCursor->close(m_wtCursor);
	if (m_wtAppend)
		m_wtAppend->close(m_wtAppend);
	m_wtSession->close(m_wtSession, NULL);
}

WT_CURSOR* WtWritableStore::getReplaceCursor() const {
	if (NULL == m_wtCursor) {
		int err = m_wtSession->open_cursor(m_wtSession, g_dataStoreUri, NULL, NULL, &m_wtCursor);
		if (err) {
			auto msg = m_wtSession->strerror(m_wtSession, err);
			THROW_STD(invalid_argument,
				"ERROR: wiredtiger store open_cursor for replace: %s", msg);
		}
	}
	return m_wtCursor;
}

WT_CURSOR* WtWritableStore::getAppendCursor() const {
	if (NULL == m_wtAppend) {
		int err = m_wtSession->open_cursor(m_wtSession, g_dataStoreUri, NULL, "append", &m_wtAppend);
		if (err) {
			auto msg = m_wtSession->strerror(m_wtSession, err);
			THROW_STD(invalid_argument,
				"ERROR: wiredtiger store open_cursor for append: %s", msg);
		}
	}
	return m_wtAppend;
}

void WtWritableStore::save(PathRef path1) const {
	tbb::mutex::scoped_lock lock(m_wtMutex);
	m_wtSession->checkpoint(m_wtSession, NULL);
}

void WtWritableStore::load(PathRef path1) {
	WT_CONNECTION* conn = m_wtSession->connection;
	boost::filesystem::path segDir = conn->get_home(conn);
	auto dataFile = segDir / "__BlobStore__.wt";
	m_dataSize = boost::filesystem::file_size(dataFile);
}

llong WtWritableStore::dataStorageSize() const {
	return m_dataSize;
}

llong WtWritableStore::dataInflateSize() const {
	return m_dataSize;
}

llong WtWritableStore::numDataRows() const {
	tbb::mutex::scoped_lock lock(m_wtMutex);
	WT_CURSOR* cursor = getReplaceCursor();
	cursor->set_key(cursor, LLONG_MAX);
	int cmp;
	int err = cursor->search_near(cursor, &cmp);
	if (WT_NOTFOUND == err) {
		return 0;
	}
	if (err) {
		THROW_STD(invalid_argument, "wiredtiger search near failed: %s"
			, m_wtSession->strerror(m_wtSession, err));
	}
	llong recno;
	cursor->get_key(cursor, &recno);
	cursor->reset(cursor);
	return recno; // max recno is the rows
}

void WtWritableStore::getValueAppend(llong id, valvec<byte>* val, DbContext* ctx0)
const {
	assert(id >= 0);
//	WtContext* ctx = dynamic_cast<WtContext*>(ctx0);
//	TERARK_RT_assert(NULL != ctx, std::invalid_argument);
	llong recno = id + 1;
	tbb::mutex::scoped_lock lock(m_wtMutex);
	auto cursor = getReplaceCursor();
	cursor->set_key(cursor, recno);
	int err = cursor->search(cursor);
	if (err) {
		WT_SESSION* ses = cursor->session;
		THROW_STD(invalid_argument
			, "wiredtiger search failed: recno=%lld, wtError=%s"
			, recno, ses->strerror(ses, err));
	}
	WT_ITEM item;
	cursor->get_value(cursor, &item);
	val->append((const byte*)item.data, item.size);
	cursor->reset(cursor);
}

StoreIterator* WtWritableStore::createStoreIterForward(DbContext*) const {
	return new WtWritableStoreIterForward(this, m_wtSession->connection);
}

StoreIterator* WtWritableStore::createStoreIterBackward(DbContext*) const {
	return new WtWritableStoreIterBackward(this, m_wtSession->connection);
}

llong WtWritableStore::append(fstring row, DbContext* ctx0) {
//	WtContext* ctx = dynamic_cast<WtContext*>(ctx0);
//	TERARK_RT_assert(NULL != ctx, std::invalid_argument);
	tbb::mutex::scoped_lock lock(m_wtMutex);
	auto cursor = getAppendCursor();
	WT_ITEM item;
	memset(&item, 0, sizeof(item));
	item.data = row.data();
	item.size = row.size();
	cursor->set_value(cursor, &item);
	int err = cursor->insert(cursor);
	if (err) {
		THROW_STD(invalid_argument
			, "wiredtiger append failed, err=%s, row=%s"
			, m_wtSession->strerror(m_wtSession, err)
			, ctx0->m_tab->rowSchema().toJsonStr(row).c_str()
			);
	}
	llong recno;
	cursor->get_key(cursor, &recno);
	llong recId = recno - 1;
	return recId;
}

void WtWritableStore::update(llong id, fstring row, DbContext* ctx0) {
	assert(id >= 0);
//	WtContext* ctx = dynamic_cast<WtContext*>(ctx0);
//	TERARK_RT_assert(NULL != ctx, std::invalid_argument);
	llong recno = id + 1;
	tbb::mutex::scoped_lock lock(m_wtMutex);
	auto cursor = getReplaceCursor();
	WT_ITEM item;
	memset(&item, 0, sizeof(item));
	item.data = row.data();
	item.size = row.size();
	cursor->set_key(cursor, recno);
	cursor->set_value(cursor, &item);
	int err = cursor->insert(cursor);
	if (err) {
		THROW_STD(invalid_argument
			, "wiredtiger replace failed, err=%s, row=%s"
			, m_wtSession->strerror(m_wtSession, err)
			, ctx0->m_tab->rowSchema().toJsonStr(row).c_str()
			);
	}
	m_dataSize += row.size() + sizeof(recno);
}

void WtWritableStore::remove(llong id, DbContext* ctx0) {
	assert(id >= 0);
//	WtContext* ctx = dynamic_cast<WtContext*>(ctx0);
//	TERARK_RT_assert(NULL != ctx, std::invalid_argument);
#if 1
	fstring emptyValue("");
	update(id, emptyValue, ctx0);
#else
	llong recno = id + 1;
	tbb::mutex::scoped_lock lock(m_wtMutex);
	auto cursor = getReplaceCursor();
	cursor->set_key(cursor, recno);
	int err = cursor->remove(cursor);
	if (err) {
		if (WT_NOTFOUND != err) {
			THROW_STD(invalid_argument
				, "wiredtiger remove failed, err=%s"
				, m_wtSession->strerror(m_wtSession, err)
				);
		} else {
			fprintf(stderr, "WARN: WtWritableStore::remove: recno=%lld not found", recno);
		}
	}
	cursor->reset(cursor);
#endif
}

void WtWritableStore::shrinkToFit() {
	// do nothing
}
/*
void WtWritableStore::clear() {
	tbb::mutex::scoped_lock lock(m_wtMutex);
	m_wtSession->truncate(m_wtSession, g_dataStoreUri, NULL, NULL, NULL);
}
*/

AppendableStore* WtWritableStore::getAppendableStore() { return this; }
UpdatableStore* WtWritableStore::getUpdatableStore() { return this; }
WritableStore* WtWritableStore::getWritableStore() { return this; }

}}} // namespace terark::db::wt
