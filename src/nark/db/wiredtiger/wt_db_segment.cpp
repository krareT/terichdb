#include "wt_db_segment.hpp"
#include "wt_db_index.hpp"
#include "wt_db_store.hpp"
#include "wt_db_context.hpp"
#include <nark/num_to_str.hpp>

namespace nark { namespace db { namespace wt {

WtWritableSegment::WtWritableSegment() {
	m_wtConn = NULL;
	m_wrRowStore = NULL;
	m_cursorIsDel = NULL;
	m_cacheSize = 1*(1ul << 30); // 1GB
}
WtWritableSegment::~WtWritableSegment() {
	m_cursorIsDel->close(m_cursorIsDel);
	m_indices.clear();
	m_rowStore.reset();
	if (m_wtConn)
		m_wtConn->close(m_wtConn, NULL);
}

static const char g_isDelTable[] = "table:__isDel__";

void WtWritableSegment::init(PathRef segDir) {
	std::string strDir = segDir.string();
	char conf[512];
	snprintf(conf, sizeof(conf)
		, "create,cache_size=%zd,"
		  "log=(enabled,recover=on),"
		  "checkpoint=(log_size=64MB,wait=60)"
		, m_cacheSize);
	int err = wiredtiger_open(strDir.c_str(), NULL, conf, &m_wtConn);
	if (err) {
		THROW_STD(invalid_argument, "FATAL: wiredtiger_open(dir=%s,conf=%s) = %s"
			, strDir.c_str(), conf, wiredtiger_strerror(err)
			);
	}
	WT_SESSION* session = NULL;
	err = m_wtConn->open_session(m_wtConn, NULL, NULL, &session);
	if (err) {
		THROW_STD(invalid_argument, "FATAL: wiredtiger open session(dir=%s) = %s"
			, strDir.c_str(), wiredtiger_strerror(err)
			);
	}
	err = session->create(session, g_isDelTable, "key_format=r,value_format=1t");
	if (err) {
		THROW_STD(invalid_argument
			, "FATAL: wiredtiger create(\"%s\", dir=%s) = %s"
			, strDir.c_str(), g_isDelTable, wiredtiger_strerror(err)
			);
	}
	err = session->open_cursor(session, g_isDelTable, NULL, NULL, &m_cursorIsDel);
	if (err) {
		THROW_STD(invalid_argument
			, "FATAL: wiredtiger open cursor(\"%s\", dir=%s) = %s"
			, strDir.c_str(), g_isDelTable, wiredtiger_strerror(err)
			);
	}
	m_rowStore = new WtWritableStore(session, segDir);
	m_wrRowStore = m_rowStore->getWritableStore();
}

ReadableIndex*
WtWritableSegment::createIndex(const Schema& schema, PathRef segDir) const {
	std::string strDir = segDir.string();
	WT_SESSION* session = NULL;
	int err = m_wtConn->open_session(m_wtConn, NULL, NULL, &session);
	if (err) {
		THROW_STD(invalid_argument, "FATAL: wiredtiger open session(dir=%s) = %s"
			, strDir.c_str(), wiredtiger_strerror(err)
			);
	}
	return new WtWritableIndex(schema, segDir, session);
}

ReadableIndex*
WtWritableSegment::openIndex(const Schema& schema, PathRef segDir) const {
	std::string strDir = segDir.string();
	WT_SESSION* session = NULL;
	int err = m_wtConn->open_session(m_wtConn, NULL, NULL, &session);
	if (err) {
		THROW_STD(invalid_argument, "FATAL: wiredtiger open session(dir=%s) = %s"
			, strDir.c_str(), wiredtiger_strerror(err)
			);
	}
	return new WtWritableIndex(schema, segDir, session);
}

llong WtWritableSegment::totalStorageSize() const {
	return m_rowStore->dataStorageSize() + totalIndexSize();
}

void WtWritableSegment::loadRecordStore(PathRef segDir) {
	m_rowStore->load(segDir);
}

void WtWritableSegment::saveRecordStore(PathRef segDir) const {
	m_rowStore->save(segDir);
}

llong WtWritableSegment::dataStorageSize() const {
	return m_rowStore->dataStorageSize();
}

void WtWritableSegment::getValueAppend(llong id, valvec<byte>* val, DbContext* ctx) const {
	return m_rowStore->getValueAppend(id, val, ctx);
}

StoreIterator* WtWritableSegment::createStoreIterForward(DbContext* ctx) const {
	return m_rowStore->createStoreIterForward(ctx);
}

StoreIterator* WtWritableSegment::createStoreIterBackward(DbContext* ctx) const {
	return m_rowStore->createStoreIterBackward(ctx);
}

llong WtWritableSegment::append(fstring row, DbContext* ctx) {
	return m_wrRowStore->append(row, ctx);
}

void WtWritableSegment::replace(llong id, fstring row, DbContext* ctx) {
#if !defined(NDEBUG) && 0
	llong rows = m_rowStore->numDataRows();
	assert(id <= rows);
#endif
	m_wrRowStore->replace(id, row, ctx);
}

void WtWritableSegment::remove(llong id, DbContext* ctx) {
	tbb::mutex::scoped_lock lock(m_wtMutex);
	llong recno = id + 1;
	m_cursorIsDel->set_key(m_cursorIsDel, recno);
	m_cursorIsDel->set_value(m_cursorIsDel, 1);
	int err = m_cursorIsDel->insert(m_cursorIsDel);
	if (err) {
		THROW_STD(invalid_argument
			, "FATAL: wiredtiger mark del(recno=%lld, dir=%s) = %s"
			, recno, m_wtConn->get_home(m_wtConn), wiredtiger_strerror(err)
			);
	}
	m_wrRowStore->remove(id, ctx);
}

void WtWritableSegment::clear() {
	m_wrRowStore->clear();
}

WritableStore* WtWritableSegment::getWritableStore() {
	return this;
}

void WtWritableSegment::save(PathRef path) const {
	m_wtConn->async_flush(m_wtConn);
	WritableSegment::save(path);
}

void WtWritableSegment::load(PathRef path) {
	init(path);
	if (boost::filesystem::exists(path / "isDel")) {
		WritableSegment::load(path);
		return;
	}

	this->openIndices(path);

	// rebuild m_isDel
	llong rows = m_rowStore->numDataRows();
	m_isDel.resize_fill(size_t(rows), 0);
	WT_CURSOR* cursor = m_cursorIsDel;
	cursor->reset(cursor);
	while (cursor->next(cursor) == 0) {
		llong recno;
		int val = 0;
		cursor->get_key(cursor, &recno);
		cursor->get_value(cursor, &val);
		FEBIRD_RT_assert(recno > 0, std::logic_error);
		if (val) {
			llong id = recno - 1;
			m_isDel.set1(size_t(id));
		}
	}
	cursor->reset(cursor);
}

}}} // namespace nark::db::wt
