#include "wt_db_segment.hpp"
#include "wt_db_index.hpp"
#include "wt_db_store.hpp"
#include "wt_db_context.hpp"

namespace nark { namespace db { namespace wt {

WtWritableSegment::WtWritableSegment() {
	m_wtConn = NULL;
	m_wrRowStore = NULL;
	m_cacheSize = 1*(1ul << 30); // 1GB
	if (const char* env = getenv("NarkDb_WrSegCacheSizeMB")) {
		m_cacheSize = (size_t)strtoull(env, NULL, 10) * 1024 * 1024;
	}
}
WtWritableSegment::~WtWritableSegment() {
	m_indices.clear();
	m_rowStore.reset();
	if (m_wtConn)
		m_wtConn->close(m_wtConn, NULL);
}

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
		size_t rows = (size_t)m_rowStore->numDataRows();
		if (rows+1 < m_isDel.size() || (rows+1 == m_isDel.size() && !m_isDel[rows])) {
			fprintf(stderr
				, "WARN: wiredtiger store: rows[saved=%zd real=%zd], some data may lossed\n"
				, m_isDel.size(), rows);
		//	m_isDel.risk_set_size(rows); // don't uncomment, because we must allow m_isDel be larger
		}
		else if (rows > m_isDel.size()) {
			fprintf(stderr
				, "WARN: wiredtiger store: rows[saved=%zd real=%zd], some error may occurred, ignore it and easy recover\n"
				, m_isDel.size(), rows);
			while (m_isDel.size() < rows) {
				this->pushIsDel(false);
			}
		}
	}
	else {
		this->openIndices(path);
	}
}

}}} // namespace nark::db::wt
