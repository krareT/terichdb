#include "wt_db_segment.hpp"
#include "wt_db_index.hpp"
#include "wt_db_store.hpp"
#include "wt_db_context.hpp"

namespace terark { namespace db { namespace wt {

WtWritableSegment::WtWritableSegment() {
	m_wtConn = NULL;
	m_wrRowStore = NULL;
	m_cacheSize = 1*(1ul << 30); // 1GB
	if (const char* env = getenv("TerarkDB_WrSegCacheSizeMB")) {
		m_cacheSize = (size_t)strtoull(env, NULL, 10) * 1024 * 1024;
	}
	m_hasLockFreePointSearch = false;
}
WtWritableSegment::~WtWritableSegment() {
	m_indices.clear();
	m_wrtStore.reset();
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
	m_wrtStore = new WtWritableStore(m_wtConn);
	m_wrRowStore = m_wrtStore->getWritableStore();
}

ReadableIndex*
WtWritableSegment::createIndex(const Schema& schema, PathRef segDir) const {
	return new WtWritableIndex(schema, m_wtConn);
}

ReadableIndex*
WtWritableSegment::openIndex(const Schema& schema, PathRef segDir) const {
	return new WtWritableIndex(schema, m_wtConn);
}

void WtWritableSegment::load(PathRef path) {
	init(path);
	if (boost::filesystem::exists(path / "isDel")) {
		PlainWritableSegment::load(path);
		size_t rows = (size_t)m_wrtStore->numDataRows();
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

}}} // namespace terark::db::wt
