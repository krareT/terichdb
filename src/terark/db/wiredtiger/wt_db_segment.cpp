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

extern const char g_dataStoreUri[];

struct WtCursor {
	WT_CURSOR* cursor;
	WtCursor() : cursor(NULL) {}
	~WtCursor() {
		if (cursor)
			cursor->close(cursor);
	}
	operator WT_CURSOR*() const { return cursor; }
};
struct WtCursor2 {
	WtCursor insert;
	WtCursor overwrite;
};
struct WtSession {
	WT_SESSION* ses; // WT_SESSION is not thread safe
	WtSession() : ses(NULL) {}
	~WtSession() {
		if (ses)
			ses->close(ses, NULL);
	}
};
struct WtItem : public WT_ITEM {
	WtItem() {
		memset(this, 0, sizeof(WtItem));
	}
	operator fstring() { return fstring((const char*)data, size); }
	const char* charData() const { return (const char*)data; }
};

class WtWritableSegment::WtDbTransaction : public DbTransaction {
	WtWritableSegment* m_seg;
	WtSession m_session;
	WtCursor  m_store;
	valvec<WtCursor2> m_indices;
	const SchemaConfig& m_sconf;
public:
	~WtDbTransaction() {
	}
	explicit WtDbTransaction(WtWritableSegment* seg)
		: m_seg(seg), m_sconf(*seg->m_schema)
	{
		WT_CONNECTION* conn = seg->m_wtConn;
		int err = conn->open_session(conn, NULL, NULL, &m_session.ses);
		if (err) {
			THROW_STD(invalid_argument
				, "FATAL: wiredtiger open session(dir=%s) = %s"
				, conn->get_home(conn), wiredtiger_strerror(err)
				);
		}
		WT_SESSION* ses = m_session.ses;
		err = ses->open_cursor(ses, g_dataStoreUri, NULL, "overwrite", &m_store.cursor);
		if (err) {
			THROW_STD(invalid_argument
				, "ERROR: wiredtiger store open cursor: %s"
				, ses->strerror(ses, err));
		}
		m_indices.resize(seg->m_indices.size());
		for (size_t indexId = 0; indexId < m_indices.size(); ++indexId) {
			ReadableIndex* index = seg->m_indices[indexId].get();
			WtWritableIndex* wtIndex = dynamic_cast<WtWritableIndex*>(index);
			assert(NULL != wtIndex);
			const char* uri = wtIndex->getIndexUri().c_str();
			err = ses->open_cursor(ses, uri, NULL, NULL, &m_indices[indexId].insert.cursor);
			if (err) {
				THROW_STD(invalid_argument
					, "ERROR: wiredtiger open index cursor: %s"
					, ses->strerror(ses, err));
			}
			err = ses->open_cursor(ses, uri, NULL, "overwrite", &m_indices[indexId].overwrite.cursor);
			if (err) {
				THROW_STD(invalid_argument
					, "ERROR: wiredtiger open index cursor: %s"
					, ses->strerror(ses, err));
			}
		}
//		m_ctx = new DbContext()
	}
	void startTransaction() override {
		WT_SESSION* ses = m_session.ses;
		int err = ses->begin_transaction(ses, "isolation=read-committed,sync=false");
		if (err) {
			THROW_STD(invalid_argument
				, "ERROR: wiredtiger begin_transaction: %s"
				, ses->strerror(ses, err));
		}
	}
	void commit() override {
		WT_SESSION* ses = m_session.ses;
		int err = ses->commit_transaction(ses, NULL);
		if (err) {
			THROW_STD(invalid_argument
				, "ERROR: wiredtiger commit_transaction: %s"
				, ses->strerror(ses, err));
		}
	}
	void rollback() override {
		WT_SESSION* ses = m_session.ses;
		int err = ses->rollback_transaction(ses, NULL);
		if (err) {
			THROW_STD(invalid_argument
				, "ERROR: wiredtiger rollback_transaction: %s"
				, ses->strerror(ses, err));
		}
	}
	bool indexInsert(size_t indexId, fstring key, llong recId) override {
		assert(indexId < m_indices.size());
		WtItem item;
		item.data = key.data();
		item.size = key.size();
		WT_SESSION* ses = m_session.ses;
		const Schema& schema = m_sconf.getIndexSchema(indexId);
		WT_CURSOR* cur = m_indices[indexId].insert;
		if (schema.m_isUnique) {
			cur->set_key(cur, &item);
			cur->set_value(cur, recId);
			int err = cur->insert(cur);
			if (WT_DUPLICATE_KEY == err) {
				return false;
			}
			if (err) {
				THROW_STD(invalid_argument
					, "ERROR: wiredtiger insert unique index: %s", ses->strerror(ses, err));
			}
		}
		else {
			llong recId = -1;
			cur->set_key(cur, &item, recId);
			cur->set_value(cur, 1);
			int cmp = 0;
			int err = cur->insert(cur);
			if (WT_DUPLICATE_KEY == err) {
				assert(0); // assert in debug
				return true; // ignore in release
			}
			if (err) {
				THROW_STD(invalid_argument
					, "ERROR: wiredtiger insert multi index: %s", ses->strerror(ses, err));
			}
		}
		return true;
	}
	void indexSearch(size_t indexId, fstring key, valvec<llong>* recIdvec) override {
		assert(indexId < m_indices.size());
		WtItem item;
		item.data = key.data();
		item.size = key.size();
		WT_SESSION* ses = m_session.ses;
		const Schema& schema = m_sconf.getIndexSchema(indexId);
		WT_CURSOR* cur = m_indices[indexId].insert;
		recIdvec->erase_all();
		if (schema.m_isUnique) {
			cur->set_key(cur, &item);
			int err = cur->search(cur);
			if (WT_NOTFOUND == err) {
				return;
			}
			if (err) {
				THROW_STD(invalid_argument
					, "ERROR: wiredtiger search: %s", ses->strerror(ses, err));
			}
			llong recId = 0;
			cur->get_value(cur, &recId);
			recIdvec->push_back(recId);
		}
		else {
			llong recId = -1;
			cur->set_key(cur, &item, recId);
			int cmp = 0;
			int err = cur->search_near(cur, &cmp);
			if (WT_NOTFOUND == err) {
				return;
			}
			if (err) {
				THROW_STD(invalid_argument
					, "ERROR: wiredtiger search_near: %s", ses->strerror(ses, err));
			}
			if (cmp >= 0) {
				do {
					cur->get_key(cur, &item, &recId);
					if (item == key) {
						recIdvec->push_back(recId);
					} else {
						break;
					}
				} while (cur->next(cur) == 0);
			}
		}
	}
	void indexRemove(size_t indexId, fstring key, llong recId) override {
		assert(indexId < m_indices.size());
		WtItem item;
		item.data = key.data();
		item.size = key.size();
		WT_SESSION* ses = m_session.ses;
		const Schema& schema = m_sconf.getIndexSchema(indexId);
		WT_CURSOR* cur = m_indices[indexId].insert;
		if (schema.m_isUnique) {
			cur->set_key(cur, &item);
		}
		else {
			cur->set_key(cur, &item, recId);
		}
		int err = cur->remove(cur);
		if (WT_NOTFOUND == err) {
			return;
		}
		if (err) {
			THROW_STD(invalid_argument
				, "ERROR: wiredtiger search_near: %s", ses->strerror(ses, err));
		}
	}
	void indexUpsert(size_t indexId, fstring key, llong recId) override {
		assert(indexId < m_indices.size());
		WtItem item;
		item.data = key.data();
		item.size = key.size();
		WT_SESSION* ses = m_session.ses;
		const Schema& schema = m_sconf.getIndexSchema(indexId);
		WT_CURSOR* cur = m_indices[indexId].overwrite;
		if (schema.m_isUnique) {
			cur->set_key(cur, &item);
			cur->set_value(cur, recId);
			int err = cur->insert(cur);
			if (err) {
				THROW_STD(invalid_argument
					, "ERROR: wiredtiger upsert unique index: %s", ses->strerror(ses, err));
			}
		}
		else {
			llong recId = -1;
			cur->set_key(cur, &item, recId);
			cur->set_value(cur, 1);
			int cmp = 0;
			int err = cur->insert(cur);
			if (err) {
				THROW_STD(invalid_argument
					, "ERROR: wiredtiger upsert multi index: %s", ses->strerror(ses, err));
			}
		}
	}
	void storeRemove(llong recId) override {
		WT_SESSION* ses = m_session.ses;
		WT_CURSOR* cur = m_store;
		int err = cur->remove(cur);
		if (WT_NOTFOUND == err) {
			return;
		}
		if (err) {
			THROW_STD(invalid_argument
				, "ERROR: wiredtiger store remove: %s", ses->strerror(ses, err));
		}
	}
	void storeUpsert(llong recId, fstring row) override {
		WtItem item;
		if (m_sconf.m_updatableColgroups.empty()) {
			item.data = row.data();
			item.size = row.size();
		}
		else {
			auto& sconf = m_sconf;
			auto seg = m_seg;
			sconf.m_rowSchema->parseRow(row, &m_cols1);
			for (size_t colgroupId : sconf.m_updatableColgroups) {
				auto store = seg->m_colgroups[colgroupId]->getUpdatableStore();
				assert(nullptr != store);
				const Schema& schema = sconf.getColgroupSchema(colgroupId);
				schema.selectParent(m_cols1, &m_wrtBuf);
				store->update(recId, m_wrtBuf, NULL);
			}
			sconf.m_wrtSchema->selectParent(m_cols1, &m_wrtBuf);
			item.data = m_wrtBuf.data();
			item.size = m_wrtBuf.size();
		}
		WT_CURSOR* cur = m_store;
		cur->set_key(cur, recId+1); // recno = recId+1
		cur->set_value(cur, &item);
		int err = cur->insert(cur);
		if (err) {
			WT_SESSION* ses = m_session.ses;
			THROW_STD(invalid_argument
				, "ERROR: wiredtiger store upsert: %s", ses->strerror(ses, err));
		}
	}
	void storeGetRow(llong recId, valvec<byte>* row) override {
		WT_SESSION* ses = m_session.ses;
		WT_CURSOR* cur = m_store;
		WtItem item;
		cur->set_key(cur, recId+1); // recno = recId+1
		int err = cur->search(cur);
		if (err) {
			THROW_STD(invalid_argument
				, "ERROR: wiredtiger store search: %s", ses->strerror(ses, err));
		}
		cur->get_value(cur, &item);
		if (m_sconf.m_updatableColgroups.empty()) {
			row->assign(item.charData(), item.size);
		}
		else {
			row->erase_all();
			auto seg = m_seg;
			m_wrtBuf.erase_all();
			m_cols1.erase_all();
			m_wrtBuf.append(item.charData(), item.size);
			const size_t ProtectCnt = 100;
			if (seg->m_isFreezed || seg->m_isDel.unused() > ProtectCnt) {
				seg->getCombineAppend(recId, row, m_wrtBuf, m_cols1, m_cols2);
			}
			else {
				SpinRwLock  lock(seg->m_segMutex, false);
				seg->getCombineAppend(recId, row, m_wrtBuf, m_cols1, m_cols2);
			}
		}
	}
	valvec<byte> m_wrtBuf;
	ColumnVec    m_cols1;
	ColumnVec    m_cols2;
};

DbTransaction* WtWritableSegment::createTransaction() {
	return new WtDbTransaction(this);
}

}}} // namespace terark::db::wt
