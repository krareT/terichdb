#include "wt_db_segment.hpp"
#include "wt_db_index.hpp"
#include "wt_db_store.hpp"
#include "wt_db_context.hpp"
#include <boost/scope_exit.hpp>

#undef min
#undef max

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

#if defined(fuck_wiredtiger_brain_damaged_collator_and_recover)
static
int DupableIndexKey_compare(WT_COLLATOR *collator,
							WT_SESSION *session,
							const WT_ITEM *key1,
							const WT_ITEM *key2,
							int *pcmp) {
	WT_ITEM ikey1, ikey2;
	int64_t recId1, recId2;
	int err;
	WT_CONNECTION* conn = session->connection;
	err = wiredtiger_struct_unpack(session, key1->data, key1->size, "uq", &ikey1, &recId1);
	if (err) {
		THROW_STD(invalid_argument, "FATAL: dir=%s, err(%d)=%s"
			, conn->get_home(conn), err, session->strerror(session, err));
	}
	err = wiredtiger_struct_unpack(session, key2->data, key2->size, "uq", &ikey2, &recId2);
	if (err) {
		THROW_STD(invalid_argument, "FATAL: dir=%s, err(%d)=%s"
			, conn->get_home(conn), err, session->strerror(session, err));
	}
	int cmp = memcmp(ikey1.data, ikey2.data, std::min(ikey1.size, ikey2.size));
	if (cmp) {
		*pcmp = cmp;
	}
	else if (ikey1.size == ikey2.size) {
		*pcmp = recId1 < recId2 ? -1
			  : recId1 > recId2 ? +1
			  : 0;
	}
	else { // assume key len are less than INT32_MAX
		*pcmp = ikey1.size - ikey2.size;
	}
	return 0;
}
#endif

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
// brain damaged wiredtiger_open trapped me into fucking log(recover...)
// Using binary encoded (key,record_id) should save the world
//	static WT_COLLATOR collator = { DupableIndexKey_compare, NULL, NULL };
//	m_wtConn->add_collator(m_wtConn, "terark_wt_dup_index_compare", &collator, NULL);
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

void WtWritableSegment::save(PathRef path) const {
	if (m_tobeDel) {
		return; // not needed
	}
	m_wtConn->async_flush(m_wtConn);
}

extern const char g_dataStoreUri[];

struct WtCursor {
	WT_CURSOR* cursor;
	WtCursor() : cursor(NULL) {}
	~WtCursor() {
		if (cursor)
			cursor->close(cursor);
	}
#if !defined(NDEBUG)
	WtCursor(const WtCursor& y) : cursor(NULL) { assert(NULL == y.cursor); }
	WtCursor& operator=(const WtCursor& y) { assert(NULL == y.cursor); }
#endif
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
#if !defined(NDEBUG)
	WtSession(const WtSession& y) : ses(NULL) { assert(NULL == y.ses); }
	WtSession& operator=(const WtSession& y) { assert(NULL == y.ses); }
#endif
	operator WT_SESSION*() const { return ses; }
};

class WtWritableSegment::WtDbTransaction : public DbTransaction {
	WtWritableSegment* m_seg;
	WtSession m_session;
	WtCursor  m_store;
	valvec<WtCursor2> m_indices;
	const SchemaConfig& m_sconf;
	std::string m_strError;
	llong m_sizeDiff;
	WtWritableStore* m_wrtStore;
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
		err = ses->open_cursor(ses, g_dataStoreUri, NULL, "overwrite=true", &m_store.cursor);
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
			err = ses->open_cursor(ses, uri, NULL, "overwrite=false", &m_indices[indexId].insert.cursor);
			if (err) {
				THROW_STD(invalid_argument
					, "ERROR: wiredtiger open index cursor: %s"
					, ses->strerror(ses, err));
			}
			err = ses->open_cursor(ses, uri, NULL, "overwrite=true", &m_indices[indexId].overwrite.cursor);
			if (err) {
				THROW_STD(invalid_argument
					, "ERROR: wiredtiger open index cursor: %s"
					, ses->strerror(ses, err));
			}
		}
		m_sizeDiff = 0;
		m_wrtStore = dynamic_cast<WtWritableStore*>(seg->m_wrtStore.get());
		assert(nullptr != m_store);
	}
#define TERARK_WT_USE_TXN 1
	void startTransaction() override {
#if TERARK_WT_USE_TXN
		WT_SESSION* ses = m_session.ses;
		const char* txnConfig = getenv("TerarkDB_WiredtigerTransactionConfig");
		if (NULL == txnConfig) {
		// wiredtiger 2.8.0 is not binary compatible with 2.7.0
			txnConfig = "isolation=read-committed,sync=false";
		}
	//	fprintf(stderr, "INFO: %s: txnConfig=%s\n", BOOST_CURRENT_FUNCTION, txnConfig);
		int err = ses->begin_transaction(ses, txnConfig);
		if (err) {
			THROW_STD(invalid_argument
				, "ERROR: wiredtiger begin_transaction: %s"
				, ses->strerror(ses, err));
		}
#endif
		m_sizeDiff = 0;
	}
	bool commit() override {
#if TERARK_WT_USE_TXN
		WT_SESSION* ses = m_session.ses;
		int err = ses->commit_transaction(ses, NULL);
		if (err) {
			m_strError = "wiredtiger commit_transaction: ";
			m_strError += ses->strerror(ses, err);
			return false;
		}
#endif
		m_wrtStore->estimateIncDataSize(m_sizeDiff);
		return true;
	}
	const std::string& strError() const override { return m_strError; }
	void rollback() override {
#if TERARK_WT_USE_TXN
		WT_SESSION* ses = m_session.ses;
		int err = ses->rollback_transaction(ses, NULL);
		if (err) {
			THROW_STD(invalid_argument
				, "ERROR: wiredtiger rollback_transaction: %s"
				, ses->strerror(ses, err));
		}
#endif
	}
	bool indexInsert(size_t indexId, fstring key, llong recId) override {
		assert(indexId < m_indices.size());
		WT_ITEM item;
		WT_SESSION* ses = m_session.ses;
		const Schema& schema = m_sconf.getIndexSchema(indexId);
		WT_CURSOR* cur = m_indices[indexId].insert;
		WtWritableIndex::setKeyVal(schema, cur, key, recId, &item, &m_wrtBuf);
		int err = cur->insert(cur);
		m_sizeDiff += sizeof(llong) + key.size();
		if (schema.m_isUnique) {
			if (WT_DUPLICATE_KEY == err) {
				return false;
			}
			if (err) {
				THROW_STD(invalid_argument
					, "ERROR: wiredtiger insert unique index: %s", ses->strerror(ses, err));
			}
		}
		else {
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
		WT_ITEM item;
		WT_SESSION* ses = m_session.ses;
		const Schema& schema = m_sconf.getIndexSchema(indexId);
		WT_CURSOR* cur = m_indices[indexId].insert;
		WtWritableIndex::setKeyVal(schema, cur, key, 0, &item, &m_wrtBuf);
		recIdvec->erase_all();
		if (schema.m_isUnique) {
			int err = cur->search(cur);
			BOOST_SCOPE_EXIT(cur) { cur->reset(cur); } BOOST_SCOPE_EXIT_END;
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
			BOOST_SCOPE_EXIT(cur) { cur->reset(cur); } BOOST_SCOPE_EXIT_END;
			if (WT_NOTFOUND == err) {
				return;
			}
			if (err) {
				THROW_STD(invalid_argument
					, "ERROR: wiredtiger search_near: %s", ses->strerror(ses, err));
			}
			if (cmp >= 0) {
				WtItem item2;
				while (0 == err) {
					llong id;
					cur->get_key(cur, &item2, &id);
					if (item2.size == item.size &&
						memcmp(item2.data, item.data, item.size) == 0)
					{
						recIdvec->push_back(id);
						err = cur->next(cur);
					}
					else break;
				}
			}
		}
		cur->reset(cur);
	}
	void indexRemove(size_t indexId, fstring key, llong recId) override {
		assert(indexId < m_indices.size());
		WT_ITEM item;
		WT_SESSION* ses = m_session.ses;
		const Schema& schema = m_sconf.getIndexSchema(indexId);
		WT_CURSOR* cur = m_indices[indexId].insert;
		WtWritableIndex::setKeyVal(schema, cur, key, recId, &item, &m_wrtBuf);
		int err = cur->remove(cur);
		BOOST_SCOPE_EXIT(cur) { cur->reset(cur); } BOOST_SCOPE_EXIT_END;
		if (WT_NOTFOUND == err) {
			return;
		}
		if (err) {
			THROW_STD(invalid_argument
				, "ERROR: wiredtiger search_near: %s", ses->strerror(ses, err));
		}
		m_sizeDiff -= sizeof(llong) + key.size();
	}
	void indexUpsert(size_t indexId, fstring key, llong recId) override {
		assert(indexId < m_indices.size());
		WtItem item;
		WT_SESSION* ses = m_session.ses;
		const Schema& schema = m_sconf.getIndexSchema(indexId);
		WT_CURSOR* cur = m_indices[indexId].overwrite;
		WtWritableIndex::setKeyVal(schema, cur, key, recId, &item, &m_wrtBuf);
		int err = cur->insert(cur);
		if (err) {
			THROW_STD(invalid_argument
				, "ERROR: wiredtiger upsert %s index: %s"
				, schema.m_isUnique ? "unique" : "multi"
				, ses->strerror(ses, err));
		}
		m_sizeDiff += sizeof(llong) + key.size();
	}
	void storeRemove(llong recId) override {
		WT_SESSION* ses = m_session.ses;
		WT_CURSOR* cur = m_store;
		cur->set_key(cur, recId + 1); // recno = recId + 1
		int err = cur->remove(cur);
		BOOST_SCOPE_EXIT(cur) { cur->reset(cur); } BOOST_SCOPE_EXIT_END;
		if (WT_NOTFOUND == err) {
			return;
		}
		if (err) {
			THROW_STD(invalid_argument
				, "ERROR: wiredtiger store remove: %s", ses->strerror(ses, err));
		}
	//	don't know how many bytes of the record
	//	m_sizeDiff += sizeof(llong) + key.size();
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
			SpinRwLock lock(m_seg->m_segMutex);
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
		m_sizeDiff += row.size();
//		StoreIteratorPtr iter = m_seg->m_wrtStore->createStoreIterForward(NULL);
//		valvec<byte> buf3;
//		iter->seekExact(recId, &buf3);
//		valvec<byte> buf2;
//		m_seg->m_wrtStore->getValue(recId, &buf2, NULL);
//		std::string js3 = m_sconf.m_wrtSchema->toJsonStr(buf3);
//		std::string js2 = m_sconf.m_wrtSchema->toJsonStr(buf2);
//		std::string js1 = m_sconf.m_wrtSchema->toJsonStr(m_wrtBuf);
	}
	void storeGetRow(llong recId, valvec<byte>* row) override {
		WT_SESSION* ses = m_session.ses;
		WT_CURSOR* cur = m_store;
		WtItem item;
		BOOST_SCOPE_EXIT(cur) { cur->reset(cur); } BOOST_SCOPE_EXIT_END;
		cur->reset(cur);
		cur->set_key(cur, recId+1); // recno = recId+1
		int err = cur->search(cur);
		if (err == WT_NOTFOUND) {
			throw ReadUncommitedRecordException(m_seg->m_segDir.string(), -1, recId);
		}
		if (err) {
			throw ReadRecordException(ses->strerror(ses, err), m_seg->m_segDir.string(), -1, recId);
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
