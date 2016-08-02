/*
 *  Created on: 2015-12-01
 *      Author: leipeng
 */

#ifndef MONGO_TERARKDB_MONGO_TERARKDB_COMMON_HPP_
#define MONGO_TERARKDB_MONGO_TERARKDB_COMMON_HPP_

#ifdef _MSC_VER
#pragma warning(disable: 4800)
#endif

#include <mongo/platform/basic.h>
#include <mongo/db/operation_context.h>
#include <mongo/db/record_id.h>
#include <mongo/db/storage/recovery_unit.h>
#include <mongo/bson/bsonobjbuilder.h>
#include <boost/filesystem.hpp>
#include <thread>
#include <tbb/enumerable_thread_specific.h>
#include <terark/db/db_table.hpp>
#include "record_codec.h"

namespace terark { namespace db {

} } // namespace terark::db

namespace mongo { namespace terarkdb {

namespace fs = boost::filesystem;
using terark::db::DbTable;
using terark::db::DbTablePtr;

using terark::ulong;
using terark::llong;
using terark::ullong;
using terark::fstring;
using terark::gold_hash_map;
using terark::hash_strmap;
using terark::valvec;

extern const std::string kTerarkDbEngineName;

class ThreadSafeTable;

class TableThreadData : public terark::RefCounter {
public:
	explicit TableThreadData(DbTable* tab);
    terark::db::DbContextPtr m_dbCtx;
    terark::valvec<unsigned char> m_buf;
    mongo::terarkdb::SchemaRecordCoder m_coder;
	llong     m_lastUseTime;
};
typedef boost::intrusive_ptr<TableThreadData> TableThreadDataPtr;

struct IndexIterData : public terark::RefCounter {
	terark::db::DbContextPtr      m_ctx;
	terark::db::IndexIteratorPtr  m_cursor;
	terark::valvec<unsigned char> m_curKey;
	terark::valvec<         char> m_qryKey;
//	mongo::terarkdb::SchemaRecordCoder m_coder;
	terark::valvec<unsigned char> m_endPositionKey;
	llong     m_lastUseTime;

	IndexIterData(DbTable* tab, size_t indexId, bool forward);
	~IndexIterData();

	int seekLowerBound(llong* recId) {
		return m_cursor->seekLowerBound(m_qryKey, recId, &m_curKey);
	}
	int seekUpperBound(llong* recId) {
		return m_cursor->seekUpperBound(m_qryKey, recId, &m_curKey);
	}
	bool increment(llong* recId) {
		return m_cursor->increment(recId, &m_curKey);
	}
	void reset() {
		m_cursor->reset();
		m_ctx->trySyncSegCtxSpeculativeLock(m_ctx->m_tab);
	}
};
typedef boost::intrusive_ptr<IndexIterData> IndexIterDataPtr;

class RecoveryUnitData : public terark::RefCounter {
public:
	struct MVCCTime {
		uint32_t insertTime;
		uint32_t deleteTime;
		MVCCTime() : insertTime(), deleteTime() {}
	};
	gold_hash_map<llong, MVCCTime> m_records;
	TableThreadDataPtr   m_ttd;
	ThreadSafeTable*     m_tst;
	uint32_t m_iterNum;
	uint32_t m_mvccTime;
	explicit RecoveryUnitData(ThreadSafeTable* tst);
	~RecoveryUnitData();
};
typedef boost::intrusive_ptr<RecoveryUnitData> RecoveryUnitDataPtr;

class RuStoreIteratorBase : public terark::db::StoreIterator {
public:
	llong  m_id;
	RecoveryUnit*         m_ru;
	ThreadSafeTable*      m_tst;
	RecoveryUnitDataPtr   m_rud;

	RuStoreIteratorBase(RecoveryUnit* ru, ThreadSafeTable* tst);
	~RuStoreIteratorBase();
	bool getVal(llong id, valvec<unsigned char>* val) const;
	void traceFunc(const char* func) const;
};

class ThreadSafeTable : public terark::RefCounter {
public:
	~ThreadSafeTable();
	void destroy(); // workaround mongodb
	DbTablePtr m_tab;
	explicit ThreadSafeTable(const fs::path& dbPath);
	TableThreadData& getMyThreadData();

	// not owned by a thread, but by a cursor
	TableThreadDataPtr allocTableThreadData();
	void releaseTableThreadData(TableThreadDataPtr ttd);

	IndexIterDataPtr allocIndexIter(size_t indexId, bool forward);
	void releaseIndexIter(size_t indexId, bool forward, IndexIterDataPtr);

	// for RecoveryUnit:
	RecoveryUnitData* getRecoveryUnitData(RecoveryUnit*);
	RecoveryUnitDataPtr tryRecoveryUnitData(RecoveryUnit*);
	void removeRecoveryUnitData(RecoveryUnit*);
	void removeRegisterEntry(RecoveryUnit*, RecoveryUnitData*, size_t f);

	void registerInsert(RecoveryUnit*, RecordId id);
	void commitInsert(RecoveryUnit*, RecordId id);
	void rollbackInsert(RecoveryUnit*, RecordId id);

	void registerDelete(RecoveryUnit*, RecordId id);
	void commitDelete(RecoveryUnit*, RecordId id);
	void rollbackDelete(RecoveryUnit*, RecordId id);

	RuStoreIteratorBase* createStoreIter(RecoveryUnit*, bool forward);

protected:
	tbb::enumerable_thread_specific<TableThreadDataPtr> m_ttd;
	std::mutex m_cursorCacheMutex;
	valvec<TableThreadDataPtr> m_cursorCache; // for RecordStore Iterator
	valvec<valvec<IndexIterDataPtr> > m_indexForwardIterCache;
	valvec<valvec<IndexIterDataPtr> > m_indexBackwardIterCache;
	llong m_cacheExpireMillisec;
	void expiringCacheItems(valvec<valvec<IndexIterDataPtr> >& vv, llong now);

	std::mutex m_ruMapMutex;
	gold_hash_map<RecoveryUnit*, RecoveryUnitDataPtr> m_ruMap;
};
typedef boost::intrusive_ptr<ThreadSafeTable> ThreadSafeTablePtr;

} }

#endif /* MONGO_TERARKDB_MONGO_TERARKDB_COMMON_HPP_ */
