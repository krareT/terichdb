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

class TableThreadData : public terark::RefCounter {
public:
	explicit TableThreadData(DbTable* tab);
    terark::db::DbContextPtr m_dbCtx;
    terark::valvec<unsigned char> m_buf;
    mongo::terarkdb::SchemaRecordCoder m_coder;
};
typedef boost::intrusive_ptr<TableThreadData> TableThreadDataPtr;

struct IndexIterData : public terark::RefCounter {
	terark::db::IndexIteratorPtr  m_cursor;
	terark::valvec<unsigned char> m_curKey;
	terark::valvec<         char> m_qryKey;
//	mongo::terarkdb::SchemaRecordCoder m_coder;
	terark::valvec<unsigned char> m_endPositionKey;

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
	}
};
typedef boost::intrusive_ptr<IndexIterData> IndexIterDataPtr;

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

protected:
	tbb::enumerable_thread_specific<TableThreadDataPtr> m_ttd;
	std::mutex m_cursorCacheMutex;
	valvec<TableThreadDataPtr> m_cursorCache; // for RecordStore Iterator
	valvec<valvec<IndexIterDataPtr> > m_indexForwardIterCache;
	valvec<valvec<IndexIterDataPtr> > m_indexBackwardIterCache;
};
typedef boost::intrusive_ptr<ThreadSafeTable> ThreadSafeTablePtr;

} }

#endif /* MONGO_TERARKDB_MONGO_TERARKDB_COMMON_HPP_ */
