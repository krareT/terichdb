/*
 * terarkdb_as_wiredtiger.hpp
 *
 *  Created on: 2015Äê12ÔÂ1ÈÕ
 *      Author: leipeng
 */

#ifndef MONGO_TERARKDB_MONGO_TERARKDB_COMMON_HPP_
#define MONGO_TERARKDB_MONGO_TERARKDB_COMMON_HPP_

#ifdef _MSC_VER
#pragma warning(disable: 4800)
#endif

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
using terark::db::CompositeTable;
using terark::db::CompositeTablePtr;

using terark::ulong;
using terark::llong;
using terark::ullong;
using terark::fstring;
using terark::gold_hash_map;
using terark::hash_strmap;

extern const std::string kTerarkDbEngineName;

class TableThreadData : public terark::RefCounter {
public:
	explicit TableThreadData(CompositeTable* tab);
    terark::db::DbContextPtr m_dbCtx;
    terark::valvec<unsigned char> m_buf;
    mongo::terarkdb::SchemaRecordCoder m_coder;
};
typedef boost::intrusive_ptr<TableThreadData> TableThreadDataPtr;

class ThreadSafeTable : public terark::RefCounter {
	tbb::enumerable_thread_specific<TableThreadDataPtr> m_ttd;
public:
	CompositeTablePtr m_tab;
	explicit ThreadSafeTable(const fs::path& dbPath);
	TableThreadData& getMyThreadData();
};
typedef boost::intrusive_ptr<ThreadSafeTable> ThreadSafeTablePtr;

} }

#endif /* MONGO_TERARKDB_MONGO_TERARKDB_COMMON_HPP_ */
