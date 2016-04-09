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
#include <terark/db/db_table.hpp>
#include <terark/db/mock_db_engine.hpp>
#include "record_codec.h"

namespace terark { namespace db {

} } // namespace terark::db

namespace mongo { namespace terarkdb {

namespace fs = boost::filesystem;
using terark::db::CompositeTable;
using terark::db::CompositeTablePtr;
using terark::db::MockCompositeTable;

using terark::ulong;
using terark::llong;
using terark::ullong;
using terark::fstring;
using terark::gold_hash_map;
using terark::hash_strmap;

extern const std::string kTerarkDbEngineName;

class MongoTerarkDbContext : public terark::db::DbContext {
public:
	SchemaRecordCoder m_coder;
};
typedef boost::intrusive_ptr<MongoTerarkDbContext> MongoTerarkDbContextPtr;

} }

#endif /* MONGO_TERARKDB_MONGO_TERARKDB_COMMON_HPP_ */
