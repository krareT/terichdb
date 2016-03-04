/*
 * terarkdb_as_wiredtiger.hpp
 *
 *  Created on: 2015Äê12ÔÂ1ÈÕ
 *      Author: leipeng
 */

#ifndef MONGO_NARKDB_MONGO_NARKDB_COMMON_HPP_
#define MONGO_NARKDB_MONGO_NARKDB_COMMON_HPP_

#ifdef _MSC_VER
#pragma warning(disable: 4800)
#endif

#include <terark/db/db_table.hpp>
#include <terark/db/mock_db_engine.hpp>

#include <mongo/db/operation_context.h>
#include <mongo/db/record_id.h>
#include <mongo/db/storage/recovery_unit.h>
#include <mongo/bson/bsonobjbuilder.h>
#include <boost/filesystem.hpp>

#include "record_codec.h"
#include <thread>

namespace terark { namespace db {

} } // namespace terark::db

namespace mongo { namespace terarkdb {

namespace fs = boost::filesystem;
using nark::db::CompositeTable;
using nark::db::CompositeTablePtr;
using nark::db::MockCompositeTable;

using nark::ulong;
using nark::llong;
using nark::ullong;
using nark::fstring;
using nark::gold_hash_map;
using nark::hash_strmap;

extern const std::string kNarkDbEngineName;

class MongoNarkDbContext : public nark::db::DbContext {
public:
	SchemaRecordCoder m_coder;
};
typedef boost::intrusive_ptr<MongoNarkDbContext> MongoNarkDbContextPtr;

} }

#endif /* MONGO_NARKDB_MONGO_NARKDB_COMMON_HPP_ */
