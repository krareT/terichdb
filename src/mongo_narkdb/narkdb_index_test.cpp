// narkdb_index_test.cpp

/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"


#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/catalog/index_catalog_entry.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/json.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/storage/sorted_data_interface_test_harness.h"
#include "narkdb_index.h"
#include "narkdb_record_store.h"
#include "narkdb_recovery_unit.h"
#include "narkdb_session_cache.h"
#include "narkdb_util.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"

namespace mongo { namespace narkdb {

using std::string;

class MyHarnessHelper final : public HarnessHelper {
public:
    MyHarnessHelper() : _dbpath("wt_test"), _conn(NULL) {
        const char* config = "create,cache_size=1G,";
        int ret = narkdb_open(_dbpath.path().c_str(), NULL, config, &_conn);
        invariantNarkDbOK(ret);

        _sessionCache = new NarkDbSessionCache(_conn);
    }

    ~MyHarnessHelper() final {
        delete _sessionCache;
        _conn->close(_conn, NULL);
    }

    std::unique_ptr<SortedDataInterface> newSortedDataInterface(bool unique) final {
        std::string ns = "test.wt";
        OperationContextNoop txn(newRecoveryUnit().release());

        BSONObj spec = BSON("key" << BSON("a" << 1) << "name"
                                  << "testIndex"
                                  << "ns" << ns);

        IndexDescriptor desc(NULL, "", spec);

        StatusWith<std::string> result =
            NarkDbIndex::generateCreateString(kNarkDbEngineName, "", "", desc);
        ASSERT_OK(result.getStatus());

        string uri = "table:" + ns;
        invariantNarkDbOK(NarkDbIndex::Create(&txn, uri, result.getValue()));

        if (unique)
            return stdx::make_unique<NarkDbIndexUnique>(&txn, uri, &desc);
        return stdx::make_unique<NarkDbIndexStandard>(&txn, uri, &desc);
    }

    std::unique_ptr<RecoveryUnit> newRecoveryUnit() final {
        return stdx::make_unique<NarkDbRecoveryUnit>(_sessionCache);
    }

private:
    unittest::TempDir _dbpath;
    NarkDb_CONNECTION* _conn;
    NarkDbSessionCache* _sessionCache;
};

std::unique_ptr<HarnessHelper> newHarnessHelper() {
    return stdx::make_unique<MyHarnessHelper>();
}

TEST(NarkDbIndexTest, GenerateCreateStringEmptyDocument) {
    BSONObj spec = fromjson("{}");
    StatusWith<std::string> result = NarkDbIndex::parseIndexOptions(spec);
    ASSERT_OK(result.getStatus());
    ASSERT_EQ(result.getValue(), "");  // "," would also be valid.
}

TEST(NarkDbIndexTest, GenerateCreateStringUnknownField) {
    BSONObj spec = fromjson("{unknownField: 1}");
    StatusWith<std::string> result = NarkDbIndex::parseIndexOptions(spec);
    const Status& status = result.getStatus();
    ASSERT_NOT_OK(status);
    ASSERT_EQUALS(ErrorCodes::InvalidOptions, status);
}

TEST(NarkDbIndexTest, GenerateCreateStringNonStringConfig) {
    BSONObj spec = fromjson("{configString: 12345}");
    StatusWith<std::string> result = NarkDbIndex::parseIndexOptions(spec);
    const Status& status = result.getStatus();
    ASSERT_NOT_OK(status);
    ASSERT_EQUALS(ErrorCodes::TypeMismatch, status);
}

TEST(NarkDbIndexTest, GenerateCreateStringEmptyConfigString) {
    BSONObj spec = fromjson("{configString: ''}");
    StatusWith<std::string> result = NarkDbIndex::parseIndexOptions(spec);
    ASSERT_OK(result.getStatus());
    ASSERT_EQ(result.getValue(), ",");  // "" would also be valid.
}

TEST(NarkDbIndexTest, GenerateCreateStringInvalidConfigStringOption) {
    BSONObj spec = fromjson("{configString: 'abc=def'}");
    ASSERT_EQ(NarkDbIndex::parseIndexOptions(spec), ErrorCodes::BadValue);
}

TEST(NarkDbIndexTest, GenerateCreateStringValidConfigStringOption) {
    BSONObj spec = fromjson("{configString: 'prefix_compression=true'}");
    ASSERT_EQ(NarkDbIndex::parseIndexOptions(spec), std::string("prefix_compression=true,"));
}

} } // namespace mongo::narkdb
