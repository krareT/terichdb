// terarkdb_util_test.cpp

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

#include <sstream>
#include <string>

#include "mongo/base/string_data.h"
#include "mongo/db/operation_context_noop.h"
#include "terarkdb_recovery_unit.h"
#include "terarkdb_session_cache.h"
#include "terarkdb_util.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"

namespace mongo { namespace terarkdb {

using std::string;
using std::stringstream;

class TerarkDbConnection {
public:
    TerarkDbConnection(StringData dbpath, StringData extraStrings) : _conn(NULL) {
        std::stringstream ss;
        ss << "create,";
        ss << extraStrings;
        string config = ss.str();
        int ret = terarkdb_open(dbpath.toString().c_str(), NULL, config.c_str(), &_conn);
        ASSERT_OK(terarkDbRCToStatus(ret));
        ASSERT(_conn);
    }
    ~TerarkDbConnection() {
        _conn->close(_conn, NULL);
    }
    TerarkDb_CONNECTION* getConnection() const {
        return _conn;
    }

private:
    TerarkDb_CONNECTION* _conn;
};

class TerarkDbUtilHarnessHelper {
public:
    TerarkDbUtilHarnessHelper(StringData extraStrings)
        : _dbpath("wt_test"),
          _connection(_dbpath.path(), extraStrings),
          _sessionCache(_connection.getConnection()) {}


    TerarkDbSessionCache* getSessionCache() {
        return &_sessionCache;
    }

    OperationContext* newOperationContext() {
        return new OperationContextNoop(new TerarkDbRecoveryUnit(getSessionCache()));
    }

private:
    unittest::TempDir _dbpath;
    TerarkDbConnection _connection;
    TerarkDbSessionCache _sessionCache;
};

class TerarkDbUtilMetadataTest : public mongo::unittest::Test {
public:
    virtual void setUp() {
        _harnessHelper.reset(new TerarkDbUtilHarnessHelper(""));
        _opCtx.reset(_harnessHelper->newOperationContext());
    }

    virtual void tearDown() {
        _opCtx.reset(NULL);
        _harnessHelper.reset(NULL);
    }

protected:
    const char* getURI() const {
        return "table:mytable";
    }

    OperationContext* getOperationContext() const {
        ASSERT(_opCtx.get());
        return _opCtx.get();
    }

    void createSession(const char* config) {
        TerarkDb_SESSION* wtSession =
            TerarkDbRecoveryUnit::get(_opCtx.get())->getSession(_opCtx.get())->getSession();
        ASSERT_OK(terarkDbRCToStatus(wtSession->create(wtSession, getURI(), config)));
    }

private:
    std::unique_ptr<TerarkDbUtilHarnessHelper> _harnessHelper;
    std::unique_ptr<OperationContext> _opCtx;
};

TEST_F(TerarkDbUtilMetadataTest, GetConfigurationStringInvalidURI) {
    StatusWith<std::string> result = TerarkDbUtil::getMetadata(getOperationContext(), getURI());
    ASSERT_NOT_OK(result.getStatus());
    ASSERT_EQUALS(ErrorCodes::NoSuchKey, result.getStatus().code());
}

TEST_F(TerarkDbUtilMetadataTest, GetConfigurationStringNull) {
    const char* config = NULL;
    createSession(config);
    StatusWith<std::string> result = TerarkDbUtil::getMetadata(getOperationContext(), getURI());
    ASSERT_OK(result.getStatus());
    ASSERT_FALSE(result.getValue().empty());
}

TEST_F(TerarkDbUtilMetadataTest, GetConfigurationStringSimple) {
    const char* config = "app_metadata=(abc=123)";
    createSession(config);
    StatusWith<std::string> result = TerarkDbUtil::getMetadata(getOperationContext(), getURI());
    ASSERT_OK(result.getStatus());
    ASSERT_STRING_CONTAINS(result.getValue(), config);
}

TEST_F(TerarkDbUtilMetadataTest, GetApplicationMetadataInvalidURI) {
    StatusWith<BSONObj> result =
        TerarkDbUtil::getApplicationMetadata(getOperationContext(), getURI());
    ASSERT_NOT_OK(result.getStatus());
    ASSERT_EQUALS(ErrorCodes::NoSuchKey, result.getStatus().code());
}

TEST_F(TerarkDbUtilMetadataTest, GetApplicationMetadataNull) {
    const char* config = NULL;
    createSession(config);
    StatusWith<BSONObj> result =
        TerarkDbUtil::getApplicationMetadata(getOperationContext(), getURI());
    ASSERT_OK(result.getStatus());
    ASSERT_TRUE(result.getValue().isEmpty());
}

TEST_F(TerarkDbUtilMetadataTest, GetApplicationMetadataString) {
    const char* config = "app_metadata=\"abc\"";
    createSession(config);
    StatusWith<BSONObj> result =
        TerarkDbUtil::getApplicationMetadata(getOperationContext(), getURI());
    ASSERT_NOT_OK(result.getStatus());
    ASSERT_EQUALS(ErrorCodes::FailedToParse, result.getStatus().code());
}

TEST_F(TerarkDbUtilMetadataTest, GetApplicationMetadataDuplicateKeys) {
    const char* config = "app_metadata=(abc=123,abc=456)";
    createSession(config);
    StatusWith<BSONObj> result =
        TerarkDbUtil::getApplicationMetadata(getOperationContext(), getURI());
    ASSERT_NOT_OK(result.getStatus());
    ASSERT_EQUALS(ErrorCodes::DuplicateKey, result.getStatus().code());
}

TEST_F(TerarkDbUtilMetadataTest, GetApplicationMetadataTypes) {
    const char* config =
        "app_metadata=(stringkey=\"abc\",boolkey1=true,boolkey2=false,"
        "idkey=def,numkey=123,"
        "structkey=(k1=v2,k2=v2))";
    createSession(config);
    StatusWith<BSONObj> result =
        TerarkDbUtil::getApplicationMetadata(getOperationContext(), getURI());
    ASSERT_OK(result.getStatus());
    const BSONObj& obj = result.getValue();

    BSONElement stringElement = obj.getField("stringkey");
    ASSERT_EQUALS(mongo::String, stringElement.type());
    ASSERT_EQUALS("abc", stringElement.String());

    BSONElement boolElement1 = obj.getField("boolkey1");
    ASSERT_TRUE(boolElement1.isBoolean());
    ASSERT_TRUE(boolElement1.boolean());

    BSONElement boolElement2 = obj.getField("boolkey2");
    ASSERT_TRUE(boolElement2.isBoolean());
    ASSERT_FALSE(boolElement2.boolean());

    BSONElement identifierElement = obj.getField("idkey");
    ASSERT_EQUALS(mongo::String, identifierElement.type());
    ASSERT_EQUALS("def", identifierElement.String());

    BSONElement numberElement = obj.getField("numkey");
    ASSERT_TRUE(numberElement.isNumber());
    ASSERT_EQUALS(123, numberElement.numberInt());

    BSONElement structElement = obj.getField("structkey");
    ASSERT_EQUALS(mongo::String, structElement.type());
    ASSERT_EQUALS("(k1=v2,k2=v2)", structElement.String());
}

TEST_F(TerarkDbUtilMetadataTest, CheckApplicationMetadataFormatVersionMissingKey) {
    createSession("app_metadata=(abc=123)");
    ASSERT_OK(TerarkDbUtil::checkApplicationMetadataFormatVersion(
        getOperationContext(), getURI(), 1, 1));
    ASSERT_NOT_OK(TerarkDbUtil::checkApplicationMetadataFormatVersion(
        getOperationContext(), getURI(), 2, 2));
}

TEST_F(TerarkDbUtilMetadataTest, CheckApplicationMetadataFormatVersionString) {
    createSession("app_metadata=(formatVersion=\"bar\")");
    ASSERT_NOT_OK(TerarkDbUtil::checkApplicationMetadataFormatVersion(
        getOperationContext(), getURI(), 1, 1));
}

TEST_F(TerarkDbUtilMetadataTest, CheckApplicationMetadataFormatVersionNumber) {
    createSession("app_metadata=(formatVersion=2)");
    ASSERT_OK(TerarkDbUtil::checkApplicationMetadataFormatVersion(
        getOperationContext(), getURI(), 2, 3));
    ASSERT_NOT_OK(TerarkDbUtil::checkApplicationMetadataFormatVersion(
        getOperationContext(), getURI(), 1, 1));
    ASSERT_NOT_OK(TerarkDbUtil::checkApplicationMetadataFormatVersion(
        getOperationContext(), getURI(), 3, 3));
}

TEST(TerarkDbUtilTest, GetStatisticsValueMissingTable) {
    TerarkDbUtilHarnessHelper harnessHelper("statistics=(all)");
    TerarkDbRecoveryUnit recoveryUnit(harnessHelper.getSessionCache());
    TerarkDbSession* session = recoveryUnit.getSession(NULL);
    StatusWith<uint64_t> result =
        TerarkDbUtil::getStatisticsValue(session->getSession(),
                                           "statistics:table:no_such_table",
                                           "statistics=(fast)",
                                           TerarkDb_STAT_DSRC_BLOCK_SIZE);
    ASSERT_NOT_OK(result.getStatus());
    ASSERT_EQUALS(ErrorCodes::CursorNotFound, result.getStatus().code());
}

TEST(TerarkDbUtilTest, GetStatisticsValueStatisticsDisabled) {
    TerarkDbUtilHarnessHelper harnessHelper("statistics=(none)");
    TerarkDbRecoveryUnit recoveryUnit(harnessHelper.getSessionCache());
    TerarkDbSession* session = recoveryUnit.getSession(NULL);
    TerarkDb_SESSION* wtSession = session->getSession();
    ASSERT_OK(terarkDbRCToStatus(wtSession->create(wtSession, "table:mytable", NULL)));
    StatusWith<uint64_t> result = TerarkDbUtil::getStatisticsValue(session->getSession(),
                                                                     "statistics:table:mytable",
                                                                     "statistics=(fast)",
                                                                     TerarkDb_STAT_DSRC_BLOCK_SIZE);
    ASSERT_NOT_OK(result.getStatus());
    ASSERT_EQUALS(ErrorCodes::CursorNotFound, result.getStatus().code());
}

TEST(TerarkDbUtilTest, GetStatisticsValueInvalidKey) {
    TerarkDbUtilHarnessHelper harnessHelper("statistics=(all)");
    TerarkDbRecoveryUnit recoveryUnit(harnessHelper.getSessionCache());
    TerarkDbSession* session = recoveryUnit.getSession(NULL);
    TerarkDb_SESSION* wtSession = session->getSession();
    ASSERT_OK(terarkDbRCToStatus(wtSession->create(wtSession, "table:mytable", NULL)));
    // Use connection statistics key which does not apply to a table.
    StatusWith<uint64_t> result = TerarkDbUtil::getStatisticsValue(session->getSession(),
                                                                     "statistics:table:mytable",
                                                                     "statistics=(fast)",
                                                                     TerarkDb_STAT_CONN_SESSION_OPEN);
    ASSERT_NOT_OK(result.getStatus());
    ASSERT_EQUALS(ErrorCodes::NoSuchKey, result.getStatus().code());
}

TEST(TerarkDbUtilTest, GetStatisticsValueValidKey) {
    TerarkDbUtilHarnessHelper harnessHelper("statistics=(all)");
    TerarkDbRecoveryUnit recoveryUnit(harnessHelper.getSessionCache());
    TerarkDbSession* session = recoveryUnit.getSession(NULL);
    TerarkDb_SESSION* wtSession = session->getSession();
    ASSERT_OK(terarkDbRCToStatus(wtSession->create(wtSession, "table:mytable", NULL)));
    // Use connection statistics key which does not apply to a table.
    StatusWith<uint64_t> result = TerarkDbUtil::getStatisticsValue(session->getSession(),
                                                                     "statistics:table:mytable",
                                                                     "statistics=(fast)",
                                                                     TerarkDb_STAT_DSRC_LSM_CHUNK_COUNT);
    ASSERT_OK(result.getStatus());
    // Expect statistics value to be zero for a LSM key on a Btree.
    ASSERT_EQUALS(0U, result.getValue());
}

TEST(TerarkDbUtilTest, GetStatisticsValueAsUInt8) {
    TerarkDbUtilHarnessHelper harnessHelper("statistics=(all)");
    TerarkDbRecoveryUnit recoveryUnit(harnessHelper.getSessionCache());
    TerarkDbSession* session = recoveryUnit.getSession(NULL);
    TerarkDb_SESSION* wtSession = session->getSession();
    ASSERT_OK(terarkDbRCToStatus(wtSession->create(wtSession, "table:mytable", NULL)));

    // Use data source statistics that has a value > 256 on an empty table.
    StatusWith<uint64_t> resultUInt64 =
        TerarkDbUtil::getStatisticsValue(session->getSession(),
                                           "statistics:table:mytable",
                                           "statistics=(fast)",
                                           TerarkDb_STAT_DSRC_ALLOCATION_SIZE);
    ASSERT_OK(resultUInt64.getStatus());
    ASSERT_GREATER_THAN(resultUInt64.getValue(),
                        static_cast<uint64_t>(std::numeric_limits<uint8_t>::max()));

    // Ensure that statistics value retrieved as an 8-bit unsigned value
    // is capped at maximum value for that type.
    StatusWith<uint8_t> resultUInt8 =
        TerarkDbUtil::getStatisticsValueAs<uint8_t>(session->getSession(),
                                                      "statistics:table:mytable",
                                                      "statistics=(fast)",
                                                      TerarkDb_STAT_DSRC_ALLOCATION_SIZE);
    ASSERT_OK(resultUInt8.getStatus());
    ASSERT_EQUALS(std::numeric_limits<uint8_t>::max(), resultUInt8.getValue());

    // Read statistics value as signed 16-bit value with alternative maximum value to
    // std::numeric_limits.
    StatusWith<int16_t> resultInt16 =
        TerarkDbUtil::getStatisticsValueAs<int16_t>(session->getSession(),
                                                      "statistics:table:mytable",
                                                      "statistics=(fast)",
                                                      TerarkDb_STAT_DSRC_ALLOCATION_SIZE,
                                                      static_cast<int16_t>(100));
    ASSERT_OK(resultInt16.getStatus());
    ASSERT_EQUALS(static_cast<uint8_t>(100), resultInt16.getValue());
}

} } // namespace mongo::terarkdb
