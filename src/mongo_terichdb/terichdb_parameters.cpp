/**
 *    Copyright (C) 2016 Terark Inc.
 *    This file is heavily modified based on MongoDB WiredTiger StorageEngine
 *    Created on: 2015-12-01
 *    Author    : leipeng, rockeet@gmail.com
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
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage
#ifdef _MSC_VER
#pragma warning(disable: 4800) // bool conversion
#pragma warning(disable: 4244) // 'return': conversion from '__int64' to 'double', possible loss of data
#pragma warning(disable: 4267) // '=': conversion from 'size_t' to 'int', possible loss of data
#endif

#include "mongo/platform/basic.h"

#include "terichdb_parameters.h"

#include "mongo/logger/parse_log_component_settings.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo { namespace db {

using std::string;

TerichDbEngineRuntimeConfigParameter::TerichDbEngineRuntimeConfigParameter(
    TerichDbKVEngine* engine)
    : ServerParameter(
          ServerParameterSet::getGlobal(), "terichDbEngineRuntimeConfig", false, true),
      _engine(engine) {}


void TerichDbEngineRuntimeConfigParameter::append(OperationContext* txn,
                                                    BSONObjBuilder& b,
                                                    const std::string& name) {
    b << name << "";
}

Status TerichDbEngineRuntimeConfigParameter::set(const BSONElement& newValueElement) {
    try {
        return setFromString(newValueElement.String());
    } catch (MsgAssertionException msg) {
        return Status(
            ErrorCodes::BadValue,
            mongoutils::str::stream()
                << "Invalid value for terichDbEngineRuntimeConfig via setParameter command: "
                << newValueElement);
    }
}

Status TerichDbEngineRuntimeConfigParameter::setFromString(const std::string& str) {
    size_t pos = str.find('\0');
    if (pos != std::string::npos) {
        return Status(ErrorCodes::BadValue,
                      (str::stream()
                       << "TerichDb configuration strings cannot have embedded null characters. "
                          "Embedded null found at position " << pos));
    }

    log() << "Reconfiguring TerichDb storage engine with config string: \"" << str << "\"";

    int ret = _engine->reconfigure(str.c_str());
    if (ret != 0) {
        string result =
            (mongoutils::str::stream() << "TerichDb reconfiguration failed with error code ("
                                       << ret << ")");
        error() << result;

        return Status(ErrorCodes::BadValue, result);
    }

    return Status::OK();
}
} }  // namespace mongo::terark

