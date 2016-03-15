/**
 *    Copyright (C) 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
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
#ifdef _MSC_VER
#pragma warning(disable: 4800) // bool conversion
#pragma warning(disable: 4244) // 'return': conversion from '__int64' to 'double', possible loss of data
#pragma warning(disable: 4267) // '=': conversion from 'size_t' to 'int', possible loss of data
#endif

#include "mongo/platform/basic.h"

#include "terarkdb_customization_hooks.h"

#include <boost/filesystem/path.hpp>

#include "mongo/base/init.h"
#include "mongo/base/string_data.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/data_protector.h"
#include "mongo/stdx/memory.h"

namespace mongo { namespace terarkdb {

/* Make a TerarkDbCustomizationHooks pointer a decoration on the global ServiceContext */
MONGO_INITIALIZER_WITH_PREREQUISITES(SetTerarkDbCustomizationHooks, ("SetGlobalEnvironment"))
(InitializerContext* context) {
    auto customizationHooks = stdx::make_unique<EmptyTerarkDbCustomizationHooks>();
    TerarkDbCustomizationHooks::set(getGlobalServiceContext(), std::move(customizationHooks));

    return Status::OK();
}

const auto getCustomizationHooks =
    ServiceContext::declareDecoration<std::unique_ptr<TerarkDbCustomizationHooks>>();

void TerarkDbCustomizationHooks::set(ServiceContext* service,
                                       std::unique_ptr<TerarkDbCustomizationHooks> custHooks) {
    auto& hooks = getCustomizationHooks(service);
    invariant(custHooks);
    hooks = std::move(custHooks);
}

TerarkDbCustomizationHooks* TerarkDbCustomizationHooks::get(ServiceContext* service) {
    return getCustomizationHooks(service).get();
}

EmptyTerarkDbCustomizationHooks::~EmptyTerarkDbCustomizationHooks() {}

bool EmptyTerarkDbCustomizationHooks::enabled() const {
    return false;
}

bool EmptyTerarkDbCustomizationHooks::restartRequired() {
    return false;
}

std::string EmptyTerarkDbCustomizationHooks::getOpenConfig(StringData tableName) {
    return "";
}


std::unique_ptr<DataProtector> EmptyTerarkDbCustomizationHooks::getDataProtector() {
    return std::unique_ptr<DataProtector>();
}

boost::filesystem::path EmptyTerarkDbCustomizationHooks::getProtectedPathSuffix() {
    return "";
}

Status EmptyTerarkDbCustomizationHooks::protectTmpData(
    const uint8_t* in, size_t inLen, uint8_t* out, size_t outLen, size_t* resultLen) {
    return Status(ErrorCodes::InternalError,
                  "Customization hooks must be enabled to use preprocessTmpData.");
}

Status EmptyTerarkDbCustomizationHooks::unprotectTmpData(
    const uint8_t* in, size_t inLen, uint8_t* out, size_t outLen, size_t* resultLen) {
    return Status(ErrorCodes::InternalError,
                  "Customization hooks must be enabled to use postprocessTmpData.");
}
} } // namespace mongo::terarkdb
