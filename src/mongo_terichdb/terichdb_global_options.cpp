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

#include "mongo/base/status.h"
#include "terichdb_global_options.h"
#include "terichdb_record_store.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/constraints.h"

namespace mongo { namespace db {

TerichDbGlobalOptions terichDbGlobalOptions;

Status TerichDbGlobalOptions::add(moe::OptionSection* options) {
    moe::OptionSection terichDbOptions("TerichDb options");

    // TerichDb storage engine options
    terichDbOptions.addOptionChaining("storage.terichDb.engineConfig.cacheSizeGB",
                                        "terichDbCacheSizeGB",
                                        moe::Int,
                                        "maximum amount of memory to allocate for cache; "
                                        "defaults to 1/2 of physical RAM").validRange(1, 10000);
    terichDbOptions.addOptionChaining(
                          "storage.terichDb.engineConfig.statisticsLogDelaySecs",
                          "terichDbStatisticsLogDelaySecs",
                          moe::Int,
                          "seconds to wait between each write to a statistics file in the dbpath; "
                          "0 means do not log statistics")
        .validRange(0, 100000)
        .setDefault(moe::Value(0));
    terichDbOptions.addOptionChaining("storage.terichDb.engineConfig.journalCompressor",
                                        "terichDbJournalCompressor",
                                        moe::String,
                                        "use a compressor for log records [none|snappy|zlib]")
        .format("(:?none)|(:?snappy)|(:?zlib)", "(none/snappy/zlib)")
        .setDefault(moe::Value(std::string("snappy")));
    terichDbOptions.addOptionChaining("storage.terichDb.engineConfig.directoryForIndexes",
                                        "terichDbDirectoryForIndexes",
                                        moe::Switch,
                                        "Put indexes and data in different directories");
    terichDbOptions.addOptionChaining("storage.terichDb.engineConfig.configString",
                                        "terichDbEngineConfigString",
                                        moe::String,
                                        "TerichDb storage engine custom "
                                        "configuration settings").hidden();

    // TerichDb collection options
    terichDbOptions.addOptionChaining("storage.terichDb.collectionConfig.blockCompressor",
                                        "terichDbCollectionBlockCompressor",
                                        moe::String,
                                        "block compression algorithm for collection data "
                                        "[none|snappy|zlib]")
        .format("(:?none)|(:?snappy)|(:?zlib)", "(none/snappy/zlib)")
        .setDefault(moe::Value(std::string("snappy")));
    terichDbOptions.addOptionChaining("storage.terichDb.collectionConfig.configString",
                                        "terichDbCollectionConfigString",
                                        moe::String,
                                        "TerichDb custom collection configuration settings")
        .hidden();


    // TerichDb index options
    terichDbOptions.addOptionChaining("storage.terichDb.indexConfig.prefixCompression",
                                        "terichDbIndexPrefixCompression",
                                        moe::Bool,
                                        "use prefix compression on row-store leaf pages")
        .setDefault(moe::Value(true));
    terichDbOptions.addOptionChaining("storage.terichDb.indexConfig.configString",
                                        "terichDbIndexConfigString",
                                        moe::String,
                                        "TerichDb custom index configuration settings").hidden();

    return options->addSection(terichDbOptions);
}

Status TerichDbGlobalOptions::store(const moe::Environment& params,
                                      const std::vector<std::string>& args) {
    // TerichDb storage engine options
    if (params.count("storage.terichDb.engineConfig.cacheSizeGB")) {
        terichDbGlobalOptions.cacheSizeGB =
            params["storage.terichDb.engineConfig.cacheSizeGB"].as<int>();
    }
    if (params.count("storage.syncPeriodSecs")) {
        terichDbGlobalOptions.checkpointDelaySecs =
            static_cast<size_t>(params["storage.syncPeriodSecs"].as<double>());
    }
    if (params.count("storage.terichDb.engineConfig.statisticsLogDelaySecs")) {
        terichDbGlobalOptions.statisticsLogDelaySecs =
            params["storage.terichDb.engineConfig.statisticsLogDelaySecs"].as<int>();
    }
    if (params.count("storage.terichDb.engineConfig.journalCompressor")) {
        terichDbGlobalOptions.journalCompressor =
            params["storage.terichDb.engineConfig.journalCompressor"].as<std::string>();
    }
    if (params.count("storage.terichDb.engineConfig.directoryForIndexes")) {
        terichDbGlobalOptions.directoryForIndexes =
            params["storage.terichDb.engineConfig.directoryForIndexes"].as<bool>();
    }
    if (params.count("storage.terichDb.engineConfig.configString")) {
        terichDbGlobalOptions.engineConfig =
            params["storage.terichDb.engineConfig.configString"].as<std::string>();
        log() << "Engine custom option: " << terichDbGlobalOptions.engineConfig;
    }

    // TerichDb collection options
    if (params.count("storage.terichDb.collectionConfig.blockCompressor")) {
        terichDbGlobalOptions.collectionBlockCompressor =
            params["storage.terichDb.collectionConfig.blockCompressor"].as<std::string>();
    }
    if (params.count("storage.terichDb.collectionConfig.configString")) {
        terichDbGlobalOptions.collectionConfig =
            params["storage.terichDb.collectionConfig.configString"].as<std::string>();
        log() << "Collection custom option: " << terichDbGlobalOptions.collectionConfig;
    }

    // TerichDb index options
    if (params.count("storage.terichDb.indexConfig.prefixCompression")) {
        terichDbGlobalOptions.useIndexPrefixCompression =
            params["storage.terichDb.indexConfig.prefixCompression"].as<bool>();
    }
    if (params.count("storage.terichDb.indexConfig.configString")) {
        terichDbGlobalOptions.indexConfig =
            params["storage.terichDb.indexConfig.configString"].as<std::string>();
        log() << "Index custom option: " << terichDbGlobalOptions.indexConfig;
    }

    return Status::OK();
}

} } // namespace mongo::terichdb
