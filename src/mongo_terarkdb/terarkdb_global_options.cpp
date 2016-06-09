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
#include "terarkdb_global_options.h"
#include "terarkdb_record_store.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/constraints.h"

namespace mongo { namespace terarkdb {

TerarkDbGlobalOptions terarkDbGlobalOptions;

Status TerarkDbGlobalOptions::add(moe::OptionSection* options) {
    moe::OptionSection terarkDbOptions("TerarkDb options");

    // TerarkDb storage engine options
    terarkDbOptions.addOptionChaining("storage.terarkDb.engineConfig.cacheSizeGB",
                                        "terarkDbCacheSizeGB",
                                        moe::Int,
                                        "maximum amount of memory to allocate for cache; "
                                        "defaults to 1/2 of physical RAM").validRange(1, 10000);
    terarkDbOptions.addOptionChaining(
                          "storage.terarkDb.engineConfig.statisticsLogDelaySecs",
                          "terarkDbStatisticsLogDelaySecs",
                          moe::Int,
                          "seconds to wait between each write to a statistics file in the dbpath; "
                          "0 means do not log statistics")
        .validRange(0, 100000)
        .setDefault(moe::Value(0));
    terarkDbOptions.addOptionChaining("storage.terarkDb.engineConfig.journalCompressor",
                                        "terarkDbJournalCompressor",
                                        moe::String,
                                        "use a compressor for log records [none|snappy|zlib]")
        .format("(:?none)|(:?snappy)|(:?zlib)", "(none/snappy/zlib)")
        .setDefault(moe::Value(std::string("snappy")));
    terarkDbOptions.addOptionChaining("storage.terarkDb.engineConfig.directoryForIndexes",
                                        "terarkDbDirectoryForIndexes",
                                        moe::Switch,
                                        "Put indexes and data in different directories");
    terarkDbOptions.addOptionChaining("storage.terarkDb.engineConfig.configString",
                                        "terarkDbEngineConfigString",
                                        moe::String,
                                        "TerarkDb storage engine custom "
                                        "configuration settings").hidden();

    // TerarkDb collection options
    terarkDbOptions.addOptionChaining("storage.terarkDb.collectionConfig.blockCompressor",
                                        "terarkDbCollectionBlockCompressor",
                                        moe::String,
                                        "block compression algorithm for collection data "
                                        "[none|snappy|zlib]")
        .format("(:?none)|(:?snappy)|(:?zlib)", "(none/snappy/zlib)")
        .setDefault(moe::Value(std::string("snappy")));
    terarkDbOptions.addOptionChaining("storage.terarkDb.collectionConfig.configString",
                                        "terarkDbCollectionConfigString",
                                        moe::String,
                                        "TerarkDb custom collection configuration settings")
        .hidden();


    // TerarkDb index options
    terarkDbOptions.addOptionChaining("storage.terarkDb.indexConfig.prefixCompression",
                                        "terarkDbIndexPrefixCompression",
                                        moe::Bool,
                                        "use prefix compression on row-store leaf pages")
        .setDefault(moe::Value(true));
    terarkDbOptions.addOptionChaining("storage.terarkDb.indexConfig.configString",
                                        "terarkDbIndexConfigString",
                                        moe::String,
                                        "TerarkDb custom index configuration settings").hidden();

    return options->addSection(terarkDbOptions);
}

Status TerarkDbGlobalOptions::store(const moe::Environment& params,
                                      const std::vector<std::string>& args) {
    // TerarkDb storage engine options
    if (params.count("storage.terarkDb.engineConfig.cacheSizeGB")) {
        terarkDbGlobalOptions.cacheSizeGB =
            params["storage.terarkDb.engineConfig.cacheSizeGB"].as<int>();
    }
    if (params.count("storage.syncPeriodSecs")) {
        terarkDbGlobalOptions.checkpointDelaySecs =
            static_cast<size_t>(params["storage.syncPeriodSecs"].as<double>());
    }
    if (params.count("storage.terarkDb.engineConfig.statisticsLogDelaySecs")) {
        terarkDbGlobalOptions.statisticsLogDelaySecs =
            params["storage.terarkDb.engineConfig.statisticsLogDelaySecs"].as<int>();
    }
    if (params.count("storage.terarkDb.engineConfig.journalCompressor")) {
        terarkDbGlobalOptions.journalCompressor =
            params["storage.terarkDb.engineConfig.journalCompressor"].as<std::string>();
    }
    if (params.count("storage.terarkDb.engineConfig.directoryForIndexes")) {
        terarkDbGlobalOptions.directoryForIndexes =
            params["storage.terarkDb.engineConfig.directoryForIndexes"].as<bool>();
    }
    if (params.count("storage.terarkDb.engineConfig.configString")) {
        terarkDbGlobalOptions.engineConfig =
            params["storage.terarkDb.engineConfig.configString"].as<std::string>();
        log() << "Engine custom option: " << terarkDbGlobalOptions.engineConfig;
    }

    // TerarkDb collection options
    if (params.count("storage.terarkDb.collectionConfig.blockCompressor")) {
        terarkDbGlobalOptions.collectionBlockCompressor =
            params["storage.terarkDb.collectionConfig.blockCompressor"].as<std::string>();
    }
    if (params.count("storage.terarkDb.collectionConfig.configString")) {
        terarkDbGlobalOptions.collectionConfig =
            params["storage.terarkDb.collectionConfig.configString"].as<std::string>();
        log() << "Collection custom option: " << terarkDbGlobalOptions.collectionConfig;
    }

    // TerarkDb index options
    if (params.count("storage.terarkDb.indexConfig.prefixCompression")) {
        terarkDbGlobalOptions.useIndexPrefixCompression =
            params["storage.terarkDb.indexConfig.prefixCompression"].as<bool>();
    }
    if (params.count("storage.terarkDb.indexConfig.configString")) {
        terarkDbGlobalOptions.indexConfig =
            params["storage.terarkDb.indexConfig.configString"].as<std::string>();
        log() << "Index custom option: " << terarkDbGlobalOptions.indexConfig;
    }

    return Status::OK();
}

} } // namespace mongo::terarkdb
