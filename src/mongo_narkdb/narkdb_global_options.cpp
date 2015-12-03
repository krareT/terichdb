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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mongo/base/status.h"
#include "narkdb_global_options.h"
#include "narkdb_record_store.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/constraints.h"

namespace mongo { namespace narkdb {

NarkDbGlobalOptions narkDbGlobalOptions;

Status NarkDbGlobalOptions::add(moe::OptionSection* options) {
    moe::OptionSection narkDbOptions("NarkDb options");

    // NarkDb storage engine options
    narkDbOptions.addOptionChaining("storage.narkDb.engineConfig.cacheSizeGB",
                                        "narkDbCacheSizeGB",
                                        moe::Int,
                                        "maximum amount of memory to allocate for cache; "
                                        "defaults to 1/2 of physical RAM").validRange(1, 10000);
    narkDbOptions.addOptionChaining(
                          "storage.narkDb.engineConfig.statisticsLogDelaySecs",
                          "narkDbStatisticsLogDelaySecs",
                          moe::Int,
                          "seconds to wait between each write to a statistics file in the dbpath; "
                          "0 means do not log statistics")
        .validRange(0, 100000)
        .setDefault(moe::Value(0));
    narkDbOptions.addOptionChaining("storage.narkDb.engineConfig.journalCompressor",
                                        "narkDbJournalCompressor",
                                        moe::String,
                                        "use a compressor for log records [none|snappy|zlib]")
        .format("(:?none)|(:?snappy)|(:?zlib)", "(none/snappy/zlib)")
        .setDefault(moe::Value(std::string("snappy")));
    narkDbOptions.addOptionChaining("storage.narkDb.engineConfig.directoryForIndexes",
                                        "narkDbDirectoryForIndexes",
                                        moe::Switch,
                                        "Put indexes and data in different directories");
    narkDbOptions.addOptionChaining("storage.narkDb.engineConfig.configString",
                                        "narkDbEngineConfigString",
                                        moe::String,
                                        "NarkDb storage engine custom "
                                        "configuration settings").hidden();

    // NarkDb collection options
    narkDbOptions.addOptionChaining("storage.narkDb.collectionConfig.blockCompressor",
                                        "narkDbCollectionBlockCompressor",
                                        moe::String,
                                        "block compression algorithm for collection data "
                                        "[none|snappy|zlib]")
        .format("(:?none)|(:?snappy)|(:?zlib)", "(none/snappy/zlib)")
        .setDefault(moe::Value(std::string("snappy")));
    narkDbOptions.addOptionChaining("storage.narkDb.collectionConfig.configString",
                                        "narkDbCollectionConfigString",
                                        moe::String,
                                        "NarkDb custom collection configuration settings")
        .hidden();


    // NarkDb index options
    narkDbOptions.addOptionChaining("storage.narkDb.indexConfig.prefixCompression",
                                        "narkDbIndexPrefixCompression",
                                        moe::Bool,
                                        "use prefix compression on row-store leaf pages")
        .setDefault(moe::Value(true));
    narkDbOptions.addOptionChaining("storage.narkDb.indexConfig.configString",
                                        "narkDbIndexConfigString",
                                        moe::String,
                                        "NarkDb custom index configuration settings").hidden();

    return options->addSection(narkDbOptions);
}

Status NarkDbGlobalOptions::store(const moe::Environment& params,
                                      const std::vector<std::string>& args) {
    // NarkDb storage engine options
    if (params.count("storage.narkDb.engineConfig.cacheSizeGB")) {
        narkDbGlobalOptions.cacheSizeGB =
            params["storage.narkDb.engineConfig.cacheSizeGB"].as<int>();
    }
    if (params.count("storage.syncPeriodSecs")) {
        narkDbGlobalOptions.checkpointDelaySecs =
            static_cast<size_t>(params["storage.syncPeriodSecs"].as<double>());
    }
    if (params.count("storage.narkDb.engineConfig.statisticsLogDelaySecs")) {
        narkDbGlobalOptions.statisticsLogDelaySecs =
            params["storage.narkDb.engineConfig.statisticsLogDelaySecs"].as<int>();
    }
    if (params.count("storage.narkDb.engineConfig.journalCompressor")) {
        narkDbGlobalOptions.journalCompressor =
            params["storage.narkDb.engineConfig.journalCompressor"].as<std::string>();
    }
    if (params.count("storage.narkDb.engineConfig.directoryForIndexes")) {
        narkDbGlobalOptions.directoryForIndexes =
            params["storage.narkDb.engineConfig.directoryForIndexes"].as<bool>();
    }
    if (params.count("storage.narkDb.engineConfig.configString")) {
        narkDbGlobalOptions.engineConfig =
            params["storage.narkDb.engineConfig.configString"].as<std::string>();
        log() << "Engine custom option: " << narkDbGlobalOptions.engineConfig;
    }

    // NarkDb collection options
    if (params.count("storage.narkDb.collectionConfig.blockCompressor")) {
        narkDbGlobalOptions.collectionBlockCompressor =
            params["storage.narkDb.collectionConfig.blockCompressor"].as<std::string>();
    }
    if (params.count("storage.narkDb.collectionConfig.configString")) {
        narkDbGlobalOptions.collectionConfig =
            params["storage.narkDb.collectionConfig.configString"].as<std::string>();
        log() << "Collection custom option: " << narkDbGlobalOptions.collectionConfig;
    }

    // NarkDb index options
    if (params.count("storage.narkDb.indexConfig.prefixCompression")) {
        narkDbGlobalOptions.useIndexPrefixCompression =
            params["storage.narkDb.indexConfig.prefixCompression"].as<bool>();
    }
    if (params.count("storage.narkDb.indexConfig.configString")) {
        narkDbGlobalOptions.indexConfig =
            params["storage.narkDb.indexConfig.configString"].as<std::string>();
        log() << "Index custom option: " << narkDbGlobalOptions.indexConfig;
    }

    return Status::OK();
}

} } // namespace mongo::narkdb
