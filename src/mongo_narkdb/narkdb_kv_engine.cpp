// narkdb_kv_engine.cpp

/**
 *    Copyright (C) 2014 MongoDB Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#ifdef _WIN32
#define NVALGRIND
#endif
#ifdef _MSC_VER
#pragma warning(disable: 4800) // bool conversion
#pragma warning(disable: 4244) // 'return': conversion from '__int64' to 'double', possible loss of data
#pragma warning(disable: 4267) // '=': conversion from 'size_t' to 'int', possible loss of data
#endif

#include "narkdb_kv_engine.h"

#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>
#if !defined(_MSC_VER)
#include <valgrind/valgrind.h>
#endif
#include "mongo/base/error_codes.h"
#include "mongo/db/catalog/collection_catalog_entry.h"
#include "mongo/db/client.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/service_context.h"
#include "narkdb_customization_hooks.h"
#include "narkdb_global_options.h"
#include "narkdb_index.h"
#include "narkdb_record_store.h"
#include "narkdb_recovery_unit.h"
//#include "narkdb_session_cache.h"
#include "narkdb_size_storer.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/util/log.h"
#include "mongo/util/background.h"
#include "mongo/util/exit.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/time_support.h"

#if !defined(__has_feature)
#define __has_feature(x) 0
#endif

namespace mongo { namespace narkdb {

using std::set;
using std::string;

NarkDbKVEngine::NarkDbKVEngine(const std::string& path,
							   const std::string& extraOpenOptions,
							   size_t cacheSizeGB,
							   bool durable,
							   bool repair)
    : _path(path),
      _durable(durable),
      _sizeStorerSyncTracker(100000, 60 * 1000) {

    boost::filesystem::path basePath = path;
	m_pathNark = basePath / "nark";
	m_pathWt = basePath / "wt";

    boost::filesystem::path journalPath = basePath / "journal";
    if (_durable) {
        if (!boost::filesystem::exists(journalPath)) {
            try {
                boost::filesystem::create_directory(journalPath);
            } catch (std::exception& e) {
                log() << "error creating journal dir " << journalPath.string() << ' ' << e.what();
                throw;
            }
        }
    }

    _previousCheckedDropsQueued = Date_t::now();

    std::stringstream ss;
    if (!_durable) {
        // If we started without the journal, but previously used the journal then open with the
        // NarkDb log enabled to perform any unclean shutdown recovery and then close and reopen in
        // the normal path without the journal.
    	/*
        if (boost::filesystem::exists(journalPath)) {
            string config = ss.str();
            log() << "Detected NarkDb journal files.  Running recovery from last checkpoint.";
            log() << "journal to nojournal transition config: " << config;
            int ret = narkdb_open(path.c_str(), &_eventHandler, config.c_str(), &_conn);
            if (ret == EINVAL) {
                fassertFailedNoTrace(28717);
            } else if (ret != 0) {
                Status s(narkDbRCToStatus(ret));
                msgassertedNoTrace(28718, s.reason());
            }
            invariantNarkDbOK(_conn->close(_conn, NULL));
        }
        // This setting overrides the earlier setting because it is later in the config string.
        ss << ",log=(enabled=false),";
        */
    }
    log() << "narkdb_open : " << path;
    for (auto& tabDir : fs::directory_iterator(m_pathNark / "tables")) {
    	std::string strTabDir = tabDir.path().string();
    	// CompositeTablePtr tab = new MockCompositeTable();
    	// tab->load(strTabDir);
    	std::string tabIdent = tabDir.path().filename().string();
    	auto ib = m_tables.insert_i(tabIdent, nullptr);
    	invariant(ib.second);
    }

    if (_durable) {
    //    _journalFlusher = stdx::make_unique<NarkDbJournalFlusher>(_sessionCache.get());
    //    _journalFlusher->go();
    }

    {
        fs::path fpath = m_pathNark / "size-store.hash_strmap";
		_sizeStorer.setFilePath(fpath.string());
        if (fs::exists(fpath)) {
			_sizeStorer.fillCache();
        }
    }
}

NarkDbKVEngine::~NarkDbKVEngine() {
    cleanShutdown();
}

void NarkDbKVEngine::cleanShutdown() {
    log() << "NarkDbKVEngine shutting down";
//  syncSizeInfo(true);
    m_tables.clear();
}

Status NarkDbKVEngine::okToRename(OperationContext* opCtx,
                                  StringData fromNS,
                                  StringData toNS,
                                  StringData ident,
                                  const RecordStore* originalRecordStore) const {
    return Status::OK();
}

int64_t NarkDbKVEngine::getIdentSize(OperationContext* opCtx, StringData ident) {
	size_t i = m_tables.find_i(ident);
	if (m_tables.end_i() == i) {
		return 0;
	}
	return m_tables.val(i)->dataStorageSize();
}

Status NarkDbKVEngine::repairIdent(OperationContext* opCtx, StringData ident) {
	return Status::OK();
}

int NarkDbKVEngine::flushAllFiles(bool sync) {
    LOG(1) << "NarkDbKVEngine::flushAllFiles";
//  syncSizeInfo(true);
    m_tables.for_each([](const TableMap::value_type& x) {
    	x.second->flush();
    });
//  _sessionCache->waitUntilDurable(true);

    return 1;
}

Status NarkDbKVEngine::beginBackup(OperationContext* txn) {
    invariant(!_backupSession);
//  _backupSession = std::move(m_tables);
    return Status::OK();
}

void NarkDbKVEngine::endBackup(OperationContext* txn) {
    _backupSession.reset();
}

RecoveryUnit* NarkDbKVEngine::newRecoveryUnit() {
    return new NarkDbRecoveryUnit();
}

void NarkDbKVEngine::setRecordStoreExtraOptions(const std::string& options) {
    _rsOptions = options;
}

void NarkDbKVEngine::setSortedDataInterfaceExtraOptions(const std::string& options) {
    _indexOptions = options;
}

Status NarkDbKVEngine::createRecordStore(OperationContext* opCtx,
										 StringData ns,
                                         StringData ident,
                                         const CollectionOptions& options) {
    StatusWith<std::string> result =
        NarkDbRecordStore::generateCreateString(ns, options, _rsOptions);
    if (!result.isOK()) {
        return result.getStatus();
    }
    std::string config = result.getValue();

    LOG(2) << "NarkDbKVEngine::createRecordStore: ns:" << ns << ", config: " << config;
    return Status::OK();
}

RecordStore* NarkDbKVEngine::getRecordStore(OperationContext* opCtx,
											StringData ns,
											StringData ident,
											const CollectionOptions& options) {
	const bool ephemeral = false;
    if (options.capped) {
		/*
        return new NarkDbRecordStore(opCtx,
									 ns,
									 _uri(ident),
									 options.capped,
									 ephemeral,
									 options.cappedSize ? options.cappedSize : 4096,
									 options.cappedMaxDocs ? options.cappedMaxDocs : -1,
									 NULL);
	*/
    } else {
        return new NarkDbRecordStore(opCtx,
									 ns,
									 _uri(ident),
									 NULL);
    }
}

string NarkDbKVEngine::_uri(StringData ident) const {
    return string("table:") + ident.toString();
}

Status NarkDbKVEngine::createSortedDataInterface(OperationContext* opCtx,
                                                 StringData ident,
                                                 const IndexDescriptor* desc) {
    std::string collIndexOptions;
    const Collection* collection = desc->getCollection();
/*
    // Treat 'collIndexOptions' as an empty string when the collection member of 'desc' is NULL in
    // order to allow for unit testing NarkDbKVEngine::createSortedDataInterface().
    if (collection) {
        const CollectionCatalogEntry* cce = collection->getCatalogEntry();
        const CollectionOptions collOptions = cce->getCollectionOptions(opCtx);

        if (!collOptions.indexOptionDefaults["storageEngine"].eoo()) {
            BSONObj storageEngineOptions = collOptions.indexOptionDefaults["storageEngine"].Obj();
            collIndexOptions = storageEngineOptions.getFieldDotted(_canonicalName + ".configString")
                                   .valuestrsafe();
        }
    }

    StatusWith<std::string> result = NarkDbIndex::generateCreateString(
        _canonicalName, _indexOptions, collIndexOptions, *desc);
    if (!result.isOK()) {
        return result.getStatus();
    }

    std::string config = result.getValue();

    LOG(2) << "NarkDbKVEngine::createSortedDataInterface ident: " << ident
           << " config: " << config;
    return narkDbRCToStatus(NarkDbIndex::Create(opCtx, _uri(ident), config));
*/
    size_t i = m_indices.find_i(ident);
    if (i < m_indices.end_i()) {

    }
    return Status::OK();
}

SortedDataInterface* NarkDbKVEngine::getSortedDataInterface(OperationContext* opCtx,
															StringData ident,
															const IndexDescriptor* desc)
{
	auto tabDir = m_pathNark / "tables" / ident.toString();
	size_t idx = m_tables.insert_i(ident).second;
	auto& tab = m_tables.val(idx);
	if (tab == nullptr) {
		tab = new MockCompositeTable();
		tab->load(tabDir.string());
	}
    if (desc->unique())
        return new NarkDbIndexUnique(&*tab, opCtx, desc);
    else
    	return new NarkDbIndexStandard(&*tab, opCtx, desc);
}

Status NarkDbKVEngine::dropIdent(OperationContext* opCtx, StringData ident) {
    LOG(1) << "NarkDb drop table: ident=" << ident << "\n";
    _drop(ident);
    return Status::OK();
}

bool NarkDbKVEngine::_drop(StringData ident) {
    m_tables.erase(ident);
    return true;
}

bool NarkDbKVEngine::supportsDocLocking() const {
    return true;
}

bool NarkDbKVEngine::supportsDirectoryPerDB() const {
    return true;
}

bool NarkDbKVEngine::hasIdent(OperationContext* opCtx, StringData ident) const {
	return m_tables.exists(ident);
}

std::vector<std::string> NarkDbKVEngine::getAllIdents(OperationContext* opCtx) const {
    std::vector<std::string> all;
    all.reserve(m_tables.size());
    for (size_t i = m_tables.beg_i(); i < m_tables.end_i(); i = m_tables.next_i(i)) {
    	all.push_back(m_tables.key(i).str());
    }
    return all;
}

int NarkDbKVEngine::reconfigure(const char* str) {
    //return _conn->reconfigure(_conn, str);
	return 0;
}

} }  // namespace mongo::nark

