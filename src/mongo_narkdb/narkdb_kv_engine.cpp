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

#include "narkdb_kv_engine.h"

#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>
#include <valgrind/valgrind.h>

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
#include "narkdb_session_cache.h"
#include "narkdb_size_storer.h"
#include "narkdb_util.h"
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

class NarkDbKVEngine::NarkDbJournalFlusher : public BackgroundJob {
public:
    explicit NarkDbJournalFlusher(NarkDbSessionCache* sessionCache)
        : BackgroundJob(false /* deleteSelf */), _sessionCache(sessionCache) {}

    virtual string name() const {
        return "NarkDbJournalFlusher";
    }

    virtual void run() {
        Client::initThread(name().c_str());

        LOG(1) << "starting " << name() << " thread";

        while (!_shuttingDown.load()) {
            try {
                _sessionCache->waitUntilDurable(false);
            } catch (const UserException& e) {
                invariant(e.getCode() == ErrorCodes::ShutdownInProgress);
            }

            int ms = storageGlobalParams.journalCommitIntervalMs;
            if (!ms) {
                ms = 100;
            }

            sleepmillis(ms);
        }
        LOG(1) << "stopping " << name() << " thread";
    }

    void shutdown() {
        _shuttingDown.store(true);
        wait();
    }

private:
    NarkDbSessionCache* _sessionCache;
    std::atomic<bool> _shuttingDown{false};  // NOLINT
};

NarkDbKVEngine::NarkDbKVEngine(const std::string& path,
							   const std::string& extraOpenOptions,
							   size_t cacheSizeGB,
							   bool durable,
							   bool repair)
    : _path(path),
      _durable(durable),
      _sizeStorerSyncTracker(100000, 60 * 1000) {

    boost::filesystem::path basePath = path;
	m_pathNark = path / "nark";
	m_pathWt = path / "wt";

    boost::filesystem::path journalPath = path;
    journalPath /= "journal";
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
    m_tables.reset(new TableMap);
    for (auto& tabDir : fs::directory_iterator(m_pathNark / "tables")) {
    	std::string strTabDir = tabDir.path().string();
    	CompositeTablePtr tab = new MockCompositeTable();
    	tab->load(strTabDir);
    	std::string tabIdent = tabDir.path().filename();
    	auto ib = m_tables->insert_i(tabIdent, tab);
    	invariant(ib.second);
    }

    if (_durable) {
    //    _journalFlusher = stdx::make_unique<NarkDbJournalFlusher>(_sessionCache.get());
    //    _journalFlusher->go();
    }

    {
        NarkDbSession session(_conn);
        fs::path fpath = m_pathNark / "size-store.hash_strmap";
		_sizeStorer.setFilePath(fpath.string());
        if (fs::exists(fpath)) {
			_sizeStorer.fillCache();
        }
    }
}

NarkDbKVEngine::~NarkDbKVEngine() {
    if (_conn) {
        cleanShutdown();
    }
    _sessionCache.reset(NULL);
}

void NarkDbKVEngine::cleanShutdown() {
    log() << "NarkDbKVEngine shutting down";
    syncSizeInfo(true);
    if (_conn) {
        // these must be the last things we do before _conn->close();
        if (_journalFlusher)
            _journalFlusher->shutdown();
        _sessionCache->shuttingDown();

// We want NarkDb to leak memory for faster shutdown except when we are running tools to
// look for memory leaks.
#if !__has_feature(address_sanitizer)
        bool leak_memory = true;
#else
        bool leak_memory = false;
#endif
        const char* config = nullptr;

        if (RUNNING_ON_VALGRIND) {
            leak_memory = false;
        }

        if (leak_memory) {
            config = "leak_memory=true";
        }
        m_tables.reset();
    }
}

Status NarkDbKVEngine::okToRename(OperationContext* opCtx,
                                      StringData fromNS,
                                      StringData toNS,
                                      StringData ident,
                                      const RecordStore* originalRecordStore) const {
    _sizeStorer.storeToCache(_uri(ident),
    		originalRecordStore->numRecords(opCtx),
			originalRecordStore->dataSize(opCtx));
    syncSizeInfo(true);
    return Status::OK();
}

int64_t NarkDbKVEngine::getIdentSize(OperationContext* opCtx, StringData ident) {
    NarkDbSession* session = NarkDbRecoveryUnit::get(opCtx)->getSession(opCtx);
    return NarkDbUtil::getIdentSize(session->getSession(), _uri(ident));
}

Status NarkDbKVEngine::repairIdent(OperationContext* opCtx, StringData ident) {
    NarkDbSession* session = NarkDbRecoveryUnit::get(opCtx)->getSession(opCtx);
    session->closeAllCursors();
    if (isEphemeral()) {
        return Status::OK();
    }
    string uri = _uri(ident);
    return _salvageIfNeeded(uri.c_str());
}

Status NarkDbKVEngine::_salvageIfNeeded(const char* uri) {
}

int NarkDbKVEngine::flushAllFiles(bool sync) {
    LOG(1) << "NarkDbKVEngine::flushAllFiles";
    syncSizeInfo(true);
    m_tables->for_each([](fstring ident, const CompositeTablePtr& tab) {
    	tab->flush();
    });
//  _sessionCache->waitUntilDurable(true);

    return 1;
}

Status NarkDbKVEngine::beginBackup(OperationContext* txn) {
    invariant(!_backupSession);
    _backupSession = std::move(m_tables);
    return Status::OK();
}

void NarkDbKVEngine::endBackup(OperationContext* txn) {
    _backupSession.reset();
}

void NarkDbKVEngine::syncSizeInfo(bool sync) const {
    if (!_sizeStorer)
        return;

    try {
        _sizeStorer->syncCache(sync);
    } catch (const WriteConflictException&) {
        // ignore, we'll try again later.
    }
}

RecoveryUnit* NarkDbKVEngine::newRecoveryUnit() {
    return new NarkDbRecoveryUnit(_sessionCache.get());
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
    _checkIdentPath(ident);
    NarkDbSession session(_conn);

    StatusWith<std::string> result =
        NarkDbRecordStore::generateCreateString(ns, options, _rsOptions);
    if (!result.isOK()) {
        return result.getStatus();
    }
    std::string config = result.getValue();

    string uri = _uri(ident);
    NarkDb_SESSION* s = session.getSession();
    LOG(2) << "NarkDbKVEngine::createRecordStore uri: " << uri << " config: " << config;
    return narkDbRCToStatus(s->create(s, uri.c_str(), config.c_str()));
}

RecordStore* NarkDbKVEngine::getRecordStore(OperationContext* opCtx,
											StringData ns,
											StringData ident,
											const CollectionOptions& options) {
	const bool ephemeral = false;
    if (options.capped) {
        return new NarkDbRecordStore(opCtx,
									 ns,
									 _uri(ident),
									 options.capped,
									 ephemeral,
									 options.cappedSize ? options.cappedSize : 4096,
									 options.cappedMaxDocs ? options.cappedMaxDocs : -1,
									 NULL,
									 _sizeStorer.get());
    } else {
        return new NarkDbRecordStore(opCtx,
									 ns,
									 _uri(ident),
									 false,
									 ephemeral,
									 -1,
									 -1,
									 NULL,
									 _sizeStorer.get());
    }
}

string NarkDbKVEngine::_uri(StringData ident) const {
    return string("table:") + ident.toString();
}

Status NarkDbKVEngine::createSortedDataInterface(OperationContext* opCtx,
                                                     StringData ident,
                                                     const IndexDescriptor* desc) {
    _checkIdentPath(ident);

    std::string collIndexOptions;
    const Collection* collection = desc->getCollection();

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
}

SortedDataInterface* NarkDbKVEngine::getSortedDataInterface(OperationContext* opCtx,
                                                                StringData ident,
                                                                const IndexDescriptor* desc) {
    if (desc->unique())
        return new NarkDbIndexUnique(opCtx, _uri(ident), desc);
    return new NarkDbIndexStandard(opCtx, _uri(ident), desc);
}

Status NarkDbKVEngine::dropIdent(OperationContext* opCtx, StringData ident) {
    _drop(ident);
    return Status::OK();
}

bool NarkDbKVEngine::_drop(StringData ident) {
    string uri = _uri(ident);

    NarkDbSession session(_conn);

    int ret = session.getSession()->drop(session.getSession(), uri.c_str(), "force");
    LOG(1) << "NarkDb drop of  " << uri << " res " << ret;

    if (ret == 0) {
        // yay, it worked
        return true;
    }

    if (ret == EBUSY) {
        // this is expected, queue it up
        {
            stdx::lock_guard<stdx::mutex> lk(_identToDropMutex);
            _identToDrop.insert(uri);
        }
        _sessionCache->closeAll();
        return false;
    }

    invariantNarkDbOK(ret);
    return false;
}

bool NarkDbKVEngine::haveDropsQueued() const {
    Date_t now = Date_t::now();
    Milliseconds delta = now - _previousCheckedDropsQueued;

    if (_sizeStorerSyncTracker.intervalHasElapsed()) {
        _sizeStorerSyncTracker.resetLastTime();
        syncSizeInfo(false);
    }

    // We only want to check the queue max once per second or we'll thrash
    // This is done in haveDropsQueued, not dropAllQueued so we skip the mutex
    if (delta < Milliseconds(1000))
        return false;

    _previousCheckedDropsQueued = now;
    stdx::lock_guard<stdx::mutex> lk(_identToDropMutex);
    return !_identToDrop.empty();
}

void NarkDbKVEngine::dropAllQueued() {
    set<string> mine;
    {
        stdx::lock_guard<stdx::mutex> lk(_identToDropMutex);
        mine = _identToDrop;
    }

    set<string> deleted;

    {
        NarkDbSession session(_conn);
        for (set<string>::const_iterator it = mine.begin(); it != mine.end(); ++it) {
            string uri = *it;
            int ret = session.getSession()->drop(session.getSession(), uri.c_str(), "force");
            LOG(1) << "NarkDb queued drop of  " << uri << " res " << ret;

            if (ret == 0) {
                deleted.insert(uri);
                continue;
            }

            if (ret == EBUSY) {
                // leave in qeuue
                continue;
            }

            invariantNarkDbOK(ret);
        }
    }

    {
        stdx::lock_guard<stdx::mutex> lk(_identToDropMutex);
        for (set<string>::const_iterator it = deleted.begin(); it != deleted.end(); ++it) {
            _identToDrop.erase(*it);
        }
    }
}

bool NarkDbKVEngine::supportsDocLocking() const {
    return true;
}

bool NarkDbKVEngine::supportsDirectoryPerDB() const {
    return true;
}

bool NarkDbKVEngine::hasIdent(OperationContext* opCtx, StringData ident) const {
    return _hasUri(NarkDbRecoveryUnit::get(opCtx)->getSession(opCtx)->getSession(),
                   _uri(ident));
}

bool NarkDbKVEngine::_hasUri(NarkDb_SESSION* session, const std::string& uri) const {
    // can't use NarkDbCursor since this is called from constructor.
    NarkDb_CURSOR* c = NULL;
    int ret = session->open_cursor(session, "metadata:", NULL, NULL, &c);
    if (ret == ENOENT)
        return false;
    invariantNarkDbOK(ret);
    ON_BLOCK_EXIT(c->close, c);

    c->set_key(c, uri.c_str());
    return c->search(c) == 0;
}

std::vector<std::string> NarkDbKVEngine::getAllIdents(OperationContext* opCtx) const {
    std::vector<std::string> all;
    NarkDbCursor cursor("metadata:", NarkDbSession::kMetadataTableId, false, opCtx);
    NarkDb_CURSOR* c = cursor.get();
    if (!c)
        return all;

    while (c->next(c) == 0) {
        const char* raw;
        c->get_key(c, &raw);
        StringData key(raw);
        size_t idx = key.find(':');
        if (idx == string::npos)
            continue;
        StringData type = key.substr(0, idx);
        if (type != "table")
            continue;

        StringData ident = key.substr(idx + 1);
        if (ident == "sizeStorer")
            continue;

        all.push_back(ident.toString());
    }

    return all;
}

int NarkDbKVEngine::reconfigure(const char* str) {
    return _conn->reconfigure(_conn, str);
}

void NarkDbKVEngine::_checkIdentPath(StringData ident) {
    size_t start = 0;
    size_t idx;
    while ((idx = ident.find('/', start)) != string::npos) {
        StringData dir = ident.substr(0, idx);

        boost::filesystem::path subdir = _path;
        subdir /= dir.toString();
        if (!boost::filesystem::exists(subdir)) {
            LOG(1) << "creating subdirectory: " << dir;
            try {
                boost::filesystem::create_directory(subdir);
            } catch (const std::exception& e) {
                error() << "error creating path " << subdir.string() << ' ' << e.what();
                throw;
            }
        }

        start = idx + 1;
    }
}
} }  // namespace mongo::nark

