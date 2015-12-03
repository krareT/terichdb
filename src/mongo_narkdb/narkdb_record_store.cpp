// narkdb_record_store.cpp

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

#include "mongo/platform/basic.h"

#include "narkdb_record_store.h"

#include "mongo_narkdb_common.hpp"

#include "mongo/base/checked_cast.h"
#include "mongo/bson/util/builder.h"
#include "mongo/db/concurrency/locker.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/oplog_hack.h"
#include "narkdb_customization_hooks.h"
#include "narkdb_global_options.h"
#include "narkdb_kv_engine.h"
#include "narkdb_record_store_oplog_stones.h"
#include "narkdb_recovery_unit.h"
#include "narkdb_session_cache.h"
#include "narkdb_size_storer.h"
#include "narkdb_util.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/time_support.h"

//#define RS_ITERATOR_TRACE(x) log() << "NarkDbRS::Iterator " << x
#define RS_ITERATOR_TRACE(x)

namespace mongo { namespace narkdb {

using std::unique_ptr;
using std::string;

namespace {

static const int kMinimumRecordStoreVersion = 1;
static const int kCurrentRecordStoreVersion = 1;  // New record stores use this by default.
static const int kMaximumRecordStoreVersion = 1;
static_assert(kCurrentRecordStoreVersion >= kMinimumRecordStoreVersion,
              "kCurrentRecordStoreVersion >= kMinimumRecordStoreVersion");
static_assert(kCurrentRecordStoreVersion <= kMaximumRecordStoreVersion,
              "kCurrentRecordStoreVersion <= kMaximumRecordStoreVersion");

bool shouldUseOplogHack(OperationContext* opCtx, const std::string& uri) {
    StatusWith<BSONObj> appMetadata = NarkDbUtil::getApplicationMetadata(opCtx, uri);
    if (!appMetadata.isOK()) {
        return false;
    }

    return (appMetadata.getValue().getIntField("oplogKeyExtractionVersion") == 1);
}

}  // namespace

MONGO_FP_DECLARE(NarkDbWriteConflictException);

class NarkDbRecordStore::Cursor final : public SeekableRecordCursor {
public:
    Cursor(OperationContext* txn, const NarkDbRecordStore& rs, bool forward = true)
        : _rs(rs),
          _txn(txn) {
    	m_ctx = dynamic_cast<MongoNarkDbContext*>(rs.m_table->createDbContext());
    	if (forward)
    		_cursor = rs.m_table->createStoreIterForward(&*m_ctx);
    	else
    		_cursor = rs.m_table->createStoreIterBackward(&*m_ctx);
    }

    boost::optional<Record> next() final {
        if (_eof)
            return {};

        llong recIdx;
        bool mustAdvance = !_skipNextAdvance;
        if (mustAdvance) {
            if (!_cursor->increment(&recIdx, &m_recBuf)) {
            	_lastReturnedId = recIdx + 1;
                _eof = true;
                return {};
            }
        	m_bson = m_ctx->m_coder.decode(&*_rs.m_table->m_rowSchema,
        			(char*)m_recBuf.data(), m_recBuf.size());
        }
        _skipNextAdvance = false;
        const RecordId id = recIdx + 1;
        _lastReturnedId = id;
        return {{id, {static_cast<const char*>(m_bson.objdata()),
        			  static_cast<int>(m_bson.objsize)}}};
    }

    boost::optional<Record> seekExact(const RecordId& id) final {
        _skipNextAdvance = false;
        llong recIdx = id.repr() - 1;
        if (!_cursor->seekExact(recIdx, &m_recBuf)) {
            _eof = true;
            return {};
        }
    	m_bson = m_ctx->m_coder.decode(&*_rs.m_table->m_rowSchema,
    			(char*)m_recBuf.data(), m_recBuf.size());
        _lastReturnedId = id;
        _eof = false;
        return {{id, {static_cast<const char*>(m_bson.objdata()),
        			  static_cast<int>(m_bson.objsize())}}};
    }

    void save() final {
        try {
        	_cursor->reset();
        } catch (const WriteConflictException& wce) {
            // Ignore since this is only called when we are about to kill our transaction
            // anyway.
        }
    }

    void saveUnpositioned() final {
        save();
        _lastReturnedId = RecordId();
    }

    bool restore() final {
        _skipNextAdvance = false;

        // If we've hit EOF, then this iterator is done and need not be restored.
        if (_eof)
            return true;

        if (_lastReturnedId.isNull())
            return true;

        llong recIdx = _lastReturnedId.repr() - 1;
        if (!_cursor->seekExact(recIdx, &m_recBuf)) {
            _eof = true;
            return false;
        }

    	m_bson = m_ctx->m_coder.decode(&*_rs.m_table->m_rowSchema,
    			(char*)m_recBuf.data(), m_recBuf.size());
    	_skipNextAdvance = true;

        return true;  // Landed right where we left off.
    }

    void detachFromOperationContext() final {
        _txn = nullptr;
        _cursor = boost::none;
    }

    void reattachToOperationContext(OperationContext* txn) final {
        _txn = txn;
    }

private:
    const NarkDbRecordStore& _rs;
    OperationContext* _txn;
    bool _skipNextAdvance = false;
    bool _eof = false;
    MongoNarkDbContextPtr m_ctx;
    nark::db::StoreIteratorPtr _cursor;
    nark::valvec<unsigned char> m_recBuf;
    BSONObj m_bson;
    RecordId _lastReturnedId;  // If null, need to seek to first/last record.
};

StatusWith<std::string> NarkDbRecordStore::parseOptionsField(const BSONObj options) {
    StringBuilder ss;
    BSONForEach(elem, options) {
        if (elem.fieldNameStringData() == "configString") {
            Status status = NarkDbUtil::checkTableCreationOptions(elem);
            if (!status.isOK()) {
                return status;
            }
            ss << elem.valueStringData() << ',';
        } else {
            // Return error on first unrecognized field.
            return StatusWith<std::string>(ErrorCodes::InvalidOptions,
                                           str::stream() << '\'' << elem.fieldNameStringData()
                                                         << '\'' << " is not a supported option.");
        }
    }
    return StatusWith<std::string>(ss.str());
}

class NarkDbRecordStore::RandomCursor final : public RecordCursor {
public:
    RandomCursor(OperationContext* txn, const NarkDbRecordStore& rs)
        : _cursor(nullptr), _rs(&rs), _txn(txn) {
        restore();
    }

    ~RandomCursor() {
        if (_cursor)
            detachFromOperationContext();
    }

    boost::optional<Record> next() final {
        int advanceRet = NarkDb_OP_CHECK(_cursor->next(_cursor));
        if (advanceRet == NarkDb_NOTFOUND)
            return {};
        invariantNarkDbOK(advanceRet);

        int64_t key;
        invariantNarkDbOK(_cursor->get_key(_cursor, &key));
        const RecordId id = _fromKey(key);

        NarkDb_ITEM value;
        invariantNarkDbOK(_cursor->get_value(_cursor, &value));

        return {{id, {static_cast<const char*>(value.data), static_cast<int>(value.size)}}};
    }

    void save() final {
        if (_cursor && !wt_keeptxnopen()) {
            try {
                _cursor->reset(_cursor);
            } catch (const WriteConflictException& wce) {
                // Ignore since this is only called when we are about to kill our transaction
                // anyway.
            }
        }
    }

    bool restore() final {
        // We can't use the CursorCache since this cursor needs a special config string.
        NarkDb_SESSION* session = NarkDbRecoveryUnit::get(_txn)->getSession(_txn)->getSession();

        if (!_cursor) {
            invariantNarkDbOK(
                session->open_cursor(session, _rs->_uri.c_str(), NULL, "next_random", &_cursor));
            invariant(_cursor);
        }
        return true;
    }
    void detachFromOperationContext() final {
        invariant(_txn);
        _txn = nullptr;
        _cursor->close(_cursor);
        _cursor = nullptr;
    }
    void reattachToOperationContext(OperationContext* txn) final {
        invariant(!_txn);
        _txn = txn;
    }

private:
    NarkDb_CURSOR* _cursor;
    const NarkDbRecordStore* _rs;
    OperationContext* _txn;
};


// static
StatusWith<std::string> NarkDbRecordStore::generateCreateString(
    const std::string& engineName,
    StringData ns,
    const CollectionOptions& options,
    StringData extraStrings) {
    // Separate out a prefix and suffix in the default string. User configuration will
    // override values in the prefix, but not values in the suffix.
    str::stream ss;
    ss << "type=file,";
    // Setting this larger than 10m can hurt latencies and throughput degradation if this
    // is the oplog.  See SERVER-16247
    ss << "memory_page_max=10m,";
    // Choose a higher split percent, since most usage is append only. Allow some space
    // for workloads where updates increase the size of documents.
    ss << "split_pct=90,";
    ss << "leaf_value_max=64MB,";
    ss << "checksum=on,";
    if (narkDbGlobalOptions.useCollectionPrefixCompression) {
        ss << "prefix_compression,";
    }

    ss << "block_compressor=" << narkDbGlobalOptions.collectionBlockCompressor << ",";

    ss << NarkDbCustomizationHooks::get(getGlobalServiceContext())->getOpenConfig(ns);

    ss << extraStrings << ",";

    StatusWith<std::string> customOptions =
        parseOptionsField(options.storageEngine.getObjectField(engineName));
    if (!customOptions.isOK())
        return customOptions;

    ss << customOptions.getValue();

    if (NamespaceString::oplog(ns)) {
        // force file for oplog
        ss << "type=file,";
        // Tune down to 10m.  See SERVER-16247
        ss << "memory_page_max=10m,";
    }

    // WARNING: No user-specified config can appear below this line. These options are required
    // for correct behavior of the server.

    ss << "key_format=q,value_format=u";

    // Record store metadata
    ss << ",app_metadata=(formatVersion=" << kCurrentRecordStoreVersion;
    if (NamespaceString::oplog(ns)) {
        ss << ",oplogKeyExtractionVersion=1";
    }
    ss << ")";

    return StatusWith<std::string>(ss);
}

NarkDbRecordStore::NarkDbRecordStore(OperationContext* ctx,
									 StringData ns,
									 StringData uri,
									 bool isCapped,
									 bool isEphemeral,
									 int64_t cappedMaxSize,
									 int64_t cappedMaxDocs,
									 CappedCallback* cappedCallback,
									 NarkDbSizeStorer* sizeStorer)
    : RecordStore(ns),
      _uri(uri.toString()),
      _tableId(NarkDbSession::genTableId()),
      _isCapped(isCapped),
      _isEphemeral(isEphemeral),
      _isOplog(NamespaceString::oplog(ns)),
      _cappedMaxSize(cappedMaxSize),
      _cappedMaxSizeSlack(std::min(cappedMaxSize / 10, int64_t(16 * 1024 * 1024))),
      _cappedMaxDocs(cappedMaxDocs),
      _cappedSleep(0),
      _cappedSleepMS(0),
      _cappedCallback(cappedCallback),
      _cappedDeleteCheckCount(0),
      _useOplogHack(shouldUseOplogHack(ctx, _uri)),
      _sizeStorer(sizeStorer),
      _sizeStorerCounter(0),
      _shuttingDown(false) {
    Status versionStatus = NarkDbUtil::checkApplicationMetadataFormatVersion(
        ctx, uri, kMinimumRecordStoreVersion, kMaximumRecordStoreVersion);
    if (!versionStatus.isOK()) {
        fassertFailedWithStatusNoTrace(28548, versionStatus);
    }

    if (_isCapped) {
        invariant(_cappedMaxSize > 0);
        invariant(_cappedMaxDocs == -1 || _cappedMaxDocs > 0);
    } else {
        invariant(_cappedMaxSize == -1);
        invariant(_cappedMaxDocs == -1);
    }

    // Find the largest RecordId currently in use and estimate the number of records.
    Cursor cursor(ctx, *this, /*forward=*/false);
    if (auto record = cursor.next()) {
        int64_t max = _makeKey(record->id);
        _oplog_highestSeen = record->id;
        _nextIdNum.store(1 + max);

        if (_sizeStorer) {
            long long numRecords;
            long long dataSize;
            _sizeStorer->loadFromCache(uri, &numRecords, &dataSize);
            _numRecords.store(numRecords);
            _dataSize.store(dataSize);
            _sizeStorer->onCreate(this, numRecords, dataSize);
        } else {
            LOG(1) << "Doing scan of collection " << ns << " to get size and count info";

            _numRecords.store(0);
            _dataSize.store(0);

            do {
                _numRecords.fetchAndAdd(1);
                _dataSize.fetchAndAdd(record->data.size());
            } while ((record = cursor.next()));
        }
    } else {
        _dataSize.store(0);
        _numRecords.store(0);
        // Need to start at 1 so we are always higher than RecordId::min()
        _nextIdNum.store(1);
        if (sizeStorer)
            _sizeStorer->onCreate(this, 0, 0);
    }

    if (NarkDbKVEngine::initRsOplogBackgroundThread(ns)) {
        _oplogStones = std::make_shared<OplogStones>(ctx, this);
    }
}

NarkDbRecordStore::~NarkDbRecordStore() {
    {
        stdx::lock_guard<boost::timed_mutex> lk(_cappedDeleterMutex);  // NOLINT
        _shuttingDown = true;
    }

    LOG(1) << "~NarkDbRecordStore for: " << ns();
    if (_sizeStorer) {
        _sizeStorer->onDestroy(this);
    }

    if (_oplogStones) {
        _oplogStones->kill();
    }
}

const char* NarkDbRecordStore::name() const {
    return kNarkDbEngineName.c_str();
}

bool NarkDbRecordStore::inShutdown() const {
    stdx::lock_guard<boost::timed_mutex> lk(_cappedDeleterMutex);  // NOLINT
    return _shuttingDown;
}

long long NarkDbRecordStore::dataSize(OperationContext* txn) const {
    return _dataSize.load();
}

long long NarkDbRecordStore::numRecords(OperationContext* txn) const {
    return _numRecords.load();
}

bool NarkDbRecordStore::isCapped() const {
    return _isCapped;
}

int64_t NarkDbRecordStore::cappedMaxDocs() const {
    invariant(_isCapped);
    return _cappedMaxDocs;
}

int64_t NarkDbRecordStore::cappedMaxSize() const {
    invariant(_isCapped);
    return _cappedMaxSize;
}

int64_t NarkDbRecordStore::storageSize(OperationContext* txn,
                                           BSONObjBuilder* extraInfo,
                                           int infoLevel) const {
    if (_isEphemeral) {
        return dataSize(txn);
    }
    NarkDbSession* session = NarkDbRecoveryUnit::get(txn)->getSession(txn);
    StatusWith<int64_t> result =
        NarkDbUtil::getStatisticsValueAs<int64_t>(session->getSession(),
                                                      "statistics:" + getURI(),
                                                      "statistics=(size)",
                                                      NarkDb_STAT_DSRC_BLOCK_SIZE);
    uassertStatusOK(result.getStatus());

    int64_t size = result.getValue();

    if (size == 0 && _isCapped) {
        // Many things assume an empty capped collection still takes up space.
        return 1;
    }
    return size;
}

// Retrieve the value from a positioned cursor.
RecordData NarkDbRecordStore::_getData(const NarkDbCursor& cursor) const {
    NarkDb_ITEM value;
    int ret = cursor->get_value(cursor.get(), &value);
    invariantNarkDbOK(ret);

    SharedBuffer data = SharedBuffer::allocate(value.size);
    memcpy(data.get(), value.data, value.size);
    return RecordData(data, value.size);
}

RecordData NarkDbRecordStore::dataFor(OperationContext* txn, const RecordId& id) const {
    // ownership passes to the shared_array created below
    NarkDbCursor curwrap(_uri, _tableId, true, txn);
    NarkDb_CURSOR* c = curwrap.get();
    invariant(c);
    c->set_key(c, _makeKey(id));
    int ret = NarkDb_OP_CHECK(c->search(c));
    massert(28556, "Didn't find RecordId in NarkDbRecordStore", ret != NarkDb_NOTFOUND);
    invariantNarkDbOK(ret);
    return _getData(curwrap);
}

bool NarkDbRecordStore::findRecord(OperationContext* txn,
                                       const RecordId& id,
                                       RecordData* out) const {
    NarkDbCursor curwrap(_uri, _tableId, true, txn);
    NarkDb_CURSOR* c = curwrap.get();
    invariant(c);
    c->set_key(c, _makeKey(id));
    int ret = NarkDb_OP_CHECK(c->search(c));
    if (ret == NarkDb_NOTFOUND) {
        return false;
    }
    invariantNarkDbOK(ret);
    *out = _getData(curwrap);
    return true;
}

void NarkDbRecordStore::deleteRecord(OperationContext* txn, const RecordId& id) {
    // Deletes should never occur on a capped collection because truncation uses
    // NarkDb_SESSION::truncate().
    invariant(!isCapped());

    NarkDbCursor cursor(_uri, _tableId, true, txn);
    cursor.assertInActiveTxn();
    NarkDb_CURSOR* c = cursor.get();
    c->set_key(c, _makeKey(id));
    int ret = NarkDb_OP_CHECK(c->search(c));
    invariantNarkDbOK(ret);

    NarkDb_ITEM old_value;
    ret = c->get_value(c, &old_value);
    invariantNarkDbOK(ret);

    int64_t old_length = old_value.size;

    ret = NarkDb_OP_CHECK(c->remove(c));
    invariantNarkDbOK(ret);

    _changeNumRecords(txn, -1);
    _increaseDataSize(txn, -old_length);
}

bool NarkDbRecordStore::cappedAndNeedDelete() const {
    if (!_isCapped)
        return false;

    if (_dataSize.load() >= _cappedMaxSize)
        return true;

    if ((_cappedMaxDocs != -1) && (_numRecords.load() > _cappedMaxDocs))
        return true;

    return false;
}

int64_t NarkDbRecordStore::cappedDeleteAsNeeded(OperationContext* txn,
                                                    const RecordId& justInserted) {
    invariant(!_oplogStones);

    // We only want to do the checks occasionally as they are expensive.
    // This variable isn't thread safe, but has loose semantics anyway.
    dassert(!_isOplog || _cappedMaxDocs == -1);

    if (!cappedAndNeedDelete())
        return 0;

    // ensure only one thread at a time can do deletes, otherwise they'll conflict.
    boost::unique_lock<boost::timed_mutex> lock(_cappedDeleterMutex, boost::defer_lock);  // NOLINT

    if (_cappedMaxDocs != -1) {
        lock.lock();  // Max docs has to be exact, so have to check every time.
    } else {
        if (!lock.try_lock()) {
            // Someone else is deleting old records. Apply back-pressure if too far behind,
            // otherwise continue.
            if ((_dataSize.load() - _cappedMaxSize) < _cappedMaxSizeSlack)
                return 0;

            // Don't wait forever: we're in a transaction, we could block eviction.
            Date_t before = Date_t::now();
            bool gotLock = lock.try_lock_for(boost::chrono::milliseconds(200));  // NOLINT
            auto delay = boost::chrono::milliseconds(                            // NOLINT
                durationCount<Milliseconds>(Date_t::now() - before));
            _cappedSleep.fetchAndAdd(1);
            _cappedSleepMS.fetchAndAdd(delay.count());
            if (!gotLock)
                return 0;

            // If we already waited, let someone else do cleanup unless we are significantly
            // over the limit.
            if ((_dataSize.load() - _cappedMaxSize) < (2 * _cappedMaxSizeSlack))
                return 0;
        }
    }

    return cappedDeleteAsNeeded_inlock(txn, justInserted);
}

int64_t NarkDbRecordStore::cappedDeleteAsNeeded_inlock(OperationContext* txn,
                                                           const RecordId& justInserted) {
    // we do this in a side transaction in case it aborts
    NarkDbRecoveryUnit* realRecoveryUnit =
        checked_cast<NarkDbRecoveryUnit*>(txn->releaseRecoveryUnit());
    invariant(realRecoveryUnit);
    NarkDbSessionCache* sc = realRecoveryUnit->getSessionCache();
    OperationContext::RecoveryUnitState const realRUstate =
        txn->setRecoveryUnit(new NarkDbRecoveryUnit(sc), OperationContext::kNotInUnitOfWork);

    NarkDbRecoveryUnit::get(txn)->markNoTicketRequired();  // realRecoveryUnit already has
    NarkDb_SESSION* session = NarkDbRecoveryUnit::get(txn)->getSession(txn)->getSession();

    int64_t dataSize = _dataSize.load();
    int64_t numRecords = _numRecords.load();

    int64_t sizeOverCap = (dataSize > _cappedMaxSize) ? dataSize - _cappedMaxSize : 0;
    int64_t sizeSaved = 0;
    int64_t docsOverCap = 0, docsRemoved = 0;
    if (_cappedMaxDocs != -1 && numRecords > _cappedMaxDocs)
        docsOverCap = numRecords - _cappedMaxDocs;

    try {
        WriteUnitOfWork wuow(txn);

        NarkDbCursor curwrap(_uri, _tableId, true, txn);
        NarkDb_CURSOR* truncateEnd = curwrap.get();
        RecordId newestIdToDelete;
        int ret = 0;
        bool positioned = false;  // Mark if the cursor is on the first key
        int64_t savedFirstKey = 0;

        // If we know where the first record is, go to it
        if (_cappedFirstRecord != RecordId()) {
            int64_t key = _makeKey(_cappedFirstRecord);
            truncateEnd->set_key(truncateEnd, key);
            ret = NarkDb_OP_CHECK(truncateEnd->search(truncateEnd));
            if (ret == 0) {
                positioned = true;
                savedFirstKey = key;
            }
        }

        // Advance the cursor truncateEnd until we find a suitable end point for our truncate
        while ((sizeSaved < sizeOverCap || docsRemoved < docsOverCap) && (docsRemoved < 20000) &&
               (positioned || (ret = NarkDb_OP_CHECK(truncateEnd->next(truncateEnd))) == 0)) {
            positioned = false;
            int64_t key;
            invariantNarkDbOK(truncateEnd->get_key(truncateEnd, &key));

            // don't go past the record we just inserted
            newestIdToDelete = _fromKey(key);
            if (newestIdToDelete >= justInserted)  // TODO: use oldest uncommitted instead
                break;

            if (_shuttingDown)
                break;

            NarkDb_ITEM old_value;
            invariantNarkDbOK(truncateEnd->get_value(truncateEnd, &old_value));

            ++docsRemoved;
            sizeSaved += old_value.size;

            if (_cappedCallback) {
                uassertStatusOK(_cappedCallback->aboutToDeleteCapped(
                    txn,
                    newestIdToDelete,
                    RecordData(static_cast<const char*>(old_value.data), old_value.size)));
            }
        }

        if (ret != NarkDb_NOTFOUND) {
            invariantNarkDbOK(ret);
        }

        if (docsRemoved > 0) {
            // if we scanned to the end of the collection or past our insert, go back one
            if (ret == NarkDb_NOTFOUND || newestIdToDelete >= justInserted) {
                ret = NarkDb_OP_CHECK(truncateEnd->prev(truncateEnd));
            }
            invariantNarkDbOK(ret);

            RecordId firstRemainingId;
            ret = truncateEnd->next(truncateEnd);
            if (ret != NarkDb_NOTFOUND) {
                invariantNarkDbOK(ret);
                int64_t key;
                invariantNarkDbOK(truncateEnd->get_key(truncateEnd, &key));
                firstRemainingId = _fromKey(key);
            }
            invariantNarkDbOK(truncateEnd->prev(truncateEnd));  // put the cursor back where it was

            NarkDbCursor startWrap(_uri, _tableId, true, txn);
            NarkDb_CURSOR* truncateStart = startWrap.get();

            // If we know where the start point is, set it for the truncate
            if (savedFirstKey != 0) {
                truncateStart->set_key(truncateStart, savedFirstKey);
            } else {
                truncateStart = NULL;
            }
            ret = session->truncate(session, NULL, truncateStart, truncateEnd, NULL);

            if (ret == ENOENT || ret == NarkDb_NOTFOUND) {
                // TODO we should remove this case once SERVER-17141 is resolved
                log() << "Soft failure truncating capped collection. Will try again later.";
                docsRemoved = 0;
            } else {
                invariantNarkDbOK(ret);
                _changeNumRecords(txn, -docsRemoved);
                _increaseDataSize(txn, -sizeSaved);
                wuow.commit();
                // Save the key for the next round
                _cappedFirstRecord = firstRemainingId;
            }
        }
    } catch (const WriteConflictException& wce) {
        delete txn->releaseRecoveryUnit();
        txn->setRecoveryUnit(realRecoveryUnit, realRUstate);
        log() << "got conflict truncating capped, ignoring";
        return 0;
    } catch (...) {
        delete txn->releaseRecoveryUnit();
        txn->setRecoveryUnit(realRecoveryUnit, realRUstate);
        throw;
    }

    delete txn->releaseRecoveryUnit();
    txn->setRecoveryUnit(realRecoveryUnit, realRUstate);
    return docsRemoved;
}

bool NarkDbRecordStore::yieldAndAwaitOplogDeletionRequest(OperationContext* txn) {
    // Create another reference to the oplog stones while holding a lock on the collection to
    // prevent it from being destructed.
    std::shared_ptr<OplogStones> oplogStones = _oplogStones;

    Locker* locker = txn->lockState();
    Locker::LockSnapshot snapshot;

    // Release any locks before waiting on the condition variable. It is illegal to access any
    // methods or members of this record store after this line because it could be deleted.
    bool releasedAnyLocks = locker->saveLockStateAndUnlock(&snapshot);
    invariant(releasedAnyLocks);

    // The top-level locks were freed, so also release any potential low-level (storage engine)
    // locks that might be held.
    txn->recoveryUnit()->abandonSnapshot();

    // Wait for an oplog deletion request, or for this record store to have been destroyed.
    oplogStones->awaitHasExcessStonesOrDead();

    // Reacquire the locks that were released.
    locker->restoreLockState(snapshot);

    return !oplogStones->isDead();
}

void NarkDbRecordStore::reclaimOplog(OperationContext* txn) {
    while (auto stone = _oplogStones->peekOldestStoneIfNeeded()) {
        invariant(stone->lastRecord.isNormal());

        LOG(1) << "Truncating the oplog between " << _oplogStones->firstRecord << " and "
               << stone->lastRecord << " to remove approximately " << stone->records
               << " records totaling to " << stone->bytes << " bytes";

        NarkDbRecoveryUnit* ru = NarkDbRecoveryUnit::get(txn);
        ru->markNoTicketRequired();  // No ticket is needed for internal operations.
        NarkDb_SESSION* session = ru->getSession(txn)->getSession();

        try {
            WriteUnitOfWork wuow(txn);

            NarkDbCursor startwrap(_uri, _tableId, true, txn);
            NarkDb_CURSOR* start = startwrap.get();
            start->set_key(start, _makeKey(_oplogStones->firstRecord));

            NarkDbCursor endwrap(_uri, _tableId, true, txn);
            NarkDb_CURSOR* end = endwrap.get();
            end->set_key(end, _makeKey(stone->lastRecord));

            invariantNarkDbOK(session->truncate(session, nullptr, start, end, nullptr));
            _changeNumRecords(txn, -stone->records);
            _increaseDataSize(txn, -stone->bytes);

            wuow.commit();

            // Remove the stone after a successful truncation.
            _oplogStones->popOldestStone();

            // Stash the truncate point for next time to cleanly skip over tombstones, etc.
            _oplogStones->firstRecord = stone->lastRecord;
        } catch (const WriteConflictException& wce) {
            LOG(1) << "Caught WriteConflictException while truncating oplog entries, retrying";
        }
    }

    LOG(1) << "Finished truncating the oplog, it now contains approximately " << _numRecords.load()
           << " records totaling to " << _dataSize.load() << " bytes";
}

Status NarkDbRecordStore::insertRecords(OperationContext* txn,
                                            std::vector<Record>* records,
                                            bool enforceQuota) {
    // We are kind of cheating on capped collections since we write all of them at once ....
    // Simplest way out would be to just block vector writes for everything except oplog ?
    int totalLength = 0;
    for (auto& record : *records)
        totalLength += record.data.size();

    // caller will retry one element at a time
    if (_isCapped && totalLength > _cappedMaxSize)
        return Status(ErrorCodes::BadValue, "object to insert exceeds cappedMaxSize");

    NarkDbCursor curwrap(_uri, _tableId, true, txn);
    curwrap.assertInActiveTxn();
    NarkDb_CURSOR* c = curwrap.get();
    invariant(c);

    RecordId highestId = RecordId();
    dassert(!records->empty());
    for (auto& record : *records) {
        if (_useOplogHack) {
            StatusWith<RecordId> status =
                oploghack::extractKey(record.data.data(), record.data.size());
            if (!status.isOK())
                return status.getStatus();
            record.id = status.getValue();
        } else if (_isCapped) {
            stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);
            record.id = _nextId();
            _addUncommitedRecordId_inlock(txn, record.id);
        } else {
            record.id = _nextId();
        }
        dassert(record.id > highestId);
        highestId = record.id;
    }

    if (_useOplogHack && (highestId > _oplog_highestSeen)) {
        stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);
        if (highestId > _oplog_highestSeen)
            _oplog_highestSeen = highestId;
    }

    for (auto& record : *records) {
        c->set_key(c, _makeKey(record.id));
        NarkDbItem value(record.data.data(), record.data.size());
        c->set_value(c, value.Get());
        int ret = NarkDb_OP_CHECK(c->insert(c));
        if (ret)
            return narkDbRCToStatus(ret, "NarkDbRecordStore::insertRecord");
    }

    _changeNumRecords(txn, records->size());
    _increaseDataSize(txn, totalLength);

    if (_oplogStones) {
        _oplogStones->updateCurrentStoneAfterInsertOnCommit(
            txn, totalLength, highestId, records->size());
    } else {
        cappedDeleteAsNeeded(txn, highestId);
    }

    return Status::OK();
}

StatusWith<RecordId> NarkDbRecordStore::insertRecord(OperationContext* txn,
                                                         const char* data,
                                                         int len,
                                                         bool enforceQuota) {
    std::vector<Record> records;
    Record record = {RecordId(), RecordData(data, len)};
    records.push_back(record);
    Status status = insertRecords(txn, &records, enforceQuota);
    if (!status.isOK())
        return StatusWith<RecordId>(status);
    return StatusWith<RecordId>(records[0].id);
}

void NarkDbRecordStore::_dealtWithCappedId(SortedRecordIds::iterator it) {
    invariant(&(*it) != NULL);
    stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);
    _uncommittedRecordIds.erase(it);
}

bool NarkDbRecordStore::isCappedHidden(const RecordId& id) const {
    stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);
    if (_uncommittedRecordIds.empty()) {
        return false;
    }
    return _uncommittedRecordIds.front() <= id;
}

RecordId NarkDbRecordStore::lowestCappedHiddenRecord() const {
    stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);
    return _uncommittedRecordIds.empty() ? RecordId() : _uncommittedRecordIds.front();
}

StatusWith<RecordId> NarkDbRecordStore::insertRecord(OperationContext* txn,
                                                         const DocWriter* doc,
                                                         bool enforceQuota) {
    const int len = doc->documentSize();

    std::unique_ptr<char[]> buf(new char[len]);
    doc->writeDocument(buf.get());

    return insertRecord(txn, buf.get(), len, enforceQuota);
}

StatusWith<RecordId> NarkDbRecordStore::updateRecord(OperationContext* txn,
                                                         const RecordId& id,
                                                         const char* data,
                                                         int len,
                                                         bool enforceQuota,
                                                         UpdateNotifier* notifier) {
    NarkDbCursor curwrap(_uri, _tableId, true, txn);
    curwrap.assertInActiveTxn();
    NarkDb_CURSOR* c = curwrap.get();
    invariant(c);
    c->set_key(c, _makeKey(id));
    int ret = NarkDb_OP_CHECK(c->search(c));
    invariantNarkDbOK(ret);

    NarkDb_ITEM old_value;
    ret = c->get_value(c, &old_value);
    invariantNarkDbOK(ret);

    int64_t old_length = old_value.size;

    if (_oplogStones && len != old_length) {
        return {ErrorCodes::IllegalOperation, "Cannot change the size of a document in the oplog"};
    }

    c->set_key(c, _makeKey(id));
    NarkDbItem value(data, len);
    c->set_value(c, value.Get());
    ret = NarkDb_OP_CHECK(c->insert(c));
    invariantNarkDbOK(ret);

    _increaseDataSize(txn, len - old_length);
    if (!_oplogStones) {
        cappedDeleteAsNeeded(txn, id);
    }

    return StatusWith<RecordId>(id);
}

bool NarkDbRecordStore::updateWithDamagesSupported() const {
    return false;
}

StatusWith<RecordData> NarkDbRecordStore::updateWithDamages(
    OperationContext* txn,
    const RecordId& id,
    const RecordData& oldRec,
    const char* damageSource,
    const mutablebson::DamageVector& damages) {
    MONGO_UNREACHABLE;
}

void NarkDbRecordStore::_oplogSetStartHack(NarkDbRecoveryUnit* wru) const {
    stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);
    if (_uncommittedRecordIds.empty()) {
        wru->setOplogReadTill(_oplog_highestSeen);
    } else {
        wru->setOplogReadTill(_uncommittedRecordIds.front());
    }
}

std::unique_ptr<SeekableRecordCursor> NarkDbRecordStore::getCursor(OperationContext* txn,
                                                                       bool forward) const {
    if (_isOplog && forward) {
        NarkDbRecoveryUnit* wru = NarkDbRecoveryUnit::get(txn);
        if (!wru->inActiveTxn() || wru->getOplogReadTill().isNull()) {
            // if we don't have a session, we have no snapshot, so we can update our view
            _oplogSetStartHack(wru);
        }
    }

    return stdx::make_unique<Cursor>(txn, *this, forward);
}

std::unique_ptr<RecordCursor> NarkDbRecordStore::getRandomCursor(OperationContext* txn) const {
    return stdx::make_unique<RandomCursor>(txn, *this);
}

std::vector<std::unique_ptr<RecordCursor>> NarkDbRecordStore::getManyCursors(
    OperationContext* txn) const {
    std::vector<std::unique_ptr<RecordCursor>> cursors(1);
    cursors[0] = stdx::make_unique<Cursor>(txn, *this, /*forward=*/true);
    return cursors;
}

Status NarkDbRecordStore::truncate(OperationContext* txn) {
    NarkDbCursor startWrap(_uri, _tableId, true, txn);
    NarkDb_CURSOR* start = startWrap.get();
    int ret = NarkDb_OP_CHECK(start->next(start));
    // Empty collections don't have anything to truncate.
    if (ret == NarkDb_NOTFOUND) {
        return Status::OK();
    }
    invariantNarkDbOK(ret);

    NarkDb_SESSION* session = NarkDbRecoveryUnit::get(txn)->getSession(txn)->getSession();
    invariantNarkDbOK(NarkDb_OP_CHECK(session->truncate(session, NULL, start, NULL, NULL)));
    _changeNumRecords(txn, -numRecords(txn));
    _increaseDataSize(txn, -dataSize(txn));

    if (_oplogStones) {
        _oplogStones->clearStonesOnCommit(txn);
    }

    return Status::OK();
}

Status NarkDbRecordStore::compact(OperationContext* txn,
                                      RecordStoreCompactAdaptor* adaptor,
                                      const CompactOptions* options,
                                      CompactStats* stats) {
    NarkDbSessionCache* cache = NarkDbRecoveryUnit::get(txn)->getSessionCache();
    if (!cache->isEphemeral()) {
        NarkDbSession* session = cache->getSession();
        NarkDb_SESSION* s = session->getSession();
        int ret = s->compact(s, getURI().c_str(), "timeout=0");
        invariantNarkDbOK(ret);
        cache->releaseSession(session);
    }
    return Status::OK();
}

Status NarkDbRecordStore::validate(OperationContext* txn,
                                       bool full,
                                       bool scanData,
                                       ValidateAdaptor* adaptor,
                                       ValidateResults* results,
                                       BSONObjBuilder* output) {
    if (!_isEphemeral) {
        int err = NarkDbUtil::verifyTable(txn, _uri, &results->errors);
        if (err == EBUSY) {
            const char* msg = "verify() returned EBUSY. Not treating as invalid.";
            warning() << msg;
            results->errors.push_back(msg);
        } else if (err) {
            std::string msg = str::stream() << "verify() returned " << narkdb_strerror(err)
                                            << ". "
                                            << "This indicates structural damage. "
                                            << "Not examining individual documents.";
            error() << msg;
            results->errors.push_back(msg);
            results->valid = false;
            return Status::OK();
        }
    }

    long long nrecords = 0;
    long long dataSizeTotal = 0;
    results->valid = true;
    Cursor cursor(txn, *this, true);
    while (auto record = cursor.next()) {
        ++nrecords;
        auto dataSize = record->data.size();
        dataSizeTotal += dataSize;
        if (full && scanData) {
            size_t validatedSize;
            Status status = adaptor->validate(record->data, &validatedSize);

            // The validatedSize equals dataSize below is not a general requirement, but must be
            // true for NarkDb today because we never pad records.
            if (!status.isOK() || validatedSize != static_cast<size_t>(dataSize)) {
                results->valid = false;
                results->errors.push_back(str::stream() << record->id << " is corrupted");
            }
        }
    }

    if (_sizeStorer && results->valid) {
        if (nrecords != _numRecords.load() || dataSizeTotal != _dataSize.load()) {
            warning() << _uri << ": Existing record and data size counters (" << _numRecords.load()
                      << " records " << _dataSize.load() << " bytes) "
                      << "are inconsistent with validation results (" << nrecords << " records "
                      << dataSizeTotal << " bytes). "
                      << "Updating counters with new values.";
        }
        _numRecords.store(nrecords);
        _dataSize.store(dataSizeTotal);
        _sizeStorer->storeToCache(_uri, _numRecords.load(), _dataSize.load());
    }

    output->appendNumber("nrecords", nrecords);
    return Status::OK();
}

void NarkDbRecordStore::appendCustomStats(OperationContext* txn,
                                              BSONObjBuilder* result,
                                              double scale) const {
    result->appendBool("capped", _isCapped);
    if (_isCapped) {
        result->appendIntOrLL("max", _cappedMaxDocs);
        result->appendIntOrLL("maxSize", static_cast<long long>(_cappedMaxSize / scale));
        result->appendIntOrLL("sleepCount", _cappedSleep.load());
        result->appendIntOrLL("sleepMS", _cappedSleepMS.load());
    }
    NarkDbSession* session = NarkDbRecoveryUnit::get(txn)->getSession(txn);
    NarkDb_SESSION* s = session->getSession();
    BSONObjBuilder bob(result->subobjStart(kNarkDbEngineName));
    {
        BSONObjBuilder metadata(bob.subobjStart("metadata"));
        Status status = NarkDbUtil::getApplicationMetadata(txn, getURI(), &metadata);
        if (!status.isOK()) {
            metadata.append("error", "unable to retrieve metadata");
            metadata.append("code", static_cast<int>(status.code()));
            metadata.append("reason", status.reason());
        }
    }

    std::string type, sourceURI;
    NarkDbUtil::fetchTypeAndSourceURI(txn, _uri, &type, &sourceURI);
    StatusWith<std::string> metadataResult = NarkDbUtil::getMetadata(txn, sourceURI);
    StringData creationStringName("creationString");
    if (!metadataResult.isOK()) {
        BSONObjBuilder creationString(bob.subobjStart(creationStringName));
        creationString.append("error", "unable to retrieve creation config");
        creationString.append("code", static_cast<int>(metadataResult.getStatus().code()));
        creationString.append("reason", metadataResult.getStatus().reason());
    } else {
        bob.append("creationString", metadataResult.getValue());
        // Type can be "lsm" or "file"
        bob.append("type", type);
    }

    Status status =
        NarkDbUtil::exportTableToBSON(s, "statistics:" + getURI(), "statistics=(fast)", &bob);
    if (!status.isOK()) {
        bob.append("error", "unable to retrieve statistics");
        bob.append("code", static_cast<int>(status.code()));
        bob.append("reason", status.reason());
    }
}

Status NarkDbRecordStore::touch(OperationContext* txn, BSONObjBuilder* output) const {
    if (_isEphemeral) {
        // Everything is already in memory.
        return Status::OK();
    }
    return Status(ErrorCodes::CommandNotSupported, "this storage engine does not support touch");
}

Status NarkDbRecordStore::oplogDiskLocRegister(OperationContext* txn, const Timestamp& opTime) {
    StatusWith<RecordId> id = oploghack::keyForOptime(opTime);
    if (!id.isOK())
        return id.getStatus();

    stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);
    _addUncommitedRecordId_inlock(txn, id.getValue());
    return Status::OK();
}

class NarkDbRecordStore::CappedInsertChange : public RecoveryUnit::Change {
public:
    CappedInsertChange(NarkDbRecordStore* rs, SortedRecordIds::iterator it)
        : _rs(rs), _it(it) {}

    virtual void commit() {
        // Do not notify here because all committed inserts notify, always.
        _rs->_dealtWithCappedId(_it);
    }

    virtual void rollback() {
        // Notify on rollback since it might make later commits visible.
        _rs->_dealtWithCappedId(_it);
        if (_rs->_cappedCallback)
            _rs->_cappedCallback->notifyCappedWaitersIfNeeded();
    }

private:
    NarkDbRecordStore* _rs;
    SortedRecordIds::iterator _it;
};

void NarkDbRecordStore::_addUncommitedRecordId_inlock(OperationContext* txn,
                                                          const RecordId& id) {
    // todo: make this a dassert at some point
    // invariant(_uncommittedRecordIds.empty() || _uncommittedRecordIds.back() < id);
    SortedRecordIds::iterator it = _uncommittedRecordIds.insert(_uncommittedRecordIds.end(), id);
    txn->recoveryUnit()->registerChange(new CappedInsertChange(this, it));
    _oplog_highestSeen = id;
}

boost::optional<RecordId> NarkDbRecordStore::oplogStartHack(
    OperationContext* txn, const RecordId& startingPosition) const {
    if (!_useOplogHack)
        return boost::none;

    {
        NarkDbRecoveryUnit* wru = NarkDbRecoveryUnit::get(txn);
        _oplogSetStartHack(wru);
    }

    NarkDbCursor cursor(_uri, _tableId, true, txn);
    NarkDb_CURSOR* c = cursor.get();

    int cmp;
    c->set_key(c, _makeKey(startingPosition));
    int ret = NarkDb_OP_CHECK(c->search_near(c, &cmp));
    if (ret == 0 && cmp > 0)
        ret = c->prev(c);  // landed one higher than startingPosition
    if (ret == NarkDb_NOTFOUND)
        return RecordId();  // nothing <= startingPosition
    invariantNarkDbOK(ret);

    int64_t key;
    ret = c->get_key(c, &key);
    invariantNarkDbOK(ret);
    return _fromKey(key);
}

void NarkDbRecordStore::updateStatsAfterRepair(OperationContext* txn,
                                                   long long numRecords,
                                                   long long dataSize) {
    _numRecords.store(numRecords);
    _dataSize.store(dataSize);

    if (_sizeStorer) {
        _sizeStorer->storeToCache(_uri, numRecords, dataSize);
    }
}

RecordId NarkDbRecordStore::_nextId() {
    invariant(!_useOplogHack);
    RecordId out = RecordId(_nextIdNum.fetchAndAdd(1));
    invariant(out.isNormal());
    return out;
}

NarkDbRecoveryUnit* NarkDbRecordStore::_getRecoveryUnit(OperationContext* txn) {
    return checked_cast<NarkDbRecoveryUnit*>(txn->recoveryUnit());
}

class NarkDbRecordStore::NumRecordsChange : public RecoveryUnit::Change {
public:
    NumRecordsChange(NarkDbRecordStore* rs, int64_t diff) : _rs(rs), _diff(diff) {}
    virtual void commit() {}
    virtual void rollback() {
        _rs->_numRecords.fetchAndAdd(-_diff);
    }

private:
    NarkDbRecordStore* _rs;
    int64_t _diff;
};

void NarkDbRecordStore::_changeNumRecords(OperationContext* txn, int64_t diff) {
    txn->recoveryUnit()->registerChange(new NumRecordsChange(this, diff));
    if (_numRecords.fetchAndAdd(diff) < 0)
        _numRecords.store(std::max(diff, int64_t(0)));
}

class NarkDbRecordStore::DataSizeChange : public RecoveryUnit::Change {
public:
    DataSizeChange(NarkDbRecordStore* rs, int64_t amount) : _rs(rs), _amount(amount) {}
    virtual void commit() {}
    virtual void rollback() {
        _rs->_increaseDataSize(NULL, -_amount);
    }

private:
    NarkDbRecordStore* _rs;
    int64_t _amount;
};

void NarkDbRecordStore::_increaseDataSize(OperationContext* txn, int64_t amount) {
    if (txn)
        txn->recoveryUnit()->registerChange(new DataSizeChange(this, amount));

    if (_dataSize.fetchAndAdd(amount) < 0)
        _dataSize.store(std::max(amount, int64_t(0)));

    if (_sizeStorer && _sizeStorerCounter++ % 1000 == 0) {
        _sizeStorer->storeToCache(_uri, _numRecords.load(), _dataSize.load());
    }
}

int64_t NarkDbRecordStore::_makeKey(const RecordId& id) {
    return id.repr();
}
RecordId NarkDbRecordStore::_fromKey(int64_t key) {
    return RecordId(key);
}

void NarkDbRecordStore::temp_cappedTruncateAfter(OperationContext* txn,
                                                     RecordId end,
                                                     bool inclusive) {
    Cursor cursor(txn, *this);

    auto record = cursor.seekExact(end);
    massert(28807, str::stream() << "Failed to seek to the record located at " << end, record);

    int64_t recordsRemoved = 0;
    int64_t bytesRemoved = 0;
    RecordId lastKeptId;
    RecordId firstRemovedId;

    if (inclusive) {
        Cursor reverseCursor(txn, *this, false);
        invariant(reverseCursor.seekExact(end));
        auto prev = reverseCursor.next();
        lastKeptId = prev ? prev->id : RecordId();
        firstRemovedId = end;
    } else {
        // If not deleting the record located at 'end', then advance the cursor to the first record
        // that is being deleted.
        record = cursor.next();
        if (!record) {
            return;  // No records to delete.
        }
        lastKeptId = end;
        firstRemovedId = record->id;
    }

    // Compute the number and associated sizes of the records to delete.
    do {
        if (_cappedCallback) {
            uassertStatusOK(_cappedCallback->aboutToDeleteCapped(txn, record->id, record->data));
        }
        recordsRemoved++;
        bytesRemoved += record->data.size();
    } while ((record = cursor.next()));

    // Truncate the collection starting from the record located at 'firstRemovedId' to the end of
    // the collection.
    WriteUnitOfWork wuow(txn);

    NarkDbCursor startwrap(_uri, _tableId, true, txn);
    NarkDb_CURSOR* start = startwrap.get();
    start->set_key(start, _makeKey(firstRemovedId));

    NarkDb_SESSION* session = NarkDbRecoveryUnit::get(txn)->getSession(txn)->getSession();
    invariantNarkDbOK(session->truncate(session, nullptr, start, nullptr, nullptr));

    _changeNumRecords(txn, -recordsRemoved);
    _increaseDataSize(txn, -bytesRemoved);

    wuow.commit();

    if (_useOplogHack) {
        // Forget that we've ever seen a higher timestamp than we now have.
        _oplog_highestSeen = lastKeptId;
    }

    if (_oplogStones) {
        _oplogStones->updateStonesAfterCappedTruncateAfter(
            recordsRemoved, bytesRemoved, firstRemovedId);
    }
}

} } // namespace mongo::narkdb
