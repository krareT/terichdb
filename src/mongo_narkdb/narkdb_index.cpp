// narkdb_index.cpp

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

#include "narkdb_index.h"

#include <set>

#include "mongo/base/checked_cast.h"
#include "mongo/db/json.h"
#include "mongo/db/catalog/index_catalog_entry.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/key_string.h"
#include "narkdb_customization_hooks.h"
#include "narkdb_global_options.h"
#include "narkdb_record_store.h"
//#include "narkdb_session_cache.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

#define TRACING_ENABLED 0

#if TRACING_ENABLED
#define TRACE_CURSOR log() << "NarkDb index (" << (const void*)&_idx << ") "
#define TRACE_INDEX log() << "NarkDb index (" << (const void*) this << ") "
#else
#define TRACE_CURSOR \
    if (0)           \
    log()
#define TRACE_INDEX \
    if (0)          \
    log()
#endif

namespace mongo { namespace narkdb {
namespace {

using std::string;
using std::vector;

static const int TempKeyMaxSize = 1024;  // this goes away with SERVER-3372

static const int kMinimumIndexVersion = 6;
static const int kCurrentIndexVersion = 6;  // New indexes use this by default.
static const int kMaximumIndexVersion = 6;
static_assert(kCurrentIndexVersion >= kMinimumIndexVersion,
              "kCurrentIndexVersion >= kMinimumIndexVersion");
static_assert(kCurrentIndexVersion <= kMaximumIndexVersion,
              "kCurrentIndexVersion <= kMaximumIndexVersion");

bool hasFieldNames(const BSONObj& obj) {
    BSONForEach(e, obj) {
        if (e.fieldName()[0])
            return true;
    }
    return false;
}

BSONObj stripFieldNames(const BSONObj& query) {
    if (!hasFieldNames(query))
        return query;

    BSONObjBuilder bb;
    BSONForEach(e, query) {
        bb.appendAs(e, StringData());
    }
    return bb.obj();
}

Status checkKeySize(const BSONObj& key) {
    if (key.objsize() >= TempKeyMaxSize) {
        string msg = mongoutils::str::stream()
            << "NarkDbIndex::insert: key too large to index, failing " << ' ' << key.objsize()
            << ' ' << key;
        return Status(ErrorCodes::KeyTooLong, msg);
    }
    return Status::OK();
}

}  // namespace

Status NarkDbIndex::dupKeyError(const BSONObj& key) {
    StringBuilder sb;
    sb << "E11000 duplicate key error";
    sb << " collection: " << _collectionNamespace;
    sb << " index: " << _indexName;
    sb << " dup key: " << key;
    return Status(ErrorCodes::DuplicateKey, sb.str());
}

// static
StatusWith<std::string> NarkDbIndex::parseIndexOptions(const BSONObj& options) {
    StringBuilder ss;
    BSONForEach(elem, options) {
        if (elem.fieldNameStringData() == "configString") {
		/*
            Status status = NarkDbUtil::checkTableCreationOptions(elem);
            if (!status.isOK()) {
                return status;
            }
            ss << elem.valueStringData() << ',';
		*/
        } else {
            // Return error on first unrecognized field.
            return StatusWith<std::string>(ErrorCodes::InvalidOptions,
                                           str::stream() << '\'' << elem.fieldNameStringData()
                                                         << '\'' << " is not a supported option.");
        }
    }
    return StatusWith<std::string>(ss.str());
}

// static
StatusWith<std::string> NarkDbIndex::generateCreateString(const std::string& engineName,
                                                              const std::string& sysIndexConfig,
                                                              const std::string& collIndexConfig,
                                                              const IndexDescriptor& desc) {
    str::stream ss;

    // Separate out a prefix and suffix in the default string. User configuration will override
    // values in the prefix, but not values in the suffix.  Page sizes are chosen so that index
    // keys (up to 1024 bytes) will not overflow.
    ss << "type=file,internal_page_max=16k,leaf_page_max=16k,";
    ss << "checksum=on,";
    if (narkDbGlobalOptions.useIndexPrefixCompression) {
        ss << "prefix_compression=true,";
    }

    ss << "block_compressor=" << narkDbGlobalOptions.indexBlockCompressor << ",";
    ss << NarkDbCustomizationHooks::get(getGlobalServiceContext())->getOpenConfig(desc.parentNS());
    ss << sysIndexConfig << ",";
    ss << collIndexConfig << ",";

    // Validate configuration object.
    // Raise an error about unrecognized fields that may be introduced in newer versions of
    // this storage engine.
    // Ensure that 'configString' field is a string. Raise an error if this is not the case.
    BSONElement storageEngineElement = desc.getInfoElement("storageEngine");
    if (storageEngineElement.isABSONObj()) {
        BSONObj storageEngine = storageEngineElement.Obj();
        StatusWith<std::string> parseStatus =
            parseIndexOptions(storageEngine.getObjectField(engineName));
        if (!parseStatus.isOK()) {
            return parseStatus;
        }
        if (!parseStatus.getValue().empty()) {
            ss << "," << parseStatus.getValue();
        }
    }

    // WARNING: No user-specified config can appear below this line. These options are required
    // for correct behavior of the server.

    // Indexes need to store the metadata for collation to work as expected.
    ss << ",key_format=u,value_format=u";

    // Index metadata
    ss << ",app_metadata=("
       << "formatVersion=" << kCurrentIndexVersion << ','
       << "infoObj=" << desc.infoObj().jsonString() << "),";

    LOG(3) << "index create string: " << ss.ss.str();
    return StatusWith<std::string>(ss);
}

NarkDbIndex::NarkDbIndex(CompositeTable* table, OperationContext* ctx, const IndexDescriptor* desc)
    : m_table(table)
	, _ordering(Ordering::make(desc->keyPattern()))
    , _collectionNamespace(desc->parentNS())
    , _indexName(desc->indexName())
{
	std::string indexColumnNames;
	BSONForEach(elem, desc->keyPattern()) {
		indexColumnNames.append(elem.fieldName());
		indexColumnNames.push_back(',');
	}
	indexColumnNames.pop_back();
	const size_t indexId = table->getIndexId(indexColumnNames);
	if (indexId == table->getIndexNum()) {
		// no such index
		THROW_STD(invalid_argument,
			"index(%s) on collection(%s) is not defined",
			indexColumnNames.c_str(),
			desc->parentNS().c_str());
	}
	m_indexId = indexId;
}

NarkDbIndex::MyThreadData& NarkDbIndex::getMyThreadData() const {
	auto tid = std::this_thread::get_id();
	std::lock_guard<std::mutex> lock(m_threadcacheMutex);
	size_t i = m_threadcache.insert_i(tid).first;
	if (m_threadcache.val(i).m_dbCtx == nullptr)
		m_threadcache.val(i).m_dbCtx = m_table->createDbContext();
	return m_threadcache.val(i);
}

Status NarkDbIndex::insert(OperationContext* txn,
                           const BSONObj& key,
                           const RecordId& id,
                           bool dupsAllowed)
{
	auto& td = getMyThreadData();
	auto indexSchema = getIndexSchema();
	td.m_coder.encode(indexSchema, nullptr, key);
	llong recIdx = id.repr() - 1;
	if (m_table->indexInsert(m_indexId, td.m_buf, recIdx, &*td.m_dbCtx)) {
		return Status::OK();
	} else {
		return Status(ErrorCodes::DuplicateKey, "dup key in NarkDbIndex::insert");
	}
}

void NarkDbIndex::unindex(OperationContext* txn,
                          const BSONObj& key,
                          const RecordId& id,
                          bool dupsAllowed)
{
    invariant(id.isNormal());
    dassert(!hasFieldNames(key));
	auto& td = getMyThreadData();
	auto indexSchema = getIndexSchema();
	td.m_coder.encode(indexSchema, nullptr, key);
	llong recIdx = id.repr() - 1;
	m_table->indexRemove(m_indexId, td.m_buf, recIdx, &*td.m_dbCtx);
}

void NarkDbIndex::fullValidate(OperationContext* txn,
							   bool full,
							   long long* numKeysOut,
							   BSONObjBuilder* output) const {
}

bool NarkDbIndex::appendCustomStats(OperationContext* txn,
									BSONObjBuilder* output,
									double scale) const {
    {
        BSONObjBuilder metadata(output->subobjStart("metadata"));
    }
    return true;
}

Status NarkDbIndex::dupKeyCheck(OperationContext* txn, const BSONObj& key, const RecordId& id) {
    invariant(!hasFieldNames(key));
    invariant(unique());

    return Status::OK();
}

bool NarkDbIndex::isEmpty(OperationContext* txn) {
    return false;
}

Status NarkDbIndex::touch(OperationContext* txn) const {
    return Status(ErrorCodes::CommandNotSupported, "this storage engine does not support touch");
}

long long NarkDbIndex::getSpaceUsedBytes(OperationContext* txn) const {
    return m_table->indexStorageSize(m_indexId);
}

Status NarkDbIndex::initAsEmpty(OperationContext* txn) {
    // No-op
    return Status::OK();
}

namespace {

/**
 * Implements the basic NarkDb_CURSOR functionality used by both unique and standard indexes.
 */
class NarkDbIndexCursorBase : public SortedDataInterface::Cursor {
public:
    NarkDbIndexCursorBase(const NarkDbIndex& idx, OperationContext* txn, bool forward)
        : _txn(txn), _idx(idx), _forward(forward) {
		if (forward)
			idx.m_table->createIndexIterForward(idx.m_indexId);
		else
			idx.m_table->createIndexIterBackward(idx.m_indexId);
    }
    boost::optional<IndexKeyEntry> next(RequestedInfo parts) override {
        if (_eof) // Advance on a cursor at the end is a no-op
            return {};
        if (!_lastMoveWasRestore) {
			llong recIdx = -1;
			_cursorAtEof = _cursor->increment(&recIdx, &m_curKey);
			if (!_cursorAtEof) {
				_id = RecordId(recIdx + 1);
		        return curr(parts);
			}
			return {};
		} else {
			_lastMoveWasRestore = false;
	        return curr(parts);
		}
    }

    void setEndPosition(const BSONObj& key, bool inclusive) override {
        TRACE_CURSOR << "setEndPosition inclusive: " << inclusive << ' ' << key;
        if (key.isEmpty()) {
            // This means scan to end of index.
            _endPositionKey.erase_all();
        }
		else {
			m_coder.encode(_idx.getIndexSchema(), nullptr, key, &_endPositionKey);
			_endPositionInclude = inclusive;
		}
    }

    boost::optional<IndexKeyEntry> seek(const BSONObj& key,
                                        bool inclusive,
                                        RequestedInfo parts) override {
        const BSONObj finalKey = stripFieldNames(key);
        const auto discriminator =
            _forward == inclusive ? KeyString::kExclusiveBefore : KeyString::kExclusiveAfter;

        // By using a discriminator other than kInclusive, there is no need to distinguish
        // unique vs non-unique key formats since both start with the key.
        // _query.resetToKey(finalKey, _idx.ordering(), discriminator);
        seekWTCursor(key);
        updatePosition();
        return curr(parts);
    }

    boost::optional<IndexKeyEntry> seek(const IndexSeekPoint& seekPoint,
                                        RequestedInfo parts) override {
        // TODO: don't go to a bson obj then to a KeyString, go straight
        BSONObj key = IndexEntryComparison::makeQueryObject(seekPoint, _forward);

        // makeQueryObject handles the discriminator in the real exclusive cases.
        seekWTCursor(key);
        updatePosition();
        return curr(parts);
    }

    void save() override {
        try {
            _cursor->reset();
        } catch (const WriteConflictException&) {
            // Ignore since this is only called when we are about to kill our transaction
            // anyway.
        }

        // Our saved position is wherever we were when we last called updatePosition().
        // Any partially completed repositions should not effect our saved position.
    }

    void saveUnpositioned() override {
        save();
        _eof = true;
    }

    void restore() override {
        if (!_eof) {
            // Unique indices *don't* include the record id in their KeyStrings. If we seek to the
            // same key with a new record id, seeking will successfully find the key and will return
            // true. This will cause us to skip the key with the new record id, since we set
            // _lastMoveWasRestore to false.
            //
            // Standard (non-unique) indices *do* include the record id in their KeyStrings. This
            // means that restoring to the same key with a new record id will return false, and we
            // will *not* skip the key with the new record id.
            _lastMoveWasRestore = !seekWTCursor();
            TRACE_CURSOR << "restore _lastMoveWasRestore:" << _lastMoveWasRestore;
        }
    }

    void detachFromOperationContext() final {
        _txn = nullptr;
        _cursor = nullptr;
    }

    void reattachToOperationContext(OperationContext* txn) final {
        _txn = txn;
        // _cursor recreated in restore() to avoid risk of WT_ROLLBACK issues.
    }

protected:
    boost::optional<IndexKeyEntry> curr(RequestedInfo parts) {
        if (_eof)
            return {};

        dassert(!atOrPastEndPointAfterSeeking());
        dassert(!_id.isNull());

        BSONObj bson;
        if (TRACING_ENABLED || (parts & kWantKey)) {
            bson = BSONObj(m_coder.decode(_idx.getIndexSchema(), fstring(m_curKey)));

            TRACE_CURSOR << " returning " << bson << ' ' << _id;
        }

        return {{std::move(bson), _id}};
    }

    bool atOrPastEndPointAfterSeeking() const {
        if (_eof)
            return true;
        if (_endPositionKey.empty())
            return false;

        const int cmp = _idx.getIndexSchema()->compareData(m_curKey, _endPositionKey);

        if (_forward) {
            // We may have landed after the end point.
            return cmp > 0 || (cmp == 0 && !_endPositionInclude);
        } else {
            // We may have landed before the end point.
            return cmp < 0 || (cmp == 0 && !_endPositionInclude);
        }
    }

    void advanceWTCursor() {
		llong recIdx = -1;
        _cursorAtEof = _cursor->increment(&recIdx, &m_curKey);
		if (!_cursorAtEof) {
			_id = RecordId(recIdx + 1);
		}
    }

    // Seeks to query. Returns true on exact match.
    bool seekWTCursor(const BSONObj& bsonKey) {
		auto indexSchema = _idx.getIndexSchema();
		m_coder.encode(indexSchema, nullptr, bsonKey, &m_qryKey);
		return seekWTCursor();
	}
    bool seekWTCursor() {
		llong recIdx = -1;
        int ret = _cursor->seekLowerBound(m_qryKey, &recIdx, &m_curKey);
        if (ret < 0) {
            _cursorAtEof = true;
            TRACE_CURSOR << "\t not found, queryKey is greater than all keys";
            return false;
        }
        _cursorAtEof = false;

        TRACE_CURSOR << "\t lowerBound ret: " << ret;

        if (ret == 0) {
            // Found it!
            return true;
        }

        return false;
    }

    /**
     * This must be called after moving the cursor to update our cached position. It should not
     * be called after a restore that did not restore to original state since that does not
     * logically move the cursor until the following call to next().
     */
    void updatePosition() {
        _lastMoveWasRestore = false;
        if (_cursorAtEof) {
            _eof = true;
            _id = RecordId();
        }
		else {
			_eof = atOrPastEndPointAfterSeeking();
		}
    }

    const NarkDbIndex& _idx;  // not owned
    OperationContext*  _txn;
    nark::db::IndexIteratorPtr  _cursor;
	nark::valvec<unsigned char> m_curKey;
	nark::valvec<         char> m_qryKey;
	mongo::narkdb::SchemaRecordCoder m_coder;

    // These are where this cursor instance is. They are not changed in the face of a failing
    // next().
    RecordId _id;

    const bool _forward;
	bool _eof = true;

    // This differs from _eof in that it always reflects the result of the most recent call to
    // reposition _cursor.
    bool _cursorAtEof = false;

    // Used by next to decide to return current position rather than moving. Should be reset to
    // false by any operation that moves the cursor, other than subsequent save/restore pairs.
    bool _lastMoveWasRestore = false;

	bool _endPositionInclude;
    nark::valvec<unsigned char> _endPositionKey;
};

}  // namespace

/**
 * Bulk builds a unique index.
 *
 * In order to support unique indexes in dupsAllowed mode this class only does an actual insert
 * after it sees a key after the one we are trying to insert. This allows us to gather up all
 * duplicate ids and insert them all together. This is necessary since bulk cursors can only
 * append data.
 */
class NarkDbIndex::BulkBuilder : public SortedDataBuilderInterface {
public:
    BulkBuilder(NarkDbIndex* idx, OperationContext* txn, bool dupsAllowed)
        : _idx(idx), _txn(txn)
		, _ctx(idx->m_table->createDbContext())
		, _dupsAllowed(dupsAllowed)
	{
	//	_ctx->syncIndex = false;
	}

    Status addKey(const BSONObj& newKey, const RecordId& id) override {
        {
            const Status s = checkKeySize(newKey);
            if (!s.isOK())
                return s;
        }
		if (_idx->insertIndexKey(newKey, id, &*_ctx)) {
	        return Status::OK();
		} else {
			return Status(ErrorCodes::DuplicateKey,
				"Dup key in NarkDbIndex::BulkBuilder");
		}
    }

    void commit(bool mayInterrupt) override {
        // TODO do we still need this?
        // this is bizarre, but required as part of the contract
        WriteUnitOfWork uow(_txn);
        uow.commit();
    }

private:
    NarkDbIndex*      const _idx;
    OperationContext* const _txn;
	const nark::db::DbContextPtr _ctx;
    const bool _dupsAllowed;
};

NarkDbIndexUnique::NarkDbIndexUnique(CompositeTable* tab,
									 OperationContext* opCtx,
                                     const IndexDescriptor* desc)
    : NarkDbIndex(tab, opCtx, desc)
{}

std::unique_ptr<SortedDataInterface::Cursor>
NarkDbIndexUnique::newCursor(OperationContext* txn, bool forward) const {
    return stdx::make_unique<NarkDbIndexCursorBase>(*this, txn, forward);
}

SortedDataBuilderInterface*
NarkDbIndexUnique::getBulkBuilder(OperationContext* txn, bool dupsAllowed) {
    return new BulkBuilder(this, txn, dupsAllowed);
}

// ------------------------------

NarkDbIndexStandard::NarkDbIndexStandard(CompositeTable* tab,
                                         OperationContext* opCtx,
                                         const IndexDescriptor* desc)
    : NarkDbIndex(tab, opCtx, desc) {}

std::unique_ptr<SortedDataInterface::Cursor>
NarkDbIndexStandard::newCursor(OperationContext* txn, bool forward) const {
    return stdx::make_unique<NarkDbIndexCursorBase>(*this, txn, forward);
}

SortedDataBuilderInterface*
NarkDbIndexStandard::getBulkBuilder(OperationContext* txn, bool dupsAllowed) {
    // We aren't unique so dups better be allowed.
    invariant(dupsAllowed);
    return new BulkBuilder(this, txn, dupsAllowed);
}

} } // namespace mongo::narkdb
