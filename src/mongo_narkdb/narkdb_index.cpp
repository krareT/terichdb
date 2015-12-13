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

#define TRACING_ENABLED 1

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

NarkDbIndex::NarkDbIndex(CompositeTable* table, OperationContext* ctx, const IndexDescriptor* desc)
    : m_table(table)
	, _ordering(Ordering::make(desc->keyPattern()))
    , _collectionNamespace(desc->parentNS())
    , _indexName(desc->indexName())
{
	LOG(2) << "NarkDbIndex::NarkDbIndex(): keyPattern=" << desc->keyPattern().toString();
	std::string indexColumnNames;
	BSONForEach(elem, desc->keyPattern()) {
		indexColumnNames.append(elem.fieldName());
		indexColumnNames.push_back(',');
	}
	indexColumnNames.pop_back();
	LOG(2) << "NarkDbIndex::NarkDbIndex(): indexColumnNames=" << indexColumnNames;
	const size_t indexId = table->getIndexId(indexColumnNames);
	if (indexId == table->getIndexNum()) {
		// no such index
		THROW_STD(invalid_argument,
			"index(%s) on collection(%s) is not defined",
			indexColumnNames.c_str(),
			desc->parentNS().c_str());
	}
	m_indexId = indexId;
	invariant(desc->unique() == getIndexSchema()->m_isUnique);
}

NarkDbIndex::MyThreadData& NarkDbIndex::getMyThreadData() const {
	auto tid = std::this_thread::get_id();
	std::lock_guard<std::mutex> lock(m_threadcacheMutex);
	auto& tdptr = m_threadcache.get_map()[tid];
	if (tdptr == nullptr) {
		tdptr = new MyThreadData();
		tdptr->m_dbCtx = m_table->createDbContext();
	}
	return *tdptr;
}

Status NarkDbIndex::insert(OperationContext* txn,
                           const BSONObj& key,
                           const RecordId& id,
                           bool dupsAllowed)
{
	auto& td = getMyThreadData();
	auto indexSchema = getIndexSchema();
	encodeIndexKey(*indexSchema, key, &td.m_buf);
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
	encodeIndexKey(*indexSchema, key, &td.m_buf);
	llong recIdx = id.repr() - 1;
	m_table->indexRemove(m_indexId, td.m_buf, recIdx, &*td.m_dbCtx);
}

void NarkDbIndex::fullValidate(OperationContext* txn,
							   bool full,
							   long long* numKeysOut,
							   BSONObjBuilder* output) const {
	LOG(2) << BOOST_CURRENT_FUNCTION << ": is in TODO list, Not supported now";
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
	auto& td = getMyThreadData();
	auto indexSchema = getIndexSchema();
	encodeIndexKey(*indexSchema, key, &td.m_buf);
	llong recIdx = m_table->indexSearchExact(m_indexId, td.m_buf, &*td.m_dbCtx);
	if (recIdx < 0 || id.repr() == recIdx+1) {
	    return Status::OK();
	}
	return Status(ErrorCodes::DuplicateKey, "NarkDbIndex::dupKeyCheck");
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

bool
NarkDbIndex::insertIndexKey(const BSONObj& newKey, const RecordId& id,
							MyThreadData* td) {
	encodeIndexKey(*getIndexSchema(), newKey, &td->m_buf);
	return m_table->indexInsert(m_indexId, td->m_buf, id.repr()-1, &*td->m_dbCtx);
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
			_cursor = idx.m_table->createIndexIterForward(idx.m_indexId);
		else
			_cursor = idx.m_table->createIndexIterBackward(idx.m_indexId);
    }
    boost::optional<IndexKeyEntry> next(RequestedInfo parts) override {
        if (_eof) { // Advance on a cursor at the end is a no-op
	        TRACE_CURSOR << "next(): _eof=true";
            return {};
		}
        if (!_lastMoveWasRestore) {
			llong recIdx = -1;
			if (_cursor->increment(&recIdx, &m_curKey)) {
				_cursorAtEof = false;
				_id = RecordId(recIdx + 1);
		        TRACE_CURSOR << "next(): curKey=" << _idx.getIndexSchema()->toJsonStr(m_curKey);
		        return curr(parts);
			}
			_cursorAtEof = true;
			return {};
		} else {
			TRACE_CURSOR << "next(): curKey=" << _idx.getIndexSchema()->toJsonStr(m_curKey);
			_lastMoveWasRestore = false;
	        return curr(parts);
		}
    }

    void setEndPosition(const BSONObj& key, bool inclusive) override {
        TRACE_CURSOR << "setEndPosition inclusive: " << inclusive << ' ' << key;
        if (key.isEmpty()) {
		EmptyKey:
            // This means scan to end of index.
            _endPositionKey.erase_all();
			_endPositionInclude = false;
        }
		else {
			BSONElement first(*key.begin());
			if (first.isABSONObj() && first.embeddedObject().isEmpty()) {
		        TRACE_CURSOR << "setEndPosition: first field is an empty object";
				goto EmptyKey;
			}
			encodeIndexKey(*_idx.getIndexSchema(), key, &_endPositionKey);
			_endPositionInclude = inclusive;
	        TRACE_CURSOR << "setEndPosition: _endPositionKey="
						 << _idx.getIndexSchema()->toJsonStr(_endPositionKey);
		}
    }

    boost::optional<IndexKeyEntry> seek(const BSONObj& key,
                                        bool inclusive,
                                        RequestedInfo parts) override {
//        const BSONObj finalKey = stripFieldNames(key);
//        const auto discriminator =
//           _forward == inclusive ? KeyString::kExclusiveBefore : KeyString::kExclusiveAfter;

        // By using a discriminator other than kInclusive, there is no need to distinguish
        // unique vs non-unique key formats since both start with the key.
        // _query.resetToKey(finalKey, _idx.ordering(), discriminator);
        seekWTCursor(key, inclusive);
        updatePosition();
		if (!_cursorAtEof)
			return curr(parts);
		else
			return {};
    }

    boost::optional<IndexKeyEntry> seek(const IndexSeekPoint& seekPoint,
                                        RequestedInfo parts) override {
        // TODO: don't go to a bson obj then to a KeyString, go straight
        // makeQueryObject handles the discriminator in the real exclusive cases.
        BSONObj key = IndexEntryComparison::makeQueryObject(seekPoint, _forward);
        seekWTCursor(key, true);
        updatePosition();
		if (!_cursorAtEof)
			return curr(parts);
		else
			return {};
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
			m_qryKey.assign(m_curKey);
            _lastMoveWasRestore = !seekWTCursor(true);
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
            bson = BSONObj(m_coder.decode(_idx.getIndexSchema(), m_curKey));

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
		if (_cursor->increment(&recIdx, &m_curKey)) {
	        _cursorAtEof = false;
			_id = RecordId(recIdx + 1);
		} else {
	        _cursorAtEof = true;
		}
    }

    // Seeks to query. Returns true on exact match.
    bool seekWTCursor(const BSONObj& bsonKey, bool inclusive) {
		auto indexSchema = _idx.getIndexSchema();
		encodeIndexKey(*indexSchema, bsonKey, &m_qryKey);
		return seekWTCursor(inclusive);
	}
    bool seekWTCursor(bool inclusive) {
		llong recIdx = -1;
        int ret = _cursor->seekLowerBound(m_qryKey, &recIdx, &m_curKey);
        if (ret < 0) {
            _cursorAtEof = true;
            TRACE_CURSOR << "\t not found, queryKey is greater than all keys";
            return false;
        }
        TRACE_CURSOR << "\t lowerBound ret: " << ret
			<< ", recIdx=" << recIdx
			<< ", curKey=" << _idx.getIndexSchema()->toJsonStr(m_curKey);
		if (!inclusive && fstring(m_qryKey) == m_curKey) {
			if (_cursor->increment(&recIdx, &m_curKey)) {
				_cursorAtEof = false;
				_id = RecordId(recIdx + 1);
			} else {
				_cursorAtEof = true;
				return false;
			}
		}
		else {
			_id = RecordId(recIdx + 1);
	        _cursorAtEof = false;
		}

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

    OperationContext*  _txn;
    const NarkDbIndex& _idx;  // not owned
    nark::db::IndexIteratorPtr  _cursor;
	nark::valvec<unsigned char> m_curKey;
	nark::valvec<         char> m_qryKey;
	mongo::narkdb::SchemaRecordCoder m_coder;

    // These are where this cursor instance is. They are not changed in the face of a failing
    // next().
    RecordId _id;

    const bool _forward;
	bool _eof = false;

    // This differs from _eof in that it always reflects the result of the most recent call to
    // reposition _cursor.
    bool _cursorAtEof = false;

    // Used by next to decide to return current position rather than moving. Should be reset to
    // false by any operation that moves the cursor, other than subsequent save/restore pairs.
    bool _lastMoveWasRestore = false;

	bool _endPositionInclude;
	bool m_isEndKeyMax = true;
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
		, _dupsAllowed(dupsAllowed)
	{
		m_td.m_dbCtx = idx->m_table->createDbContext();
	}

    Status addKey(const BSONObj& newKey, const RecordId& id) override {
        {
            const Status s = checkKeySize(newKey);
            if (!s.isOK())
                return s;
        }
		if (_idx->insertIndexKey(newKey, id, &m_td)) {
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
	MyThreadData  m_td;
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

bool NarkDbIndexUnique::unique() const {
	return true;
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

bool NarkDbIndexStandard::unique() const {
	return false;
}

} } // namespace mongo::narkdb
