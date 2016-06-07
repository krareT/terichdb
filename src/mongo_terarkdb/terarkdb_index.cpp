// terarkdb_index.cpp

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

#include "terarkdb_index.h"

#include <set>

#include "mongo/base/checked_cast.h"
#include "mongo/db/json.h"
#include "mongo/db/catalog/index_catalog_entry.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/key_string.h"
#include "terarkdb_customization_hooks.h"
#include "terarkdb_global_options.h"
#include "terarkdb_record_store.h"
//#include "terarkdb_session_cache.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

#define TRACING_ENABLED TERARK_IF_DEBUG(1, 0)

#if TRACING_ENABLED
#define TRACE_CURSOR log() << "TerarkDb index (" << (const void*)&_idx << "), " << (_forward?"Forward":"Backward") << "Cursor."
#define TRACE_INDEX log() << "TerarkDb index (" << (const void*) this << ") "
#else
#define TRACE_CURSOR \
    if (0)           \
    log()
#define TRACE_INDEX \
    if (0)          \
    log()
#endif

namespace mongo { namespace terarkdb {
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
            << "TerarkDbIndex::insert: key too large to index, failing " << ' ' << key.objsize()
            << ' ' << key;
        return Status(ErrorCodes::KeyTooLong, msg);
    }
    return Status::OK();
}

}  // namespace

Status TerarkDbIndex::dupKeyError(const BSONObj& key) {
    StringBuilder sb;
    sb << "E11000 duplicate key error";
    sb << " collection: " << _collectionNamespace;
    sb << " index: " << _indexName;
    sb << " dup key: " << key;
    return Status(ErrorCodes::DuplicateKey, sb.str());
}

TerarkDbIndex::TerarkDbIndex(ThreadSafeTable* table, OperationContext* ctx, const IndexDescriptor* desc)
    : m_table(table)
	, _ordering(Ordering::make(desc->keyPattern()))
    , _collectionNamespace(desc->parentNS())
    , _indexName(desc->indexName())
{
	log() << "mongo_terarkdb@panda index TerarkDbIndex";
	LOG(2) << "TerarkDbIndex::TerarkDbIndex(): keyPattern=" << desc->keyPattern().toString();
	std::string indexColumnNames;
	BSONForEach(elem, desc->keyPattern()) {
		indexColumnNames.append(elem.fieldName());
		indexColumnNames.push_back(',');
	}
	indexColumnNames.pop_back();
	LOG(2) << "TerarkDbIndex::TerarkDbIndex(): indexColumnNames=" << indexColumnNames;
	DbTable* tab = table->m_tab.get();
	const size_t indexId = tab->getIndexId(indexColumnNames);
	if (indexId == tab->getIndexNum()) {
		// no such index
		THROW_STD(invalid_argument,
			"index(%s) on collection(%s) is not defined",
			indexColumnNames.c_str(),
			desc->parentNS().c_str());
	}
	m_indexId = indexId;
	invariant(desc->unique() == getIndexSchema()->m_isUnique);
}

TerarkDbIndex::~TerarkDbIndex() {
	DbTable* tab = m_table->m_tab.get();
    LOG(1) << BOOST_CURRENT_FUNCTION << ": dir: " << tab->getDir().string();
}

Status TerarkDbIndex::insert(OperationContext* txn,
                           const BSONObj& key,
                           const RecordId& id,
                           bool dupsAllowed)
{
	struct timespec start, end;
	clock_gettime(CLOCK_MONOTONIC, &start);
	auto& td = m_table->getMyThreadData();
	auto indexSchema = getIndexSchema();
	encodeIndexKey(*indexSchema, key, &td.m_buf);
	llong recIdx = id.repr() - 1;
	DbTable* tab = m_table->m_tab.get();
	bool result = tab->indexInsert(m_indexId, td.m_buf, recIdx, &*td.m_dbCtx);
	clock_gettime(CLOCK_MONOTONIC, &end);
	long long timeuse = 1000000000LL * ( end.tv_sec - start.tv_sec ) + end.tv_nsec - start.tv_nsec;

	log() << "mongo_terarkdb@panda index insert timeuse(ns) " << timeuse;

	if (result) {
		return Status::OK();
	} else {
		return Status(ErrorCodes::DuplicateKey, "dup key in TerarkDbIndex::insert");
	}
}

void TerarkDbIndex::unindex(OperationContext* txn,
                          const BSONObj& key,
                          const RecordId& id,
                          bool dupsAllowed)
{
    log() << "mongo_terarkdb@panda index unindex";
    invariant(id.isNormal());
    dassert(!hasFieldNames(key));
	auto& td = m_table->getMyThreadData();
	auto indexSchema = getIndexSchema();
	encodeIndexKey(*indexSchema, key, &td.m_buf);
	llong recIdx = id.repr() - 1;
	DbTable* tab = m_table->m_tab.get();
	tab->indexRemove(m_indexId, td.m_buf, recIdx, &*td.m_dbCtx);
}

void TerarkDbIndex::fullValidate(OperationContext* txn,
							   bool full,
							   long long* numKeysOut,
							   ValidateResults* output) const {
	log() << "mongo_terarkdb@panda index fullValidate";
	LOG(2) << BOOST_CURRENT_FUNCTION << ": is in TODO list, Not supported now";
}

bool TerarkDbIndex::appendCustomStats(OperationContext* txn,
									BSONObjBuilder* output,
									double scale) const {
    {
        BSONObjBuilder metadata(output->subobjStart("metadata"));
    }
    return true;
}

Status TerarkDbIndex::dupKeyCheck(OperationContext* txn, const BSONObj& key, const RecordId& id) {
	log() << "mongo_terarkdb@panda index dupKeyCheck";
    invariant(!hasFieldNames(key));
    invariant(unique());
	auto& td = m_table->getMyThreadData();
	auto indexSchema = getIndexSchema();
	encodeIndexKey(*indexSchema, key, &td.m_buf);
	auto& tmpIdvec = td.m_dbCtx->exactMatchRecIdvec;
	DbTable* tab = m_table->m_tab.get();
	tab->indexSearchExact(m_indexId, td.m_buf, &tmpIdvec, &*td.m_dbCtx);
	if (tmpIdvec.empty()) {
	    return Status::OK();
	}
	llong recIdx = tmpIdvec[0];
	if (id.repr() == recIdx+1) {
	    return Status::OK();
	}
	return Status(ErrorCodes::DuplicateKey, "TerarkDbIndex::dupKeyCheck");
}

bool TerarkDbIndex::isEmpty(OperationContext* txn) {
	DbTable* tab = m_table->m_tab.get();
    return tab->numDataRows() == 0;
}

Status TerarkDbIndex::touch(OperationContext* txn) const {
    return Status(ErrorCodes::CommandNotSupported, "this storage engine does not support touch");
}

long long TerarkDbIndex::getSpaceUsedBytes(OperationContext* txn) const {
	DbTable* tab = m_table->m_tab.get();
    return tab->indexStorageSize(m_indexId);
}

Status TerarkDbIndex::initAsEmpty(OperationContext* txn) {
    // No-op
    return Status::OK();
}

bool
TerarkDbIndex::insertIndexKey(const BSONObj& newKey, const RecordId& id,
							TableThreadData* td) {
	log() << "mongo_terarkdb@panda index insertIndexKey";
	encodeIndexKey(*getIndexSchema(), newKey, &td->m_buf);
	DbTable* tab = m_table->m_tab.get();
	return tab->indexInsert(m_indexId, td->m_buf, id.repr()-1, &*td->m_dbCtx);
}

namespace {

/**
 * Implements the basic TerarkDb_CURSOR functionality used by both unique and standard indexes.
 */
class TerarkDbIndexCursorBase : public SortedDataInterface::Cursor {
public:
    TerarkDbIndexCursorBase(const TerarkDbIndex& idx, OperationContext* txn, bool forward)
        : _txn(txn), _idx(idx), _forward(forward), _endPositionInclude(false) {
	struct timespec start, end;
	clock_gettime(CLOCK_MONOTONIC, &start);
		_cursor = idx.m_table->allocIndexIter(idx.m_indexId, forward);
	clock_gettime(CLOCK_MONOTONIC, &end);
	long long timeuse = 1000000000LL * ( end.tv_sec - start.tv_sec ) + end.tv_nsec - start.tv_nsec;
	log() << "mongo_terarkdb@panda TerarkDbIndexCursorBase timeuse(ns) " << timeuse;
    }
	~TerarkDbIndexCursorBase() {
		_idx.m_table->releaseIndexIter(_idx.m_indexId, _forward, _cursor);
	}
    boost::optional<IndexKeyEntry> next(RequestedInfo parts) override {
	log() << "mongo_terarkdb@panda TerarkDbIndexCursorBase next";
        if (_eof) { // Advance on a cursor at the end is a no-op
	        TRACE_CURSOR << "next(): _eof=true";
            return {};
		}
 		auto cur = _cursor.get();
        if (!_lastMoveWasRestore) {
			llong recIdx = -1;
			if (_cursor->increment(&recIdx)) {
				_cursorAtEof = false;
				_id = RecordId(recIdx + 1);
		        TRACE_CURSOR << "next(): increment() ret true, curKey=" << _idx.getIndexSchema()->toJsonStr(cur->m_curKey);
		        return curr(parts);
			}
			else {
		        TRACE_CURSOR << "next(): increment() ret false, reached eof";
				_cursorAtEof = true;
				return {};
			}
		}
		else {
			TRACE_CURSOR << "next(): curKey=" << _idx.getIndexSchema()->toJsonStr(cur->m_curKey);
			_lastMoveWasRestore = false;
	        return curr(parts);
		}
    }

    void setEndPosition(const BSONObj& key, bool inclusive) override {
        TRACE_CURSOR << "setEndPosition: inclusive = " << inclusive << ", bsonKey = " << key;
		auto cur = _cursor.get();
        if (key.isEmpty()) {
		EmptyKey:
            // This means scan to end of index.
            cur->m_endPositionKey.erase_all();
			_endPositionInclude = false;
        }
		else {
			BSONElement first(*key.begin());
			if (first.isABSONObj() && first.embeddedObject().isEmpty()) {
		        TRACE_CURSOR << "setEndPosition: first field is an empty object";
				goto EmptyKey;
			}
			encodeIndexKey(*_idx.getIndexSchema(), key, &cur->m_endPositionKey);
			_endPositionInclude = inclusive;
	        TRACE_CURSOR << "setEndPosition: _endPositionKey="
						 << _idx.getIndexSchema()->toJsonStr(cur->m_endPositionKey);
		}
    }

    boost::optional<IndexKeyEntry> seek(const BSONObj& key,
                                        bool inclusive,
                                        RequestedInfo parts) override {
	struct timespec start, end;
	clock_gettime(CLOCK_MONOTONIC, &start);
//        const BSONObj finalKey = stripFieldNames(key);
//        const auto discriminator =
//           _forward == inclusive ? KeyString::kExclusiveBefore : KeyString::kExclusiveAfter;

        // By using a discriminator other than kInclusive, there is no need to distinguish
        // unique vs non-unique key formats since both start with the key.
        // _query.resetToKey(finalKey, _idx.ordering(), discriminator);
        TRACE_CURSOR << "seek3(): key=" << key.jsonString() << ", inclusive=" << inclusive;
        seekWTCursor(key, inclusive);
        updatePosition();
	clock_gettime(CLOCK_MONOTONIC, &end);
	long long timeuse = 1000000000LL * ( end.tv_sec - start.tv_sec ) + end.tv_nsec - start.tv_nsec;
	log() << "mongo_terarkdb@panda TerarkDbIndexCursorBase seek key timeuse(ns) " << timeuse;
		if (!_cursorAtEof)
			return curr(parts);
		else
			return {};
    }

    boost::optional<IndexKeyEntry> seek(const IndexSeekPoint& seekPoint,
                                        RequestedInfo parts) override {
	log() << "mongo_terarkdb@panda TerarkDbIndexCursorBase seek seekPoint";
        // TODO: don't go to a bson obj then to a KeyString, go straight
        // makeQueryObject handles the discriminator in the real exclusive cases.
        BSONObj key = IndexEntryComparison::makeQueryObject(seekPoint, _forward);
        TRACE_CURSOR << "seek2(): key=" << key.jsonString();
        seekWTCursor(key, true);
        updatePosition();
		if (!_cursorAtEof)
			return curr(parts);
		else
			return {};
    }

    void save() override {
        TRACE_CURSOR << "save()";
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
        TRACE_CURSOR << "saveUnpositioned()";
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
			auto cur = _cursor.get();
			cur->m_qryKey.assign(cur->m_curKey);
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
	struct timespec start, end;
	clock_gettime(CLOCK_MONOTONIC, &start);
        if (_eof)
            return {};
        if (atOrPastEndPointAfterSeeking())
            return {};
        dassert(!_id.isNull());
        BSONObj bson;
        if (TRACING_ENABLED || (parts & kWantKey)) {
            bson = BSONObj(decodeIndexKey(*_idx.getIndexSchema(), _cursor->m_curKey));
            TRACE_CURSOR << "curr() returning " << bson << ' ' << _id;
        }
	clock_gettime(CLOCK_MONOTONIC, &end);
	long long timeuse = 1000000000LL * ( end.tv_sec - start.tv_sec ) + end.tv_nsec - start.tv_nsec;
	log() << "mongo_terarkdb@panda TerarkDbIndexCursorBase curr timeuse(ns) " << timeuse;
        return {{std::move(bson), _id}};
    }

    bool atOrPastEndPointAfterSeeking() const {
        if (_eof)
            return true;
		auto cur = _cursor.get();
        if (cur->m_endPositionKey.empty())
            return false;
        const int cmp = _idx.getIndexSchema()->compareData(cur->m_curKey, cur->m_endPositionKey);
		bool ret;
        if (_forward) {
            ret = cmp > 0 || (cmp == 0 && !_endPositionInclude);
        } else {
            ret = cmp < 0 || (cmp == 0 && !_endPositionInclude);
        }
		TRACE_CURSOR << "atOrPastEndPointAfterSeeking(): returning " << ret
			<< "\\\n\tcurKey=" << _idx.getIndexSchema()->toJsonStr(cur->m_curKey)
			<< ", endKey=" << _idx.getIndexSchema()->toJsonStr(cur->m_endPositionKey)
			<< ", cmp=" << cmp << ", endInclude=" << _endPositionInclude;
		return ret;
    }

    void advanceWTCursor() {
		llong recIdx = -1;
		if (_cursor->increment(&recIdx)) {
	        _cursorAtEof = false;
			_id = RecordId(recIdx + 1);
			TRACE_CURSOR << "advanceWTCursor(): increment() = true, _id = " << _id;
		} else {
			TRACE_CURSOR << "advanceWTCursor(): increment() = false, reach eof";
	        _cursorAtEof = true;
		}
    }

    // Seeks to query. Returns true on exact match.
    bool seekWTCursor(const BSONObj& bsonKey, bool inclusive) {
		auto indexSchema = _idx.getIndexSchema();
		encodeIndexKey(*indexSchema, bsonKey, &_cursor->m_qryKey);
		return seekWTCursor(inclusive);
	}
    bool seekWTCursor(bool inclusive) {
		llong recIdx = -1;
		_eof = false;
        int ret;
		const char* funcName;
		auto cur = _cursor.get();
	//	m_curKey.erase_all();
		if (inclusive) {
			funcName = "seekLowerBound";
			ret = cur->seekLowerBound(&recIdx);
		} else {
			funcName = "seekUpperBound";
			ret = cur->seekUpperBound(&recIdx);
		}
		bool isEqual = fstring(cur->m_qryKey) == cur->m_curKey;
		TRACE_CURSOR << "seekWTCursor(): " << funcName << " ret: " << ret
			<< ", _id = " << (recIdx + 1)
			<< ", qryKey = " << _idx.getIndexSchema()->toJsonStr(cur->m_qryKey)
			<< ", curKey = " << _idx.getIndexSchema()->toJsonStr(cur->m_curKey)
			<< ", inclusive = " << inclusive
			<< ", isEqual = " << isEqual
			;
        if (ret < 0) {
            _cursorAtEof = true;
            TRACE_CURSOR << "seekWTCursor(): not found, queryKey is greater than all keys";
            return false;
        }

		_id = RecordId(recIdx + 1);
	    _cursorAtEof = false;
		TRACE_CURSOR << "seekWTCursor(): returning true, _id = " << _id;
	    return true;
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
    const TerarkDbIndex& _idx;  // not owned
    IndexIterDataPtr  _cursor;

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
class TerarkDbIndex::BulkBuilder : public SortedDataBuilderInterface {
public:
    BulkBuilder(TerarkDbIndex* idx, OperationContext* txn, bool dupsAllowed)
        : _idx(idx), _txn(txn)
		, m_td(idx->m_table->m_tab.get())
		, _dupsAllowed(dupsAllowed)
	{
	}

    Status addKey(const BSONObj& newKey, const RecordId& id) override {
        {
		log() << "mongo_terarkdb@panda BulkBuilder addKey";
            const Status s = checkKeySize(newKey);
            if (!s.isOK())
                return s;
        }
		if (_idx->insertIndexKey(newKey, id, &m_td)) {
	        return Status::OK();
		} else {
			return Status(ErrorCodes::DuplicateKey,
				"Dup key in TerarkDbIndex::BulkBuilder");
		}
    }

    void commit(bool mayInterrupt) override {
	log() << "mongo_terarkdb@panda BulkBuilder commit";
        // TODO do we still need this?
        // this is bizarre, but required as part of the contract
        WriteUnitOfWork uow(_txn);
        uow.commit();
    }

private:
    TerarkDbIndex*      const _idx;
    OperationContext* const _txn;
	TableThreadData  m_td;
    const bool _dupsAllowed;
};

TerarkDbIndexUnique::TerarkDbIndexUnique(ThreadSafeTable* tab,
									 OperationContext* opCtx,
                                     const IndexDescriptor* desc)
    : TerarkDbIndex(tab, opCtx, desc)
{
	log() << "mongo_terarkdb@panda TerarkDbIndexUnique";
}

std::unique_ptr<SortedDataInterface::Cursor>
TerarkDbIndexUnique::newCursor(OperationContext* txn, bool forward) const {
    return stdx::make_unique<TerarkDbIndexCursorBase>(*this, txn, forward);
}

SortedDataBuilderInterface*
TerarkDbIndexUnique::getBulkBuilder(OperationContext* txn, bool dupsAllowed) {
    return new BulkBuilder(this, txn, dupsAllowed);
}

bool TerarkDbIndexUnique::unique() const {
	return true;
}

// ------------------------------

TerarkDbIndexStandard::TerarkDbIndexStandard(ThreadSafeTable* tab,
                                         OperationContext* opCtx,
                                         const IndexDescriptor* desc)
    : TerarkDbIndex(tab, opCtx, desc) {

	log() << "mongo_terarkdb@panda TerarkDbIndexStandard";
}

std::unique_ptr<SortedDataInterface::Cursor>
TerarkDbIndexStandard::newCursor(OperationContext* txn, bool forward) const {
    return stdx::make_unique<TerarkDbIndexCursorBase>(*this, txn, forward);
}

SortedDataBuilderInterface*
TerarkDbIndexStandard::getBulkBuilder(OperationContext* txn, bool dupsAllowed) {
    // We aren't unique so dups better be allowed.
    invariant(dupsAllowed);
    return new BulkBuilder(this, txn, dupsAllowed);
}

bool TerarkDbIndexStandard::unique() const {
	return false;
}

} } // namespace mongo::terarkdb
