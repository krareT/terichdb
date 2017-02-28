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

#include "terichdb_index.h"

#include <set>

#include "mongo/base/checked_cast.h"
#include "mongo/db/json.h"
#include "mongo/db/catalog/index_catalog_entry.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/key_string.h"
#include "terichdb_customization_hooks.h"
#include "terichdb_global_options.h"
#include "terichdb_record_store.h"
//#include "terichdb_session_cache.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

#define TRACING_ENABLED TERARK_IF_DEBUG(1, 0)

#if TRACING_ENABLED
#define TRACE_CURSOR log() << "TerichDb index (" << (const void*)&_idx << "), " << (_forward?"Forward":"Backward") << "Cursor."
#define TRACE_INDEX log() << "TerichDb index (" << (const void*) this << ") "
#else
#define TRACE_CURSOR \
    if (0)           \
    log()
#define TRACE_INDEX \
    if (0)          \
    log()
#endif

namespace mongo { namespace db {

using std::string;
using std::vector;

namespace {

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
            << "TerichDbIndex::insert: key too large to index, failing " << ' ' << key.objsize()
            << ' ' << key;
        return Status(ErrorCodes::KeyTooLong, msg);
    }
    return Status::OK();
}

}  // namespace

Status TerichDbIndex::dupKeyError(const BSONObj& key) {
    StringBuilder sb;
    sb << "E11000 duplicate key error";
    sb << " collection: " << _collectionNamespace;
    sb << " index: " << _indexName;
    sb << " dup key: " << key;
    return Status(ErrorCodes::DuplicateKey, sb.str());
}

TerichDbIndex::TerichDbIndex(ThreadSafeTable* table, OperationContext* ctx, const IndexDescriptor* desc)
    : m_table(table)
	, _ordering(Ordering::make(desc->keyPattern()))
    , _collectionNamespace(desc->parentNS())
    , _indexName(desc->indexName())
{
	LOG(2) << "TerichDbIndex::TerichDbIndex(): keyPattern=" << desc->keyPattern().toString();
	std::string indexColumnNames;
	BSONForEach(elem, desc->keyPattern()) {
		indexColumnNames.append(elem.fieldName());
		indexColumnNames.push_back(',');
	}
	indexColumnNames.pop_back();
	LOG(2) << "TerichDbIndex::TerichDbIndex(): indexColumnNames=" << indexColumnNames;
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

TerichDbIndex::~TerichDbIndex() {
	DbTable* tab = m_table->m_tab.get();
    LOG(1) << BOOST_CURRENT_FUNCTION << ": dir: " << tab->getDir().string();
}

struct UnindexOnFail : RecoveryUnit::Change {
    void rollback() override {
		m_index->do_unindex("UnindexOnFail::unindex()", m_txn, m_key, m_id, m_dupsAllowed);
	}
    void commit() override {
		// do nothing
	}
	UnindexOnFail(TerichDbIndex* rs, OperationContext* txn, const BSONObj& key, RecordId id, bool dupsAllowed)
		: m_index(rs), m_txn(txn), m_key(key), m_id(id), m_dupsAllowed(dupsAllowed) {}
	TerichDbIndex* m_index;
	OperationContext* m_txn;
	const BSONObj m_key;
	RecordId m_id;
	bool m_dupsAllowed;
};

Status TerichDbIndex::insert(OperationContext* txn,
                           const BSONObj& key,
                           const RecordId& id,
                           bool dupsAllowed)
{
	ThreadSafeTable* tst = m_table.get();
	DbTable* tab = tst->m_tab.get();
    LOG(2) << "TerichDbIndex::insert(): key = " << key << ", id = " << id
		<< ",  dir: " << tab->getDir().string();
	RecoveryUnitDataPtr rud = NULL;
	TableThreadData* ttd = NULL;
	if (txn && txn->recoveryUnit()) {
		rud = tst->getRecoveryUnitData(txn->recoveryUnit());
		ttd = rud->m_ttd.get();
		assert(NULL != ttd);
	} else {
		ttd = &tst->getMyThreadData();
	}
	if (insertIndexKey(key, id, dupsAllowed, txn, ttd)) {
		return Status::OK();
	} else {
		return Status(ErrorCodes::DuplicateKey, "dup key in TerichDbIndex::insert");
	}
}

void TerichDbIndex::unindex(OperationContext* txn,
                          const BSONObj& key,
                          const RecordId& id,
                          bool dupsAllowed) {
	do_unindex("TerichDbIndex::unindex()", txn, key, id, dupsAllowed);
}

void TerichDbIndex::do_unindex(const char* func,
							OperationContext* txn,
                            const BSONObj& key,
                            const RecordId& id,
                            bool dupsAllowed)
{
    invariant(id.isNormal());
    dassert(!hasFieldNames(key));
	auto& td = m_table->getMyThreadData();
	auto indexSchema = getIndexSchema();
	encodeIndexKey(*indexSchema, key, &td.m_buf);
	llong recIdx = id.repr() - 1;
	DbTable* tab = m_table->m_tab.get();
    LOG(2) << func << ": key = " << key << ", id = " << id
		<< ",  dir: " << tab->getDir().string();
	tab->indexRemove(m_indexId, td.m_buf, recIdx, &*td.m_dbCtx);
}

void TerichDbIndex::fullValidate(OperationContext* txn,
							   long long* numKeysOut,
							   ValidateResults* output) const {
	LOG(2) << BOOST_CURRENT_FUNCTION << ": just get the [numKeysOut]";
	if (numKeysOut) {
		*numKeysOut = m_table->m_tab->existingRows();
	}
}

bool TerichDbIndex::appendCustomStats(OperationContext* txn,
									BSONObjBuilder* output,
									double scale) const {
    {
        BSONObjBuilder metadata(output->subobjStart("metadata"));
    }
    return true;
}

Status TerichDbIndex::dupKeyCheck(OperationContext* txn, const BSONObj& key, const RecordId& id) {
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
	return Status(ErrorCodes::DuplicateKey, "TerichDbIndex::dupKeyCheck");
}

bool TerichDbIndex::isEmpty(OperationContext* txn) {
	DbTable* tab = m_table->m_tab.get();
    return tab->numDataRows() == 0;
}

Status TerichDbIndex::touch(OperationContext* txn) const {
    return Status(ErrorCodes::CommandNotSupported, "this storage engine does not support touch");
}

long long TerichDbIndex::getSpaceUsedBytes(OperationContext* txn) const {
	DbTable* tab = m_table->m_tab.get();
    return tab->indexStorageSize(m_indexId);
}

Status TerichDbIndex::initAsEmpty(OperationContext* txn) {
    // No-op
    return Status::OK();
}

bool
TerichDbIndex::insertIndexKey(const BSONObj& newKey, const RecordId& id, bool dupsAllowed,
							  OperationContext* txn, TableThreadData* td) {
	encodeIndexKey(*getIndexSchema(), newKey, &td->m_buf);
	DbTable* tab = m_table->m_tab.get();
	if (tab->indexInsert(m_indexId, td->m_buf, id.repr()-1, &*td->m_dbCtx)) {
		if (txn && txn->recoveryUnit()) {
			txn->recoveryUnit()->registerChange(new UnindexOnFail(this, txn, newKey, id, dupsAllowed));
		}
		return true;
	}
	return false;
}

/**
 * Implements the basic TerichDb_CURSOR functionality used by both unique and standard indexes.
 */
class TerichDbIndexCursorBase : public SortedDataInterface::Cursor, public ICleanOnOwnerDead {
	IndexIterData* getCursor() const {
		if (terark_likely(_cursor.get() != nullptr))
			return _cursor.get();
		_cursor = _idx.m_table->allocIndexIter(_idx.m_indexId, _forward);
		return _cursor.get();
	}
	void releaseCursor() {
		if (_cursor)
			_idx.m_table->releaseIndexIter(_idx.m_indexId, _forward, std::move(_cursor));
	}
public:
    TerichDbIndexCursorBase(const TerichDbIndex& idx, OperationContext* txn, bool forward)
        : _txn(txn), _idx(idx), _forward(forward), _endPositionInclude(false) {
		idx.m_table->registerCleanOnOwnerDead(this);
    }
	~TerichDbIndexCursorBase() {
		if (!m_isOwnerAlive) {
			return;
		}
		releaseCursor();
		_idx.m_table->unregisterCleanOnOwnerDead(this);
	}
	void onOwnerPrematureDeath() override final {
		_cursor = nullptr;
		m_isOwnerAlive = false;
	}
    boost::optional<IndexKeyEntry> next(RequestedInfo parts) override {
        if (_eof) { // Advance on a cursor at the end is a no-op
	        TRACE_CURSOR << "next(): _eof=true";
            return {};
		}
 		auto cur = getCursor();
        if (!_lastMoveWasRestore) {
			llong recIdx = -1;
			if (cur->increment(&recIdx)) {
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
		auto cur = getCursor();
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
//        const BSONObj finalKey = stripFieldNames(key);
//        const auto discriminator =
//           _forward == inclusive ? KeyString::kExclusiveBefore : KeyString::kExclusiveAfter;

        // By using a discriminator other than kInclusive, there is no need to distinguish
        // unique vs non-unique key formats since both start with the key.
        // _query.resetToKey(finalKey, _idx.ordering(), discriminator);
        TRACE_CURSOR << "seek3(): key=" << key.jsonString() << ", inclusive=" << inclusive;
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
        TRACE_CURSOR << "seek2(): key=" << key.jsonString();
        seekWTCursor(key, true);
        updatePosition();
		if (!_cursorAtEof)
			return curr(parts);
		else
			return {};
    }

    boost::optional<IndexKeyEntry>
	seekExact(const BSONObj& bsonKey, RequestedInfo parts) override {
        TRACE_CURSOR << "seekExact(): key=" << bsonKey.jsonString();
		auto& ttd = _idx.m_table->getMyThreadData();
		auto& ctx = *ttd.m_dbCtx;
		auto  indexSchema = _idx.getIndexSchema();
		if (_cursor) {
			_cursor->m_ctx->trySyncSegCtxSpeculativeLock(ctx.m_tab);
		}
		encodeIndexKey(*indexSchema, bsonKey, &ttd.m_buf);
		ctx.indexSearchExact(_idx.m_indexId, ttd.m_buf, &ctx.exactMatchRecIdvec);
		if (!ctx.exactMatchRecIdvec.empty()) {
			llong recIdx = ctx.exactMatchRecIdvec[0];
			return {{bsonKey, RecordId(recIdx+1)}};
		}
		return {};
	}

    void save() override {
        TRACE_CURSOR << "save()";
        try {
			if (_cursor)
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
			auto cur = getCursor();
			cur->m_qryKey.assign(cur->m_curKey);
            _lastMoveWasRestore = !seekWTCursor(true);
            TRACE_CURSOR << "restore _lastMoveWasRestore:" << _lastMoveWasRestore;
        }
    }

    void detachFromOperationContext() final {
        _txn = nullptr;
        releaseCursor();
    }

    void reattachToOperationContext(OperationContext* txn) final {
        _txn = txn;
        // _cursor recreated in restore() to avoid risk of WT_ROLLBACK issues.
		// _cursor = _idx.m_table->allocIndexIter(_idx.m_indexId, _forward);
    }

protected:
    boost::optional<IndexKeyEntry> curr(RequestedInfo parts) {
        if (_eof)
            return {};
        if (atOrPastEndPointAfterSeeking())
            return {};
        dassert(!_id.isNull());
		invariant(nullptr != _cursor);
        BSONObj bson;
        if (TRACING_ENABLED || (parts & kWantKey)) {
            bson = BSONObj(decodeIndexKey(*_idx.getIndexSchema(), _cursor->m_curKey));
            TRACE_CURSOR << "curr() returning " << bson << ' ' << _id;
        }
        return {{std::move(bson), _id}};
    }

    bool atOrPastEndPointAfterSeeking() const {
        if (_eof)
            return true;
		auto cur = getCursor();
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
		if (getCursor()->increment(&recIdx)) {
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
		encodeIndexKey(*indexSchema, bsonKey, &getCursor()->m_qryKey);
		return seekWTCursor(inclusive);
	}
    bool seekWTCursor(bool inclusive) {
		llong recIdx = -1;
		_eof = false;
        int ret;
		const char* funcName;
		auto cur = getCursor();
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
    const TerichDbIndex& _idx;  // not owned
    mutable IndexIterDataPtr  _cursor;

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
	bool m_isOwnerAlive = true;
};

/**
 * Bulk builds a unique index.
 *
 * In order to support unique indexes in dupsAllowed mode this class only does an actual insert
 * after it sees a key after the one we are trying to insert. This allows us to gather up all
 * duplicate ids and insert them all together. This is necessary since bulk cursors can only
 * append data.
 */
class TerichDbIndex::BulkBuilder : public SortedDataBuilderInterface {
public:
    BulkBuilder(TerichDbIndex* idx, OperationContext* txn, bool dupsAllowed)
        : _idx(idx), _txn(txn)
		, _dupsAllowed(dupsAllowed)
	{
		auto tst = idx->m_table.get();
		if (txn && txn->recoveryUnit()) {
			m_rud = tst->getRecoveryUnitData(txn->recoveryUnit());
			m_ttd = m_rud->m_ttd;
			assert(m_ttd.get() != NULL);
		}
		else {
			m_ttd = tst->allocTableThreadData();
		}
	}

    Status addKey(const BSONObj& newKey, const RecordId& id) override {
        {
            const Status s = checkKeySize(newKey);
            if (!s.isOK())
                return s;
        }
		if (_idx->insertIndexKey(newKey, id, _dupsAllowed, _txn, &*m_ttd)) {
	        return Status::OK();
		} else {
			return Status(ErrorCodes::DuplicateKey,
				"Dup key in TerichDbIndex::BulkBuilder");
		}
    }

    void commit(bool mayInterrupt) override {
        // TODO do we still need this?
        // this is bizarre, but required as part of the contract
        WriteUnitOfWork uow(_txn);
        uow.commit();
    }

private:
    TerichDbIndex*    const _idx;
    OperationContext* const _txn;
	RecoveryUnitDataPtr     m_rud;
	TableThreadDataPtr      m_ttd;
    const bool _dupsAllowed;
};

TerichDbIndexUnique::TerichDbIndexUnique(ThreadSafeTable* tab,
									 OperationContext* opCtx,
                                     const IndexDescriptor* desc)
    : TerichDbIndex(tab, opCtx, desc)
{}

std::unique_ptr<SortedDataInterface::Cursor>
TerichDbIndexUnique::newCursor(OperationContext* txn, bool forward) const {
    return stdx::make_unique<TerichDbIndexCursorBase>(*this, txn, forward);
}

SortedDataBuilderInterface*
TerichDbIndexUnique::getBulkBuilder(OperationContext* txn, bool dupsAllowed) {
    return new BulkBuilder(this, txn, dupsAllowed);
}

bool TerichDbIndexUnique::unique() const {
	return true;
}

// ------------------------------

TerichDbIndexStandard::TerichDbIndexStandard(ThreadSafeTable* tab,
                                         OperationContext* opCtx,
                                         const IndexDescriptor* desc)
    : TerichDbIndex(tab, opCtx, desc) {}

std::unique_ptr<SortedDataInterface::Cursor>
TerichDbIndexStandard::newCursor(OperationContext* txn, bool forward) const {
    return stdx::make_unique<TerichDbIndexCursorBase>(*this, txn, forward);
}

SortedDataBuilderInterface*
TerichDbIndexStandard::getBulkBuilder(OperationContext* txn, bool dupsAllowed) {
    // We aren't unique so dups better be allowed.
    invariant(dupsAllowed);
    return new BulkBuilder(this, txn, dupsAllowed);
}

bool TerichDbIndexStandard::unique() const {
	return false;
}

} } // namespace mongo::terichdb
