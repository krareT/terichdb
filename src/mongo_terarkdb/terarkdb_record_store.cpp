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

#include "terarkdb_record_store.h"
#include "mongo/base/checked_cast.h"
#include "mongo/bson/util/builder.h"
#include "mongo/db/concurrency/locker.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/oplog_hack.h"
#include "terarkdb_customization_hooks.h"
#include "terarkdb_global_options.h"
//#include "terarkdb_kv_engine.h"
//#include "terarkdb_record_store_oplog_stones.h"
#include "terarkdb_recovery_unit.h"
//#include "terarkdb_session_cache.h"
#include "terarkdb_size_storer.h"
//#include "terarkdb_util.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/time_support.h"
#include <boost/none.hpp>

//#define RS_ITERATOR_TRACE(x) log() << "TerarkDbRS::Iterator " << x
#define RS_ITERATOR_TRACE(x)

namespace mongo { namespace terarkdb {

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

}  // namespace

//MONGO_FP_DECLARE(TerarkDbWriteConflictException);

class TerarkDbRecordStore::Cursor final : public SeekableRecordCursor, public ICleanOnOwnerDead {
public:
    Cursor(OperationContext* txn, const TerarkDbRecordStore& rs, bool forward)
        : _rs(rs),
          _txn(txn), _forward(forward) {
		LOG(1) << "TerarkDbRecordStore::Cursor::Cursor(): forward = " << forward;
		init(txn);
		rs.m_table->registerCleanOnOwnerDead(this);
    }

	void init(OperationContext* txn) {
		ThreadSafeTable* tst = _rs.m_table.get();
		DbTable* tab = tst->m_tab.get();
		if (txn && txn->recoveryUnit()) {
			auto iter = tst->createStoreIter(txn->recoveryUnit(), _forward);
			_cursor = iter;
			m_ttd = iter->m_rud->m_ttd;
			m_hasRecoveryUnit = true;
		}
		else {
			m_hasRecoveryUnit = false;
	    	m_ttd = tst->allocTableThreadData();
    		if (_forward)
    			_cursor = tab->createStoreIterForward(m_ttd->m_dbCtx.get());
    		else
    			_cursor = tab->createStoreIterBackward(m_ttd->m_dbCtx.get());
		}
	}

	~Cursor() {
		LOG(1) << "TerarkDbRecordStore::Cursor::~Cursor(): forward = " << _forward
			<< ", m_isOwnerAlive = " << m_isOwnerAlive
			<< ", m_hasRecoveryUnit = " << m_hasRecoveryUnit;
		if (!m_isOwnerAlive) {
			return;
		}
		ThreadSafeTable* tst = _rs.m_table.get();
		if (!m_hasRecoveryUnit) {
			tst->releaseTableThreadData(m_ttd);
		}
		m_ttd = nullptr;
		_cursor = nullptr;
		tst->unregisterCleanOnOwnerDead(this);
	}

	void onOwnerPrematureDeath() override final {
		ThreadSafeTable* tst = _rs.m_table.get();
		if (!m_hasRecoveryUnit) {
			tst->releaseTableThreadData(m_ttd);
		}
		m_ttd = nullptr;
		_cursor = nullptr;
		m_isOwnerAlive = false;
	}

    boost::optional<Record> next() final {
        if (_eof)
            return {};

        llong recIdx = _lastReturnedId.repr() - 1;
        if (!_skipNextAdvance) {
            if (!_cursor->increment(&recIdx, &m_ttd->m_buf)) {
                _eof = true;
                return {};
            }
			if (_forward && _lastReturnedId.repr() >= recIdx+1) {
				LOG(1) << "TerarkDbRecordStore::Cursor::next -- c->next_key ( " << RecordId(recIdx+1)
					   << ") was not greater than _lastReturnedId (" << _lastReturnedId
					   << ") which is a bug, _cursor class: " << demangleName(typeid(*_cursor));
				// Force a retry of the operation from our last known position by acting as-if
				// we received a WT_ROLLBACK error.
				assert(!m_ttd->m_buf.empty());
				throw WriteConflictException();
			}
			assert(!m_ttd->m_buf.empty());
        }
		else {
			assert(!m_ttd->m_buf.empty());
		}
		DbTable* tab = _rs.m_table->m_tab.get();
        SharedBuffer sbuf = m_ttd->m_coder.decode(&tab->rowSchema(), m_ttd->m_buf);
        const RecordId id(recIdx + 1);
		int len = ConstDataView(sbuf.get()).read<LittleEndian<int>>();
		LOG(1) << "TerarkDbRecordStore::Cursor::next(): _skipNextAdvance = " << _skipNextAdvance
			   << ", RecordBson(" << id << "): "
			   << BSONObj(sbuf.get()).toString() << ", _lastReturnedId = " << _lastReturnedId
			   << ", _cursor class: " << demangleName(typeid(*_cursor))
			;
        _skipNextAdvance = false;
        _lastReturnedId = id;
		return {{id, {sbuf, len}}};
    }

    /**
     * Seeks to a Record with the provided id.
     *
     * If an exact match can't be found, boost::none will be returned and the resulting position
     * of the cursor is unspecified.
     */
	// if successed, the cursor should point to the next record
    boost::optional<Record> seekExact(const RecordId& id) final {
		LOG(1) << "TerarkDbRecordStore::Cursor::seekExact(): id = " << id
			<< ", _skipNextAdvance = " << _skipNextAdvance
			<< ", _eof = " << _eof << ", _lastReturnedId = " << _lastReturnedId;
        _skipNextAdvance = false;
		DbTable& tab = *_rs.m_table->m_tab;
        llong recIdx = id.repr() - 1;
		assert(recIdx >= 0);
		if (recIdx < 0) {
			return boost::none;
		}
		if (recIdx >= tab.numDataRows()) {
			_eof = true;
			return boost::none;
		}
		auto& ttd = *m_ttd;
		if (!_cursor->seekExact(recIdx, &ttd.m_buf)) {
			return boost::none;
		}
		assert(!ttd.m_buf.empty());
        SharedBuffer sbuf = ttd.m_coder.decode(&tab.rowSchema(), ttd.m_buf);
		int len = ConstDataView(sbuf.get()).read<LittleEndian<int>>();
        _lastReturnedId = id;
        _eof = false;
        return {{id, {sbuf, len}}};
    }

    void save() final {
		LOG(1) << "TerarkDbRecordStore::Cursor::save(): _skipNextAdvance = " << _skipNextAdvance
			<< ", _eof = " << _eof << ", _lastReturnedId = " << _lastReturnedId;
		do_save();
	}

	void do_save() {
        try {
        	_cursor->reset();
        } catch (const WriteConflictException&) {
            // Ignore since this is only called when we are about to kill our transaction
            // anyway.
        }
    }

    void saveUnpositioned() final {
		LOG(1) << "TerarkDbRecordStore::Cursor::saveUnpositioned(): _skipNextAdvance = " << _skipNextAdvance
			<< ", _eof = " << _eof << ", _lastReturnedId = " << _lastReturnedId;
        do_save();
		_lastReturnedId = RecordId();
    }

    bool restore() override final {
        // If we've hit EOF, then this iterator is done and need not be restored.
        if (_eof) {
			LOG(1) << "TerarkDbRecordStore::Cursor::restore(): _skipNextAdvance = " << _skipNextAdvance
				<< ", _eof = " << _eof << ", _lastReturnedId = " << _lastReturnedId;
	        _skipNextAdvance = false;
            return true;
		}
        if (_lastReturnedId.isNull()) {
			LOG(1) << "TerarkDbRecordStore::Cursor::restore(): _skipNextAdvance = " << _skipNextAdvance
				<< ", _eof = " << _eof << ", _lastReturnedId = " << _lastReturnedId;
	        _skipNextAdvance = false;
            return true;
		}
        llong recIdx = _lastReturnedId.repr() - 1;
		llong recIdx2 = _cursor->seekLowerBound(recIdx, &m_ttd->m_buf);
		LOG(1) << "TerarkDbRecordStore::Cursor::restore(): _skipNextAdvance = " << _skipNextAdvance
			<< ", _eof = " << _eof << ", _lastReturnedId = " << _lastReturnedId
			<< ", next = " << RecordId(recIdx2 + 1)
			<< ", _cursor class: " << demangleName(typeid(*_cursor));
        if (recIdx2 < 0) {
			_skipNextAdvance = false;
            _eof = true;
            return false;
        }
		invariant(!m_ttd->m_buf.empty());
		if (recIdx2 > recIdx) {
			if (_forward) {
				_lastReturnedId = RecordId(recIdx2 + 1);
				_skipNextAdvance = true;
			}
		}
		else if (recIdx2 < recIdx) {
			if (!_forward) {
				_lastReturnedId = RecordId(recIdx2 + 1);
				_skipNextAdvance = true;
			}
		}
        return true;
    }

    void detachFromOperationContext() final {
		LOG(1) << "TerarkDbRecordStore::Cursor::detachFromOperationContext(): _skipNextAdvance = " << _skipNextAdvance
			<< ", _eof = " << _eof << ", _lastReturnedId = " << _lastReturnedId;
		if (m_ttd && !m_hasRecoveryUnit) {
			_rs.m_table->releaseTableThreadData(m_ttd);
		}
		m_ttd = nullptr;
        _txn = nullptr;
		_cursor = nullptr;
    }

    void reattachToOperationContext(OperationContext* txn) final {
		LOG(1) << "TerarkDbRecordStore::Cursor::reattachToOperationContext(): _skipNextAdvance = " << _skipNextAdvance
			<< ", _eof = " << _eof << ", _lastReturnedId = " << _lastReturnedId;
        _txn = txn;
		init(txn);
    }

private:
    const TerarkDbRecordStore& _rs;
    OperationContext* _txn;
    bool _skipNextAdvance = false;
    bool _eof = false;
	bool m_hasRecoveryUnit = false;
	bool m_isOwnerAlive = true;
	const bool _forward;
	TableThreadDataPtr m_ttd;
    terark::db::StoreIteratorPtr _cursor;
    RecordId _lastReturnedId;  // If null, need to seek to first/last record.
};

StatusWith<std::string> parseOptionsField(const BSONObj options) {
    StringBuilder ss;
    BSONForEach(elem, options) {
        if (elem.fieldNameStringData() == "configString") {
        /*    Status status = TerarkDbUtil::checkTableCreationOptions(elem);
            if (!status.isOK()) {
                return status;
            }*/
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

TerarkDbRecordStore::TerarkDbRecordStore(OperationContext* ctx,
									 StringData ns,
									 StringData ident,
									 ThreadSafeTable* tab,
									 TerarkDbSizeStorer* sizeStorer)
		: RecordStore(ns),
		  m_table(tab),
		  _ident(ident.toString()),
		  _shuttingDown(false)
{
}

TerarkDbRecordStore::~TerarkDbRecordStore() {
    _shuttingDown = true;
	DbTable* tab = m_table->m_tab.get();
	tab->flush();
    LOG(1) << BOOST_CURRENT_FUNCTION << ": namespace: " << ns() << ", dir: " << tab->getDir().string();
}

const char* TerarkDbRecordStore::name() const {
    return kTerarkDbEngineName.c_str();
}

bool TerarkDbRecordStore::inShutdown() const {
    return _shuttingDown;
}

long long TerarkDbRecordStore::dataSize(OperationContext* txn) const {
    return m_table->m_tab->dataStorageSize();
}

long long TerarkDbRecordStore::numRecords(OperationContext* txn) const {
	auto tab = m_table->m_tab.get();
	long long existing = tab->existingRows();
	long long total = tab->inlineGetRowNum();
    LOG(1) << "TerarkDbRecordStore::numRecords(): existing = " << existing
		<< ", total" << total << ", dir: " << tab->getDir().string();
    return existing;
}

bool TerarkDbRecordStore::isCapped() const {
    return false;
}

int64_t TerarkDbRecordStore::storageSize(OperationContext* txn,
									   BSONObjBuilder* extraInfo,
									   int infoLevel) const {
	return m_table->m_tab->dataStorageSize();
}

RecordData
TerarkDbRecordStore::dataFor(OperationContext* txn, const RecordId& id) const {
	return RecordStore::dataFor(txn, id);
}

bool TerarkDbRecordStore::findRecord(OperationContext* txn,
								   const RecordId& id,
								   RecordData* out) const {
	DbTable* tab = m_table->m_tab.get();
	if (id.isNull()) {
		LOG(2) << "TerarkDbRecordStore::findRecord(): id = null, dir: " << tab->getDir().string();
		return false;
	}
    llong recIdx = id.repr() - 1;
	RecoveryUnitDataPtr rud = NULL;
	TableThreadData* ttd = NULL;
	if (txn && txn->recoveryUnit()) {
		rud = m_table->tryRecoveryUnitData(txn->recoveryUnit());
		if (rud) {
			ttd = rud->m_ttd.get();
			assert(NULL != ttd);
		}
	}
	if (!ttd) {
		rud = NULL;
		ttd = &m_table->getMyThreadData();
	}
    tab->getValue(recIdx, &ttd->m_buf, ttd->m_dbCtx.get());
    SharedBuffer bson = ttd->m_coder.decode(&tab->rowSchema(), ttd->m_buf);
	LOG(2) << "TerarkDbRecordStore::findRecord(): id = " << id
		<< ", rud = " << (void*)rud.get()
		<< ", bson = " << BSONObj(bson.get())
		<< ", dir: " << tab->getDir().string();

    int bufsize = ConstDataView(bson.get()).read<LittleEndian<int>>();
    *out = RecordData(bson, bufsize);
    return true;
}

void TerarkDbRecordStore::deleteRecord(OperationContext* txn, const RecordId& id) {
    auto& td = m_table->getMyThreadData();
	auto tab = m_table->m_tab.get();
	if (txn && txn->recoveryUnit()) {
		LOG(2) << "TerarkDbRecordStore::deleteRecord(): id = " << id
			<< ", dir: " << tab->getDir().string() << ", to registerDelete()";
		m_table->registerDelete(txn->recoveryUnit(), id);
	}
	else {
		bool ok = tab->removeRow(id.repr()-1, &*td.m_dbCtx);
		LOG(2) << "TerarkDbRecordStore::deleteRecord(): id = " << id
			<< ", dir: " << tab->getDir().string() << ", return = " << ok;
	}
}

Status TerarkDbRecordStore::insertRecords(OperationContext* txn,
										std::vector<Record>* records,
										bool enforceQuota) {
	DbTable* tab = m_table->m_tab.get();
    auto& td = m_table->getMyThreadData();
	if (0 == records->size()) {
	    LOG(1) << "TerarkDbRecordStore::insertRecords(): records->size() = 0";
	}
    for (size_t i = 0; i < records->size(); ++i) {
		Record& rec = (*records)[i];
    	BSONObj bson(rec.data.data());
		try {
			td.m_coder.encode(&tab->rowSchema(), nullptr, bson, &td.m_buf);
		} catch (const std::exception& ex) {
			return Status(ErrorCodes::InvalidBSON, ex.what());
		}
		try {
			rec.id = RecordId(1 + tab->insertRow(td.m_buf, &*td.m_dbCtx));
		} catch (const std::exception& ex) {
			return Status(ErrorCodes::InternalError, ex.what());
		}
		if (txn && txn->recoveryUnit()) {
			m_table->registerInsert(txn->recoveryUnit(), rec.id);
		}
	    LOG(2) << "TerarkDbRecordStore::insertRecords(): i = " << i
			<< ", bson = " << bson.toString() << ", id = " << rec.id;
    }
    return Status::OK();
}

StatusWith<RecordId> TerarkDbRecordStore::insertRecord(OperationContext* txn,
													 const char* data,
													 int len,
													 bool enforceQuota) {
	DbTable* tab = m_table->m_tab.get();
    auto& td = m_table->getMyThreadData();
    BSONObj bson(data);
	invariant(bson.objsize() == len);
    LOG(2) << "TerarkDbRecordStore::insertRecord(): bson = " << bson.toString();
    try {
		td.m_coder.encode(&tab->rowSchema(), nullptr, bson, &td.m_buf);
	} catch (const std::exception& ex) {
		return Status(ErrorCodes::InvalidBSON, ex.what());
	}
	try {
		llong recIdx = tab->insertRow(td.m_buf, &*td.m_dbCtx);
		if (txn && txn->recoveryUnit()) {
			m_table->registerInsert(txn->recoveryUnit(), RecordId(recIdx + 1));
		}
		return {RecordId(recIdx + 1)};
	} catch (const std::exception& ex) {
		return Status(ErrorCodes::InternalError, ex.what());
	}
}

Status
TerarkDbRecordStore::insertRecordsWithDocWriter(OperationContext* txn,
                                            const DocWriter* const* docs,
                                            size_t nDocs,
                                            RecordId* idsOut) {
	if (0 == nDocs) {
	    LOG(1) << "TerarkDbRecordStore::insertRecordsWithDocWriter(): nDocs = 0";
		return Status::OK();
	}
	DbTable* tab = m_table->m_tab.get();
    auto& td = m_table->getMyThreadData();
    std::unique_ptr<Record[]> records(new Record[nDocs]);

    // First get all the sizes so we can allocate a single buffer for all documents. Eventually it
    // would be nice if we could either hand off the buffers to WT without copying or write them
    // in-place as we do with MMAPv1, but for now this is the best we can do.
    size_t totalSize = 0;
    for (size_t i = 0; i < nDocs; i++) {
        const size_t docSize = docs[i]->documentSize();
        records[i].data = RecordData(nullptr, docSize);  // We fill in the real ptr in next loop.
        totalSize += docSize;
    }

    std::unique_ptr<char[]> buffer(new char[totalSize]);
    char* pos = buffer.get();
    for (size_t i = 0; i < nDocs; i++) {
        docs[i]->writeDocument(pos);
        const size_t size = records[i].data.size();
        records[i].data = RecordData(pos, size);
        pos += size;
    }
    invariant(pos == (buffer.get() + totalSize));
	terark::db::BatchWriter batch(tab, td.m_dbCtx.get());
    for (size_t i = 0; i < nDocs; i++) {
        docs[i]->writeDocument(pos);
        BSONObj bson = records[i].data.releaseToBson();
		try {
			td.m_coder.encode(&tab->rowSchema(), nullptr, bson, &td.m_buf);
		} catch (const std::exception& ex) {
			return Status(ErrorCodes::InvalidBSON, ex.what());
		}
		try {
			llong recIdx = batch.upsertRow(td.m_buf);
			records[i].id = RecordId(recIdx + 1);
			if (txn && txn->recoveryUnit()) {
				m_table->registerInsert(txn->recoveryUnit(), RecordId(recIdx + 1));
			}
			LOG(2) << "TerarkDbRecordStore::insertRecordsWithDocWriter(): i = " << i
				<< ", id = " << RecordId(recIdx + 1) << ", bson = " << bson.toString();
		} catch (const std::exception& ex) {
			return Status(ErrorCodes::InternalError, ex.what());
		}
    }
	if (!batch.commit()) {
		return Status(ErrorCodes::OperationFailed, "TerarkDbRecordStore::insertRecordsWithDocWriter: terark::db::BatchWriter::commit failed");
	}

    if (idsOut) {
        for (size_t i = 0; i < nDocs; i++) {
            idsOut[i] = records[i].id;
        }
    }

    return Status::OK();
}

Status
TerarkDbRecordStore::updateRecord(OperationContext* txn,
								const RecordId& id,
								const char* data,
								int len,
								bool enforceQuota,
								UpdateNotifier* notifier) {
	DbTable* tab = m_table->m_tab.get();
	terark::db::IncrementGuard_size_t incrGuard(tab->m_inprogressWritingCount);
	invariant(id.repr() != 0);
	llong recId = id.repr() - 1;
    BSONObj bson(data);
	{
		terark::db::MyRwLock lock(tab->m_rwMutex, false);
		size_t segIdx = tab->getSegmentIndexOfRecordIdNoLock(recId);
		if (segIdx >= tab->getSegNum()) {
			LOG(2) << "TerarkDbRecordStore::updateRecord(): bson = " << bson.toString()
				<< ", id = " << id << " is out of range";
			return {ErrorCodes::InvalidIdField, "record id is out of range"};
		}
		auto seg = tab->getSegmentPtr(segIdx);
		if (seg->m_isFreezed) {
			LOG(2) << "TerarkDbRecordStore::updateRecord(): bson = " << bson.toString()
				<< ", id = " << id << ", NeedsDocumentMove because segment of record is frozen";
			return {ErrorCodes::NeedsDocumentMove, "segment of record is frozen"};
		}
		if (txn && txn->recoveryUnit()) {
			LOG(2) << "TerarkDbRecordStore::updateRecord(): bson = " << bson.toString()
				<< ", id = " << id << ", NeedsDocumentMove because is in recovery unit";
			return {ErrorCodes::NeedsDocumentMove, "TerarkDB is in recovery unit, needs move"};
		}
	}
	LOG(2) << "TerarkDbRecordStore::updateRecord(): bson = " << bson.toString()
		<< ", id = " << id << ", do inplace update";
    auto& td = m_table->getMyThreadData();
	try {
	    td.m_coder.encode(&tab->rowSchema(), nullptr, bson, &td.m_buf);
	} catch (const std::exception& ex) {
		return Status(ErrorCodes::InvalidBSON, ex.what());
	}
	try {
		llong newRecId = tab->updateRow(recId, td.m_buf, &*td.m_dbCtx);
		invariant(newRecId == recId);
		return Status::OK();
	} catch (const std::exception& ex) {
		return Status(ErrorCodes::InternalError, ex.what());
	}
}

bool TerarkDbRecordStore::updateWithDamagesSupported() const {
    return false;
}

StatusWith<RecordData> TerarkDbRecordStore::updateWithDamages(
							OperationContext* txn,
							const RecordId& id,
							const RecordData& oldRec,
							const char* damageSource,
							const mutablebson::DamageVector& damages)
{
    MONGO_UNREACHABLE;
}

std::unique_ptr<SeekableRecordCursor>
TerarkDbRecordStore::getCursor(OperationContext* txn, bool forward) const {
    return stdx::make_unique<Cursor>(txn, *this, forward);
}

std::unique_ptr<RecordCursor>
TerarkDbRecordStore::getRandomCursor(OperationContext* txn) const {
    return nullptr;
}

std::vector<std::unique_ptr<RecordCursor>>
TerarkDbRecordStore::getManyCursors(OperationContext* txn) const {
    std::vector<std::unique_ptr<RecordCursor>> cursors(1);
    cursors[0] = stdx::make_unique<Cursor>(txn, *this, /*forward=*/true);
    return cursors;
}

Status TerarkDbRecordStore::truncate(OperationContext* txn) {
	DbTable* tab = m_table->m_tab.get();
	LOG(2) << "TerarkDbRecordStore::truncate()";
	tab->clear();
    return Status::OK();
}

Status TerarkDbRecordStore::compact(OperationContext* txn,
								  RecordStoreCompactAdaptor* adaptor,
								  const CompactOptions* options,
								  CompactStats* stats) {
	DbTable* tab = m_table->m_tab.get();
	tab->compact(); // will wait for compact complete
    return Status::OK();
}

Status TerarkDbRecordStore::validate(OperationContext* txn,
								   ValidateCmdLevel level,
                                   ValidateAdaptor* adaptor,
                                   ValidateResults* results,
                                   BSONObjBuilder* output) {
	DbTable* tab = m_table->m_tab.get();
    output->appendNumber("nrecords", tab->numDataRows());
    return Status::OK();
}

void TerarkDbRecordStore::appendCustomStats(OperationContext* txn,
										  BSONObjBuilder* result,
										  double scale) const {
    result->appendBool("capped", false);
}

Status TerarkDbRecordStore::touch(OperationContext* txn, BSONObjBuilder* output) const {
    return Status::OK();
}

void TerarkDbRecordStore::updateStatsAfterRepair(OperationContext* txn,
											   long long numRecords,
											   long long dataSize) {
	LOG(2) << BOOST_CURRENT_FUNCTION << ": is in TODO list, not implemented now";
}

void TerarkDbRecordStore::temp_cappedTruncateAfter(OperationContext* txn,
												 RecordId end,
												 bool inclusive) {
	LOG(2) << BOOST_CURRENT_FUNCTION << ": is in TODO list, not implemented now";
}


} } // namespace mongo::terarkdb
