// terarkdb_record_store.cpp

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
#ifdef _MSC_VER
#pragma warning(disable: 4800) // bool conversion
#pragma warning(disable: 4244) // 'return': conversion from '__int64' to 'double', possible loss of data
#pragma warning(disable: 4267) // '=': conversion from 'size_t' to 'int', possible loss of data
#endif

#include "mongo/platform/basic.h"

#include "terarkdb_record_store.h"

#include "mongo_terarkdb_common.hpp"

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
//#include "terarkdb_recovery_unit.h"
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

class TerarkDbRecordStore::Cursor final : public SeekableRecordCursor {
public:
    Cursor(OperationContext* txn, const TerarkDbRecordStore& rs, bool forward)
        : _rs(rs),
          _txn(txn) {
	struct timespec start, end;
	clock_gettime(CLOCK_MONOTONIC, &start);
		ThreadSafeTable* tst = rs.m_table.get();
		DbTable* tab = tst->m_tab.get();
    	m_ttd = tst->allocTableThreadData();
    	if (forward)
    		_cursor = tab->createStoreIterForward(m_ttd->m_dbCtx.get());
    	else
    		_cursor = tab->createStoreIterBackward(m_ttd->m_dbCtx.get());
	clock_gettime(CLOCK_MONOTONIC, &end);
	long long timeuse = 1000000000LL * ( end.tv_sec - start.tv_sec ) + end.tv_nsec - start.tv_nsec;
	log() << "mongo_terarkdb@panda Cursor timeuse(ns) " << timeuse;
    }

	~Cursor() {
		ThreadSafeTable* tst = _rs.m_table.get();
		tst->releaseTableThreadData(m_ttd);
	}

    boost::optional<Record> next() final {
	log() << "mongo_terarkdb@panda Cursor next";
        if (_eof)
            return {};

        llong recIdx = _lastReturnedId.repr() - 1;
        if (!_skipNextAdvance) {
            if (!_cursor->increment(&recIdx, &m_ttd->m_buf)) {
                _eof = true;
                return {};
            }
			assert(!m_ttd->m_buf.empty());
        }
		else {
			assert(!m_ttd->m_buf.empty());
		}
		DbTable* tab = _rs.m_table->m_tab.get();
        SharedBuffer sbuf = m_ttd->m_coder.decode(&tab->rowSchema(), m_ttd->m_buf);
        _skipNextAdvance = false;
        const RecordId id(recIdx + 1);
        _lastReturnedId = id;
		int len = ConstDataView(sbuf.get()).read<LittleEndian<int>>();
		LOG(2) << "RecordBson: " << BSONObj(sbuf.get()).toString();
		return {{id, {sbuf, len}}};
    }

    boost::optional<Record> seekExact(const RecordId& id) final {
	struct timespec start, end;
	clock_gettime(CLOCK_MONOTONIC, &start);
        _skipNextAdvance = false;
        llong recIdx = id.repr() - 1;
        if (!_cursor->seekExact(recIdx, &m_ttd->m_buf)) {
            _eof = true;
            return {};
        }
		assert(!m_ttd->m_buf.empty());
		DbTable* tab = _rs.m_table->m_tab.get();
        SharedBuffer sbuf = m_ttd->m_coder.decode(&tab->rowSchema(), m_ttd->m_buf);
        _lastReturnedId = id;
		int len = ConstDataView(sbuf.get()).read<LittleEndian<int>>();
	clock_gettime(CLOCK_MONOTONIC, &end);
	long long timeuse = 1000000000LL * ( end.tv_sec - start.tv_sec ) + end.tv_nsec - start.tv_nsec;
	log() << "mongo_terarkdb@panda Cursor seekExact timeuse(ns) " << timeuse;
        return {{id, {sbuf, len}}};
    }

    void save() final {
	log() << "mongo_terarkdb@panda Cursor save";
        try {
        	_cursor->reset();
        } catch (const WriteConflictException&) {
            // Ignore since this is only called when we are about to kill our transaction
            // anyway.
        }
    }

    void saveUnpositioned() final {
        save();
        _eof = true;
    }

    bool restore() override final {
        _skipNextAdvance = false;

        // If we've hit EOF, then this iterator is done and need not be restored.
        if (_eof)
            return true;

        if (_lastReturnedId.isNull())
            return true;

        llong recIdx = _lastReturnedId.repr() - 1;
        if (!_cursor->seekExact(recIdx, &m_ttd->m_buf)) {
            _eof = true;
            return false;
        }

    	_skipNextAdvance = true;

        return true;  // Landed right where we left off.
    }

    void detachFromOperationContext() final {
        _txn = nullptr;
        _cursor = nullptr;
    }

    void reattachToOperationContext(OperationContext* txn) final {
        _txn = txn;
    }

private:
    const TerarkDbRecordStore& _rs;
    OperationContext* _txn;
    bool _skipNextAdvance = false;
    bool _eof = false;
	TableThreadDataPtr m_ttd;
    terark::db::StoreIteratorPtr _cursor;
    RecordId _lastReturnedId;  // If null, need to seek to first/last record.
};

StatusWith<std::string> parseOptionsField(const BSONObj options) {
    log() <<"mongo_terarkdb@panda parseOptionsField";
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

// static
StatusWith<std::string> TerarkDbRecordStore::generateCreateString(
								StringData ns,
								const CollectionOptions& options,
								StringData extraStrings) {
   log() << "mongo_terarkdb@panda generateCreateString";
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
    if (terarkDbGlobalOptions.useCollectionPrefixCompression) {
        ss << "prefix_compression,";
    }

    ss << "block_compressor=" << terarkDbGlobalOptions.collectionBlockCompressor << ",";

    ss << TerarkDbCustomizationHooks::get(getGlobalServiceContext())->getOpenConfig(ns);

    ss << extraStrings << ",";

    StatusWith<std::string> customOptions =
        parseOptionsField(options.storageEngine.getObjectField(kTerarkDbEngineName));
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
/*    Status versionStatus = TerarkDbUtil::checkApplicationMetadataFormatVersion(
        ctx, uri, kMinimumRecordStoreVersion, kMaximumRecordStoreVersion);
    if (!versionStatus.isOK()) {
        fassertFailedWithStatusNoTrace(28548, versionStatus);
    }
*/
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
    return m_table->m_tab->numDataRows();
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
	log() << "mongo_terarkdb@panda findRecord";
	if (id.isNull())
		return false;
    llong recIdx = id.repr() - 1;
	DbTable* tab = m_table->m_tab.get();
    auto& td = m_table->getMyThreadData();
    tab->getValue(recIdx, &td.m_buf, &*td.m_dbCtx);
    SharedBuffer bson = td.m_coder.decode(&tab->rowSchema(), td.m_buf);

//  size_t bufsize = sizeof(SharedBuffer::Holder) + bson.objsize();
    int bufsize = ConstDataView(bson.get()).read<LittleEndian<int>>();
    *out = RecordData(bson, bufsize);
    return true;
}

void TerarkDbRecordStore::deleteRecord(OperationContext* txn, const RecordId& id) {
    log() << "mongo_terarkdb@panda deleteRecord";
    auto& td = m_table->getMyThreadData();
    m_table->m_tab->removeRow(id.repr()-1, &*td.m_dbCtx);
}

Status TerarkDbRecordStore::insertRecords(OperationContext* txn,
										std::vector<Record>* records,
										bool enforceQuota) {
	struct timespec start, end;
	clock_gettime(CLOCK_MONOTONIC, &start);
	DbTable* tab = m_table->m_tab.get();
    auto& td = m_table->getMyThreadData();
    for (Record& rec : *records) {
    	BSONObj bson(rec.data.data());
    	td.m_coder.encode(&tab->rowSchema(), nullptr, bson, &td.m_buf);
    	rec.id = RecordId(1 + tab->insertRow(td.m_buf, &*td.m_dbCtx));
    }
    clock_gettime(CLOCK_MONOTONIC, &end);
    long long timeuse = 1000000000LL * ( end.tv_sec - start.tv_sec ) + end.tv_nsec - start.tv_nsec;
    log() << "mongo_terarkdb@panda insertRecords records timeuse(ns) " << timeuse;
    return Status::OK();
}

StatusWith<RecordId> TerarkDbRecordStore::insertRecord(OperationContext* txn,
													 const char* data,
													 int len,
													 bool enforceQuota) {
   log() << "mongo_terarkdb@panda insertRecord data";
	DbTable* tab = m_table->m_tab.get();
    auto& td = m_table->getMyThreadData();
    BSONObj bson(data);
	invariant(bson.objsize() == len);
    td.m_coder.encode(&tab->rowSchema(), nullptr, bson, &td.m_buf);
    llong recIdx = tab->insertRow(td.m_buf, &*td.m_dbCtx);
	return {RecordId(recIdx + 1)};
}

StatusWith<RecordId> TerarkDbRecordStore::insertRecord(OperationContext* txn,
													 const DocWriter* doc,
													 bool enforceQuota) {
    log() << "mongo_terarkdb@panda insertRecord doc";
    const int len = doc->documentSize();

    std::unique_ptr<char[]> buf(new char[len]);
    doc->writeDocument(buf.get());

    return insertRecord(txn, buf.get(), len, enforceQuota);
}

Status
TerarkDbRecordStore::updateRecord(OperationContext* txn,
								const RecordId& id,
								const char* data,
								int len,
								bool enforceQuota,
								UpdateNotifier* notifier) {
        log() << "mongo_terarkdb@panda updateRecord data";
	DbTable* tab = m_table->m_tab.get();
	terark::db::IncrementGuard_size_t incrGuard(tab->m_inprogressWritingCount);
	llong recId = id.repr() - 1;
	{
		terark::db::MyRwLock lock(tab->m_rwMutex, false);
		size_t segIdx = tab->getSegmentIndexOfRecordIdNoLock(recId);
		if (segIdx >= tab->getSegNum()) {
			return {ErrorCodes::InvalidIdField, "record id is out of range"};
		}
		auto seg = tab->getSegmentPtr(segIdx);
		if (seg->m_isFreezed) {
			return {ErrorCodes::NeedsDocumentMove, "segment of record is frozen"};
		}
	}
    auto& td = m_table->getMyThreadData();
    BSONObj bson(data);
    td.m_coder.encode(&tab->rowSchema(), nullptr, bson, &td.m_buf);
	llong newRecId = tab->updateRow(recId, td.m_buf, &*td.m_dbCtx);
	invariant(newRecId == recId);
	return Status::OK();
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

std::unique_ptr<SeekableRecordCursor> TerarkDbRecordStore::getCursor(OperationContext* txn,
                                                                       bool forward) const {
    return stdx::make_unique<Cursor>(txn, *this, forward);
}

std::unique_ptr<RecordCursor> TerarkDbRecordStore::getRandomCursor(OperationContext* txn) const {
    return nullptr;
}

std::vector<std::unique_ptr<RecordCursor>>
TerarkDbRecordStore::getManyCursors(OperationContext* txn) const {
    log() << "mongo_terarkdb@panda getManyCursors";
    std::vector<std::unique_ptr<RecordCursor>> cursors(1);
    cursors[0] = stdx::make_unique<Cursor>(txn, *this, /*forward=*/true);
    return cursors;
}

Status TerarkDbRecordStore::truncate(OperationContext* txn) {
	DbTable* tab = m_table->m_tab.get();
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
                                   bool full,
                                   bool scanData,
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
#if 0
    if (true/*_isEphemeral*/) {
        // Everything is already in memory.
        return Status::OK();
    }
    return Status(ErrorCodes::CommandNotSupported, "this storage engine does not support touch");
#endif
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
