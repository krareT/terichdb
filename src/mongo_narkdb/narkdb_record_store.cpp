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
#ifdef _MSC_VER
#pragma warning(disable: 4800) // bool conversion
#pragma warning(disable: 4244) // 'return': conversion from '__int64' to 'double', possible loss of data
#pragma warning(disable: 4267) // '=': conversion from 'size_t' to 'int', possible loss of data
#endif

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
//#include "narkdb_kv_engine.h"
//#include "narkdb_record_store_oplog_stones.h"
//#include "narkdb_recovery_unit.h"
//#include "narkdb_session_cache.h"
#include "narkdb_size_storer.h"
//#include "narkdb_util.h"
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

}  // namespace

//MONGO_FP_DECLARE(NarkDbWriteConflictException);

class NarkDbRecordStore::Cursor final : public SeekableRecordCursor {
public:
    Cursor(OperationContext* txn, const NarkDbRecordStore& rs, bool forward = true)
        : _rs(rs),
          _txn(txn) {
    	m_ctx = rs.m_table->createDbContext();
    	if (forward)
    		_cursor = rs.m_table->createStoreIterForward(m_ctx.get());
    	else
    		_cursor = rs.m_table->createStoreIterBackward(m_ctx.get());
    }

    boost::optional<Record> next() final {
        if (_eof)
            return {};

        llong recIdx = _lastReturnedId.repr() - 1;
        if (!_skipNextAdvance) {
            if (!_cursor->increment(&recIdx, &m_recBuf)) {
                _eof = true;
                return {};
            }
			assert(!m_recBuf.empty());
        }
		else {
			assert(!m_recBuf.empty());
		}
        SharedBuffer sbuf = m_coder.decode(&_rs.m_table->rowSchema(), m_recBuf);
        _skipNextAdvance = false;
        const RecordId id(recIdx + 1);
        _lastReturnedId = id;
		int len = ConstDataView(sbuf.get()).read<LittleEndian<int>>();
        return {{id, {sbuf, len}}};
    }

    boost::optional<Record> seekExact(const RecordId& id) final {
        _skipNextAdvance = false;
        llong recIdx = id.repr() - 1;
        if (!_cursor->seekExact(recIdx, &m_recBuf)) {
            _eof = true;
            return {};
        }
		assert(!m_recBuf.empty());
        SharedBuffer sbuf = m_coder.decode(&_rs.m_table->rowSchema(), m_recBuf);
        _lastReturnedId = id;
		int len = ConstDataView(sbuf.get()).read<LittleEndian<int>>();
        return {{id, {sbuf, len}}};
    }

    void save() final {
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
        if (!_cursor->seekExact(recIdx, &m_recBuf)) {
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
    const NarkDbRecordStore& _rs;
    OperationContext* _txn;
    bool _skipNextAdvance = false;
    bool _eof = false;
	SchemaRecordCoder m_coder;
    nark::db::DbContextPtr m_ctx;
    nark::db::StoreIteratorPtr _cursor;
    nark::valvec<unsigned char> m_recBuf;
    RecordId _lastReturnedId;  // If null, need to seek to first/last record.
};

StatusWith<std::string> parseOptionsField(const BSONObj options) {
    StringBuilder ss;
    BSONForEach(elem, options) {
        if (elem.fieldNameStringData() == "configString") {
        /*    Status status = NarkDbUtil::checkTableCreationOptions(elem);
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
StatusWith<std::string> NarkDbRecordStore::generateCreateString(
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
        parseOptionsField(options.storageEngine.getObjectField(kNarkDbEngineName));
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
									 StringData ident,
									 CompositeTable* tab,
									 NarkDbSizeStorer* sizeStorer)
		: RecordStore(ns),
		  m_table(tab),
		  _ident(ident.toString()),
		  _shuttingDown(false)
{
/*    Status versionStatus = NarkDbUtil::checkApplicationMetadataFormatVersion(
        ctx, uri, kMinimumRecordStoreVersion, kMaximumRecordStoreVersion);
    if (!versionStatus.isOK()) {
        fassertFailedWithStatusNoTrace(28548, versionStatus);
    }
*/
}

NarkDbRecordStore::~NarkDbRecordStore() {
    _shuttingDown = true;
	m_table->flush();
    LOG(1) << "~NarkDbRecordStore for: " << ns();
}

const char* NarkDbRecordStore::name() const {
    return kNarkDbEngineName.c_str();
}

bool NarkDbRecordStore::inShutdown() const {
    return _shuttingDown;
}

long long NarkDbRecordStore::dataSize(OperationContext* txn) const {
    return m_table->dataStorageSize();
}

long long NarkDbRecordStore::numRecords(OperationContext* txn) const {
    return m_table->numDataRows();
}

bool NarkDbRecordStore::isCapped() const {
    return false;
}

int64_t NarkDbRecordStore::storageSize(OperationContext* txn,
									   BSONObjBuilder* extraInfo,
									   int infoLevel) const {
	return m_table->dataStorageSize();
}

NarkDbRecordStore::MyThreadData& NarkDbRecordStore::getMyThreadData() const {
//	nark::db::MyRwLock lock(m_threadcacheMutex, false);
	std::lock_guard<std::mutex> lock(m_threadcacheMutex);
	MyThreadData*& td = m_threadcache.get_map()[std::this_thread::get_id()];
	if (td == nullptr) {
		td = new MyThreadData();
		td->m_dbCtx = m_table->createDbContext();
		td->m_dbCtx->syncIndex = false;
	}
	return *td;
}

RecordData
NarkDbRecordStore::dataFor(OperationContext* txn, const RecordId& id) const {
	return RecordStore::dataFor(txn, id);
}

bool NarkDbRecordStore::findRecord(OperationContext* txn,
								   const RecordId& id,
								   RecordData* out) const {
	if (id.isNull())
		return false;
    llong recIdx = id.repr() - 1;
    auto& td = getMyThreadData();
    m_table->getValue(recIdx, &td.m_recData, &*td.m_dbCtx);
    SharedBuffer bson = td.m_coder.decode(&m_table->rowSchema(), td.m_recData);

//  size_t bufsize = sizeof(SharedBuffer::Holder) + bson.objsize();
    int bufsize = ConstDataView(bson.get()).read<LittleEndian<int>>();
    *out = RecordData(bson, bufsize);
    return true;
}

void NarkDbRecordStore::deleteRecord(OperationContext* txn, const RecordId& id) {
    auto& td = getMyThreadData();
    m_table->removeRow(id.repr()-1, &*td.m_dbCtx);
}

Status NarkDbRecordStore::insertRecords(OperationContext* txn,
										std::vector<Record>* records,
										bool enforceQuota) {
    auto& td = getMyThreadData();
    for (Record& rec : *records) {
    	BSONObj bson(rec.data.data());
    	td.m_coder.encode(&m_table->rowSchema(), nullptr, bson, &td.m_recData);
    	rec.id = RecordId(1 + m_table->insertRow(td.m_recData, &*td.m_dbCtx));
    }
    return Status::OK();
}

StatusWith<RecordId> NarkDbRecordStore::insertRecord(OperationContext* txn,
													 const char* data,
													 int len,
													 bool enforceQuota) {
    auto& td = getMyThreadData();
    BSONObj bson(data);
	invariant(bson.objsize() == len);
    td.m_coder.encode(&m_table->rowSchema(), nullptr, bson, &td.m_recData);
    llong recIdx = m_table->insertRow(td.m_recData, &*td.m_dbCtx);
	return {RecordId(recIdx + 1)};
}

StatusWith<RecordId> NarkDbRecordStore::insertRecord(OperationContext* txn,
													 const DocWriter* doc,
													 bool enforceQuota) {
    const int len = doc->documentSize();

    std::unique_ptr<char[]> buf(new char[len]);
    doc->writeDocument(buf.get());

    return insertRecord(txn, buf.get(), len, enforceQuota);
}

StatusWith<RecordId>
NarkDbRecordStore::updateRecord(OperationContext* txn,
								const RecordId& id,
								const char* data,
								int len,
								bool enforceQuota,
								UpdateNotifier* notifier) {
    auto& td = getMyThreadData();
    BSONObj bson(data);
    td.m_coder.encode(&m_table->rowSchema(), nullptr, bson, &td.m_recData);
	llong newIdx = m_table->replaceRow(id.repr()-1, td.m_recData, &*td.m_dbCtx);
	return StatusWith<RecordId>(RecordId(newIdx + 1));
}

bool NarkDbRecordStore::updateWithDamagesSupported() const {
    return false;
}

StatusWith<RecordData> NarkDbRecordStore::updateWithDamages(
							OperationContext* txn,
							const RecordId& id,
							const RecordData& oldRec,
							const char* damageSource,
							const mutablebson::DamageVector& damages)
{
    MONGO_UNREACHABLE;
}

std::unique_ptr<SeekableRecordCursor> NarkDbRecordStore::getCursor(OperationContext* txn,
                                                                       bool forward) const {
    return stdx::make_unique<Cursor>(txn, *this, forward);
}

std::unique_ptr<RecordCursor> NarkDbRecordStore::getRandomCursor(OperationContext* txn) const {
    return nullptr;
}

std::vector<std::unique_ptr<RecordCursor>>
NarkDbRecordStore::getManyCursors(OperationContext* txn) const {
    std::vector<std::unique_ptr<RecordCursor>> cursors(1);
    cursors[0] = stdx::make_unique<Cursor>(txn, *this, /*forward=*/true);
    return cursors;
}

Status NarkDbRecordStore::truncate(OperationContext* txn) {
	m_table->clear();
    return Status::OK();
}

Status NarkDbRecordStore::compact(OperationContext* txn,
								  RecordStoreCompactAdaptor* adaptor,
								  const CompactOptions* options,
								  CompactStats* stats) {
	m_table->syncFinishWriting();
    return Status::OK();
}

Status NarkDbRecordStore::validate(OperationContext* txn,
                                   bool full,
                                   bool scanData,
                                   ValidateAdaptor* adaptor,
                                   ValidateResults* results,
                                   BSONObjBuilder* output) {
    output->appendNumber("nrecords", m_table->numDataRows());
    return Status::OK();
}

void NarkDbRecordStore::appendCustomStats(OperationContext* txn,
										  BSONObjBuilder* result,
										  double scale) const {
    result->appendBool("capped", false);
}

Status NarkDbRecordStore::touch(OperationContext* txn, BSONObjBuilder* output) const {
    return Status::OK();
#if 0
    if (true/*_isEphemeral*/) {
        // Everything is already in memory.
        return Status::OK();
    }
    return Status(ErrorCodes::CommandNotSupported, "this storage engine does not support touch");
#endif
}

void NarkDbRecordStore::updateStatsAfterRepair(OperationContext* txn,
											   long long numRecords,
											   long long dataSize) {
	LOG(2) << BOOST_CURRENT_FUNCTION << ": is in TODO list, not implemented now";
}

void NarkDbRecordStore::temp_cappedTruncateAfter(OperationContext* txn,
												 RecordId end,
												 bool inclusive) {
	LOG(2) << BOOST_CURRENT_FUNCTION << ": is in TODO list, not implemented now";
}

} } // namespace mongo::narkdb
