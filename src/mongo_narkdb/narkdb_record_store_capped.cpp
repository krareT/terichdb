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

#include "narkdb_record_store_capped.h"

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

#include "mongo/db/storage/wiredtiger/wiredtiger_record_store.h"

namespace mongo { namespace narkdb {

using std::unique_ptr;
using std::string;

class NarkDbRecordStoreCapped::Cursor final : public SeekableRecordCursor {
public:
    Cursor(OperationContext* txn, const NarkDbRecordStoreCapped& rs, bool forward)
        : m_rs(rs)
		, m_wtCursor(rs.m_wiredtigerCappedStore->getCursor(txn, forward))
	{}

    boost::optional<Record> next() final {
		return m_wtCursor->next();
    }

    boost::optional<Record> seekExact(const RecordId& id) final {
		return m_wtCursor->seekExact(id);
    }

    void save() final {
		return m_wtCursor->save();
    }

    void saveUnpositioned() final {
		return m_wtCursor->saveUnpositioned();
    }

    bool restore() final {
		return m_wtCursor->restore();
    }

    void detachFromOperationContext() final {
		m_wtCursor->detachFromOperationContext();
    }

    void reattachToOperationContext(OperationContext* txn) final {
        m_wtCursor->reattachToOperationContext(txn);
    }

private:
    const NarkDbRecordStoreCapped& m_rs;
	std::unique_ptr<SeekableRecordCursor> m_wtCursor;
};

NarkDbRecordStoreCapped::NarkDbRecordStoreCapped(OperationContext* ctx,
												 StringData ns,
												 StringData uri,
												 int64_t cappedMaxSize,
												 int64_t cappedMaxDocs,
												 CappedCallback* cappedCallback,
												 WiredTigerSizeStorer* sizeStorer)
	: RecordStore(ns)
{
	const bool ephemeral = false;
	const bool isCapped = true;
	m_wiredtigerCappedStore.reset(
		new WiredTigerRecordStore(ctx, ns, uri,
				kWiredTigerEngineName, isCapped, ephemeral,
                cappedMaxSize, cappedMaxDocs, cappedCallback, sizeStorer));
}

NarkDbRecordStoreCapped::~NarkDbRecordStoreCapped() {
    LOG(1) << "~NarkDbRecordStoreCapped for: " << ns();
}

const char* NarkDbRecordStoreCapped::name() const {
    return m_wiredtigerCappedStore->name();
}

long long NarkDbRecordStoreCapped::dataSize(OperationContext* txn) const {
    return m_wiredtigerCappedStore->dataSize(txn);
}

long long NarkDbRecordStoreCapped::numRecords(OperationContext* txn) const {
    return m_wiredtigerCappedStore->numRecords(txn);
}

bool NarkDbRecordStoreCapped::isCapped() const {
	invariant(m_wiredtigerCappedStore->isCapped());
    return true;
}

int64_t NarkDbRecordStoreCapped::storageSize(OperationContext* txn,
									   BSONObjBuilder* extraInfo,
									   int infoLevel) const {
	return m_wiredtigerCappedStore->storageSize(txn, extraInfo, infoLevel);
}

RecordData
NarkDbRecordStoreCapped::dataFor(OperationContext* txn, const RecordId& id)
const {
	return m_wiredtigerCappedStore->dataFor(txn, id);
}

bool NarkDbRecordStoreCapped::findRecord(OperationContext* txn,
								   const RecordId& id,
								   RecordData* out) const {
	return m_wiredtigerCappedStore->findRecord(txn, id, out);
}

void NarkDbRecordStoreCapped::deleteRecord(OperationContext* txn, const RecordId& id) {
	m_wiredtigerCappedStore->deleteRecord(txn, id);
}

Status NarkDbRecordStoreCapped::insertRecords(OperationContext* txn,
										std::vector<Record>* records,
										bool enforceQuota) {
	return m_wiredtigerCappedStore->insertRecords(txn, records, enforceQuota);
}

StatusWith<RecordId> NarkDbRecordStoreCapped::insertRecord(OperationContext* txn,
													 const char* data,
													 int len,
													 bool enforceQuota) {
	return m_wiredtigerCappedStore->insertRecord(txn, data, len, enforceQuota);
}

StatusWith<RecordId>
NarkDbRecordStoreCapped::insertRecord(OperationContext* txn,
									  const DocWriter* doc,
									  bool enforceQuota) {
	return m_wiredtigerCappedStore->insertRecord(txn, doc, enforceQuota);
}

StatusWith<RecordId>
NarkDbRecordStoreCapped::updateRecord(OperationContext* txn,
									  const RecordId& id,
									  const char* data,
									  int len,
									  bool enforceQuota,
									  UpdateNotifier* notifier) {
	return m_wiredtigerCappedStore->
		updateRecord(txn, id, data, len, enforceQuota, notifier);
}

bool NarkDbRecordStoreCapped::updateWithDamagesSupported() const {
    return false;
}

StatusWith<RecordData> NarkDbRecordStoreCapped::updateWithDamages(
							OperationContext* txn,
							const RecordId& id,
							const RecordData& oldRec,
							const char* damageSource,
							const mutablebson::DamageVector& damages)
{
    MONGO_UNREACHABLE;
}

std::unique_ptr<SeekableRecordCursor>
NarkDbRecordStoreCapped::getCursor(OperationContext* txn, bool forward) const {
    return stdx::make_unique<Cursor>(txn, *this, forward);
}

std::unique_ptr<RecordCursor>
NarkDbRecordStoreCapped::getRandomCursor(OperationContext* txn) const {
    return m_wiredtigerCappedStore->getRandomCursor(txn);
}

std::vector<std::unique_ptr<RecordCursor>>
NarkDbRecordStoreCapped::getManyCursors(OperationContext* txn) const {
	return m_wiredtigerCappedStore->getManyCursors(txn);
}

Status NarkDbRecordStoreCapped::truncate(OperationContext* txn) {
	return m_wiredtigerCappedStore->truncate(txn);
}

bool NarkDbRecordStoreCapped::compactSupported() const {
	return m_wiredtigerCappedStore->compactSupported();
}

bool NarkDbRecordStoreCapped::compactsInPlace() const {
	return m_wiredtigerCappedStore->compactsInPlace();
}

Status NarkDbRecordStoreCapped::compact(OperationContext* txn,
										RecordStoreCompactAdaptor* adaptor,
										const CompactOptions* options,
										CompactStats* stats) {
	return m_wiredtigerCappedStore->compact(txn, adaptor, options, stats);
}

Status NarkDbRecordStoreCapped::validate(OperationContext* txn,
										 bool full,
										 bool scanData,
										 ValidateAdaptor* adaptor,
										 ValidateResults* results,
										 BSONObjBuilder* output) {
	return m_wiredtigerCappedStore->
		validate(txn, full, scanData, adaptor, results, output);
}

void NarkDbRecordStoreCapped::appendCustomStats(OperationContext* txn,
												BSONObjBuilder* result,
												double scale) const {
    return m_wiredtigerCappedStore->appendCustomStats(txn, result, scale);
}

Status
NarkDbRecordStoreCapped::touch(OperationContext* txn, BSONObjBuilder* output)
const {
	return m_wiredtigerCappedStore->touch(txn, output);
}

void NarkDbRecordStoreCapped::updateStatsAfterRepair(OperationContext* txn,
											   long long numRecords,
											   long long dataSize) {
	return m_wiredtigerCappedStore->
		updateStatsAfterRepair(txn, numRecords, dataSize);
}

void NarkDbRecordStoreCapped::temp_cappedTruncateAfter(OperationContext* txn,
												 RecordId end,
												 bool inclusive) {
	return m_wiredtigerCappedStore->
		temp_cappedTruncateAfter(txn, end, inclusive);
}

boost::optional<RecordId>
NarkDbRecordStoreCapped::oplogStartHack(OperationContext* txn,
										const RecordId& startingPosition) const {
	return m_wiredtigerCappedStore->oplogStartHack(txn, startingPosition);
}

Status
NarkDbRecordStoreCapped::oplogDiskLocRegister(OperationContext* txn,
											  const Timestamp& opTime) {
	return m_wiredtigerCappedStore->oplogDiskLocRegister(txn, opTime);
}


} } // namespace mongo::narkdb
