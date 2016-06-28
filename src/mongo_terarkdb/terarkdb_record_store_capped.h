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

#pragma once

#include <boost/thread/mutex.hpp>
#include <set>
#include <string>

#include "mongo_terarkdb_common.hpp"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/storage/capped_callback.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/fail_point_service.h"
#include "terarkdb_size_storer.h"

namespace mongo {
	class WiredTigerSizeStorer;
}

namespace mongo { namespace terarkdb {

class TerarkDbKVEngine;

class TerarkDbRecordStoreCapped : public RecordStore {
public:
    TerarkDbRecordStoreCapped(OperationContext* ctx,
							StringData ns,
							StringData uri,
							int64_t cappedMaxSize,
							int64_t cappedMaxDocs,
							CappedCallback* cappedCallback,
							WiredTigerSizeStorer* sizeStorer);

    virtual ~TerarkDbRecordStoreCapped();

    // name of the RecordStore implementation
    virtual const char* name() const override;

    virtual long long dataSize(OperationContext* txn) const override;

    virtual long long numRecords(OperationContext* txn) const override;

    virtual bool isCapped() const override;

    virtual int64_t storageSize(OperationContext* txn,
                                BSONObjBuilder* extraInfo,
                                int infoLevel) const override;

    // CRUD related

    virtual RecordData dataFor(OperationContext* txn, const RecordId& id) const override;

    virtual bool findRecord(OperationContext* txn, const RecordId& id, RecordData* out) const override;

    virtual void deleteRecord(OperationContext* txn, const RecordId& id) override;

    virtual Status insertRecords(OperationContext* txn,
                                 std::vector<Record>* records,
                                 bool enforceQuota) override;

    virtual StatusWith<RecordId> insertRecord(OperationContext* txn,
                                              const char* data,
                                              int len,
                                              bool enforceQuota) override;

    virtual Status updateRecord(OperationContext* txn,
                                              const RecordId& oldLocation,
                                              const char* data,
                                              int len,
                                              bool enforceQuota,
                                              UpdateNotifier* notifier) override;

    virtual bool updateWithDamagesSupported() const override;

    StatusWith<RecordData>
	updateWithDamages(OperationContext* txn,
                      const RecordId& id,
                      const RecordData& oldRec,
                      const char* damageSource,
                      const mutablebson::DamageVector& damages) override;

    std::unique_ptr<SeekableRecordCursor>
	getCursor(OperationContext* txn, bool forward) const override final;

    std::unique_ptr<RecordCursor> getRandomCursor(OperationContext*) const override final;

    std::vector<std::unique_ptr<RecordCursor>>
	getManyCursors(OperationContext* txn) const override final;

    virtual Status truncate(OperationContext* txn);

    virtual bool compactSupported() const override;
    virtual bool compactsInPlace() const override;

    virtual Status compact(OperationContext* txn,
                           RecordStoreCompactAdaptor* adaptor,
                           const CompactOptions* options,
                           CompactStats* stats);

    virtual Status validate(OperationContext* txn,
                            ValidateCmdLevel level,
                            ValidateAdaptor* adaptor,
                            ValidateResults* results,
                            BSONObjBuilder* output) override;

    virtual void appendCustomStats(OperationContext* txn,
                                   BSONObjBuilder* result,
                                   double scale) const override;

    virtual Status touch(OperationContext* txn, BSONObjBuilder* output) const override;

    virtual void temp_cappedTruncateAfter(OperationContext* txn, RecordId end, bool inclusive) override;

    boost::optional<RecordId> oplogStartHack(OperationContext* txn,
                                             const RecordId& startingPosition) const override;

    Status oplogDiskLocRegister(OperationContext* txn, const Timestamp& opTime) override;

    void updateStatsAfterRepair(OperationContext* txn,
                                long long numRecords,
                                long long dataSize) override;

	std::unique_ptr<RecordStore> m_wiredtigerCappedStore;

private:
    class Cursor;
};

// TerarkDb failpoint to throw write conflict exceptions randomly
MONGO_FP_FORWARD_DECLARE(TerarkDbWriteConflictException);
} }  // namespace mongo::terark

