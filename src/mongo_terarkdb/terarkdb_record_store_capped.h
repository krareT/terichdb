// terarkdb_record_store.h

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
#include "mongo/util/concurrency/synchronization.h"
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

    virtual long long dataSize(OperationContext* txn) const;

    virtual long long numRecords(OperationContext* txn) const;

    virtual bool isCapped() const;

    virtual int64_t storageSize(OperationContext* txn,
                                BSONObjBuilder* extraInfo = NULL,
                                int infoLevel = 0) const;

    // CRUD related

    virtual RecordData dataFor(OperationContext* txn, const RecordId& id) const;

    virtual bool findRecord(OperationContext* txn, const RecordId& id, RecordData* out) const;

    virtual void deleteRecord(OperationContext* txn, const RecordId& id);

    virtual Status insertRecords(OperationContext* txn,
                                 std::vector<Record>* records,
                                 bool enforceQuota);

    virtual StatusWith<RecordId> insertRecord(OperationContext* txn,
                                              const char* data,
                                              int len,
                                              bool enforceQuota);

    virtual StatusWith<RecordId> insertRecord(OperationContext* txn,
                                              const DocWriter* doc,
                                              bool enforceQuota);

    virtual Status updateRecord(OperationContext* txn,
                                              const RecordId& oldLocation,
                                              const char* data,
                                              int len,
                                              bool enforceQuota,
                                              UpdateNotifier* notifier);

    virtual bool updateWithDamagesSupported() const;

    StatusWith<RecordData>
	updateWithDamages(OperationContext* txn,
                      const RecordId& id,
                      const RecordData& oldRec,
                      const char* damageSource,
                      const mutablebson::DamageVector& damages);

    std::unique_ptr<SeekableRecordCursor>
	getCursor(OperationContext* txn, bool forward) const final;

    std::unique_ptr<RecordCursor> getRandomCursor(OperationContext*) const final;

    std::vector<std::unique_ptr<RecordCursor>>
	getManyCursors(OperationContext* txn) const final;

    virtual Status truncate(OperationContext* txn);

    virtual bool compactSupported() const;
    virtual bool compactsInPlace() const;

    virtual Status compact(OperationContext* txn,
                           RecordStoreCompactAdaptor* adaptor,
                           const CompactOptions* options,
                           CompactStats* stats);

    virtual Status validate(OperationContext* txn,
                            bool full,
                            bool scanData,
                            ValidateAdaptor* adaptor,
                            ValidateResults* results,
                            BSONObjBuilder* output);

    virtual void appendCustomStats(OperationContext* txn,
                                   BSONObjBuilder* result,
                                   double scale) const;

    virtual Status touch(OperationContext* txn, BSONObjBuilder* output) const;

    virtual void temp_cappedTruncateAfter(OperationContext* txn, RecordId end, bool inclusive);

    boost::optional<RecordId> oplogStartHack(OperationContext* txn,
                                             const RecordId& startingPosition) const override;

    Status oplogDiskLocRegister(OperationContext* txn, const Timestamp& opTime) override;

    void updateStatsAfterRepair(OperationContext* txn,
                                long long numRecords,
                                long long dataSize) override;

	TerarkDbKVEngine* m_terarkEngine;
	std::unique_ptr<RecordStore> m_wiredtigerCappedStore;

private:
    class Cursor;
};

// TerarkDb failpoint to throw write conflict exceptions randomly
MONGO_FP_FORWARD_DECLARE(TerarkDbWriteConflictException);
} }  // namespace mongo::terark

