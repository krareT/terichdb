// terarkdb_recovery_unit.cpp

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
#ifdef _MSC_VER
#pragma warning(disable: 4800) // bool conversion
#pragma warning(disable: 4244) // 'return': conversion from '__int64' to 'double', possible loss of data
#pragma warning(disable: 4267) // '=': conversion from 'size_t' to 'int', possible loss of data
#endif

#include "mongo/base/checked_cast.h"
#include "mongo/base/init.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/commands/server_status_metric.h"
#include "mongo/db/server_parameters.h"
#include "terarkdb_recovery_unit.h"
//#include "terarkdb_session_cache.h"
//#include "terarkdb_util.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/concurrency/ticketholder.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/stacktrace.h"

namespace mongo { namespace terarkdb {

TerarkDbRecoveryUnit::TerarkDbRecoveryUnit()
    :
      _inUnitOfWork(false),
      _active(false),
      _myTransactionCount(1),
      _everStartedWrite(false),
      _noTicketNeeded(false) {}

TerarkDbRecoveryUnit::~TerarkDbRecoveryUnit() {
    invariant(!_inUnitOfWork);
    _abort();
}

void TerarkDbRecoveryUnit::reportState(BSONObjBuilder* b) const {
    b->append("wt_inUnitOfWork", _inUnitOfWork);
    b->append("wt_active", _active);
    b->append("wt_everStartedWrite", _everStartedWrite);
    b->append("wt_hasTicket", _ticket.hasTicket());
    b->appendNumber("wt_myTransactionCount", static_cast<long long>(_myTransactionCount));
    if (_active)
        b->append("wt_millisSinceCommit", _timer.millis());
}

void TerarkDbRecoveryUnit::prepareForCreateSnapshot(OperationContext* opCtx) {
    invariant(!_active);  // Can't already be in a TerarkDb transaction.
    invariant(!_inUnitOfWork);
    invariant(!_readFromMajorityCommittedSnapshot);

    _areWriteUnitOfWorksBanned = true;
}

void TerarkDbRecoveryUnit::_commit() {
    try {
        if (_active) {
            _txnClose(true);
        }

        for (Changes::const_iterator it = _changes.begin(), end = _changes.end(); it != end; ++it) {
            (*it)->commit();
        }
        _changes.clear();

        invariant(!_active);
    } catch (...) {
        std::terminate();
    }
}

void TerarkDbRecoveryUnit::_abort() {
    try {
        if (_active) {
            _txnClose(false);
        }

        for (Changes::const_reverse_iterator it = _changes.rbegin(), end = _changes.rend();
             it != end;
             ++it) {
            Change* change = *it;
            LOG(2) << "CUSTOM ROLLBACK " << demangleName(typeid(*change));
            change->rollback();
        }
        _changes.clear();

        invariant(!_active);
    } catch (...) {
        std::terminate();
    }
}

void TerarkDbRecoveryUnit::beginUnitOfWork(OperationContext* opCtx) {
    invariant(!_areWriteUnitOfWorksBanned);
    invariant(!_inUnitOfWork);
    _inUnitOfWork = true;
    _everStartedWrite = true;
    _getTicket(opCtx);
}

void TerarkDbRecoveryUnit::commitUnitOfWork() {
    invariant(_inUnitOfWork);
    _inUnitOfWork = false;
    _commit();
}

void TerarkDbRecoveryUnit::abortUnitOfWork() {
    invariant(_inUnitOfWork);
    _inUnitOfWork = false;
    _abort();
}

void TerarkDbRecoveryUnit::_ensureSession() {
}

bool TerarkDbRecoveryUnit::waitUntilDurable() {
    invariant(!_inUnitOfWork);
    return true;
}

void TerarkDbRecoveryUnit::registerChange(Change* change) {
    invariant(_inUnitOfWork);
    _changes.push_back(change);
}

TerarkDbRecoveryUnit* TerarkDbRecoveryUnit::get(OperationContext* txn) {
    invariant(txn);
    return checked_cast<TerarkDbRecoveryUnit*>(txn->recoveryUnit());
}

void TerarkDbRecoveryUnit::assertInActiveTxn() const {
    fassert(28575, _active);
}

void TerarkDbRecoveryUnit::abandonSnapshot() {
    invariant(!_inUnitOfWork);
    if (_active) {
        // Can't be in a WriteUnitOfWork, so safe to rollback
        _txnClose(false);
    }
    _areWriteUnitOfWorksBanned = false;
}

void TerarkDbRecoveryUnit::setOplogReadTill(const RecordId& id) {
    _oplogReadTill = id;
}

namespace {


class TicketServerParameter : public ServerParameter {
    MONGO_DISALLOW_COPYING(TicketServerParameter);

public:
    TicketServerParameter(TicketHolder* holder, const std::string& name)
        : ServerParameter(ServerParameterSet::getGlobal(), name, true, true), _holder(holder) {}

    virtual void append(OperationContext* txn, BSONObjBuilder& b, const std::string& name) {
        b.append(name, _holder->outof());
    }

    virtual Status set(const BSONElement& newValueElement) {
        if (!newValueElement.isNumber())
            return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be a number");
        return _set(newValueElement.numberInt());
    }

    virtual Status setFromString(const std::string& str) {
        int num = 0;
        Status status = parseNumberFromString(str, &num);
        if (!status.isOK())
            return status;
        return _set(num);
    }

    Status _set(int newNum) {
        if (newNum <= 0) {
            return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be > 0");
        }

        return _holder->resize(newNum);
    }

private:
    TicketHolder* _holder;
};

TicketHolder openWriteTransaction(128);
TicketServerParameter openWriteTransactionParam(&openWriteTransaction,
                                                "terarkDbConcurrentWriteTransactions");

TicketHolder openReadTransaction(128);
TicketServerParameter openReadTransactionParam(&openReadTransaction,
                                               "terarkDbConcurrentReadTransactions");
}

void TerarkDbRecoveryUnit::appendGlobalStats(BSONObjBuilder& b) {
    BSONObjBuilder bb(b.subobjStart("concurrentTransactions"));
    {
        BSONObjBuilder bbb(bb.subobjStart("write"));
        bbb.append("out", openWriteTransaction.used());
        bbb.append("available", openWriteTransaction.available());
        bbb.append("totalTickets", openWriteTransaction.outof());
        bbb.done();
    }
    {
        BSONObjBuilder bbb(bb.subobjStart("read"));
        bbb.append("out", openReadTransaction.used());
        bbb.append("available", openReadTransaction.available());
        bbb.append("totalTickets", openReadTransaction.outof());
        bbb.done();
    }
    bb.done();
}

void TerarkDbRecoveryUnit::_txnClose(bool commit) {
    invariant(_active);
    _active = false;
    _myTransactionCount++;
    _ticket.reset(NULL);
}

SnapshotId TerarkDbRecoveryUnit::getSnapshotId() const {
    // TODO: use actual terarkdb txn id
    return SnapshotId(_myTransactionCount);
}

Status TerarkDbRecoveryUnit::setReadFromMajorityCommittedSnapshot() {
    _readFromMajorityCommittedSnapshot = true;
    return Status::OK();
}

boost::optional<SnapshotName> TerarkDbRecoveryUnit::getMajorityCommittedSnapshot() const {
    if (!_readFromMajorityCommittedSnapshot)
        return {};
    return _majorityCommittedSnapshot;
}

void TerarkDbRecoveryUnit::markNoTicketRequired() {
    invariant(!_ticket.hasTicket());
    _noTicketNeeded = true;
}

void TerarkDbRecoveryUnit::_getTicket(OperationContext* opCtx) {
    // already have a ticket
    if (_ticket.hasTicket())
        return;

    if (_noTicketNeeded)
        return;

    bool writeLocked;

    // If we have a strong lock, waiting for a ticket can cause a deadlock.
    if (opCtx != NULL && opCtx->lockState() != NULL) {
        if (opCtx->lockState()->hasStrongLocks())
            return;
        writeLocked = opCtx->lockState()->isWriteLocked();
    } else {
        writeLocked = _everStartedWrite;
    }

    TicketHolder* holder = writeLocked ? &openWriteTransaction : &openReadTransaction;

    holder->waitForTicket();
    _ticket.reset(holder);
}

void TerarkDbRecoveryUnit::_txnOpen(OperationContext* opCtx) {
    invariant(!_active);
    _getTicket(opCtx);

    LOG(2) << "TerarkDb begin_transaction";
    _timer.reset();
    _active = true;
}

} }  // namespace mongo::terark

