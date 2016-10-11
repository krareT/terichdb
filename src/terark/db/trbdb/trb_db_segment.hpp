#pragma once

#include <mutex>
#include <terark/db/db_segment.hpp>

namespace terark { namespace db { namespace trb {

class TERARK_DB_DLL MutexLockTransaction;

class TERARK_DB_DLL TrbColgroupSegment : public ColgroupWritableSegment {

protected:
    friend class MutexLockTransaction;
    std::mutex  m_txnMutex;

public:
	class TrbDbTransaction; friend class TrbDbTransaction;
	DbTransaction* createTransaction(DbContext*) override;

    TrbColgroupSegment();
	~TrbColgroupSegment();

    void load(PathRef path) override;
    void save(PathRef path) const override;

protected:
    ReadableIndex *openIndex(const Schema &, PathRef segDir) const override;
    ReadableIndex *createIndex(const Schema &, PathRef segDir) const override;
    ReadableStore *createStore(const Schema &, PathRef segDir) const override;

public:
    void indexSearchExactAppend(size_t mySegIdx, size_t indexId,
                                fstring key, valvec<llong>* recIdvec,
                                DbContext*) const override;

    llong append(fstring, DbContext *) override;
    void remove(llong, DbContext *) override;
    void update(llong, fstring, DbContext *) override;

    void shrinkToFit(void) override;

    void saveRecordStore(PathRef segDir) const override;
    void loadRecordStore(PathRef segDir) override;

    llong dataStorageSize() const override;
    llong totalStorageSize() const override;
};

}}} // namespace terark::db::wt
